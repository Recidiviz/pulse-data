# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Identity Service import processor: loads a tenant's clustering results from
BigQuery into the service's Postgres state.

Cloud Tasks calls it through the internal processing endpoint after the enqueue
side (import_task_enqueuer) schedules the work.
"""
import datetime
import logging
import uuid
from collections import Counter
from collections.abc import Callable
from enum import Enum
from typing import TypeVar

from sqlalchemy.orm import Session

from recidiviz.common.constants.identity import IdentityStatus
from recidiviz.common.constants.tenants import Tenant
from recidiviz.persistence.database.schema.identity import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.services.identity.bq_snapshot_reader import (
    ClusterSnapshot,
    read_cluster_snapshots,
)
from recidiviz.services.identity.create_pass import build_identity_rows
from recidiviz.services.identity.demographic_guard import DemographicGuard
from recidiviz.services.identity.update_pass import apply_identity_update

_IDENTITY_DATABASE_KEY = SQLAlchemyDatabaseKey.for_schema(SchemaType.IDENTITY)

# The identities table's child tables, which reference identities.recidiviz_id
# without ON DELETE CASCADE and so must be cleared before their identities rows.
_IDENTITY_CHILD_TABLES = (
    schema.ExternalId,
    schema.Name,
    schema.DateOfBirth,
    schema.Gender,
    schema.Race,
    schema.Sex,
    schema.Ethnicity,
    schema.PhoneNumber,
    schema.Email,
)

# Number of clusters written per transaction. Committing in chunks rather than
# per cluster bounds the round trips to Postgres, which is what keeps a
# large-tenant import inside the Cloud Tasks dispatch deadline; a failed chunk
# falls back to one transaction per cluster so a single bad cluster is still
# isolated.
_IMPORT_CHUNK_SIZE = 500

# One pass's unit of per-cluster work handed to _apply_clusters_in_chunks: a
# bare snapshot for the create pass, a (snapshot, recidiviz_id) pair for the
# update pass.
_ClusterItemT = TypeVar("_ClusterItemT")


class _ImportPass(Enum):
    """The import pass a chunk of per-cluster work belongs to, named in
    failure logs."""

    CREATE = "create"
    UPDATE = "update"


def process_import(
    *,
    tenant: Tenant,
    snapshot_timestamp: datetime.datetime,
    should_clear_first: bool,
) -> None:
    """Loads the given tenant's clustering results into the service's state.

    Reads the tenant's cluster snapshots from BigQuery, brings identities whose
    clusters have changed up to date, and creates identities for new clusters.

    The update pass runs before the create pass so both draw on one
    email-ownership set. The set only grows within a run, so an address an
    update drops stays unavailable for the rest of the run; a cluster that lost
    an address to the exclusion only picks it up once the cluster's own content
    changes, since the import skips unchanged clusters. TODO(OBT-49081) tracks
    recording these exclusions as reviewable conflicts.

    TODO(OBT-37728): Run split detection before the update and create passes,
    so an identity whose external ids appear in more than one cluster is split
    apart (or flagged for human review) before its clusters are categorized.

    Args:
        tenant: The tenant whose clustering results to load.
        snapshot_timestamp: Last-modified time of the tenant's identity cluster
            dataset as the enqueuer saw it, logged so a run can be traced to
            the data it read.
        should_clear_first: When true, deletes the tenant's existing
            identities up front (see _clear_tenant_identities), so every
            cluster is new and the create pass alone rebuilds the tenant's
            identities.
            When false, brings matched identities up to date in place and
            creates identities only for clusters matching none.
    """
    logging.info(
        "Processing identity import for tenant [%s], cluster data last modified "
        "[%s], should_clear_first [%s].",
        tenant.value,
        snapshot_timestamp.isoformat(),
        should_clear_first,
    )
    snapshots = read_cluster_snapshots(tenant)
    if should_clear_first:
        _clear_tenant_identities(tenant)
        new_snapshots = snapshots
        update_pairs: list[tuple[ClusterSnapshot, uuid.UUID]] = []
        # The tenant was just cleared, so it has no pre-existing emails.
        existing_email_hashes: set[str] = set()
    else:
        new_snapshots, update_pairs = _categorize_clusters(
            tenant=tenant, snapshots=snapshots
        )
        existing_email_hashes = _existing_email_hashes(tenant)
    _update_identities(
        tenant=tenant,
        update_pairs=update_pairs,
        committed_email_hashes=existing_email_hashes,
    )
    _create_identities(
        tenant=tenant,
        snapshots=new_snapshots,
        committed_email_hashes=existing_email_hashes,
    )


def _categorize_clusters(
    *, tenant: Tenant, snapshots: list[ClusterSnapshot]
) -> tuple[list[ClusterSnapshot], list[tuple[ClusterSnapshot, uuid.UUID]]]:
    """Categorizes the given ClusterSnapshots according to how many identities
    they match, returning the new_snapshots and update_pairs lists described
    below. The rules:

    - A cluster matches an identity when the cluster carries an external id the
      identity holds as active; only the tenant's non-RETIRED identities count
      (see _identity_ids_by_external_id).

    - If a cluster matches no identity, it describes a person the service has
      not seen and must create a new identity for; this function adds it to the
      new_snapshots list.

    - If a cluster matches exactly one identity, and no other cluster matches
      that identity, it carries the latest version of that identity's data;
      this function adds it and the matched identity's recidiviz_id to the
      update_pairs list.

    - If a cluster matches two or more identities, it is possible that two
      identities in the service are actually one person. Since the service
      cannot merge identities yet, this function skips the cluster.
      TODO(OBT-37726): File a PENDING merge_candidates row for each such
      cluster.

    - If a cluster matches exactly one identity, but another cluster also
      matches that identity, the identity's external ids appear in more than
      one cluster and it may represent two actual people. This function skips
      the cluster and leaves the identity untouched, since applying it would
      replace the identity's attributes with values built from only some of
      its external ids. Split detection (OBT-37728) will eventually resolve
      this case; it will run before this categorization.
    """
    identity_ids_by_external_id = _identity_ids_by_external_id(tenant)
    new_snapshots: list[ClusterSnapshot] = []
    single_match_pairs: list[tuple[ClusterSnapshot, uuid.UUID]] = []
    matched_cluster_counts: Counter[uuid.UUID] = Counter()
    skipped_multiple_identity_count = 0
    for snapshot in snapshots:
        matched_identity_ids = {
            identity_ids_by_external_id[key]
            for external_id in snapshot.cluster.external_ids
            if (key := (external_id.id_type, external_id.external_id))
            in identity_ids_by_external_id
        }
        matched_cluster_counts.update(matched_identity_ids)
        if not matched_identity_ids:
            new_snapshots.append(snapshot)
        elif len(matched_identity_ids) == 1:
            single_match_pairs.append((snapshot, matched_identity_ids.pop()))
        else:
            skipped_multiple_identity_count += 1
    update_pairs = [
        (snapshot, recidiviz_id)
        for snapshot, recidiviz_id in single_match_pairs
        if matched_cluster_counts[recidiviz_id] == 1
    ]
    skipped_shared_identity_cluster_count = len(single_match_pairs) - len(update_pairs)
    logging.info(
        "Of [%s] clusters for tenant [%s]: [%s] new, [%s] updating one existing "
        "identity, [%s] matching multiple existing identities (skipped), [%s] "
        "sharing a matched identity with another cluster (skipped).",
        len(snapshots),
        tenant.value,
        len(new_snapshots),
        len(update_pairs),
        skipped_multiple_identity_count,
        skipped_shared_identity_cluster_count,
    )
    return new_snapshots, update_pairs


def _identity_ids_by_external_id(
    tenant: Tenant,
) -> dict[tuple[str, str], uuid.UUID]:
    """Returns the recidiviz_id of the identity holding each active external
    id, over the tenant's non-RETIRED identities, keyed by
    (id_type value, external_id).

    Only active external ids count; an inactive row is left behind by a split
    and no longer associates its identity with the id for lookups. The unique
    index on active ids guarantees each key maps to exactly one identity.
    Keys are raw id_type strings so a snapshot carrying an unrecognized
    id_type matches nothing here and instead fails in the create pass, which
    isolates the bad cluster rather than aborting the run.
    """
    with SessionFactory.using_database(_IDENTITY_DATABASE_KEY) as session:
        rows = (
            session.query(
                schema.ExternalId.id_type,
                schema.ExternalId.external_id,
                schema.ExternalId.recidiviz_id,
            )
            .join(
                schema.Identity,
                schema.Identity.recidiviz_id == schema.ExternalId.recidiviz_id,
            )
            .filter(
                schema.Identity.tenant == tenant,
                schema.Identity.status != IdentityStatus.RETIRED,
                schema.ExternalId.is_active.is_(True),
            )
            .all()
        )
    return {
        (id_type.value, external_id): recidiviz_id
        for id_type, external_id, recidiviz_id in rows
    }


def _existing_email_hashes(tenant: Tenant) -> set[str]:
    """Returns the address hashes of the emails attached to the tenant's
    non-RETIRED identities, used to exclude an address a new identity would
    otherwise duplicate."""
    with SessionFactory.using_database(_IDENTITY_DATABASE_KEY) as session:
        rows = (
            session.query(schema.Email.address_hash)
            .join(
                schema.Identity,
                schema.Identity.recidiviz_id == schema.Email.recidiviz_id,
            )
            .filter(
                schema.Identity.tenant == tenant,
                schema.Identity.status != IdentityStatus.RETIRED,
            )
            .all()
        )
    return {address_hash for (address_hash,) in rows}


def _update_identities(
    *,
    tenant: Tenant,
    update_pairs: list[tuple[ClusterSnapshot, uuid.UUID]],
    committed_email_hashes: set[str],
) -> None:
    """Brings each matched identity up to date with its cluster.

    This function first skips every cluster whose hash matches its identity's
    last_cluster_hash, meaning the identity already applied that exact cluster
    in an earlier run and there is nothing to update (see
    _drop_unchanged_clusters). It hands the remaining pairs to
    _apply_clusters_in_chunks, which commits the updates in chunks and retries
    a failed chunk one cluster at a time, so one bad cluster cannot block the
    rest.

    Args:
        tenant: The tenant the clusters belong to, named in failure logs.
        update_pairs: The clusters to apply, each with the recidiviz_id of the
            identity its external ids matched.
        committed_email_hashes: Hashes of addresses the tenant's identities
            already own; the pass excludes any address whose hash is here, and
            each committed chunk folds in the addresses it used, so an address
            reaches at most one identity.
    """
    if not update_pairs:
        return
    changed_pairs = _drop_unchanged_clusters(tenant=tenant, update_pairs=update_pairs)
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    guard = DemographicGuard()

    def apply_update(
        session: Session,
        pair: tuple[ClusterSnapshot, uuid.UUID],
        committed_email_hashes: set[str],
        staged_email_hashes: set[str],
    ) -> None:
        snapshot, recidiviz_id = pair
        _update_identity_in_session(
            session=session,
            snapshot=snapshot,
            recidiviz_id=recidiviz_id,
            guard=guard,
            now=now,
            committed_email_hashes=committed_email_hashes,
            staged_email_hashes=staged_email_hashes,
        )

    _apply_clusters_in_chunks(
        tenant=tenant,
        import_pass=_ImportPass.UPDATE,
        items=changed_pairs,
        committed_email_hashes=committed_email_hashes,
        apply_item=apply_update,
        cluster_id_of=lambda pair: pair[0].cluster.identity_cluster_id,
    )


def _drop_unchanged_clusters(
    *,
    tenant: Tenant,
    update_pairs: list[tuple[ClusterSnapshot, uuid.UUID]],
) -> list[tuple[ClusterSnapshot, uuid.UUID]]:
    """Returns the pairs whose cluster changed since the identity's last
    successful import, per its stored last_cluster_hash."""
    with SessionFactory.using_database(_IDENTITY_DATABASE_KEY) as session:
        last_cluster_hash_by_identity_id = dict(
            session.query(
                schema.Identity.recidiviz_id, schema.Identity.last_cluster_hash
            ).filter(schema.Identity.tenant == tenant)
        )
    changed_pairs = [
        (snapshot, recidiviz_id)
        for snapshot, recidiviz_id in update_pairs
        if last_cluster_hash_by_identity_id[recidiviz_id]
        != snapshot.stored_cluster_hash
    ]
    logging.info(
        "Of [%s] clusters matching one existing identity for tenant [%s], "
        "updating [%s]; skipping [%s] unchanged since their last import.",
        len(update_pairs),
        tenant.value,
        len(changed_pairs),
        len(update_pairs) - len(changed_pairs),
    )
    return changed_pairs


def _update_identity_in_session(
    *,
    session: Session,
    snapshot: ClusterSnapshot,
    recidiviz_id: uuid.UUID,
    guard: DemographicGuard,
    now: datetime.datetime,
    committed_email_hashes: set[str],
    staged_email_hashes: set[str],
) -> None:
    """Applies one cluster's update to its identity on the given session,
    unless the demographic guard blocks it.

    A block deliberately holds back the entire update, the additive external-id
    adds included, so a cluster is always applied whole or not at all.
    """
    identity = (
        session.query(schema.Identity)
        .filter(schema.Identity.recidiviz_id == recidiviz_id)
        .one()
    )
    if not guard.allows_update(identity=identity, cluster=snapshot.cluster):
        # TODO(OBT-37727): Create a PENDING update_attribute_candidates row
        # here instead of only logging.
        logging.info(
            "Demographic guard blocked the update for cluster [%s]; skipping.",
            snapshot.cluster.identity_cluster_id,
        )
        return
    apply_identity_update(
        session=session,
        identity=identity,
        snapshot=snapshot,
        now=now,
        committed_email_hashes=committed_email_hashes,
        staged_email_hashes=staged_email_hashes,
    )
    # TODO(OBT-37727): Clear identity.skip_demographic_guard here, now that the
    # update it was set to allow has been applied.


def _create_identities(
    *,
    tenant: Tenant,
    snapshots: list[ClusterSnapshot],
    committed_email_hashes: set[str],
) -> None:
    """Creates one identity per snapshot through _apply_clusters_in_chunks,
    which chunks the transactions and isolates failing clusters.

    Args:
        tenant: The tenant the snapshots belong to, named in failure logs.
        snapshots: The clusters to create identities from.
        committed_email_hashes: Hashes of addresses the tenant's existing
            identities already own; the pass excludes any address whose hash is
            here, and each committed chunk folds in the addresses it used, so an
            address reaches at most one identity.
    """
    now = datetime.datetime.now(tz=datetime.timezone.utc)

    def apply_creation(
        session: Session,
        snapshot: ClusterSnapshot,
        committed_email_hashes: set[str],
        staged_email_hashes: set[str],
    ) -> None:
        session.add_all(
            build_identity_rows(
                snapshot=snapshot,
                now=now,
                committed_email_hashes=committed_email_hashes,
                staged_email_hashes=staged_email_hashes,
            )
        )

    _apply_clusters_in_chunks(
        tenant=tenant,
        import_pass=_ImportPass.CREATE,
        items=snapshots,
        committed_email_hashes=committed_email_hashes,
        apply_item=apply_creation,
        cluster_id_of=lambda snapshot: snapshot.cluster.identity_cluster_id,
    )


def _apply_clusters_in_chunks(
    *,
    tenant: Tenant,
    import_pass: _ImportPass,
    items: list[_ClusterItemT],
    committed_email_hashes: set[str],
    apply_item: Callable[[Session, _ClusterItemT, set[str], set[str]], None],
    cluster_id_of: Callable[[_ClusterItemT], str],
) -> None:
    """Applies one pass's per-cluster items, _IMPORT_CHUNK_SIZE items per
    transaction. A chunk whose transaction fails is retried one item per
    transaction, so a single bad cluster is isolated and logged while the rest
    of the chunk still lands.

    Each committed transaction folds the email hashes it staged into
    committed_email_hashes so later transactions exclude them; a failed
    transaction rolls back and leaves the set untouched.

    Args:
        tenant: The tenant the clusters belong to, named in failure logs.
        import_pass: The import pass the items belong to, named in failure logs.
        items: The pass's per-cluster units of work.
        committed_email_hashes: Hashes of addresses the tenant's identities
            already own; grows as transactions commit.
        apply_item: Stages one item's writes; called with the session, the
            item, committed_email_hashes, and the transaction's staged email
            hash set.
        cluster_id_of: Returns the item's cluster id, named in failure logs.
    """
    for chunk_start in range(0, len(items), _IMPORT_CHUNK_SIZE):
        chunk = items[chunk_start : chunk_start + _IMPORT_CHUNK_SIZE]
        staged_email_hashes: set[str] = set()
        try:
            with SessionFactory.using_database(_IDENTITY_DATABASE_KEY) as session:
                for item in chunk:
                    apply_item(
                        session, item, committed_email_hashes, staged_email_hashes
                    )
        except Exception:
            logging.exception(
                "Failed to apply a chunk of [%s] clusters in the [%s] pass for "
                "tenant [%s] in one transaction; retrying the chunk one cluster "
                "at a time.",
                len(chunk),
                import_pass.value,
                tenant.value,
            )
            _apply_clusters_individually(
                tenant=tenant,
                import_pass=import_pass,
                chunk=chunk,
                committed_email_hashes=committed_email_hashes,
                apply_item=apply_item,
                cluster_id_of=cluster_id_of,
            )
            continue
        committed_email_hashes |= staged_email_hashes


def _apply_clusters_individually(
    *,
    tenant: Tenant,
    import_pass: _ImportPass,
    chunk: list[_ClusterItemT],
    committed_email_hashes: set[str],
    apply_item: Callable[[Session, _ClusterItemT, set[str], set[str]], None],
    cluster_id_of: Callable[[_ClusterItemT], str],
) -> None:
    """Applies each item in its own transaction, isolating a failing cluster."""
    for item in chunk:
        staged_email_hashes: set[str] = set()
        try:
            with SessionFactory.using_database(_IDENTITY_DATABASE_KEY) as session:
                apply_item(session, item, committed_email_hashes, staged_email_hashes)
        except Exception:
            logging.exception(
                "Failed to apply cluster [%s] in the [%s] pass for tenant [%s]; "
                "continuing with the remaining clusters.",
                cluster_id_of(item),
                import_pass.value,
                tenant.value,
            )
            continue
        committed_email_hashes |= staged_email_hashes


def _clear_tenant_identities(tenant: Tenant) -> None:
    """Deletes the tenant's identities and all of their child rows.

    Runs when an import is processed with should_clear_first, so that the
    create pass alone rebuilds the tenant's identities from its clustering
    results; with the database empty, every cluster is new. A clear-first run
    regenerates the tenant's recidiviz_ids, so no caller may persist them
    across one.

    TODO(OBT-48249): Clear-first stays the trigger endpoint's default until the
    update pass (plus merge and split detection) makes runs without it correct.

    The candidate, audit, and no-merge tables are left untouched; they stay
    empty until the passes that write them exist.
    """
    with SessionFactory.using_database(_IDENTITY_DATABASE_KEY) as session:
        tenant_identity_ids = (
            session.query(schema.Identity.recidiviz_id)
            .filter(schema.Identity.tenant == tenant)
            .scalar_subquery()
        )
        for child_table in _IDENTITY_CHILD_TABLES:
            session.query(child_table).filter(
                child_table.recidiviz_id.in_(tenant_identity_ids)
            ).delete(synchronize_session=False)
        deleted_identity_count = (
            session.query(schema.Identity)
            .filter(schema.Identity.tenant == tenant)
            .delete(synchronize_session=False)
        )
    logging.info(
        "Cleared [%s] existing identities for tenant [%s] before import.",
        deleted_identity_count,
        tenant.value,
    )
