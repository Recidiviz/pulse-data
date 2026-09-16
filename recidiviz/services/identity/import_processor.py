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
"""Processing of Identity Service import work.

process_import loads a tenant's clustering results from BigQuery into the
service's Postgres state. In clear-first mode it clears the tenant's existing
identities and creates one identity per cluster; otherwise it creates
identities only for clusters whose external ids match no existing identity.
Cloud Tasks calls it through the internal processing endpoint after the
enqueue side (import_task_enqueuer) schedules the work.
"""
import datetime
import logging
import uuid

from recidiviz.common.constants.identity import IdentityStatus
from recidiviz.common.constants.tenants import Tenant
from recidiviz.persistence.database.schema.identity import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.services.identity.bq_snapshot_reader import (
    ClusterSnapshot,
    read_cluster_snapshot,
)
from recidiviz.services.identity.create_pass import build_identity_rows

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


def process_import(
    *,
    tenant: Tenant,
    snapshot_timestamp: datetime.datetime,
    should_clear_first: bool,
) -> None:
    """Loads the given tenant's clustering results into the service's state.

    Reads the tenant's cluster snapshot from BigQuery, then creates identities
    from it.

    TODO(OBT-37725): Until the update pass (and then merge and split
    detection) lands, a run without should_clear_first never brings an
    existing identity up to date with its cluster; it only adds identities for
    new clusters.

    Args:
        tenant: The tenant whose clustering results to load.
        snapshot_timestamp: Last-modified time of the cluster snapshot the
            enqueuer saw, logged so a run can be traced to its snapshot.
        should_clear_first: When true, deletes the tenant's existing
            identities up front (see _clear_tenant_identities), so every
            cluster is new and the create pass alone loads the full snapshot.
            When false, leaves existing identities untouched and creates
            identities only for clusters whose external ids match no existing
            identity.
    """
    logging.info(
        "Processing identity import for tenant [%s], snapshot [%s], "
        "should_clear_first [%s].",
        tenant.value,
        snapshot_timestamp.isoformat(),
        should_clear_first,
    )
    snapshots = read_cluster_snapshot(tenant)
    if should_clear_first:
        _clear_tenant_identities(tenant)
        new_snapshots = snapshots
        # The tenant was just cleared, so it has no pre-existing emails.
        existing_email_hashes: set[str] = set()
    else:
        new_snapshots = _filter_to_new_clusters(tenant=tenant, snapshots=snapshots)
        existing_email_hashes = _existing_email_hashes(tenant)
    _create_identities(
        tenant=tenant,
        snapshots=new_snapshots,
        committed_email_hashes=existing_email_hashes,
    )


def _filter_to_new_clusters(
    *, tenant: Tenant, snapshots: list[ClusterSnapshot]
) -> list[ClusterSnapshot]:
    """Returns the snapshots whose external ids match no existing identity.

    A match is against the active external ids of the tenant's non-RETIRED
    identities (see _identity_ids_by_external_id).

    TODO(OBT-37725): A cluster matching exactly one identity is skipped here
    until the update pass brings that identity up to date with the cluster.
    TODO(OBT-37726): A cluster matching two or more identities is skipped here
    until merge detection flags it for review.
    """
    identity_ids_by_external_id = _identity_ids_by_external_id(tenant)
    new_snapshots: list[ClusterSnapshot] = []
    skipped_single_identity_count = 0
    skipped_multiple_identity_count = 0
    for snapshot in snapshots:
        matched_identity_ids = {
            identity_ids_by_external_id[key]
            for external_id in snapshot.cluster.external_ids
            if (key := (external_id.id_type, external_id.external_id))
            in identity_ids_by_external_id
        }
        if not matched_identity_ids:
            new_snapshots.append(snapshot)
        elif len(matched_identity_ids) == 1:
            skipped_single_identity_count += 1
        else:
            skipped_multiple_identity_count += 1
    logging.info(
        "Of [%s] clusters for tenant [%s], creating [%s] new identities; skipping "
        "[%s] clusters matching one existing identity and [%s] clusters matching "
        "multiple existing identities.",
        len(snapshots),
        tenant.value,
        len(new_snapshots),
        skipped_single_identity_count,
        skipped_multiple_identity_count,
    )
    return new_snapshots


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


def _create_identities(
    *,
    tenant: Tenant,
    snapshots: list[ClusterSnapshot],
    committed_email_hashes: set[str],
) -> None:
    """Creates one identity per snapshot, committing _IMPORT_CHUNK_SIZE at a time.

    A chunk whose commit fails is retried one cluster per transaction, so a
    single bad cluster is isolated and logged while the rest of the chunk still
    lands.

    Args:
        tenant: The tenant the snapshots belong to, named in failure logs.
        snapshots: The clusters to create identities from.
        committed_email_hashes: Hashes of addresses the tenant's existing
            identities already own, seeding the cross-cluster email exclusion;
            each committed chunk folds its own addresses in, so an address
            reaches at most one identity.
    """
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    for chunk_start in range(0, len(snapshots), _IMPORT_CHUNK_SIZE):
        chunk = snapshots[chunk_start : chunk_start + _IMPORT_CHUNK_SIZE]
        try:
            _write_chunk(
                chunk=chunk, now=now, committed_email_hashes=committed_email_hashes
            )
        except Exception:
            logging.exception(
                "Failed to write a chunk of [%s] clusters for tenant [%s] in one "
                "transaction; retrying the chunk one cluster at a time.",
                len(chunk),
                tenant.value,
            )
            _write_clusters_individually(
                tenant=tenant,
                chunk=chunk,
                now=now,
                committed_email_hashes=committed_email_hashes,
            )


def _write_chunk(
    *,
    chunk: list[ClusterSnapshot],
    now: datetime.datetime,
    committed_email_hashes: set[str],
) -> None:
    """Writes every snapshot in the chunk in a single transaction.

    On success, folds the chunk's newly used email hashes into
    committed_email_hashes so later chunks exclude them; on failure the
    transaction rolls back and the set is left untouched.
    """
    staged_email_hashes: set[str] = set()
    with SessionFactory.using_database(_IDENTITY_DATABASE_KEY) as session:
        for snapshot in chunk:
            session.add_all(
                build_identity_rows(
                    snapshot=snapshot,
                    now=now,
                    committed_email_hashes=committed_email_hashes,
                    staged_email_hashes=staged_email_hashes,
                )
            )
    committed_email_hashes |= staged_email_hashes


def _write_clusters_individually(
    *,
    tenant: Tenant,
    chunk: list[ClusterSnapshot],
    now: datetime.datetime,
    committed_email_hashes: set[str],
) -> None:
    """Writes each snapshot in its own transaction, isolating a failing cluster."""
    for snapshot in chunk:
        staged_email_hashes: set[str] = set()
        try:
            with SessionFactory.using_database(_IDENTITY_DATABASE_KEY) as session:
                session.add_all(
                    build_identity_rows(
                        snapshot=snapshot,
                        now=now,
                        committed_email_hashes=committed_email_hashes,
                        staged_email_hashes=staged_email_hashes,
                    )
                )
        except Exception:
            logging.exception(
                "Failed to import cluster [%s] for tenant [%s]; continuing with the "
                "remaining clusters.",
                snapshot.cluster.identity_cluster_id,
                tenant.value,
            )
            continue
        committed_email_hashes |= staged_email_hashes


def _clear_tenant_identities(tenant: Tenant) -> None:
    """Deletes the tenant's identities and all of their child rows.

    Runs when an import is processed with should_clear_first, so that the
    create pass alone loads the tenant's full clustering snapshot; with the
    database empty, every cluster is new. A clear-first run regenerates the tenant's
    recidiviz_ids, so no caller may persist them across one.

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
