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
service's Postgres state: it reads the snapshot, clears the tenant's existing
identities, and creates one identity per cluster. Cloud Tasks calls it through
the internal processing endpoint after the enqueue side (import_task_enqueuer)
schedules the work.
"""
import datetime
import logging

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


def process_import(*, tenant: Tenant, snapshot_timestamp: datetime.datetime) -> None:
    """Loads the given tenant's clustering results into the service's state.

    Reads the tenant's cluster snapshot from BigQuery, clears the tenant's
    existing identities, and creates one identity per cluster.

    TODO(OBT-37725): This is the temporary clear-first shape (see
    _clear_tenant_identities). When clear-first is removed, this regains the
    per-cluster idempotency skip and the update, merge, and split passes rather
    than creating every cluster afresh.
    """
    logging.info(
        "Processing identity import for tenant [%s], snapshot [%s].",
        tenant.value,
        snapshot_timestamp.isoformat(),
    )
    snapshots = read_cluster_snapshot(tenant)
    _clear_tenant_identities(tenant)
    _create_identities(tenant=tenant, snapshots=snapshots)


def _create_identities(*, tenant: Tenant, snapshots: list[ClusterSnapshot]) -> None:
    """Creates one identity per snapshot, committing _IMPORT_CHUNK_SIZE at a time.

    A chunk whose commit fails is retried one cluster per transaction, so a
    single bad cluster is isolated and logged while the rest of the chunk still
    lands.
    """
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    # Under clear-first the tenant was just cleared, so it has no pre-existing
    # emails; cross-chunk exclusion is carried entirely by the accumulating
    # committed/staged sets. TODO(OBT-37725): once incremental imports arrive,
    # seed this from the tenant's existing email hashes so the create pass
    # excludes an address already attached to another identity.
    committed_email_hashes: set[str] = set()
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

    TODO(OBT-37725): This clear-first step is temporary scaffolding so that the
    create pass alone loads a tenant's full clustering snapshot; with the
    database empty, every cluster is new. Remove it once the update pass (plus
    merge and split detection) makes incremental runs correct. While it is
    active, every import run regenerates the tenant's recidiviz_ids, so no
    caller may persist them.

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
