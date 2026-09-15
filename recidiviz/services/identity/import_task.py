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
"""Enqueuing and processing of Identity Service import work.

The trigger_import endpoint enqueues a Cloud Task for a tenant and returns 202.
Cloud Tasks then calls the internal processing endpoint, which runs
process_import to load that tenant's clustering results from BigQuery into the
service's Postgres state: it reads the snapshot, clears the tenant's existing
identities, and creates one identity per cluster.
"""
import datetime
import logging
import uuid

from google.api_core.exceptions import AlreadyExists, NotFound
from google.cloud import bigquery

from recidiviz.common.constants.tenants import Tenant
from recidiviz.common.google_cloud.single_cloud_task_queue_manager import (
    CloudTaskQueueInfo,
    SingleCloudTaskQueueManager,
)
from recidiviz.persistence.database.schema.identity import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.identity.identity_cluster_entities import (
    IdentityCluster,
)
from recidiviz.pipelines.ingest.identity.dataset_config import (
    identity_cluster_dataset_for_tenant,
)
from recidiviz.services.identity.bq_snapshot_reader import (
    ClusterSnapshot,
    read_cluster_snapshot,
)
from recidiviz.services.identity.constants import (
    IDENTITY_IMPORT_QUEUE,
    IMPORT_PROCESS_INTERNAL_ROUTE,
)
from recidiviz.services.identity.create_pass import build_identity_rows
from recidiviz.services.identity.exceptions import ClusterSnapshotNotFoundError
from recidiviz.utils import metadata
from recidiviz.utils.metadata import CloudRunMetadata

# Keys of the Cloud Task body, shared between the enqueue side and the internal
# processing endpoint that reads it back.
TENANT_BODY_KEY = "tenant"
SNAPSHOT_TIMESTAMP_BODY_KEY = "snapshot_timestamp"

# How long Cloud Tasks waits for the processing endpoint to respond before
# treating the attempt as failed. Set to the Cloud Tasks maximum because the
# worker creates an identity for every cluster in a tenant's snapshot within the
# single request, which can be slow for a large tenant.
_IMPORT_DISPATCH_DEADLINE_SECONDS = 30 * 60

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


def enqueue_import_task(
    *, tenant: Tenant, cloud_run_metadata: CloudRunMetadata
) -> None:
    """Enqueues a Cloud Task to import the given tenant's clustering results.

    Names the task deterministically from the tenant's cluster snapshot timestamp
    so Cloud Tasks dedupes rapid repeated triggers for the same snapshot. When
    Cloud Tasks rejects the name as a duplicate because a task for this snapshot
    is still in the queue, this function enqueues nothing. When the name is
    rejected but no such task is in the queue (the name is tombstoned because a
    task with it already ran or was deleted), this function re-enqueues the
    import under a fresh name, so a permanently failed import can be recovered
    by triggering again.

    Raises ClusterSnapshotNotFoundError if the tenant's identity_cluster table
    does not exist yet.
    """
    snapshot_timestamp = _cluster_snapshot_timestamp(tenant)
    task_id = _import_task_id(tenant=tenant, snapshot_timestamp=snapshot_timestamp)
    queue_manager: SingleCloudTaskQueueManager[
        CloudTaskQueueInfo
    ] = SingleCloudTaskQueueManager(
        queue_info_cls=CloudTaskQueueInfo, queue_name=IDENTITY_IMPORT_QUEUE
    )
    try:
        _create_import_task(
            queue_manager=queue_manager,
            task_id=task_id,
            tenant=tenant,
            snapshot_timestamp=snapshot_timestamp,
            cloud_run_metadata=cloud_run_metadata,
        )
    except AlreadyExists:
        # Cloud Tasks rejects a task name that is either currently enqueued or
        # tombstoned for a window after a task with that name completed, failed
        # permanently, or was deleted. Checking the queue tells the two cases
        # apart; because the check matches by prefix, a pending retry task
        # created below also counts as already enqueued.
        if not queue_manager.get_queue_info(task_id_prefix=task_id).is_empty():
            logging.info(
                "Import task [%s] for tenant [%s] is already enqueued; nothing "
                "to do.",
                task_id,
                tenant.value,
            )
            return
        # The task already ran (succeeded or failed permanently) or was deleted;
        # Cloud Tasks cannot say which after the fact. Re-enqueue under a fresh
        # name so that if the task failed permanently, this trigger recovers the
        # import rather than leaving it blocked until the name's tombstone
        # expires. If the task actually succeeded, the re-run is harmless
        # because process_import is idempotent, as Cloud Tasks' at-least-once
        # delivery already requires.
        logging.info(
            "Import task [%s] for tenant [%s] already ran or was deleted "
            "recently; re-enqueuing under a fresh name.",
            task_id,
            tenant.value,
        )
        _create_import_task(
            queue_manager=queue_manager,
            task_id=f"{task_id}-retry-{uuid.uuid4().hex[:8]}",
            tenant=tenant,
            snapshot_timestamp=snapshot_timestamp,
            cloud_run_metadata=cloud_run_metadata,
        )


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


def _cluster_snapshot_timestamp(tenant: Tenant) -> datetime.datetime:
    """Returns the last-modified time of the tenant's identity_cluster table.

    The identity ingest pipeline rewrites this table on every run, so its
    last-modified time identifies the snapshot and stays stable across repeated
    triggers of the same run.
    """
    # Uses the raw google.cloud.bigquery client rather than the repo's
    # BigQueryClientImpl because the identity server's source-visibility test
    # forbids importing recidiviz.big_query; this is a single table-metadata get.
    bq_client = bigquery.Client(project=metadata.project_id())
    table_id = (
        f"{identity_cluster_dataset_for_tenant(tenant.value)}."
        f"{IdentityCluster.get_table_id()}"
    )
    try:
        table = bq_client.get_table(table_id)
    except NotFound as e:
        raise ClusterSnapshotNotFoundError(
            f"No identity_cluster table found for tenant [{tenant.value}] at "
            f"[{table_id}]; the identity ingest pipeline may not have written its "
            f"clustering results yet."
        ) from e
    return table.modified


def _import_task_id(*, tenant: Tenant, snapshot_timestamp: datetime.datetime) -> str:
    return f"import-{tenant.value}-{int(snapshot_timestamp.timestamp())}"


def _create_import_task(
    *,
    queue_manager: SingleCloudTaskQueueManager[CloudTaskQueueInfo],
    task_id: str,
    tenant: Tenant,
    snapshot_timestamp: datetime.datetime,
    cloud_run_metadata: CloudRunMetadata,
) -> None:
    """Creates the Cloud Task that imports the given tenant's cluster snapshot.

    Raises AlreadyExists if a task with this name is already enqueued or the
    name is tombstoned.
    """
    queue_manager.create_task(
        task_id=task_id,
        absolute_uri=f"{cloud_run_metadata.url}{IMPORT_PROCESS_INTERNAL_ROUTE}",
        body={
            TENANT_BODY_KEY: tenant.value,
            SNAPSHOT_TIMESTAMP_BODY_KEY: snapshot_timestamp.isoformat(),
        },
        service_account_email=cloud_run_metadata.service_account_email,
        dispatch_deadline_seconds=_IMPORT_DISPATCH_DEADLINE_SECONDS,
    )
    logging.info(
        "Enqueued import task [%s] for tenant [%s], snapshot [%s].",
        task_id,
        tenant.value,
        snapshot_timestamp.isoformat(),
    )
