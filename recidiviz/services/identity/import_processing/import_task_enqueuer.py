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
"""Enqueuing of Identity Service import work.

The trigger_import endpoint calls enqueue_import_task, which schedules a Cloud
Task for a tenant and returns 202. Cloud Tasks later calls the internal
processing endpoint, which runs the processing side (import_processor) to load
that tenant's clustering results into Postgres.
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
from recidiviz.persistence.entity.identity.identity_cluster_entities import (
    IdentityCluster,
)
from recidiviz.pipelines.ingest.identity.dataset_config import (
    identity_cluster_dataset_for_tenant,
)
from recidiviz.services.identity.constants import (
    IDENTITY_IMPORT_QUEUE,
    IMPORT_PROCESS_INTERNAL_ROUTE,
)
from recidiviz.services.identity.exceptions import ClusterSnapshotNotFoundError
from recidiviz.utils import metadata
from recidiviz.utils.metadata import CloudRunMetadata

# Keys of the Cloud Task body, shared between the enqueue side and the internal
# processing endpoint that reads it back.
TENANT_BODY_KEY = "tenant"
SNAPSHOT_TIMESTAMP_BODY_KEY = "snapshot_timestamp"
CLEAR_FIRST_BODY_KEY = "clear_first"

# How long Cloud Tasks waits for the processing endpoint to respond before
# treating the attempt as failed. Set to the Cloud Tasks maximum because the
# worker creates an identity for every cluster in a tenant's snapshot within the
# single request, which can be slow for a large tenant.
_IMPORT_DISPATCH_DEADLINE_SECONDS = 30 * 60


def enqueue_import_task(
    *,
    tenant: Tenant,
    cloud_run_metadata: CloudRunMetadata,
    should_clear_first: bool,
) -> None:
    """Enqueues a Cloud Task to import the given tenant's clustering results.

    Names the task deterministically from the tenant's cluster snapshot
    timestamp and the run's mode, so Cloud Tasks dedupes rapid repeated
    triggers for the same snapshot but never dedupes one mode's request
    against the other's. When Cloud Tasks rejects the name as a duplicate
    because a task for this snapshot is still in the queue, this function
    enqueues nothing. When the name is rejected but no such task is in the
    queue (the name is tombstoned because a task with it already ran or was
    deleted), this function re-enqueues the import under a fresh name, so a
    permanently failed import can be recovered by triggering again.

    Args:
        tenant: The tenant whose clustering results to import.
        cloud_run_metadata: Coordinates of the running service, used to
            address the processing endpoint and as the identity the task
            authenticates with.
        should_clear_first: Whether the import deletes the tenant's existing
            identities before creating from the snapshot, rather than leaving
            them in place and creating only new clusters. Carried to the
            processing endpoint in the task body.

    Raises:
        ClusterSnapshotNotFoundError: If the tenant's identity_cluster table
            does not exist yet.
    """
    snapshot_timestamp = _cluster_snapshot_timestamp(tenant)
    task_id = _import_task_id(
        tenant=tenant,
        snapshot_timestamp=snapshot_timestamp,
        should_clear_first=should_clear_first,
    )
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
            should_clear_first=should_clear_first,
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
            should_clear_first=should_clear_first,
            cloud_run_metadata=cloud_run_metadata,
        )


def _cluster_snapshot_timestamp(tenant: Tenant) -> datetime.datetime:
    """Returns the last-modified time of the tenant's identity_cluster table.

    The identity ingest pipeline rewrites this table on every run, so its
    last-modified time identifies the snapshot and stays stable across repeated
    triggers of the same run.
    """
    # Uses the raw google.cloud.bigquery client rather than the repo's
    # BigQueryClientImpl to keep the identity server clear of the
    # BigQueryClientImpl dependency cascade; this is a single table-metadata get.
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


def _import_task_id(
    *,
    tenant: Tenant,
    snapshot_timestamp: datetime.datetime,
    should_clear_first: bool,
) -> str:
    # Both modes carry a suffix so that neither task name is a prefix of the
    # other; the AlreadyExists queue check in enqueue_import_task matches by
    # prefix, and a bare name would let a pending task in one mode swallow a
    # trigger for the other.
    mode_suffix = "-clear-first" if should_clear_first else "-update"
    return f"import-{tenant.value}-{int(snapshot_timestamp.timestamp())}{mode_suffix}"


def _create_import_task(
    *,
    queue_manager: SingleCloudTaskQueueManager[CloudTaskQueueInfo],
    task_id: str,
    tenant: Tenant,
    snapshot_timestamp: datetime.datetime,
    should_clear_first: bool,
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
            CLEAR_FIRST_BODY_KEY: should_clear_first,
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
