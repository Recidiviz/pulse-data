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
"""Tests for enqueuing and processing Identity Service import work."""
import datetime
from unittest import TestCase
from unittest.mock import patch

from google.api_core.exceptions import AlreadyExists, NotFound

from recidiviz.common.constants.tenants import Tenant
from recidiviz.common.google_cloud.single_cloud_task_queue_manager import (
    CloudTaskQueueInfo,
)
from recidiviz.services.identity.constants import IDENTITY_IMPORT_QUEUE
from recidiviz.services.identity.exceptions import ClusterSnapshotNotFoundError
from recidiviz.services.identity.import_task import enqueue_import_task, process_import
from recidiviz.utils.metadata import CloudRunMetadata

_SNAPSHOT = datetime.datetime(2026, 8, 8, tzinfo=datetime.timezone.utc)
_SNAPSHOT_EPOCH = int(_SNAPSHOT.timestamp())
_TASK_ID = f"import-US_OZ-{_SNAPSHOT_EPOCH}"

_CLOUD_RUN_METADATA = CloudRunMetadata(
    project_id="recidiviz-staging",
    region="us-central1",
    url="https://identity-service-abc.a.run.app",
    service_account_email="identity-service-cr@fake-project.iam.gserviceaccount.com",
)

_IMPORT_TASK_MODULE = "recidiviz.services.identity.import_task"


class EnqueueImportTaskTest(TestCase):
    """Tests for enqueue_import_task."""

    def setUp(self) -> None:
        self.bq_patcher = patch(f"{_IMPORT_TASK_MODULE}.bigquery.Client")
        self.project_patcher = patch(
            f"{_IMPORT_TASK_MODULE}.metadata.project_id",
            return_value="recidiviz-staging",
        )
        self.queue_patcher = patch(f"{_IMPORT_TASK_MODULE}.SingleCloudTaskQueueManager")
        self.mock_bq_client = self.bq_patcher.start().return_value
        self.project_patcher.start()
        self.mock_queue_manager = self.queue_patcher.start()
        self.addCleanup(self.bq_patcher.stop)
        self.addCleanup(self.project_patcher.stop)
        self.addCleanup(self.queue_patcher.stop)
        self.mock_bq_client.get_table.return_value.modified = _SNAPSHOT

    def test_enqueues_task_scoped_to_the_import_queue(self) -> None:
        enqueue_import_task(tenant=Tenant.US_OZ, cloud_run_metadata=_CLOUD_RUN_METADATA)
        self.assertEqual(
            IDENTITY_IMPORT_QUEUE,
            self.mock_queue_manager.call_args.kwargs["queue_name"],
        )

    def test_reads_snapshot_from_the_cluster_table(self) -> None:
        enqueue_import_task(tenant=Tenant.US_OZ, cloud_run_metadata=_CLOUD_RUN_METADATA)
        self.mock_bq_client.get_table.assert_called_once_with(
            "us_oz_identity_cluster.identity_cluster"
        )

    def test_task_dedupe_key_and_payload(self) -> None:
        enqueue_import_task(tenant=Tenant.US_OZ, cloud_run_metadata=_CLOUD_RUN_METADATA)
        self.mock_queue_manager.return_value.create_task.assert_called_once_with(
            task_id=f"import-US_OZ-{_SNAPSHOT_EPOCH}",
            absolute_uri="https://identity-service-abc.a.run.app/_internal/import/process",
            body={
                "tenant": "US_OZ",
                "snapshot_timestamp": "2026-08-08T00:00:00+00:00",
            },
            service_account_email="identity-service-cr@fake-project.iam.gserviceaccount.com",
            dispatch_deadline_seconds=1800,
        )

    def test_missing_cluster_table_raises_without_enqueuing(self) -> None:
        self.mock_bq_client.get_table.side_effect = NotFound("no such table")
        with self.assertRaises(ClusterSnapshotNotFoundError):
            enqueue_import_task(
                tenant=Tenant.US_OZ, cloud_run_metadata=_CLOUD_RUN_METADATA
            )
        self.mock_queue_manager.return_value.create_task.assert_not_called()

    def test_duplicate_of_task_still_in_queue_logs_info(self) -> None:
        self.mock_queue_manager.return_value.create_task.side_effect = AlreadyExists(
            "task already exists"
        )
        self.mock_queue_manager.return_value.get_queue_info.return_value = (
            CloudTaskQueueInfo(
                queue_name=IDENTITY_IMPORT_QUEUE,
                task_names=[f"projects/p/locations/l/queues/q/tasks/{_TASK_ID}"],
            )
        )
        with self.assertLogs(level="INFO") as logs:
            # Does not raise.
            enqueue_import_task(
                tenant=Tenant.US_OZ, cloud_run_metadata=_CLOUD_RUN_METADATA
            )
        self.assertFalse([r for r in logs.records if r.levelname == "WARNING"])
        # Nothing was re-enqueued beyond the rejected attempt.
        self.mock_queue_manager.return_value.create_task.assert_called_once()

    def test_duplicate_of_tombstoned_task_reenqueues_under_fresh_name(self) -> None:
        # The tombstoned-name case: a task with this name already ran (or was
        # deleted), and Cloud Tasks cannot say whether it succeeded or failed
        # permanently. The trigger re-enqueues under a fresh name so a
        # permanently failed import is recoverable; process_import is
        # idempotent, so re-running a successful one is harmless.
        self.mock_queue_manager.return_value.create_task.side_effect = [
            AlreadyExists("task already exists"),
            None,
        ]
        self.mock_queue_manager.return_value.get_queue_info.return_value = (
            CloudTaskQueueInfo(queue_name=IDENTITY_IMPORT_QUEUE, task_names=[])
        )
        enqueue_import_task(tenant=Tenant.US_OZ, cloud_run_metadata=_CLOUD_RUN_METADATA)

        (
            first_call,
            retry_call,
        ) = self.mock_queue_manager.return_value.create_task.call_args_list
        self.assertEqual(_TASK_ID, first_call.kwargs["task_id"])
        retry_task_id = retry_call.kwargs["task_id"]
        self.assertRegex(retry_task_id, rf"^{_TASK_ID}-retry-[0-9a-f]{{8}}$")
        # The retry task is identical to the original apart from its name.
        self.assertEqual(
            {k: v for k, v in first_call.kwargs.items() if k != "task_id"},
            {k: v for k, v in retry_call.kwargs.items() if k != "task_id"},
        )


class ProcessImportTest(TestCase):
    """Tests for process_import."""

    def test_process_import_runs(self) -> None:
        # The per-cluster passes are not implemented yet; this just confirms the
        # worker entrypoint accepts the tenant and snapshot and returns.
        process_import(tenant=Tenant.US_OZ, snapshot_timestamp=_SNAPSHOT)
