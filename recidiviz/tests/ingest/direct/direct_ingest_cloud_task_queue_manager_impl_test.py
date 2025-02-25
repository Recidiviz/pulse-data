# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Tests for the DirectIngestCloudTaskQueueManagerImpl."""
import json
import unittest
from unittest import TestCase
from urllib.parse import urlencode

import attr
import mock
from freezegun import freeze_time
from google.cloud import tasks_v2
from mock import patch

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.common.google_cloud.google_cloud_tasks_client_wrapper import (
    QUEUES_REGION,
)
from recidiviz.ingest.direct import (
    direct_ingest_cloud_task_queue_manager,
    direct_ingest_regions,
)
from recidiviz.ingest.direct.direct_ingest_cloud_task_queue_manager import (
    DirectIngestCloudTaskQueueManagerImpl,
    RawDataImportCloudTaskQueueInfo,
    _build_task_id,
    get_direct_ingest_queues_for_state,
)
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    to_normalized_unprocessed_raw_file_name,
)
from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_direct_ingest_bucket_for_state,
)
from recidiviz.ingest.direct.types.cloud_task_args import GcsfsRawDataBQImportArgs
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance

_REGION = direct_ingest_regions.DirectIngestRegion(
    region_code="us_xx",
    agency_name="agency_name",
    environment="production",
)

_PRIMARY_INGEST_BUCKET = gcsfs_direct_ingest_bucket_for_state(
    project_id="recidiviz-456",
    region_code=_REGION.region_code,
    ingest_instance=DirectIngestInstance.PRIMARY,
)
_SECONDARY_INGEST_BUCKET = gcsfs_direct_ingest_bucket_for_state(
    project_id="recidiviz-456",
    region_code=_REGION.region_code,
    ingest_instance=DirectIngestInstance.SECONDARY,
)

QUEUE_MANAGER_MODULE = direct_ingest_cloud_task_queue_manager.__name__


class TestRawDataImportQueueInfo(TestCase):
    """Tests for the RawDataImportCloudTaskQueueInfo."""

    def setUp(self) -> None:
        self.raw_data_file_path = GcsfsFilePath.from_directory_and_file_name(
            _PRIMARY_INGEST_BUCKET,
            to_normalized_unprocessed_raw_file_name("file_path.csv"),
        )
        self.raw_data_import_args = GcsfsRawDataBQImportArgs(
            raw_data_file_path=self.raw_data_file_path
        )

    def test_info_no_tasks(self) -> None:
        info = RawDataImportCloudTaskQueueInfo(queue_name="queue_name", task_names=[])

        for instance in DirectIngestInstance:
            self.assertFalse(
                info.has_any_tasks_for_instance(_REGION.region_code, instance)
            )

        self.assertFalse(
            info.is_raw_data_import_task_already_queued(self.raw_data_import_args)
        )
        self.assertFalse(info.has_raw_data_import_jobs_queued())

    def test_single_raw_task(self) -> None:
        full_task_name = _build_task_id(
            _REGION.region_code,
            DirectIngestInstance.PRIMARY,
            self.raw_data_import_args.task_id_tag(),
        )

        other_args = attr.evolve(
            self.raw_data_import_args,
            raw_data_file_path=GcsfsFilePath.from_directory_and_file_name(
                _PRIMARY_INGEST_BUCKET,
                to_normalized_unprocessed_raw_file_name("other_file.csv"),
            ),
        )

        info = RawDataImportCloudTaskQueueInfo(
            queue_name="queue_name",
            task_names=[
                f"projects/path/to/{full_task_name}",
            ],
        )

        self.assertTrue(
            info.has_any_tasks_for_instance(
                _REGION.region_code, DirectIngestInstance.PRIMARY
            )
        )

        self.assertFalse(
            info.has_any_tasks_for_instance(
                _REGION.region_code, DirectIngestInstance.SECONDARY
            )
        )

        self.assertTrue(
            info.is_raw_data_import_task_already_queued(self.raw_data_import_args)
        )
        self.assertFalse(info.is_raw_data_import_task_already_queued(other_args))
        self.assertTrue(info.has_raw_data_import_jobs_queued())


class TestDirectIngestCloudTaskQueueManagerImpl(TestCase):
    """Tests for the DirectIngestCloudTaskQueueManagerImpl."""

    def setUp(self) -> None:
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id = "recidiviz-456"
        self.mock_project_id_fn.return_value = self.mock_project_id

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    @patch(f"{QUEUE_MANAGER_MODULE}.uuid")
    @patch("google.cloud.tasks_v2.CloudTasksClient")
    @freeze_time("2019-07-20")
    def test_create_direct_ingest_scheduler_queue_task(
        self,
        mock_client: mock.MagicMock,
        mock_uuid: mock.MagicMock,
    ) -> None:
        # Arrange
        body_encoded = json.dumps({}).encode()
        uuid = "random-uuid"
        mock_uuid.uuid4.return_value = uuid
        queue_name = "direct-ingest-state-us-xx-scheduler"
        queue_path = f"{queue_name}-path"
        ingest_instance = DirectIngestInstance.PRIMARY

        task_name = queue_name + f"/{_REGION.region_code}-2019-07-20-{uuid}"
        task = tasks_v2.Task(
            mapping={
                "name": task_name,
                "app_engine_http_request": {
                    "http_method": "POST",
                    "relative_uri": f"/direct/scheduler?region={_REGION.region_code}&"
                    f"ingest_instance={ingest_instance.value.lower()}",
                    "body": body_encoded,
                },
            }
        )

        mock_client.return_value.task_path.return_value = task_name
        mock_client.return_value.queue_path.return_value = queue_path

        # Act
        DirectIngestCloudTaskQueueManagerImpl().create_direct_ingest_scheduler_queue_task(
            region=_REGION,
            ingest_instance=ingest_instance,
        )

        # Assert
        mock_client.return_value.queue_path.assert_called_with(
            self.mock_project_id, QUEUES_REGION, queue_name
        )
        mock_client.return_value.create_task.assert_called_with(
            parent=queue_path, task=task
        )

        mock_client.return_value.list_tasks.return_value = [task]

        self.assertFalse(
            DirectIngestCloudTaskQueueManagerImpl().all_ingest_instance_queues_are_empty(
                _REGION, DirectIngestInstance.PRIMARY
            )
        )

    @patch(f"{QUEUE_MANAGER_MODULE}.uuid")
    @patch("google.cloud.tasks_v2.CloudTasksClient")
    @freeze_time("2019-07-20")
    def test_create_direct_ingest_scheduler_queue_task_secondary(
        self,
        mock_client: mock.MagicMock,
        mock_uuid: mock.MagicMock,
    ) -> None:
        # Arrange
        body_encoded = json.dumps({}).encode()
        uuid = "random-uuid"
        mock_uuid.uuid4.return_value = uuid
        queue_path = "us_xx-scheduler-queue-path"
        queue_name = "direct-ingest-state-us-xx-scheduler-secondary"
        ingest_instance = DirectIngestInstance.SECONDARY

        task_name = f"{queue_name}/{_REGION.region_code}-2019-07-20-{uuid}"
        task = tasks_v2.Task(
            mapping={
                "name": task_name,
                "app_engine_http_request": {
                    "http_method": "POST",
                    "relative_uri": f"/direct/scheduler?region={_REGION.region_code}&"
                    f"ingest_instance={ingest_instance.value.lower()}",
                    "body": body_encoded,
                },
            }
        )

        mock_client.return_value.task_path.return_value = task_name
        mock_client.return_value.queue_path.return_value = queue_path

        # Act
        DirectIngestCloudTaskQueueManagerImpl().create_direct_ingest_scheduler_queue_task(
            region=_REGION,
            ingest_instance=ingest_instance,
        )

        # Assert
        mock_client.return_value.queue_path.assert_called_with(
            self.mock_project_id, QUEUES_REGION, queue_name
        )
        mock_client.return_value.create_task.assert_called_with(
            parent=queue_path, task=task
        )

        mock_client.return_value.list_tasks.return_value = [task]

        self.assertFalse(
            DirectIngestCloudTaskQueueManagerImpl().all_ingest_instance_queues_are_empty(
                _REGION, DirectIngestInstance.PRIMARY
            )
        )

    @patch(f"{QUEUE_MANAGER_MODULE}.uuid")
    @patch("google.cloud.tasks_v2.CloudTasksClient")
    @freeze_time("2019-07-20")
    def test_create_direct_ingest_raw_data_import_task(
        self,
        mock_client: mock.MagicMock,
        mock_uuid: mock.MagicMock,
    ) -> None:
        # Arrange
        raw_data_path = GcsfsFilePath.from_directory_and_file_name(
            _PRIMARY_INGEST_BUCKET,
            to_normalized_unprocessed_raw_file_name(file_name="raw_data_path.csv"),
        )
        import_args = GcsfsRawDataBQImportArgs(raw_data_file_path=raw_data_path)
        body = {
            "cloud_task_args": import_args.to_serializable(),
            "args_type": "GcsfsRawDataBQImportArgs",
        }
        body_encoded = json.dumps(body).encode()
        uuid = "random-uuid"
        mock_uuid.uuid4.return_value = uuid
        date = "2019-07-20"
        queue_name = "direct-ingest-state-us-xx-raw-data-import"
        queue_path = f"{queue_name}-path"

        task_name = queue_name + f"/{_REGION.region_code}-{date}-{uuid}"
        url_params = {
            "region": _REGION.region_code,
            "file_path": raw_data_path.abs_path(),
        }
        task = tasks_v2.Task(
            mapping={
                "name": task_name,
                "app_engine_http_request": {
                    "http_method": "POST",
                    "relative_uri": f"/direct/raw_data_import?{urlencode(url_params)}",
                    "body": body_encoded,
                },
            }
        )

        mock_client.return_value.task_path.return_value = task_name
        mock_client.return_value.queue_path.return_value = queue_path

        # Act
        DirectIngestCloudTaskQueueManagerImpl().create_direct_ingest_raw_data_import_task(
            _REGION, DirectIngestInstance.PRIMARY, import_args
        )

        # Assert
        mock_client.return_value.queue_path.assert_called_with(
            self.mock_project_id,
            QUEUES_REGION,
            queue_name,
        )
        mock_client.return_value.create_task.assert_called_with(
            parent=queue_path, task=task
        )

        mock_client.return_value.list_tasks.return_value = [task]

        self.assertFalse(
            DirectIngestCloudTaskQueueManagerImpl().all_ingest_instance_queues_are_empty(
                _REGION, DirectIngestInstance.PRIMARY
            )
        )

    @patch("google.cloud.tasks_v2.CloudTasksClient")
    def test_all_ingest_related_queues_are_empty(
        self,
        _: mock.MagicMock,
    ) -> None:
        """Assert that all ingest-related queues are empty when no tasks are added to the relevant queues."""
        self.assertTrue(
            DirectIngestCloudTaskQueueManagerImpl().all_ingest_instance_queues_are_empty(
                region=_REGION,
                ingest_instance=DirectIngestInstance.PRIMARY,
            )
        )

    @patch("google.cloud.tasks_v2.CloudTasksClient")
    def test_update_queue_state_str(
        self,
        client_mock: mock.MagicMock,
    ) -> None:
        impl = DirectIngestCloudTaskQueueManagerImpl()
        impl.update_ingest_queue_states_str(StateCode.US_CA, "PAUSED")
        client_mock.return_value.pause_queue.assert_called()

        client_mock.reset_mock()

        impl.update_ingest_queue_states_str(StateCode.US_CA, "RUNNING")
        client_mock.return_value.resume_queue.assert_called()


class TestDirectIngestQueuesForState(unittest.TestCase):
    def test_get_direct_ingest_queues_for_state(self) -> None:
        queue_names = get_direct_ingest_queues_for_state(StateCode.US_XX)
        self.assertEqual(
            [
                "direct-ingest-state-us-xx-scheduler",
                "direct-ingest-state-us-xx-scheduler-secondary",
                "direct-ingest-state-us-xx-raw-data-import",
                "direct-ingest-state-us-xx-raw-data-import-secondary",
            ],
            queue_names,
        )
