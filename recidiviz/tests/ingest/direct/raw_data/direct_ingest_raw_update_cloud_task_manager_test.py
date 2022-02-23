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
"""Tests for DirectIngestRawUpdateCloudTaskManager"""

import json
import unittest

import mock
from freezegun import freeze_time
from google.cloud import tasks_v2
from mock import patch

from recidiviz.common.google_cloud.google_cloud_tasks_client_wrapper import (
    QUEUES_REGION,
)
from recidiviz.common.google_cloud.google_cloud_tasks_shared_queues import (
    BIGQUERY_QUEUE_V2,
)
from recidiviz.ingest.direct.raw_data import direct_ingest_raw_update_cloud_task_manager
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_update_cloud_task_manager import (
    DirectIngestRawUpdateCloudTaskManager,
)

CLOUD_TASK_MANAGER_PACKAGE_NAME = direct_ingest_raw_update_cloud_task_manager.__name__


class TestDirectIngestRawUpdateCloudTaskManager(unittest.TestCase):
    """Tests for DirectIngestRawUpdateCloudTaskManager"""

    def setUp(self) -> None:
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id = "recidiviz-456"
        self.mock_project_id_fn.return_value = self.mock_project_id

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    @patch(f"{CLOUD_TASK_MANAGER_PACKAGE_NAME}.uuid")
    @patch("google.cloud.tasks_v2.CloudTasksClient")
    @freeze_time("2019-04-12")
    def test_create_bq_test(
        self, mock_client: mock.MagicMock, mock_uuid: mock.MagicMock
    ) -> None:
        # Arrange
        uuid = "random-uuid"
        mock_uuid.uuid4.return_value = uuid

        region_code = "us_xx"
        queue_path = f"queue_path/{self.mock_project_id}/{QUEUES_REGION}"
        task_id = "us_xx-update_raw_data_latest_views-2019-04-12-random-uuid"
        task_path = f"{queue_path}/{task_id}"
        relative_uri = (
            f"/direct/update_raw_data_latest_views_for_state?region={region_code}"
        )

        task = tasks_v2.types.task_pb2.Task(
            name=task_path,
            app_engine_http_request={
                "http_method": "POST",
                "relative_uri": relative_uri,
                "body": json.dumps({}).encode(),
            },
        )

        mock_client.return_value.task_path.return_value = task_path
        mock_client.return_value.queue_path.return_value = queue_path

        # Act
        DirectIngestRawUpdateCloudTaskManager().create_raw_data_latest_view_update_task(
            region_code
        )

        # Assert
        mock_client.return_value.queue_path.assert_called_with(
            self.mock_project_id, QUEUES_REGION, BIGQUERY_QUEUE_V2
        )
        mock_client.return_value.task_path.assert_called_with(
            self.mock_project_id, QUEUES_REGION, BIGQUERY_QUEUE_V2, task_id
        )
        mock_client.return_value.create_task.assert_called_with(
            parent=queue_path, task=task
        )
