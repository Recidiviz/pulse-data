# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Tests for ViewExportCloudTaskManager."""

import json
import unittest

from google.cloud import tasks_v2
from mock import patch

from freezegun import freeze_time
from parameterized import parameterized

from recidiviz.common.google_cloud.google_cloud_tasks_client_wrapper import (
    QUEUES_REGION,
)
from recidiviz.common.google_cloud.google_cloud_tasks_shared_queues import (
    BIGQUERY_QUEUE_V2,
)
from recidiviz.metrics.export import view_export_cloud_task_manager
from recidiviz.metrics.export.view_export_cloud_task_manager import (
    ViewExportCloudTaskManager,
)

CLOUD_TASK_MANAGER_PACKAGE_NAME = view_export_cloud_task_manager.__name__


class ViewExportCloudTaskManagerTest(unittest.TestCase):
    """Tests for ViewExportCloudTaskManager."""

    def setUp(self) -> None:
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id = "recidiviz-456"
        self.mock_project_id_fn.return_value = self.mock_project_id

        self.mock_client_patcher = patch("google.cloud.tasks_v2.CloudTasksClient")
        self.mock_client = self.mock_client_patcher.start()

        self.mock_uuid_patcher = patch(f"{CLOUD_TASK_MANAGER_PACKAGE_NAME}.uuid")
        self.mock_uuid = self.mock_uuid_patcher.start()

    def tearDown(self) -> None:
        self.mock_uuid_patcher.stop()
        self.mock_client_patcher.stop()
        self.metadata_patcher.stop()

    @parameterized.expand(
        [
            (
                "with_state_code_and_export_name",
                "CORE",
                "US_ND",
                "/export/metric_view_data?export_job_name=CORE&state_code=US_ND",
            ),
        ]
    )
    @freeze_time("2019-04-12")
    def test_create_metric_view_data_export_task(
        self, _name: str, export_job_name: str, state_code: str, expected_url: str
    ) -> None:

        # Arrange
        uuid = "random-uuid"
        self.mock_uuid.uuid4.return_value = uuid

        queue_path = f"queue_path/{self.mock_project_id}/{QUEUES_REGION}"
        task_id = f"view_export-{export_job_name}-{state_code}-2019-04-12-random-uuid"
        task_path = f"{queue_path}/{task_id}"

        task = tasks_v2.types.task_pb2.Task(
            name=task_path,
            app_engine_http_request={
                "http_method": "POST",
                "relative_uri": expected_url,
                "body": json.dumps({}).encode(),
            },
        )

        self.mock_client.return_value.task_path.return_value = task_path
        self.mock_client.return_value.queue_path.return_value = queue_path

        # Act
        ViewExportCloudTaskManager().create_metric_view_data_export_task(
            export_job_name=export_job_name, state_code=state_code
        )

        # Assert
        self.mock_client.return_value.queue_path.assert_called_with(
            self.mock_project_id, QUEUES_REGION, BIGQUERY_QUEUE_V2
        )
        self.mock_client.return_value.task_path.assert_called_with(
            self.mock_project_id, QUEUES_REGION, BIGQUERY_QUEUE_V2, task_id
        )
        self.mock_client.return_value.create_task.assert_called_with(
            parent=queue_path, task=task
        )

    @parameterized.expand(
        [
            (
                "with_state_code_and_export_name",
                "CORE",
                "/export/metric_view_data?export_job_name=CORE",
            ),
        ]
    )
    @freeze_time("2019-04-12")
    def test_create_metric_view_data_export_task_state_agnostic(
        self, _name: str, export_job_name: str, expected_url: str
    ) -> None:

        # Arrange
        uuid = "random-uuid"
        self.mock_uuid.uuid4.return_value = uuid

        queue_path = f"queue_path/{self.mock_project_id}/{QUEUES_REGION}"
        task_id = f"view_export-{export_job_name}-2019-04-12-random-uuid"
        task_path = f"{queue_path}/{task_id}"

        task = tasks_v2.types.task_pb2.Task(
            name=task_path,
            app_engine_http_request={
                "http_method": "POST",
                "relative_uri": expected_url,
                "body": json.dumps({}).encode(),
            },
        )

        self.mock_client.return_value.task_path.return_value = task_path
        self.mock_client.return_value.queue_path.return_value = queue_path

        # Act
        ViewExportCloudTaskManager().create_metric_view_data_export_task(
            export_job_name=export_job_name,
        )

        # Assert
        self.mock_client.return_value.queue_path.assert_called_with(
            self.mock_project_id, QUEUES_REGION, BIGQUERY_QUEUE_V2
        )
        self.mock_client.return_value.task_path.assert_called_with(
            self.mock_project_id, QUEUES_REGION, BIGQUERY_QUEUE_V2, task_id
        )
        self.mock_client.return_value.create_task.assert_called_with(
            parent=queue_path, task=task
        )
