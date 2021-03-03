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
"""Tests for CalculateCloudTaskManager"""

import datetime
import json
import unittest

import mock
from freezegun import freeze_time
from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2
from mock import patch

from recidiviz.calculator.pipeline.utils import calculate_cloud_task_manager
from recidiviz.calculator.pipeline.utils.calculate_cloud_task_manager import (
    CalculateCloudTaskManager,
)
from recidiviz.common.google_cloud.google_cloud_tasks_client_wrapper import (
    QUEUES_REGION,
)
from recidiviz.common.google_cloud.google_cloud_tasks_shared_queues import (
    JOB_MONITOR_QUEUE_V2,
)

CLOUD_TASK_MANAGER_PACKAGE_NAME = calculate_cloud_task_manager.__name__


class TestCalculateCloudTaskManager(unittest.TestCase):
    """Tests for CalculateCloudTaskManager"""

    def setUp(self) -> None:
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id = "recidiviz-456"
        self.mock_project_id_fn.return_value = self.mock_project_id

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    @patch(f"{CLOUD_TASK_MANAGER_PACKAGE_NAME}.uuid")
    @patch("google.cloud.tasks_v2.CloudTasksClient")
    @freeze_time("2019-04-14")
    def test_create_dataflow_monitor_task(
        self, mock_client: mock.MagicMock, mock_uuid: mock.MagicMock
    ) -> None:
        # Arrange
        delay_sec = 300
        now_utc_timestamp = int(datetime.datetime.now().timestamp())

        uuid = "random-uuid"
        mock_uuid.uuid4.return_value = uuid

        job_id = "12345"
        location = "fake_location"
        topic = "fake.topic"
        body = {
            "project_id": self.mock_project_id,
            "job_id": job_id,
            "location": location,
            "topic": topic,
        }

        queue_path = f"queue_path/{self.mock_project_id}/{QUEUES_REGION}"

        task_id = "12345-2019-04-14-random-uuid"
        task_path = f"{queue_path}/{task_id}"
        task = tasks_v2.types.task_pb2.Task(
            name=task_path,
            schedule_time=timestamp_pb2.Timestamp(
                seconds=(now_utc_timestamp + delay_sec)
            ),
            app_engine_http_request={
                "http_method": "POST",
                "relative_uri": "/dataflow_monitor/monitor",
                "body": json.dumps(body).encode(),
            },
        )

        mock_client.return_value.task_path.return_value = task_path
        mock_client.return_value.queue_path.return_value = queue_path

        # Act
        CalculateCloudTaskManager().create_dataflow_monitor_task(
            job_id, location, topic
        )

        # Assert
        mock_client.return_value.queue_path.assert_called_with(
            self.mock_project_id, QUEUES_REGION, JOB_MONITOR_QUEUE_V2
        )
        mock_client.return_value.task_path.assert_called_with(
            self.mock_project_id, QUEUES_REGION, JOB_MONITOR_QUEUE_V2, task_id
        )
        mock_client.return_value.create_task.assert_called_with(
            parent=queue_path, task=task
        )
