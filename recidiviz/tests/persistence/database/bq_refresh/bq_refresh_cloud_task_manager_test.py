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
"""Tests for BQRefreshCloudTaskManager"""

import datetime
import json
import unittest

import mock
from freezegun import freeze_time
from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2
from mock import patch

from recidiviz.calculator.pipeline.pipeline_type import PipelineRunType
from recidiviz.common.google_cloud.google_cloud_tasks_client_wrapper import (
    QUEUES_REGION,
)
from recidiviz.common.google_cloud.google_cloud_tasks_shared_queues import (
    BIGQUERY_QUEUE_V2,
    JOB_MONITOR_QUEUE_V2,
)
from recidiviz.persistence.database.bq_refresh import bq_refresh_cloud_task_manager
from recidiviz.persistence.database.bq_refresh.bq_refresh_cloud_task_manager import (
    BQRefreshCloudTaskManager,
)
from recidiviz.persistence.database.schema_utils import SchemaType

CLOUD_TASK_MANAGER_PACKAGE_NAME = bq_refresh_cloud_task_manager.__name__


class TestBQRefreshCloudTaskManager(unittest.TestCase):
    """Tests for BQRefreshCloudTaskManager"""

    def setUp(self) -> None:
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id = "recidiviz-456"
        self.mock_project_id_fn.return_value = self.mock_project_id

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    @patch(f"{CLOUD_TASK_MANAGER_PACKAGE_NAME}.uuid")
    @patch("google.cloud.tasks_v2.CloudTasksClient")
    @freeze_time("2019-04-13")
    def test_reattempt_create_refresh_tasks_task(
        self, mock_client: mock.MagicMock, mock_uuid: mock.MagicMock
    ) -> None:
        # Arrange
        delay_sec = 60
        now_utc_timestamp = int(datetime.datetime.now().timestamp())

        uuid = "random-uuid"
        mock_uuid.uuid4.return_value = uuid

        schema = "fake_schema"
        lock_id = "fake_lock_id"
        queue_path = f"queue_path/{self.mock_project_id}/{QUEUES_REGION}"
        task_id = "reenqueue_wait_task-2019-04-13-random-uuid"
        task_path = f"{queue_path}/{task_id}"
        pipeline_run_type = PipelineRunType.INCREMENTAL.value

        body = {
            "lock_id": lock_id,
            "pipeline_run_type": pipeline_run_type,
        }

        mock_client.return_value.task_path.return_value = task_path
        mock_client.return_value.queue_path.return_value = queue_path

        # Act
        BQRefreshCloudTaskManager().create_reattempt_create_refresh_tasks_task(
            schema=schema,
            pipeline_run_type=pipeline_run_type,
            lock_id=lock_id,
        )

        # Assert
        mock_client.return_value.queue_path.assert_called_with(
            self.mock_project_id, QUEUES_REGION, JOB_MONITOR_QUEUE_V2
        )
        mock_client.return_value.task_path.assert_called_with(
            self.mock_project_id, QUEUES_REGION, JOB_MONITOR_QUEUE_V2, task_id
        )

        expected_task = tasks_v2.types.task_pb2.Task(
            name=task_path,
            schedule_time=timestamp_pb2.Timestamp(
                seconds=(now_utc_timestamp + delay_sec)
            ),
            app_engine_http_request={
                "http_method": "POST",
                "relative_uri": "/cloud_sql_to_bq/create_refresh_bq_schema_task/fake_schema",
                "body": json.dumps(body).encode(),
            },
        )

        mock_client.return_value.create_task.assert_called_with(
            parent=queue_path, task=expected_task
        )

    @patch(f"{CLOUD_TASK_MANAGER_PACKAGE_NAME}.uuid")
    @patch("google.cloud.tasks_v2.CloudTasksClient")
    @freeze_time("2019-04-12")
    def test_create_refresh_bq_schema_task(
        self, mock_client: mock.MagicMock, mock_uuid: mock.MagicMock
    ) -> None:
        # Arrange
        uuid = "random-uuid"
        mock_uuid.uuid4.return_value = uuid

        schema_type = SchemaType.STATE.value
        queue_path = f"queue_path/{self.mock_project_id}/{QUEUES_REGION}"
        task_id = f"{schema_type}-2019-04-12-random-uuid"
        task_path = f"{queue_path}/{task_id}"
        body = {"pipeline_run_type": PipelineRunType.INCREMENTAL.value}

        task = tasks_v2.types.task_pb2.Task(
            name=task_path,
            app_engine_http_request={
                "http_method": "POST",
                "relative_uri": "/cloud_sql_to_bq/refresh_bq_schema/STATE",
                "body": json.dumps(body).encode(),
            },
        )

        mock_client.return_value.task_path.return_value = task_path
        mock_client.return_value.queue_path.return_value = queue_path

        # Act
        BQRefreshCloudTaskManager().create_refresh_bq_schema_task(
            schema_type=SchemaType.STATE, body=body
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
