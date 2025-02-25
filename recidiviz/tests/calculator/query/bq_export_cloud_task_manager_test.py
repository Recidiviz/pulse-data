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
"""Tests for BQExportCloudTaskManager"""

import datetime
import json
import unittest

from freezegun import freeze_time
from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2
from mock import patch

from recidiviz.calculator.query import bq_export_cloud_task_manager
from recidiviz.calculator.query.bq_export_cloud_task_manager import \
    BQExportCloudTaskManager
from recidiviz.common.google_cloud.google_cloud_tasks_client_wrapper import (
    QUEUES_REGION
)
from recidiviz.common.google_cloud.google_cloud_tasks_shared_queues import \
    JOB_MONITOR_QUEUE_V2, BIGQUERY_QUEUE_V2

CLOUD_TASK_MANAGER_PACKAGE_NAME = bq_export_cloud_task_manager.__name__


class TestBQExportCloudTaskManager(unittest.TestCase):
    """Tests for BQExportCloudTaskManager"""

    @patch(f'{CLOUD_TASK_MANAGER_PACKAGE_NAME}.uuid')
    @patch(f'google.cloud.tasks_v2.CloudTasksClient')
    @freeze_time('2019-04-12')
    def test_create_bq_test(self, mock_client, mock_uuid):
        # Arrange
        uuid = 'random-uuid'
        mock_uuid.uuid4.return_value = uuid

        project_id = 'recidiviz-456'
        table_name = 'test_table'
        schema_type = 'test_schema_type'
        queue_path = f'queue_path/{project_id}/{QUEUES_REGION}'
        task_id = 'test_table-test_schema_type-2019-04-12-random-uuid'
        task_path = f'{queue_path}/{task_id}'

        body = {
            'table_name': table_name,
            'schema_type': schema_type
        }

        task = tasks_v2.types.task_pb2.Task(
            name=task_path,
            app_engine_http_request={
                'http_method': 'POST',
                'relative_uri': '/export_manager/export',
                'body': json.dumps(body).encode()
            }
        )

        mock_client.return_value.task_path.return_value = task_path
        mock_client.return_value.queue_path.return_value = queue_path

        # Act
        BQExportCloudTaskManager(project_id=project_id). \
            create_bq_task(table_name=table_name, schema_type=schema_type)

        # Assert
        mock_client.return_value.queue_path.assert_called_with(
            project_id,
            QUEUES_REGION,
            BIGQUERY_QUEUE_V2)
        mock_client.return_value.task_path.assert_called_with(
            project_id,
            QUEUES_REGION,
            BIGQUERY_QUEUE_V2,
            task_id)
        mock_client.return_value.create_task.assert_called_with(
            queue_path, task)

    @patch(f'{CLOUD_TASK_MANAGER_PACKAGE_NAME}.uuid')
    @patch(f'google.cloud.tasks_v2.CloudTasksClient')
    @freeze_time('2019-04-13')
    def test_create_bq_monitor_test(
            self, mock_client, mock_uuid):
        # Arrange
        delay_sec = 60
        now_utc_timestamp = int(datetime.datetime.now().timestamp())

        uuid = 'random-uuid'
        mock_uuid.uuid4.return_value = uuid

        project_id = 'recidiviz-456'
        topic = 'fake.topic'
        message = 'A fake message'
        queue_path = f'queue_path/{project_id}/{QUEUES_REGION}'
        task_id = 'fake-topic-2019-04-13-random-uuid'
        task_path = f'{queue_path}/{task_id}'

        body = {
            'topic': topic,
            'message': message,
        }

        task = tasks_v2.types.task_pb2.Task(
            name=task_path,
            schedule_time=timestamp_pb2.Timestamp(
                seconds=(now_utc_timestamp + delay_sec)),
            app_engine_http_request={
                'http_method': 'POST',
                'relative_uri': '/export_manager/bq_monitor',
                'body': json.dumps(body).encode()
            }
        )

        mock_client.return_value.task_path.return_value = task_path
        mock_client.return_value.queue_path.return_value = queue_path

        # Act
        BQExportCloudTaskManager(project_id=project_id). \
            create_bq_monitor_task(topic=topic, message=message)

        # Assert
        mock_client.return_value.queue_path.assert_called_with(
            project_id,
            QUEUES_REGION,
            JOB_MONITOR_QUEUE_V2)
        mock_client.return_value.task_path.assert_called_with(
            project_id,
            QUEUES_REGION,
            JOB_MONITOR_QUEUE_V2,
            task_id)
        mock_client.return_value.create_task.assert_called_with(
            queue_path, task)
