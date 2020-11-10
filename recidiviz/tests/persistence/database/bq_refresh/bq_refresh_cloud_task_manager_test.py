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

from freezegun import freeze_time
from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2
import mock
from mock import patch

from recidiviz.persistence.database.bq_refresh import bq_refresh_cloud_task_manager
from recidiviz.persistence.database.bq_refresh.bq_refresh_cloud_task_manager import \
    BQRefreshCloudTaskManager
from recidiviz.common.google_cloud.google_cloud_tasks_client_wrapper import (
    QUEUES_REGION
)
from recidiviz.common.google_cloud.google_cloud_tasks_shared_queues import \
    JOB_MONITOR_QUEUE_V2, BIGQUERY_QUEUE_V2
from recidiviz.persistence.database.sqlalchemy_engine_manager import SchemaType

CLOUD_TASK_MANAGER_PACKAGE_NAME = bq_refresh_cloud_task_manager.__name__


class TestBQRefreshCloudTaskManager(unittest.TestCase):
    """Tests for BQRefreshCloudTaskManager"""

    @patch(f'{CLOUD_TASK_MANAGER_PACKAGE_NAME}.uuid')
    @patch('google.cloud.tasks_v2.CloudTasksClient')
    @freeze_time('2019-04-12')
    def test_create_refresh_bq_table_task(
            self, mock_client: mock.MagicMock, mock_uuid: mock.MagicMock) -> None:
        # Arrange
        uuid = 'random-uuid'
        mock_uuid.uuid4.return_value = uuid

        project_id = 'recidiviz-456'
        table_name = 'test_table'
        schema_type = SchemaType.JAILS.value
        queue_path = f'queue_path/{project_id}/{QUEUES_REGION}'
        task_id = f'test_table-{schema_type}-2019-04-12-random-uuid'
        task_path = f'{queue_path}/{task_id}'

        body = {
            'table_name': table_name,
            'schema_type': schema_type
        }

        task = tasks_v2.types.task_pb2.Task(
            name=task_path,
            app_engine_http_request={
                'http_method': 'POST',
                'relative_uri': '/cloud_sql_to_bq/refresh_bq_table',
                'body': json.dumps(body).encode()
            }
        )

        mock_client.return_value.task_path.return_value = task_path
        mock_client.return_value.queue_path.return_value = queue_path

        # Act
        BQRefreshCloudTaskManager(project_id=project_id). \
            create_refresh_bq_table_task(table_name=table_name, schema_type=SchemaType.JAILS)

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
            parent=queue_path, task=task)

    @patch(f'{CLOUD_TASK_MANAGER_PACKAGE_NAME}.uuid')
    @patch('google.cloud.tasks_v2.CloudTasksClient')
    @freeze_time('2019-04-13')
    def test_create_bq_refresh_monitor_task(
            self, mock_client: mock.MagicMock, mock_uuid: mock.MagicMock) -> None:
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
                'relative_uri': '/cloud_sql_to_bq/monitor_refresh_bq_tasks',
                'body': json.dumps(body).encode()
            }
        )

        mock_client.return_value.task_path.return_value = task_path
        mock_client.return_value.queue_path.return_value = queue_path

        # Act
        BQRefreshCloudTaskManager(project_id=project_id). \
            create_bq_refresh_monitor_task(topic=topic, message=message)

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
            parent=queue_path, task=task)
