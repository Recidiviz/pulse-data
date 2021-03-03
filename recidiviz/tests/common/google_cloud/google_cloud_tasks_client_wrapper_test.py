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
"""Tests for GoogleCloudTasksClientWrapper."""
import datetime
import os
import unittest
from typing import List, Set

from freezegun import freeze_time
from google.cloud import tasks_v2, exceptions
from google.cloud.tasks_v2.proto import queue_pb2
from google.protobuf import timestamp_pb2
from mock import create_autospec, patch

from recidiviz.common.google_cloud.google_cloud_tasks_client_wrapper import (
    GoogleCloudTasksClientWrapper,
)


class TestGoogleCloudTasksClientWrapper(unittest.TestCase):
    """Tests for GoogleCloudTasksClientWrapper."""

    PROJECT_ID = "my-project-id"
    INSTANCE_REGION = "us-east1"
    QUEUE_NAME = "queue-name"
    QUEUE_NAME_2 = "queue-name-2"

    @staticmethod
    def create_mock_cloud_tasks_client():
        mock_client = create_autospec(tasks_v2.CloudTasksClient)
        mock_client.queue_path.side_effect = tasks_v2.CloudTasksClient.queue_path

        mock_client.task_path.side_effect = tasks_v2.CloudTasksClient.task_path

        return mock_client

    def setUp(self):
        self.mock_client_patcher = patch(
            "google.cloud.tasks_v2.CloudTasksClient",
            return_value=self.create_mock_cloud_tasks_client(),
        )

        self.mock_client_cls = self.mock_client_patcher.start()
        self.mock_client = self.mock_client_cls()

        with patch(
            "recidiviz.utils.metadata.region", return_value=self.INSTANCE_REGION
        ):
            with patch(
                "recidiviz.utils.metadata.project_id", return_value=self.PROJECT_ID
            ):
                self.client_wrapper = GoogleCloudTasksClientWrapper()

    def tearDown(self):
        self.mock_client_patcher.stop()

    def test_format_queue_path(self):
        queue_path = self.client_wrapper.format_queue_path(self.QUEUE_NAME)
        self.assertEqual(
            queue_path, "projects/my-project-id/locations/us-east1/queues/queue-name"
        )

        queue_path = self.client_wrapper.format_queue_path(self.QUEUE_NAME_2)
        self.assertEqual(
            queue_path, "projects/my-project-id/locations/us-east1/queues/queue-name-2"
        )

    def test_format_task_path(self):
        task_path = self.client_wrapper.format_task_path(
            self.QUEUE_NAME, "task-name-1234"
        )
        self.assertEqual(
            task_path,
            "projects/my-project-id/locations/us-east1/queues/queue-name/tasks/"
            "task-name-1234",
        )

        task_path = self.client_wrapper.format_task_path(
            self.QUEUE_NAME_2, "task-name-3456"
        )
        self.assertEqual(
            task_path,
            "projects/my-project-id/locations/us-east1/queues/queue-name-2/"
            "tasks/task-name-3456",
        )

    def test_initialize_cloud_task_queue(self):
        # Arrange
        queue = queue_pb2.Queue(name=self.client_wrapper.format_queue_path("queue1"))

        # Act
        self.client_wrapper.initialize_cloud_task_queue(queue)

        # Assert
        self.mock_client.update_queue.assert_called_with(queue=queue)

    @staticmethod
    def _tasks_to_ids(tasks: List[tasks_v2.types.task_pb2.Task]) -> Set[str]:
        return {task_id for _, task_id in {os.path.split(task.name) for task in tasks}}

    def test_list_tasks_with_prefix(self):
        all_tasks = [
            tasks_v2.types.task_pb2.Task(
                name=self.client_wrapper.format_task_path(
                    self.QUEUE_NAME, "us-nd-task-1"
                )
            ),
            tasks_v2.types.task_pb2.Task(
                name=self.client_wrapper.format_task_path(
                    self.QUEUE_NAME, "us-nd-task-2"
                )
            ),
            tasks_v2.types.task_pb2.Task(
                name=self.client_wrapper.format_task_path(
                    self.QUEUE_NAME, "us-mo-task-1"
                )
            ),
        ]

        self.mock_client.list_tasks.return_value = all_tasks

        # Empty prefix
        tasks = self.client_wrapper.list_tasks_with_prefix(self.QUEUE_NAME, "")

        self.mock_client.list_tasks.assert_called_with(
            parent=self.client_wrapper.format_queue_path(self.QUEUE_NAME)
        )

        self.assertTrue(len(tasks), 3)

        # Prefix that returns all
        tasks = self.client_wrapper.list_tasks_with_prefix(self.QUEUE_NAME, "u")
        self.mock_client.list_tasks.assert_called_with(
            parent=self.client_wrapper.format_queue_path(self.QUEUE_NAME)
        )

        self.assertTrue(len(tasks), 3)

        # Prefix that returns only some
        tasks = self.client_wrapper.list_tasks_with_prefix(self.QUEUE_NAME, "us-nd")
        self.mock_client.list_tasks.assert_called_with(
            parent=self.client_wrapper.format_queue_path(self.QUEUE_NAME)
        )

        self.assertTrue(len(tasks), 2)

        self.assertEqual(self._tasks_to_ids(tasks), {"us-nd-task-1", "us-nd-task-2"})

        # Prefix that is exact match
        tasks = self.client_wrapper.list_tasks_with_prefix(
            self.QUEUE_NAME, "us-nd-task-2"
        )
        self.mock_client.list_tasks.assert_called_with(
            parent=self.client_wrapper.format_queue_path(self.QUEUE_NAME)
        )

        self.assertTrue(len(tasks), 1)

        self.assertEqual(self._tasks_to_ids(tasks), {"us-nd-task-2"})

        # Prefix with no matches
        tasks = self.client_wrapper.list_tasks_with_prefix(
            self.QUEUE_NAME, "no-match-prefix"
        )
        self.mock_client.list_tasks.assert_called_with(
            parent=self.client_wrapper.format_queue_path(self.QUEUE_NAME)
        )

        self.assertFalse(tasks)

    def test_create_task_no_schedule_delay(self):
        self.client_wrapper.create_task(
            task_id="us_mo-file_name_1-123456",
            queue_name=self.QUEUE_NAME,
            relative_uri="/process_job?region=us_mo",
            body={"arg1": "arg1-val", "arg2": 123},
        )

        self.mock_client.create_task.assert_called_with(
            parent="projects/my-project-id/locations/us-east1/queues/queue-name",
            task=tasks_v2.types.task_pb2.Task(
                name="projects/my-project-id/locations/us-east1/queues/"
                "queue-name/tasks/us_mo-file_name_1-123456",
                app_engine_http_request={
                    "http_method": "POST",
                    "relative_uri": "/process_job?region=us_mo",
                    "body": b'{"arg1": "arg1-val", "arg2": 123}',
                },
            ),
        )

    @freeze_time("2019-04-14")
    def test_create_task_schedule_delay(self):
        now_timestamp_sec = int(datetime.datetime.utcnow().timestamp())

        self.client_wrapper.create_task(
            task_id="us_mo-file_name_1-123456",
            queue_name=self.QUEUE_NAME,
            relative_uri="/process_job?region=us_mo",
            body={},
            schedule_delay_seconds=3,
        )

        self.mock_client.create_task.assert_called_with(
            parent="projects/my-project-id/locations/us-east1/queues/queue-name",
            task=tasks_v2.types.task_pb2.Task(
                name="projects/my-project-id/locations/us-east1/queues/"
                "queue-name/tasks/us_mo-file_name_1-123456",
                app_engine_http_request={
                    "http_method": "POST",
                    "relative_uri": "/process_job?region=us_mo",
                    "body": b"{}",
                },
                schedule_time=timestamp_pb2.Timestamp(seconds=(now_timestamp_sec + 3)),
            ),
        )

    def test_delete_task(self):
        self.client_wrapper.delete_task(tasks_v2.types.task_pb2.Task(name="task_name"))

        self.mock_client.delete_task.assert_called_with(name="task_name")

    def test_delete_task_not_found(self):
        self.mock_client.delete_task.side_effect = exceptions.NotFound(
            message="message"
        )

        self.client_wrapper.delete_task(tasks_v2.types.task_pb2.Task(name="task_name"))

        self.mock_client.delete_task.assert_called_with(name="task_name")

        # Note: does not crash
