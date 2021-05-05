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
"""Wrapper with convenience functions on top of the
tasks_v2.CloudTasksClient.
"""

from datetime import datetime, timedelta
import json
import logging
from enum import Enum
from typing import List, Dict, Optional, Any

import pytz
from google.cloud import tasks_v2, exceptions
from google.cloud.tasks_v2.types import queue_pb2, task_pb2
from google.protobuf import timestamp_pb2

from recidiviz.common.common_utils import retry_grpc
from recidiviz.common.google_cloud.protobuf_builder import ProtobufBuilder
from recidiviz.utils import metadata


class HttpMethod(Enum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"


QUEUES_REGION = "us-east1"


class GoogleCloudTasksClientWrapper:
    """Wrapper with convenience functions on top of the
    tasks_v2.CloudTasksClient.
    """

    NUM_GRPC_RETRIES = 2

    def __init__(
        self,
        cloud_tasks_client: Optional[tasks_v2.CloudTasksClient] = None,
        project_id: Optional[str] = None,
    ):
        self.client = (
            cloud_tasks_client if cloud_tasks_client else tasks_v2.CloudTasksClient()
        )
        self.project_id = project_id if project_id else metadata.project_id()
        self.queues_region = QUEUES_REGION

    def format_queue_path(self, queue_name: str) -> str:
        """Formats a queue name into its full Cloud Tasks queue path.

        Args:
            queue_name: `str` queue name.
        Returns:
            A Cloud Tasks queue path string.
        """
        return self.client.queue_path(self.project_id, self.queues_region, queue_name)

    def initialize_cloud_task_queue(self, queue_config: queue_pb2.Queue) -> None:
        """
        Initializes a task queue with the given config. If a queue with a given
        name already exists, it is updated to have the given config. If it does
        not exist, it will be created by this function.
        """
        # Creates queue if it does not exist, or updates it to have the given config.
        retry_grpc(self.NUM_GRPC_RETRIES, self.client.update_queue, queue=queue_config)

    def format_task_path(self, queue_name: str, task_name: str) -> str:
        """Creates a task path out of the necessary parts.

        Task path is of the form:
            '/projects/{project}/locations/{location}'
            '/queues/{queue}/tasks/{task_name}'
        """
        return self.client.task_path(
            self.project_id, self.queues_region, queue_name, task_name
        )

    def list_tasks_with_prefix(
        self,
        queue_name: str,
        task_id_prefix: str,
    ) -> List[task_pb2.Task]:
        """List tasks for the given queue with the given task path prefix."""

        task_name_prefix = self.format_task_path(queue_name, task_id_prefix)
        return [
            task
            for task in retry_grpc(
                self.NUM_GRPC_RETRIES,
                self.client.list_tasks,
                parent=self.format_queue_path(queue_name),
            )
            if task.name.startswith(task_name_prefix)
        ]

    def create_task(
        self,
        *,
        queue_name: str,
        relative_uri: str,
        task_id: Optional[str] = None,
        body: Optional[Dict[str, Any]] = None,
        schedule_delay_seconds: int = 0,
        http_method: HttpMethod = HttpMethod.POST
    ) -> None:
        """Creates a task with the given details.

        Args:
            task_id: (optional) ID of the task to include in the task name
            Specifying task IDs enables task de-duplication for the queue. Subsequent requests to enqueue a task with the
            same ID as a recently completed task will raise `409 Conflict (entity already exists)
            If left unspecified, Cloud Tasks will automatically generate an ID for the task
            https://cloud.google.com/tasks/docs/reference/rest/v2beta3/projects.locations.queues.tasks/create#body.request_body.FIELDS.task
            queue_name: The queue on which to schedule the task
            schedule_delay_seconds: The number of seconds by which to delay the
                scheduling of the given task.
            relative_uri: The relative uri to hit.
            body: Dictionary of values that will be converted to JSON and
            included in the request.
            http_method: The method for this request (i.e. GET or POST)
        """

        schedule_timestamp = None
        if schedule_delay_seconds > 0:
            schedule_timestamp = timestamp_pb2.Timestamp()
            schedule_timestamp.FromDatetime(
                datetime.now(tz=pytz.UTC) + timedelta(seconds=schedule_delay_seconds)
            )

        task_builder = ProtobufBuilder(task_pb2.Task).update_args(
            app_engine_http_request={
                "relative_uri": relative_uri,
            },
        )

        if task_id is not None:
            task_name = self.format_task_path(queue_name, task_id)
            task_builder.update_args(
                name=task_name,
            )

        if schedule_timestamp:
            task_builder.update_args(
                schedule_time=schedule_timestamp,
            )

        if http_method is not None:
            task_builder.update_args(
                app_engine_http_request={
                    "http_method": http_method.value,
                },
            )

        if http_method in (HttpMethod.POST, HttpMethod.PUT, HttpMethod.PATCH):
            if body is None:
                body = {}
            task_builder.update_args(
                app_engine_http_request={"body": json.dumps(body).encode()},
            )

        task = task_builder.build()

        logging.info("Queueing task to queue [%s]: [%s]", queue_name, task.name)

        retry_grpc(
            self.NUM_GRPC_RETRIES,
            self.client.create_task,
            parent=self.format_queue_path(queue_name),
            task=task,
        )

    def delete_task(self, task: task_pb2.Task) -> None:
        try:
            retry_grpc(self.NUM_GRPC_RETRIES, self.client.delete_task, name=task.name)
        except exceptions.NotFound as e:
            logging.debug("Task not found: [%s]", e)
