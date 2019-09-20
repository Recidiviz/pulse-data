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
from typing import List, Dict

from google.api_core import datetime_helpers
from google.api_core.exceptions import AlreadyExists
from google.cloud import tasks_v2
from google.cloud.tasks_v2.types import queue_pb2, task_pb2
from google.protobuf import timestamp_pb2

from recidiviz.common.common_utils import retry_grpc
from recidiviz.common.google_cloud.protobuf_builder import ProtobufBuilder
from recidiviz.utils import metadata


class GoogleCloudTasksClientWrapper:
    """Wrapper with convenience functions on top of the
    tasks_v2.CloudTasksClient.
    """
    NUM_GRPC_RETRIES = 2

    def __init__(self):
        self.client = tasks_v2.CloudTasksClient()
        self.project_id = metadata.project_id()
        self.instance_region = metadata.region()

    def format_queue_path(self, queue_name: str) -> str:
        """Formats a queue name into its full Cloud Tasks queue path.

        Args:
            queue_name: `str` queue name.
        Returns:
            A Cloud Tasks queue path string.
        """
        return self.client.queue_path(
            self.project_id,
            self.instance_region,
            queue_name)

    def initialize_cloud_task_queues(self,
                                     queue_configs: List[queue_pb2.Queue]):
        """
        Initializes a provided list of task queues. If a queue with a given name
        already exists, it is updated to have the given config. If it does not
        exist, it will be created by this function.
        """
        logging.info("Start creating/updating Cloud Task queues.")
        for queue in queue_configs:
            logging.info("Updating queue [%s].", queue.name)
            try:
                # Creates queue if it does not exist, or updates it to have the
                #  given config.
                retry_grpc(
                    self.NUM_GRPC_RETRIES,
                    self.client.update_queue,
                    queue
                )
            except AlreadyExists as e:
                # TODO(2428): This error shows up intermittently on launch -
                #  this could be some weird interaction that is happening
                #  because we have some queues defined in queue.yaml - needs
                #  investigation.
                logging.warning(e)

            # TODO(2428): This only is necessary while we're still ever
            #  deploying a queue.yaml. Deploying a queue.yaml will deactivate
            #  any queue not in the queue.yaml. This step is required to make
            #  sure the queue is reactivated.
            retry_grpc(
                self.NUM_GRPC_RETRIES,
                self.client.resume_queue,
                queue.name
            )

        logging.info("Finished creating/updating Cloud Task queues.")

    def format_task_path(self,
                         queue_name: str,
                         task_name: str) -> str:
        """Creates a task path out of the necessary parts.

        Task path is of the form:
            '/projects/{project}/locations/{location}'
            '/queues/{queue}/tasks/{task_name}'
        """
        return self.client.task_path(
            self.project_id,
            self.instance_region,
            queue_name,
            task_name)

    def list_tasks_with_prefix(
            self,
            queue_name: str,
            task_id_prefix: str,
    ) -> List[task_pb2.Task]:
        """List tasks for the given queue with the given task path prefix."""

        task_name_prefix = self.format_task_path(queue_name, task_id_prefix)
        return [task for task in retry_grpc(self.NUM_GRPC_RETRIES,
                                            self.client.list_tasks,
                                            self.format_queue_path(queue_name))
                if task.name.startswith(task_name_prefix)]

    def create_task(self,
                    task_id: str,
                    queue_name: str,
                    relative_uri: str,
                    body: Dict[str, str],
                    schedule_delay_seconds: int = 0):
        """Creates a task with the given details.

        Args:
            task_id: Id of the task to include in the task name
            queue_name: The queue on which to schedule the task
            schedule_delay_seconds: The number of seconds by which to delay the
                scheduling of the given task.
            relative_uri: The relative uri to hit.
            body: Dictionary of values that will be converted to JSON and
            included in the request.
        """
        task_name = self.format_task_path(queue_name, task_id)

        schedule_timestamp = None
        if schedule_delay_seconds > 0:
            schedule_time = datetime.now() + timedelta(
                seconds=schedule_delay_seconds)
            schedule_time_sec = datetime_helpers.to_milliseconds(
                schedule_time) // 1000
            schedule_timestamp = \
                timestamp_pb2.Timestamp(seconds=schedule_time_sec)

        task_builder = ProtobufBuilder(task_pb2.Task).update_args(
            name=task_name,
            app_engine_http_request={
                'relative_uri': relative_uri,
                'body': json.dumps(body).encode()
            },
        )

        if schedule_timestamp:
            task_builder.update_args(
                schedule_time=schedule_timestamp,
            )

        task = task_builder.build()

        logging.info("Queueing task to queue [%s]: [%s]",
                     queue_name, task.name)

        retry_grpc(
            self.NUM_GRPC_RETRIES,
            self.client.create_task,
            self.format_queue_path(queue_name),
            task
        )
