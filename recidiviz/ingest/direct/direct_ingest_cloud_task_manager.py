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
"""Class for interacting with the cloud task queues."""
import abc
import datetime
import json
import logging
import os
import uuid
from typing import Optional, Dict, List

import attr
from google.api_core import datetime_helpers
from google.cloud import tasks
from google.protobuf import timestamp_pb2

from recidiviz.common.common_utils import retry_grpc
from recidiviz.common.queues import format_task_path, NUM_GRPC_RETRIES, \
    client, format_queue_path, list_tasks_with_prefix
from recidiviz.ingest.direct.controllers.direct_ingest_types import \
    IngestArgs
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import \
    GcsfsIngestArgs
from recidiviz.utils.regions import Region

DIRECT_INGEST_SCHEDULER_QUEUE = 'direct-ingest-scheduler'
DIRECT_INGEST_STATE_TASK_QUEUE = 'direct-ingest-state-task-queue'
DIRECT_INGEST_JAILS_TASK_QUEUE = 'direct-ingest-jpp-task-queue'


def _build_task_id(region_code: str,
                   task_id_tag: Optional[str],
                   prefix_only: bool = False):
    """Creates a task id for a task running for a particular region. Ids take
    the form:
    <region_code>(-<task_id_tag>)(-<uuid>).

    For example:
    _build_task_id('us_nd', 'elite_offenders', prefix_only=False) ->
        'us_nd-elite_offenders-d1315074-8cea-4848-afe1-af20af07e275

    _build_task_id('us_nd', None, prefix_only=False) ->
        'us_nd-d1315074-8cea-4848-afe1-af20af07e275

    _build_task_id('us_nd', 'elite_offenders', prefix_only=True) ->
        'us_nd-elite_offenders

    """
    task_id_parts = [region_code]
    if task_id_tag:
        task_id_parts.append(task_id_tag)
    if not prefix_only:
        task_id_parts.append(str(uuid.uuid4()))

    return '-'.join(task_id_parts)


@attr.s
class CloudTaskQueueInfo:
    """Holds info about a Cloud Task queue."""
    queue_name: str = attr.ib()

    # Task names for tasks in queue, in order.
    # pylint:disable=not-an-iterable
    task_names: List[str] = attr.ib(factory=list)

    def is_task_queued(self,
                       region: Region,
                       ingest_args: IngestArgs) -> bool:
        """Returns true if the ingest_args correspond to a task currently in
        the queue.
        """

        task_id_prefix = _build_task_id(
            region.region_code,
            ingest_args.task_id_tag(),
            prefix_only=True)

        for task_name in self.task_names:
            _, task_id = os.path.split(task_name)
            if task_id.startswith(task_id_prefix):
                return True
        return False

    def size(self):
        """Number of tasks currently queued in the queue for the given region.
        If this is generated from the queue itself, it will return at least 1.
        """
        return len(self.task_names)


class DirectIngestCloudTaskManager:
    """Abstract interface for a class that interacts with Cloud Task queues."""

    @abc.abstractmethod
    def get_process_job_queue_info(self, region: Region) -> CloudTaskQueueInfo:
        """Returns information about tasks in the job processing queue for the
         given region."""

    @abc.abstractmethod
    def get_scheduler_queue_info(self, region: Region) -> CloudTaskQueueInfo:
        """Returns information about the tasks in the job scheduler queue for
        the given region."""

    @abc.abstractmethod
    def create_direct_ingest_process_job_task(self,
                                              region: Region,
                                              ingest_args: IngestArgs):
        """Queues a direct ingest process job task. All direct ingest data
        processing should happen through this endpoint.
        Args:
            region: `Region` direct ingest region.
            ingest_args: `IngestArgs` args for the current direct ingest task.
        """

    @abc.abstractmethod
    def create_direct_ingest_scheduler_queue_task(
            self,
            region: Region,
            just_finished_job: bool,
            delay_sec: int):
        """Creates a scheduler task for direct ingest for a given region.
        Scheduler tasks should be short-running and queue process_job tasks if
        there is more work to do.

        Args:
            region: `Region` direct ingest region.
            just_finished_job: True if this schedule is coming as a result
                of just having finished a job.
            delay_sec: `int` the number of seconds to wait before the next task.
        """

    @staticmethod
    def json_to_ingest_args(json_data):
        if 'ingest_args' in json_data and 'args_type' in json_data:
            args_type = json_data['args_type']
            ingest_args = json_data['ingest_args']
            if args_type == IngestArgs.__name__:
                return IngestArgs.from_serializable(ingest_args)
            if args_type == GcsfsIngestArgs.__name__:
                return GcsfsIngestArgs.from_serializable(ingest_args)
            logging.error('Unexpected args_type in json_data: %s', args_type)
        return None

    @staticmethod
    def _get_body_from_args(ingest_args: IngestArgs) -> Dict:
        body = {
            'ingest_args': ingest_args.to_serializable(),
            'args_type': ingest_args.__class__.__name__
        }
        return body


class DirectIngestCloudTaskManagerImpl(DirectIngestCloudTaskManager):
    """Real implementation of the DirectIngestCloudTaskManager that interacts
    with actual GCP Cloud Task queues."""

    @staticmethod
    def _build_task_name_for_queue_and_region(
            queue_name: str,
            region_code: str,
            task_id_tag: Optional[str]) -> List[tasks.types.Task]:
        # pylint: disable=line-too-long
        """
        Creates a name for a task. Names take the form:
        projects/<project_id>/locations/<locale>/queues/<queue_name>/tasks/<task_id>

        Args:
            queue_name: (str) The name of the queue to dispatch to
            region_code: (str) Region code for this queue
            task_id_tag: (str) Optional string added to the task id to allow us
                to identify duplicate tasks.
        """
        task_id = _build_task_id(
            region_code, task_id_tag, prefix_only=False)

        return format_task_path(queue_name, task_id)

    @staticmethod
    def _tasks_for_queue_and_region(queue_name: str,
                                    region_code: str) -> List[tasks.types.Task]:
        task_prefix = format_task_path(queue_name, region_code)
        return list_tasks_with_prefix(task_prefix, queue_name)

    def _get_queue_info(self,
                        queue_name: str,
                        region_code: str) -> CloudTaskQueueInfo:
        tasks_list = self._tasks_for_queue_and_region(queue_name,
                                                      region_code)
        task_names = [task.name for task in tasks_list] if tasks_list else []
        return CloudTaskQueueInfo(queue_name=queue_name,
                                  task_names=task_names)

    @staticmethod
    def _queue_task(queue_name: str, task: tasks.types.Task):
        logging.info("Queueing task to queue [%s]: [%s]",
                     queue_name, task.name)
        retry_grpc(
            NUM_GRPC_RETRIES,
            client().create_task,
            format_queue_path(queue_name),
            task
        )

    def get_process_job_queue_info(self, region: Region) -> CloudTaskQueueInfo:
        return self._get_queue_info(region.get_queue_name(),
                                    region.region_code)

    def get_scheduler_queue_info(self, region: Region) -> CloudTaskQueueInfo:
        return self._get_queue_info(DIRECT_INGEST_SCHEDULER_QUEUE,
                                    region.region_code)

    def create_direct_ingest_process_job_task(self,
                                              region: Region,
                                              ingest_args: IngestArgs):
        body = self._get_body_from_args(ingest_args)

        task_name = self._build_task_name_for_queue_and_region(
            region.get_queue_name(),
            region.region_code,
            ingest_args.task_id_tag())

        task = tasks.types.Task(
            name=task_name,
            app_engine_http_request={
                'relative_uri':
                    f'/direct/process_job?region={region.region_code}',
                'body': json.dumps(body).encode()
            }
        )
        self._queue_task(region.get_queue_name(), task)

    def create_direct_ingest_scheduler_queue_task(
            self,
            region: Region,
            just_finished_job: bool,
            delay_sec: int,
    ):
        schedule_time = datetime.datetime.now() + \
                        datetime.timedelta(seconds=delay_sec)

        schedule_time_sec = datetime_helpers.to_milliseconds(
            schedule_time) // 1000
        schedule_timestamp = timestamp_pb2.Timestamp(seconds=schedule_time_sec)

        task_name = self._build_task_name_for_queue_and_region(
            DIRECT_INGEST_SCHEDULER_QUEUE, region.region_code, None)
        task = tasks.types.Task(
            name=task_name,
            schedule_time=schedule_timestamp,
            app_engine_http_request={
                'relative_uri':
                    f'/direct/scheduler?region={region.region_code}&'
                    f'just_finished_job={just_finished_job}',
                'body': json.dumps({}).encode()
            }
        )
        self._queue_task(DIRECT_INGEST_SCHEDULER_QUEUE, task)
