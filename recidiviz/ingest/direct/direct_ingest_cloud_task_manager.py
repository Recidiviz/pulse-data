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
import uuid
from typing import Optional, Dict

from google.api_core import datetime_helpers
from google.cloud import tasks
from google.protobuf import timestamp_pb2

from recidiviz.common.common_utils import retry_grpc
from recidiviz.common.queues import format_task_path, NUM_GRPC_RETRIES, \
    client, format_queue_path, list_tasks_with_prefix
from recidiviz.ingest.direct.controllers.direct_ingest_types import \
    IngestArgsType, IngestArgs
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import \
    GcsfsIngestArgs
from recidiviz.utils.regions import Region

DIRECT_INGEST_SCHEDULER_QUEUE = 'direct-ingest-scheduler'
DIRECT_INGEST_STATE_TASK_QUEUE = 'direct-ingest-state-task-queue'
DIRECT_INGEST_JAILS_TASK_QUEUE = 'direct-ingest-jpp-task-queue'


class DirectIngestCloudTaskManager:
    """Abstract interface for a class that interacts with Cloud Task queues."""
    @abc.abstractmethod
    def in_progress_process_job_name(self, region: Region) -> Optional[str]:
        """Returns the name of the first in-progress task scheduled in the job
        processing queue (i.e. via create_direct_ingest_process_job_task).
        """

    @abc.abstractmethod
    def create_direct_ingest_process_job_task(self,
                                              region: Region,
                                              ingest_args: IngestArgsType):
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
            delay_sec: int):
        """Creates a scheduler task for direct ingest for a given region.
        Scheduler tasks should be short-running and queue process_job tasks if
        there is more work to do.

        Args:
            region: `Region` direct ingest region.
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
    def _get_body_from_args(ingest_args: IngestArgsType) -> Dict:
        body = {
            'ingest_args': ingest_args.to_serializable(),
            'args_type': ingest_args.__class__.__name__
        }
        return body


class DirectIngestCloudTaskManagerImpl(DirectIngestCloudTaskManager):
    """Real implementation of the DirectIngestCloudTaskManager that interacts
    with actual GCP Cloud Task queues."""

    def in_progress_process_job_name(self, region: Region) -> Optional[str]:
        task_prefix = format_task_path(region.get_queue_name(),
                                       region.region_code)

        tasks_list = list_tasks_with_prefix(task_prefix,
                                            region.get_queue_name())

        if not tasks_list:
            return None

        # TODO(1628): Consider looking more closely at the task to figure out if
        #  it has a failure status, etc.
        return tasks_list[0].name

    def create_direct_ingest_process_job_task(self,
                                              region: Region,
                                              ingest_args: IngestArgsType):
        body = self._get_body_from_args(ingest_args)
        task_id = '{}-{}-{}'.format(
            region.region_code, str(datetime.date.today()), uuid.uuid4())
        task_name = format_task_path(region.get_queue_name(), task_id)
        task = tasks.types.Task(
            name=task_name,
            app_engine_http_request={
                'relative_uri':
                    f'/direct/process_job?region={region.region_code}',
                'body': json.dumps(body).encode()
            }
        )
        retry_grpc(
            NUM_GRPC_RETRIES,
            client().create_task,
            format_queue_path(region.get_queue_name()),
            task
        )

    def create_direct_ingest_scheduler_queue_task(
            self,
            region: Region,
            delay_sec: int,
    ):
        schedule_time = datetime.datetime.now() + \
                        datetime.timedelta(seconds=delay_sec)

        schedule_time_sec = datetime_helpers.to_milliseconds(
            schedule_time) // 1000
        schedule_timestamp = timestamp_pb2.Timestamp(seconds=schedule_time_sec)

        task_id = '{}-{}-{}'.format(
            region.region_code, str(datetime.date.today()), uuid.uuid4())
        task_name = format_task_path(DIRECT_INGEST_SCHEDULER_QUEUE, task_id)
        task = tasks.types.Task(
            name=task_name,
            schedule_time=schedule_timestamp,
            app_engine_http_request={
                'relative_uri':
                    f'/direct/scheduler?region={region.region_code}',
                'body': json.dumps({}).encode()
            }
        )
        retry_grpc(
            NUM_GRPC_RETRIES,
            client().create_task,
            format_queue_path(DIRECT_INGEST_SCHEDULER_QUEUE),
            task
        )
