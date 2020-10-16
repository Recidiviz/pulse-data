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
"""Class for interacting with the direct ingest cloud task queues."""
import abc
import logging
import os
import uuid
from typing import Optional, Dict, List, TypeVar, Type, Union

import attr

from recidiviz.common.google_cloud.google_cloud_tasks_shared_queues import \
    DIRECT_INGEST_SCHEDULER_QUEUE_V2, DIRECT_INGEST_BQ_IMPORT_EXPORT_QUEUE_V2
from recidiviz.common.google_cloud.google_cloud_tasks_client_wrapper import \
    GoogleCloudTasksClientWrapper
from recidiviz.ingest.direct.controllers.direct_ingest_types import CloudTaskArgs, IngestArgs
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import \
    GcsfsIngestArgs, GcsfsRawDataBQImportArgs, GcsfsIngestViewExportArgs
from recidiviz.utils.regions import Region


def _build_task_id(region_code: str,
                   task_id_tag: Optional[str],
                   prefix_only: bool = False) -> str:
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

    def size(self) -> int:
        """Number of tasks currently queued in the queue for the given region.
        If this is generated from the queue itself, it will return at least 1.
        """
        return len(self.task_names)


QueueInfoType = TypeVar('QueueInfoType', bound=CloudTaskQueueInfo)


@attr.s
class SchedulerCloudTaskQueueInfo(CloudTaskQueueInfo):
    pass


@attr.s
class ProcessIngestJobCloudTaskQueueInfo(CloudTaskQueueInfo):

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


@attr.s
class BQImportExportCloudTaskQueueInfo(CloudTaskQueueInfo):
    @staticmethod
    def _is_raw_data_export_task(task_name: str) -> bool:
        return 'raw_data_import' in task_name

    @staticmethod
    def _is_ingest_view_export_job(task_name: str) -> bool:
        return 'ingest_view_export' in task_name

    def has_task_already_scheduled(self,
                                   task_args: Union[GcsfsRawDataBQImportArgs, GcsfsIngestViewExportArgs]) -> bool:
        return any(task_args.task_id_tag() in task_name for task_name in self.task_names)

    def has_raw_data_import_jobs_queued(self) -> bool:
        return any(self._is_raw_data_export_task(task_name) for task_name in self.task_names)

    def has_ingest_view_export_jobs_queued(self) -> bool:
        return any(self._is_ingest_view_export_job(task_name) for task_name in self.task_names)


class DirectIngestCloudTaskManager:
    """Abstract interface for a class that interacts with Cloud Task queues."""

    @abc.abstractmethod
    def get_process_job_queue_info(self, region: Region) -> ProcessIngestJobCloudTaskQueueInfo:
        """Returns information about tasks in the job processing queue for the
         given region."""

    @abc.abstractmethod
    def get_scheduler_queue_info(self, region: Region) -> SchedulerCloudTaskQueueInfo:
        """Returns information about the tasks in the job scheduler queue for
        the given region."""

    @abc.abstractmethod
    def get_bq_import_export_queue_info(self, region: Region) -> BQImportExportCloudTaskQueueInfo:
        """Returns information about the tasks in the BQ import export queue for
        the given region."""

    @abc.abstractmethod
    def create_direct_ingest_process_job_task(self,
                                              region: Region,
                                              ingest_args: IngestArgs) -> None:
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
            delay_sec: int) -> None:
        """Creates a scheduler task for direct ingest for a given region.
        Scheduler tasks should be short-running and queue process_job tasks if
        there is more work to do.

        Args:
            region: `Region` direct ingest region.
            just_finished_job: True if this schedule is coming as a result
                of just having finished a job.
            delay_sec: `int` the number of seconds to wait before the next task.
        """

    @abc.abstractmethod
    def create_direct_ingest_handle_new_files_task(self,
                                                   region: Region,
                                                   can_start_ingest: bool) -> None:
        pass

    @abc.abstractmethod
    def create_direct_ingest_raw_data_import_task(self,
                                                  region: Region,
                                                  data_import_args: GcsfsRawDataBQImportArgs) -> None:
        pass

    @abc.abstractmethod
    def create_direct_ingest_ingest_view_export_task(self,
                                                     region: Region,
                                                     ingest_view_export_args: GcsfsIngestViewExportArgs) -> None:
        pass

    @staticmethod
    def json_to_cloud_task_args(json_data: dict) -> Optional[CloudTaskArgs]:
        if 'cloud_task_args' in json_data and 'args_type' in json_data:
            args_type = json_data['args_type']
            cloud_task_args_dict = json_data['cloud_task_args']
            if args_type == IngestArgs.__name__:
                return IngestArgs.from_serializable(cloud_task_args_dict)
            if args_type == GcsfsIngestArgs.__name__:
                return GcsfsIngestArgs.from_serializable(cloud_task_args_dict)
            if args_type == GcsfsRawDataBQImportArgs.__name__:
                return GcsfsRawDataBQImportArgs.from_serializable(cloud_task_args_dict)
            if args_type == GcsfsIngestViewExportArgs.__name__:
                return GcsfsIngestViewExportArgs.from_serializable(cloud_task_args_dict)
            logging.error('Unexpected args_type in json_data: %s', args_type)
        return None

    @staticmethod
    def _get_body_from_args(cloud_task_args: CloudTaskArgs) -> Dict:
        body = {
            'cloud_task_args': cloud_task_args.to_serializable(),
            'args_type': cloud_task_args.__class__.__name__
        }
        return body


class DirectIngestCloudTaskManagerImpl(DirectIngestCloudTaskManager):
    """Real implementation of the DirectIngestCloudTaskManager that interacts
    with actual GCP Cloud Task queues."""

    def __init__(self, project_id: Optional[str] = None):
        self.cloud_task_client = \
            GoogleCloudTasksClientWrapper(project_id=project_id)

    def _get_queue_info(self,
                        queue_info_cls: Type[QueueInfoType],
                        queue_name: str,
                        region_code: str) -> QueueInfoType:
        tasks_list = \
            self.cloud_task_client.list_tasks_with_prefix(
                queue_name=queue_name,
                task_id_prefix=region_code)
        task_names = [task.name for task in tasks_list] if tasks_list else []
        return queue_info_cls(queue_name=queue_name, task_names=task_names)

    def get_process_job_queue_info(self, region: Region) -> ProcessIngestJobCloudTaskQueueInfo:
        return self._get_queue_info(
            ProcessIngestJobCloudTaskQueueInfo,
            region.get_queue_name(),
            region.region_code)

    def get_scheduler_queue_info(self, region: Region) -> SchedulerCloudTaskQueueInfo:
        return self._get_queue_info(
            SchedulerCloudTaskQueueInfo,
            DIRECT_INGEST_SCHEDULER_QUEUE_V2,
            region.region_code)

    def get_bq_import_export_queue_info(self, region: Region) -> BQImportExportCloudTaskQueueInfo:
        return self._get_queue_info(
            BQImportExportCloudTaskQueueInfo,
            DIRECT_INGEST_BQ_IMPORT_EXPORT_QUEUE_V2,
            region.region_code)

    def create_direct_ingest_process_job_task(self,
                                              region: Region,
                                              ingest_args: IngestArgs) -> None:
        task_id = _build_task_id(region.region_code,
                                 ingest_args.task_id_tag(),
                                 prefix_only=False)
        relative_uri = f'/direct/process_job?region={region.region_code}'
        body = self._get_body_from_args(ingest_args)

        self.cloud_task_client.create_task(
            task_id=task_id,
            queue_name=region.get_queue_name(),
            relative_uri=relative_uri,
            body=body,
        )

    def create_direct_ingest_scheduler_queue_task(
            self,
            region: Region,
            just_finished_job: bool,
            delay_sec: int,
    ) -> None:
        task_id = _build_task_id(region.region_code,
                                 task_id_tag='scheduler',
                                 prefix_only=False)
        relative_uri = f'/direct/scheduler?region={region.region_code}&' \
            f'just_finished_job={just_finished_job}'

        self.cloud_task_client.create_task(
            task_id=task_id,
            queue_name=DIRECT_INGEST_SCHEDULER_QUEUE_V2,
            relative_uri=relative_uri,
            body={},
            schedule_delay_seconds=delay_sec
        )

    def create_direct_ingest_handle_new_files_task(self,
                                                   region: Region,
                                                   can_start_ingest: bool) -> None:
        task_id = _build_task_id(region.region_code,
                                 task_id_tag='handle_new_files',
                                 prefix_only=False)
        relative_uri = \
            f'/direct/handle_new_files?region={region.region_code}&' \
            f'can_start_ingest={can_start_ingest}'

        self.cloud_task_client.create_task(
            task_id=task_id,
            queue_name=DIRECT_INGEST_SCHEDULER_QUEUE_V2,
            relative_uri=relative_uri,
            body={},
        )

    def create_direct_ingest_raw_data_import_task(self,
                                                  region: Region,
                                                  data_import_args: GcsfsRawDataBQImportArgs) -> None:
        task_id = _build_task_id(region.region_code,
                                 task_id_tag=data_import_args.task_id_tag(),
                                 prefix_only=False)
        relative_uri = f'/direct/raw_data_import?region={region.region_code}'

        body = self._get_body_from_args(data_import_args)

        self.cloud_task_client.create_task(
            task_id=task_id,
            queue_name=DIRECT_INGEST_BQ_IMPORT_EXPORT_QUEUE_V2,
            relative_uri=relative_uri,
            body=body,
        )

    def create_direct_ingest_ingest_view_export_task(self,
                                                     region: Region,
                                                     ingest_view_export_args: GcsfsIngestViewExportArgs) -> None:
        task_id = _build_task_id(region.region_code,
                                 task_id_tag=ingest_view_export_args.task_id_tag(),
                                 prefix_only=False)
        relative_uri = f'/direct/ingest_view_export?region={region.region_code}'

        body = self._get_body_from_args(ingest_view_export_args)

        self.cloud_task_client.create_task(
            task_id=task_id,
            queue_name=DIRECT_INGEST_BQ_IMPORT_EXPORT_QUEUE_V2,
            relative_uri=relative_uri,
            body=body,
        )
