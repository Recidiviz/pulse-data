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
from enum import Enum
from typing import Dict, Generator, List, Optional
from urllib.parse import urlencode

import attr
from google.cloud import tasks_v2

from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath
from recidiviz.common.constants.states import StateCode
from recidiviz.common.google_cloud.cloud_task_queue_manager import (
    CloudTaskQueueInfo,
    CloudTaskQueueManager,
)
from recidiviz.common.google_cloud.google_cloud_tasks_shared_queues import (
    DIRECT_INGEST_BQ_IMPORT_EXPORT_QUEUE_V2,
    DIRECT_INGEST_JAILS_PROCESS_JOB_QUEUE_V2,
    DIRECT_INGEST_SCHEDULER_QUEUE_V2,
)
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    GcsfsIngestArgs,
    GcsfsIngestViewExportArgs,
    GcsfsRawDataBQImportArgs,
)
from recidiviz.ingest.direct.direct_ingest_region_utils import (
    get_direct_ingest_states_with_sftp_queue,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.direct_ingest_types import CloudTaskArgs
from recidiviz.utils.regions import Region

SCHEDULER_TASK_ID_TAG = "scheduler"
HANDLE_NEW_FILES_TASK_ID_TAG = "handle_new_files"
HANDLE_SFTP_DOWNLOAD_TASK_ID_TAG = "handle_sftp_download"


def _build_task_id(
    region_code: str,
    ingest_instance: DirectIngestInstance,
    task_id_tag: Optional[str],
    prefix_only: bool = False,
) -> str:
    """Creates a task id for a task running for a particular region. Ids take
    the form:
    <region_code>(-<task_id_tag>)(-<uuid>).

    For example:
    _build_task_id('us_nd', DirectIngestInstance.PRIMARY, 'elite_offenders', prefix_only=False) ->
        'us_nd-primary-elite_offenders-d1315074-8cea-4848-afe1-af20af07e275

    _build_task_id('us_nd', DirectIngestInstance.SECONDARY, None, prefix_only=False) ->
        'us_nd-secondary-d1315074-8cea-4848-afe1-af20af07e275

    _build_task_id('us_nd', DirectIngestInstance.PRIMARY, 'elite_offenders', prefix_only=True) ->
        'us_nd-primary-elite_offenders

    """
    task_id_parts = [region_code.lower(), ingest_instance.value.lower()]
    if task_id_tag:
        task_id_parts.append(task_id_tag)
    if not prefix_only:
        task_id_parts.append(str(uuid.uuid4()))

    return "-".join(task_id_parts)


def build_scheduler_task_id(
    region: Region, ingest_instance: DirectIngestInstance, prefix_only: bool = False
) -> str:
    return _build_task_id(
        region.region_code,
        ingest_instance,
        task_id_tag=SCHEDULER_TASK_ID_TAG,
        prefix_only=prefix_only,
    )


def build_handle_new_files_task_id(
    region: Region, ingest_instance: DirectIngestInstance, prefix_only: bool = False
) -> str:
    return _build_task_id(
        region.region_code,
        ingest_instance,
        task_id_tag=HANDLE_NEW_FILES_TASK_ID_TAG,
        prefix_only=prefix_only,
    )


def build_raw_data_import_task_id(
    region: Region,
    data_import_args: GcsfsRawDataBQImportArgs,
) -> str:
    return _build_task_id(
        region.region_code,
        data_import_args.ingest_instance(),
        task_id_tag=data_import_args.task_id_tag(),
        prefix_only=False,
    )


def build_ingest_view_export_task_id(
    region: Region,
    ingest_view_export_args: GcsfsIngestViewExportArgs,
) -> str:
    return _build_task_id(
        region.region_code,
        ingest_view_export_args.ingest_instance(),
        task_id_tag=ingest_view_export_args.task_id_tag(),
        prefix_only=False,
    )


def build_process_job_task_id(region: Region, ingest_args: GcsfsIngestArgs) -> str:
    return _build_task_id(
        region.region_code,
        ingest_args.ingest_instance(),
        ingest_args.task_id_tag(),
        prefix_only=False,
    )


def build_sftp_download_task_id(region: Region) -> str:
    return _build_task_id(
        region.region_code,
        ingest_instance=DirectIngestInstance.PRIMARY,
        task_id_tag=HANDLE_SFTP_DOWNLOAD_TASK_ID_TAG,
        prefix_only=False,
    )


class DirectIngestQueueType(Enum):
    SFTP_QUEUE = "sftp-queue"
    SCHEDULER = "scheduler"
    PROCESS_JOB_QUEUE = "process-job-queue"
    RAW_DATA_IMPORT = "raw-data-import"
    INGEST_VIEW_EXPORT = "ingest-view-export"
    # TODO(#9713): Legacy queue - will be replaced by RAW_DATA_IMPORT /
    #  INGEST_VIEW_EXPORT. Delete once those queues are fully in use.
    BQ_IMPORT_EXPORT = "bq-import-export"

    def exists_for_instance(self, ingest_instance: DirectIngestInstance) -> bool:
        return ingest_instance is DirectIngestInstance.PRIMARY or self not in (
            DirectIngestQueueType.SFTP_QUEUE,
            DirectIngestQueueType.RAW_DATA_IMPORT,
            DirectIngestQueueType.BQ_IMPORT_EXPORT,
        )


def _build_direct_ingest_queue_name(
    region_code: str,
    queue_type: DirectIngestQueueType,
    ingest_instance: DirectIngestInstance,
) -> str:
    """Creates a cloud task queue name for a specific task for a particular region.
    Names take the form: direct-ingest-<region_code>-<queue_type><optional instance suffix>.

    For example:
        _build_direct_ingest_queue_name('us_id', SFTP_QUEUE, PRIMARY) ->
        'direct-ingest-state-us-id-sftp-queue'

         _build_direct_ingest_queue_name('us_nd', PROCESS_JOB_QUEUE, SECONDARY) ->
        'direct-ingest-state-us-nd-process-job-queue-secondary'
    """
    if not queue_type.exists_for_instance(ingest_instance):
        raise ValueError(
            f"Queue type [{queue_type}] does not exist for [{ingest_instance}] instance."
        )

    # TODO(#9713): Remove BQ_IMPORT_EXPORT check when we delete that queue and type.
    if (
        ingest_instance == DirectIngestInstance.PRIMARY
        or queue_type is DirectIngestQueueType.BQ_IMPORT_EXPORT
    ):
        instance_suffix = ""
    elif ingest_instance == DirectIngestInstance.SECONDARY:
        instance_suffix = "-secondary"
    else:
        raise ValueError(f"Unexpected ingest instance [{ingest_instance}]")
    return (
        f"direct-ingest-state-{region_code.lower().replace('_', '-')}"
        f"-{queue_type.value}{instance_suffix}"
    )


def _queue_name_for_queue_type(
    queue_type: DirectIngestQueueType,
    region_code: str,
    ingest_instance: DirectIngestInstance,
) -> str:
    system_level = SystemLevel.for_region_code(region_code, is_direct_ingest=True)
    if system_level is SystemLevel.STATE:
        return _build_direct_ingest_queue_name(region_code, queue_type, ingest_instance)

    if system_level is SystemLevel.COUNTY:
        if queue_type == DirectIngestQueueType.SFTP_QUEUE:
            raise ValueError("No SFTP queue yet configured for county direct ingest")
        if queue_type == DirectIngestQueueType.SCHEDULER:
            return DIRECT_INGEST_SCHEDULER_QUEUE_V2
        if queue_type == DirectIngestQueueType.PROCESS_JOB_QUEUE:
            return DIRECT_INGEST_JAILS_PROCESS_JOB_QUEUE_V2
        if queue_type == DirectIngestQueueType.BQ_IMPORT_EXPORT:
            return DIRECT_INGEST_BQ_IMPORT_EXPORT_QUEUE_V2
        raise ValueError(f"Unexpected queue_type: [{queue_type}]")

    raise ValueError(f"Unexpected system_level: [{system_level}]")


def get_direct_ingest_queues_for_state(state_code: StateCode) -> List[str]:
    queue_names = []
    for queue_type in DirectIngestQueueType:
        if queue_type == DirectIngestQueueType.SFTP_QUEUE:
            if state_code not in get_direct_ingest_states_with_sftp_queue():
                continue
        for ingest_instance in DirectIngestInstance:
            if not queue_type.exists_for_instance(ingest_instance):
                continue
            queue_names.append(
                _queue_name_for_queue_type(
                    queue_type, state_code.value, ingest_instance
                )
            )
    return queue_names


@attr.s
class DirectIngestCloudTaskQueueInfo(CloudTaskQueueInfo):
    def _task_names_for_instance(
        self, region_code: str, ingest_instance: DirectIngestInstance
    ) -> Generator[str, None, None]:
        instance_prefix = _build_task_id(
            region_code,
            ingest_instance,
            task_id_tag=None,
            prefix_only=True,
        )
        for task in self.task_names_for_task_id_prefix(instance_prefix):
            yield task

    def task_names_for_task_id_prefix(
        self, task_prefix: str
    ) -> Generator[str, None, None]:
        for task_name in self.task_names:
            _, task_id = os.path.split(task_name)
            if task_id.startswith(task_prefix):
                yield task_name

    def has_any_tasks_for_instance(
        self, region_code: str, ingest_instance: DirectIngestInstance
    ) -> bool:
        return any(self._task_names_for_instance(region_code, ingest_instance))


@attr.s
class SchedulerCloudTaskQueueInfo(DirectIngestCloudTaskQueueInfo):
    pass


@attr.s
class SftpCloudTaskQueueInfo(DirectIngestCloudTaskQueueInfo):
    pass


@attr.s
class ProcessIngestJobCloudTaskQueueInfo(DirectIngestCloudTaskQueueInfo):
    """Class containing information about tasks in a given region's ingest view job
    processing queue.
    """

    def is_task_already_queued(
        self, region_code: str, ingest_args: GcsfsIngestArgs
    ) -> bool:
        """Returns true if the ingest_args correspond to a task currently in
        the queue.
        """

        task_id_prefix = _build_task_id(
            region_code,
            ingest_args.ingest_instance(),
            ingest_args.task_id_tag(),
            prefix_only=True,
        )

        return any(self.task_names_for_task_id_prefix(task_id_prefix))


@attr.s
class RawDataImportCloudTaskQueueInfo(DirectIngestCloudTaskQueueInfo):
    """Class containing information about tasks in a given region's raw data import /
    ingest view export task queue.
    """

    @staticmethod
    def _is_raw_data_export_task(task_name: str) -> bool:
        return "raw_data_import" in task_name

    def is_raw_data_import_task_already_queued(
        self, task_args: GcsfsRawDataBQImportArgs
    ) -> bool:
        return any(
            self._is_raw_data_export_task(task_name)
            and task_args.task_id_tag() in task_name
            for task_name in self.task_names
        )

    def has_raw_data_import_jobs_queued(self) -> bool:
        return any(
            self._is_raw_data_export_task(task_name) for task_name in self.task_names
        )


@attr.s
class IngestViewExportCloudTaskQueueInfo(DirectIngestCloudTaskQueueInfo):
    """Class containing information about tasks in a given region's raw data import /
    ingest view export task queue.
    """

    @staticmethod
    def _is_ingest_view_export_job(task_name: str) -> bool:
        return "ingest_view_export" in task_name

    def is_ingest_view_export_task_already_queued(
        self,
        region_code: str,
        task_args: GcsfsIngestViewExportArgs,
    ) -> bool:
        return any(
            self._is_ingest_view_export_job(task_name)
            and task_args.task_id_tag() in task_name
            for task_name in self._task_names_for_instance(
                region_code,
                task_args.ingest_instance(),
            )
        )

    def has_ingest_view_export_jobs_queued(
        self, region_code: str, ingest_instance: DirectIngestInstance
    ) -> bool:
        return any(
            self._is_ingest_view_export_job(task_name)
            for task_name in self._task_names_for_instance(region_code, ingest_instance)
        )


# TODO(#9713): Delete this legacy queue info type when we delete this queue.
@attr.s
class BQImportExportCloudTaskQueueInfo(DirectIngestCloudTaskQueueInfo):
    """Class containing information about tasks in a given region's raw data import /
    ingest view export task queue.
    """

    @staticmethod
    def _is_raw_data_export_task(task_name: str) -> bool:
        return "raw_data_import" in task_name

    @staticmethod
    def _is_ingest_view_export_job(task_name: str) -> bool:
        return "ingest_view_export" in task_name

    def is_raw_data_import_task_already_queued(
        self, task_args: GcsfsRawDataBQImportArgs
    ) -> bool:
        return any(
            self._is_raw_data_export_task(task_name)
            and task_args.task_id_tag() in task_name
            for task_name in self.task_names
        )

    def is_ingest_view_export_task_already_queued(
        self,
        region_code: str,
        task_args: GcsfsIngestViewExportArgs,
    ) -> bool:
        return any(
            self._is_ingest_view_export_job(task_name)
            and task_args.task_id_tag() in task_name
            for task_name in self._task_names_for_instance(
                region_code,
                task_args.ingest_instance(),
            )
        )

    def has_raw_data_import_jobs_queued(self) -> bool:
        return any(
            self._is_raw_data_export_task(task_name) for task_name in self.task_names
        )

    def has_ingest_view_export_jobs_queued(
        self, region_code: str, ingest_instance: DirectIngestInstance
    ) -> bool:
        return any(
            self._is_ingest_view_export_job(task_name)
            for task_name in self._task_names_for_instance(region_code, ingest_instance)
        )


class DirectIngestCloudTaskManager:
    """Abstract interface for a class that interacts with Cloud Task queues."""

    @abc.abstractmethod
    def get_raw_data_import_queue_info(
        self, region: Region
    ) -> RawDataImportCloudTaskQueueInfo:
        """Returns information about the tasks in the raw data import queue for
        the given region."""

    @abc.abstractmethod
    def get_process_job_queue_info(
        self, region: Region, ingest_instance: DirectIngestInstance
    ) -> ProcessIngestJobCloudTaskQueueInfo:
        """Returns information about tasks in the job processing queue for the
        given region."""

    @abc.abstractmethod
    def get_scheduler_queue_info(
        self, region: Region, ingest_instance: DirectIngestInstance
    ) -> SchedulerCloudTaskQueueInfo:
        """Returns information about the tasks in the job scheduler queue for
        the given region."""

    @abc.abstractmethod
    def get_ingest_view_export_queue_info(
        self, region: Region, ingest_instance: DirectIngestInstance
    ) -> IngestViewExportCloudTaskQueueInfo:
        """Returns information about the tasks in the raw data import queue for
        the given region."""

    # TODO(#9713): Delete this function when we delete this queue.
    @abc.abstractmethod
    def get_bq_import_export_queue_info(
        self, region: Region
    ) -> BQImportExportCloudTaskQueueInfo:
        """Returns information about the tasks in the BQ import export queue for
        the given region."""

    @abc.abstractmethod
    def get_sftp_queue_info(self, region: Region) -> SftpCloudTaskQueueInfo:
        """Returns information about the tasks in the sftp queue for the given region."""

    @abc.abstractmethod
    def create_direct_ingest_process_job_task(
        self,
        region: Region,
        ingest_args: GcsfsIngestArgs,
    ) -> None:
        """Queues a direct ingest process job task. All direct ingest data
        processing should happen through this endpoint.
        Args:
            region: `Region` direct ingest region.
            ingest_args: `GcsfsIngestArgs` args for the current direct ingest task.
        """

    @abc.abstractmethod
    def create_direct_ingest_scheduler_queue_task(
        self,
        region: Region,
        ingest_bucket: GcsfsBucketPath,
        just_finished_job: bool,
    ) -> None:
        """Creates a scheduler task for direct ingest for a given region.
        Scheduler tasks should be short-running and queue process_job tasks if
        there is more work to do.

        Args:
            region: `Region` direct ingest region.
            ingest_bucket: `GcsfsBucketPath` of the ingest bucket (e.g.
                gs://recidiviz-staging-direct-ingest-state-us-xx) corresponding to the
                ingest instance that work should be scheduled for.
            just_finished_job: True if this schedule is coming as a result
                of just having finished a job.
        """

    @abc.abstractmethod
    def create_direct_ingest_handle_new_files_task(
        self,
        region: Region,
        ingest_bucket: GcsfsBucketPath,
        can_start_ingest: bool,
    ) -> None:
        """Creates a Cloud Task for for a given region that identifies and registers
        new files that have been added to the provided ingest bucket.

        Args:
            region: `Region` direct ingest region.
            ingest_bucket: `GcsfsBucketPath` of the ingest bucket (e.g.
                gs://recidiviz-staging-direct-ingest-state-us-xx) to look for new files.
            can_start_ingest: True if the controller can proceed with scheduling
                more jobs to process the data once the files have been properly named
                and registered.
        """

    @abc.abstractmethod
    def create_direct_ingest_raw_data_import_task(
        self,
        region: Region,
        data_import_args: GcsfsRawDataBQImportArgs,
    ) -> None:
        pass

    @abc.abstractmethod
    def create_direct_ingest_ingest_view_export_task(
        self,
        region: Region,
        ingest_view_export_args: GcsfsIngestViewExportArgs,
    ) -> None:
        pass

    @abc.abstractmethod
    def create_direct_ingest_sftp_download_task(self, region: Region) -> None:
        """Creates a sftp download task for direct ingest for a given region.

        Args:
            region: `Region` direct ingest region.
        """

    @abc.abstractmethod
    def delete_scheduler_queue_task(
        self, region: Region, ingest_instance: DirectIngestInstance, task_name: str
    ) -> None:
        """Deletes a single task from the region's scheduler queue with the
        fully-qualified |task_name| which follows this format:
           '/projects/{project}/locations/{location}/queues/{queue}/tasks/{task_id}'
        """

    @staticmethod
    def json_to_cloud_task_args(json_data: dict) -> Optional[CloudTaskArgs]:
        if "cloud_task_args" in json_data and "args_type" in json_data:
            args_type = json_data["args_type"]
            cloud_task_args_dict = json_data["cloud_task_args"]
            if args_type == GcsfsIngestArgs.__name__:
                return GcsfsIngestArgs.from_serializable(cloud_task_args_dict)
            if args_type == GcsfsRawDataBQImportArgs.__name__:
                return GcsfsRawDataBQImportArgs.from_serializable(cloud_task_args_dict)
            if args_type == GcsfsIngestViewExportArgs.__name__:
                return GcsfsIngestViewExportArgs.from_serializable(cloud_task_args_dict)
            logging.error("Unexpected args_type in json_data: %s", args_type)
        return None

    @staticmethod
    def _get_body_from_args(cloud_task_args: CloudTaskArgs) -> Dict:
        body = {
            "cloud_task_args": cloud_task_args.to_serializable(),
            "args_type": cloud_task_args.__class__.__name__,
        }
        return body


class DirectIngestCloudTaskManagerImpl(DirectIngestCloudTaskManager):
    """Real implementation of the DirectIngestCloudTaskManager that interacts
    with actual GCP Cloud Task queues."""

    def __init__(self) -> None:
        self.cloud_tasks_client = tasks_v2.CloudTasksClient()

    def _get_scheduler_queue_manager(
        self, region: Region, ingest_instance: DirectIngestInstance
    ) -> CloudTaskQueueManager[SchedulerCloudTaskQueueInfo]:
        queue_name = _queue_name_for_queue_type(
            DirectIngestQueueType.SCHEDULER, region.region_code, ingest_instance
        )

        return CloudTaskQueueManager(
            queue_info_cls=SchedulerCloudTaskQueueInfo,
            queue_name=queue_name,
            cloud_tasks_client=self.cloud_tasks_client,
        )

    def _get_raw_data_import_queue_manager(
        self, region: Region
    ) -> CloudTaskQueueManager[RawDataImportCloudTaskQueueInfo]:
        queue_name = _queue_name_for_queue_type(
            DirectIngestQueueType.RAW_DATA_IMPORT,
            region.region_code,
            DirectIngestInstance.PRIMARY,
        )

        return CloudTaskQueueManager(
            queue_info_cls=RawDataImportCloudTaskQueueInfo,
            queue_name=queue_name,
            cloud_tasks_client=self.cloud_tasks_client,
        )

    def _get_ingest_view_export_queue_manager(
        self, region: Region, ingest_instance: DirectIngestInstance
    ) -> CloudTaskQueueManager[IngestViewExportCloudTaskQueueInfo]:
        queue_name = _queue_name_for_queue_type(
            DirectIngestQueueType.INGEST_VIEW_EXPORT,
            region.region_code,
            ingest_instance,
        )

        return CloudTaskQueueManager(
            queue_info_cls=IngestViewExportCloudTaskQueueInfo,
            queue_name=queue_name,
            cloud_tasks_client=self.cloud_tasks_client,
        )

    def _get_process_job_queue_manager(
        self, region: Region, ingest_instance: DirectIngestInstance
    ) -> CloudTaskQueueManager[ProcessIngestJobCloudTaskQueueInfo]:
        queue_name = _queue_name_for_queue_type(
            DirectIngestQueueType.PROCESS_JOB_QUEUE, region.region_code, ingest_instance
        )

        return CloudTaskQueueManager(
            queue_info_cls=ProcessIngestJobCloudTaskQueueInfo,
            queue_name=queue_name,
            cloud_tasks_client=self.cloud_tasks_client,
        )

    def _get_sftp_queue_manager(
        self, region: Region
    ) -> CloudTaskQueueManager[SftpCloudTaskQueueInfo]:
        """Returns the appropriate SFTP queue for teh given region. This uses a standardized
        naming scheme based on the queue type."""
        return CloudTaskQueueManager(
            queue_info_cls=SftpCloudTaskQueueInfo,
            queue_name=_queue_name_for_queue_type(
                DirectIngestQueueType.SFTP_QUEUE,
                region.region_code,
                DirectIngestInstance.PRIMARY,
            ),
            cloud_tasks_client=self.cloud_tasks_client,
        )

    def get_process_job_queue_info(
        self, region: Region, ingest_instance: DirectIngestInstance
    ) -> ProcessIngestJobCloudTaskQueueInfo:
        return self._get_process_job_queue_manager(
            region, ingest_instance
        ).get_queue_info(task_id_prefix=region.region_code)

    def get_scheduler_queue_info(
        self, region: Region, ingest_instance: DirectIngestInstance
    ) -> SchedulerCloudTaskQueueInfo:
        return self._get_scheduler_queue_manager(
            region, ingest_instance
        ).get_queue_info(task_id_prefix=region.region_code)

    # TODO(#9713): Delete this function when we delete this queue.
    def get_bq_import_export_queue_info(
        self, region: Region
    ) -> BQImportExportCloudTaskQueueInfo:
        raise NotImplementedError(
            "TODO(#9713): Function no longer in use, soon to be deleted."
        )

    def get_raw_data_import_queue_info(
        self, region: Region
    ) -> RawDataImportCloudTaskQueueInfo:
        return self._get_raw_data_import_queue_manager(region).get_queue_info(
            task_id_prefix=region.region_code
        )

    def get_ingest_view_export_queue_info(
        self, region: Region, ingest_instance: DirectIngestInstance
    ) -> IngestViewExportCloudTaskQueueInfo:
        return self._get_ingest_view_export_queue_manager(
            region, ingest_instance
        ).get_queue_info(task_id_prefix=region.region_code)

    def get_sftp_queue_info(self, region: Region) -> SftpCloudTaskQueueInfo:
        return self._get_sftp_queue_manager(region).get_queue_info(
            task_id_prefix=region.region_code
        )

    def create_direct_ingest_process_job_task(
        self,
        region: Region,
        ingest_args: GcsfsIngestArgs,
    ) -> None:
        task_id = build_process_job_task_id(region, ingest_args)
        params = {
            "region": region.region_code.lower(),
            "file_path": ingest_args.file_path.abs_path(),
        }
        relative_uri = f"/direct/process_job?{urlencode(params)}"
        body = self._get_body_from_args(ingest_args)

        self._get_process_job_queue_manager(
            region, ingest_args.ingest_instance()
        ).create_task(
            task_id=task_id,
            relative_uri=relative_uri,
            body=body,
        )

    def create_direct_ingest_scheduler_queue_task(
        self,
        region: Region,
        ingest_bucket: GcsfsBucketPath,
        just_finished_job: bool,
    ) -> None:
        ingest_instance = DirectIngestInstance.for_ingest_bucket(ingest_bucket)
        task_id = build_scheduler_task_id(region, ingest_instance)

        params = {
            "region": region.region_code.lower(),
            "bucket": ingest_bucket.bucket_name,
            "just_finished_job": just_finished_job,
        }

        relative_uri = f"/direct/scheduler?{urlencode(params)}"

        self._get_scheduler_queue_manager(region, ingest_instance).create_task(
            task_id=task_id,
            relative_uri=relative_uri,
            body={},
        )

    def create_direct_ingest_handle_new_files_task(
        self,
        region: Region,
        ingest_bucket: GcsfsBucketPath,
        can_start_ingest: bool,
    ) -> None:
        ingest_instance = DirectIngestInstance.for_ingest_bucket(ingest_bucket)
        task_id = build_handle_new_files_task_id(region, ingest_instance)

        params = {
            "region": region.region_code.lower(),
            "bucket": ingest_bucket.bucket_name,
            "can_start_ingest": can_start_ingest,
        }
        relative_uri = f"/direct/handle_new_files?{urlencode(params)}"

        self._get_scheduler_queue_manager(region, ingest_instance).create_task(
            task_id=task_id,
            relative_uri=relative_uri,
            body={},
        )

    def create_direct_ingest_raw_data_import_task(
        self,
        region: Region,
        data_import_args: GcsfsRawDataBQImportArgs,
    ) -> None:
        task_id = build_raw_data_import_task_id(region, data_import_args)

        params = {
            "region": region.region_code.lower(),
            "file_path": data_import_args.raw_data_file_path.abs_path(),
        }
        relative_uri = f"/direct/raw_data_import?{urlencode(params)}"

        body = self._get_body_from_args(data_import_args)

        self._get_raw_data_import_queue_manager(region).create_task(
            task_id=task_id,
            relative_uri=relative_uri,
            body=body,
        )

    def create_direct_ingest_ingest_view_export_task(
        self,
        region: Region,
        ingest_view_export_args: GcsfsIngestViewExportArgs,
    ) -> None:
        task_id = build_ingest_view_export_task_id(region, ingest_view_export_args)
        params = {
            "region": region.region_code.lower(),
            "output_bucket": ingest_view_export_args.output_bucket_name,
        }
        relative_uri = f"/direct/ingest_view_export?{urlencode(params)}"

        body = self._get_body_from_args(ingest_view_export_args)

        self._get_ingest_view_export_queue_manager(
            region, ingest_view_export_args.ingest_instance()
        ).create_task(
            task_id=task_id,
            relative_uri=relative_uri,
            body=body,
        )

    def create_direct_ingest_sftp_download_task(self, region: Region) -> None:
        task_id = build_sftp_download_task_id(region)
        params = {
            "region": region.region_code.lower(),
        }
        relative_uri = f"/direct/upload_from_sftp?{urlencode(params)}"
        self._get_sftp_queue_manager(region).create_task(
            task_id=task_id, relative_uri=relative_uri, body={}
        )

    def delete_scheduler_queue_task(
        self, region: Region, ingest_instance: DirectIngestInstance, task_name: str
    ) -> None:
        self._get_scheduler_queue_manager(region, ingest_instance).delete_task(
            task_name=task_name
        )
