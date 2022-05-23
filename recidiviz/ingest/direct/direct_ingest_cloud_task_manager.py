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
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_with_sftp_queue,
)
from recidiviz.ingest.direct.types.cloud_task_args import (
    BQIngestViewMaterializationArgs,
    CloudTaskArgs,
    ExtractAndMergeArgs,
    GcsfsIngestViewExportArgs,
    GcsfsRawDataBQImportArgs,
    IngestViewMaterializationArgs,
    LegacyExtractAndMergeArgs,
    NewExtractAndMergeArgs,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.direct_ingest_instance_factory import (
    DirectIngestInstanceFactory,
)
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


def build_ingest_view_materialization_task_id(
    region: Region,
    materialization_args: IngestViewMaterializationArgs,
) -> str:
    return _build_task_id(
        region.region_code,
        materialization_args.ingest_instance,
        task_id_tag=materialization_args.task_id_tag(),
        prefix_only=False,
    )


def build_extract_and_merge_task_id(
    region: Region, extract_and_merge_args: ExtractAndMergeArgs
) -> str:
    return _build_task_id(
        region.region_code,
        extract_and_merge_args.ingest_instance,
        extract_and_merge_args.task_id_tag(),
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
    # TODO(#11424): Delete PROCESS_JOB_QUEUE once BQ materialization is fully shipped.
    PROCESS_JOB_QUEUE = "process-job-queue"
    EXTRACT_AND_MERGE = "extract-and-merge"
    RAW_DATA_IMPORT = "raw-data-import"
    # TODO(#11424): Delete INGEST_VIEW_EXPORT once BQ materialization is fully shipped.
    INGEST_VIEW_EXPORT = "ingest-view-export"
    MATERIALIZE_INGEST_VIEW = "materialize-ingest-view"

    def exists_for_instance(self, ingest_instance: DirectIngestInstance) -> bool:
        return ingest_instance is DirectIngestInstance.PRIMARY or self not in (
            # Primary-instance-only queues
            DirectIngestQueueType.SFTP_QUEUE,
            DirectIngestQueueType.RAW_DATA_IMPORT,
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

         _build_direct_ingest_queue_name('us_nd', EXTRACT_AND_MERGE, SECONDARY) ->
        'direct-ingest-state-us-nd-extract-and-merge-secondary'
    """
    if not queue_type.exists_for_instance(ingest_instance):
        raise ValueError(
            f"Queue type [{queue_type}] does not exist for [{ingest_instance}] instance."
        )

    if ingest_instance == DirectIngestInstance.PRIMARY:
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
    if system_level != SystemLevel.STATE:
        raise ValueError(f"Unexpected system_level: [{system_level}]")

    return _build_direct_ingest_queue_name(region_code, queue_type, ingest_instance)


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
class ExtractAndMergeCloudTaskQueueInfo(DirectIngestCloudTaskQueueInfo):
    """Class containing information about tasks in a given region's extract and merge
    task processing queue.
    """

    def is_task_already_queued(
        self, region_code: str, ingest_args: ExtractAndMergeArgs
    ) -> bool:
        """Returns true if the ingest_args correspond to a task currently in
        the queue.
        """

        task_id_prefix = _build_task_id(
            region_code,
            ingest_args.ingest_instance,
            ingest_args.task_id_tag(),
            prefix_only=True,
        )

        return any(self.task_names_for_task_id_prefix(task_id_prefix))


@attr.s
class RawDataImportCloudTaskQueueInfo(DirectIngestCloudTaskQueueInfo):
    """Class containing information about tasks in a given region's raw data import task
    queue.
    """

    @staticmethod
    def _is_raw_data_import_task(task_name: str) -> bool:
        return "raw_data_import" in task_name

    def is_raw_data_import_task_already_queued(
        self, task_args: GcsfsRawDataBQImportArgs
    ) -> bool:
        return any(
            self._is_raw_data_import_task(task_name)
            and task_args.task_id_tag() in task_name
            for task_name in self.task_names
        )

    def has_raw_data_import_jobs_queued(self) -> bool:
        return any(
            self._is_raw_data_import_task(task_name) for task_name in self.task_names
        )


@attr.s
class IngestViewMaterializationCloudTaskQueueInfo(DirectIngestCloudTaskQueueInfo):
    """Class containing information about tasks in a given region's ingest view
    materialization task queue.
    """

    def is_ingest_view_materialization_task_already_queued(
        self,
        region_code: str,
        task_args: IngestViewMaterializationArgs,
    ) -> bool:
        return any(
            task_args.task_id_tag() in task_name
            for task_name in self._task_names_for_instance(
                region_code,
                task_args.ingest_instance,
            )
        )

    def has_ingest_view_materialization_jobs_queued(
        self, region_code: str, ingest_instance: DirectIngestInstance
    ) -> bool:
        return bool(list(self._task_names_for_instance(region_code, ingest_instance)))


class DirectIngestCloudTaskManager:
    """Abstract interface for a class that interacts with Cloud Task queues."""

    @abc.abstractmethod
    def get_raw_data_import_queue_info(
        self, region: Region
    ) -> RawDataImportCloudTaskQueueInfo:
        """Returns information about the tasks in the raw data import queue for
        the given region."""

    @abc.abstractmethod
    def get_extract_and_merge_queue_info(
        self,
        region: Region,
        ingest_instance: DirectIngestInstance,
        is_bq_materialization_enabled: bool,
    ) -> ExtractAndMergeCloudTaskQueueInfo:
        """Returns information about tasks in the job processing queue for the
        given region."""

    @abc.abstractmethod
    def get_scheduler_queue_info(
        self, region: Region, ingest_instance: DirectIngestInstance
    ) -> SchedulerCloudTaskQueueInfo:
        """Returns information about the tasks in the job scheduler queue for
        the given region."""

    @abc.abstractmethod
    def get_ingest_view_materialization_queue_info(
        self,
        region: Region,
        ingest_instance: DirectIngestInstance,
        is_bq_materialization_enabled: bool,
    ) -> IngestViewMaterializationCloudTaskQueueInfo:
        """Returns information about the tasks in the raw data import queue for
        the given region."""

    @abc.abstractmethod
    def get_sftp_queue_info(self, region: Region) -> SftpCloudTaskQueueInfo:
        """Returns information about the tasks in the sftp queue for the given region."""

    @abc.abstractmethod
    def create_direct_ingest_extract_and_merge_task(
        self,
        region: Region,
        task_args: NewExtractAndMergeArgs,
        is_bq_materialization_enabled: bool,
    ) -> None:
        """Queues a direct ingest process job task. All direct ingest data
        processing should happen through this endpoint.
        Args:
            region: `Region` direct ingest region.
            task_args: `ExtractAndMergeArgs` args for the current extract and merge task.
            is_bq_materialization_enabled: Whether BQ materialization is enabled for this region
        """

    @abc.abstractmethod
    def create_direct_ingest_scheduler_queue_task(
        self,
        region: Region,
        ingest_bucket: GcsfsBucketPath,
        just_finished_job: bool,
    ) -> None:
        """Creates a scheduler task for direct ingest for a given region.
        Scheduler tasks should be short-running and queue extract_and_merge tasks if
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
    def create_direct_ingest_view_materialization_task(
        self,
        region: Region,
        task_args: IngestViewMaterializationArgs,
        is_bq_materialization_enabled: bool,
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
            # TODO(#11424): Delete this block once BQ materialization is fully shipped.
            if args_type == LegacyExtractAndMergeArgs.__name__:
                return LegacyExtractAndMergeArgs.from_serializable(cloud_task_args_dict)
            if args_type == GcsfsRawDataBQImportArgs.__name__:
                return GcsfsRawDataBQImportArgs.from_serializable(cloud_task_args_dict)
            if args_type == BQIngestViewMaterializationArgs.__name__:
                return BQIngestViewMaterializationArgs.from_serializable(
                    cloud_task_args_dict
                )
            if args_type == NewExtractAndMergeArgs.__name__:
                return NewExtractAndMergeArgs.from_serializable(cloud_task_args_dict)
            # TODO(#11424): Delete this block once BQ materialization is fully shipped.
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
        self,
        region: Region,
        ingest_instance: DirectIngestInstance,
        is_bq_materialization_enabled: bool,
    ) -> CloudTaskQueueManager[IngestViewMaterializationCloudTaskQueueInfo]:
        # TODO(#11424): Always use MATERIALIZE_INGEST_VIEW once BQ materialization is
        #  enabled for all states.
        queue_type = (
            DirectIngestQueueType.MATERIALIZE_INGEST_VIEW
            if is_bq_materialization_enabled
            else DirectIngestQueueType.INGEST_VIEW_EXPORT
        )

        queue_name = _queue_name_for_queue_type(
            queue_type,
            region.region_code,
            ingest_instance,
        )

        return CloudTaskQueueManager(
            queue_info_cls=IngestViewMaterializationCloudTaskQueueInfo,
            queue_name=queue_name,
            cloud_tasks_client=self.cloud_tasks_client,
        )

    def _get_extract_and_merge_queue_manager(
        self,
        region: Region,
        ingest_instance: DirectIngestInstance,
        is_bq_materialization_enabled: bool,
    ) -> CloudTaskQueueManager[ExtractAndMergeCloudTaskQueueInfo]:
        # TODO(#11424): Always use EXTRACT_AND_MERGE once BQ materialization is
        #  enabled for all states.
        queue_type = (
            DirectIngestQueueType.EXTRACT_AND_MERGE
            if is_bq_materialization_enabled
            else DirectIngestQueueType.PROCESS_JOB_QUEUE
        )

        queue_name = _queue_name_for_queue_type(
            queue_type, region.region_code, ingest_instance
        )

        return CloudTaskQueueManager(
            queue_info_cls=ExtractAndMergeCloudTaskQueueInfo,
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

    def get_extract_and_merge_queue_info(
        self,
        region: Region,
        ingest_instance: DirectIngestInstance,
        is_bq_materialization_enabled: bool,
    ) -> ExtractAndMergeCloudTaskQueueInfo:
        return self._get_extract_and_merge_queue_manager(
            region,
            ingest_instance,
            is_bq_materialization_enabled=is_bq_materialization_enabled,
        ).get_queue_info(task_id_prefix=region.region_code)

    def get_scheduler_queue_info(
        self, region: Region, ingest_instance: DirectIngestInstance
    ) -> SchedulerCloudTaskQueueInfo:
        return self._get_scheduler_queue_manager(
            region, ingest_instance
        ).get_queue_info(task_id_prefix=region.region_code)

    def get_raw_data_import_queue_info(
        self, region: Region
    ) -> RawDataImportCloudTaskQueueInfo:
        return self._get_raw_data_import_queue_manager(region).get_queue_info(
            task_id_prefix=region.region_code
        )

    def get_ingest_view_materialization_queue_info(
        self,
        region: Region,
        ingest_instance: DirectIngestInstance,
        is_bq_materialization_enabled: bool,
    ) -> IngestViewMaterializationCloudTaskQueueInfo:
        return self._get_ingest_view_export_queue_manager(
            region,
            ingest_instance,
            is_bq_materialization_enabled=is_bq_materialization_enabled,
        ).get_queue_info(task_id_prefix=region.region_code)

    def get_sftp_queue_info(self, region: Region) -> SftpCloudTaskQueueInfo:
        return self._get_sftp_queue_manager(region).get_queue_info(
            task_id_prefix=region.region_code
        )

    def create_direct_ingest_extract_and_merge_task(
        self,
        region: Region,
        task_args: NewExtractAndMergeArgs,
        is_bq_materialization_enabled: bool,
    ) -> None:
        task_id = build_extract_and_merge_task_id(region, task_args)

        if isinstance(task_args, LegacyExtractAndMergeArgs):
            # TODO(#11424): Delete this block once all traffic has been directed to the
            #  new /direct/extract_and_merge endpoint.
            if is_bq_materialization_enabled:
                raise ValueError(
                    "Cannot pass LegacyExtractAndMergeArgs if "
                    "is_bq_materialization_enabled is True: [{task_args}]."
                )
            params = {
                "region": region.region_code.lower(),
                "file_path": task_args.file_path.abs_path(),
            }
            relative_uri = f"/direct/process_job?{urlencode(params)}"
        elif isinstance(task_args, NewExtractAndMergeArgs):
            if not is_bq_materialization_enabled:
                raise ValueError(
                    f"Cannot pass NewExtractAndMergeArgs if "
                    f"is_bq_materialization_enabled is False: [{task_args}]."
                )
            params = {
                "region": region.region_code.lower(),
                "ingest_instance": task_args.ingest_instance.value.lower(),
                "ingest_view_name": task_args.ingest_view_name,
            }
            relative_uri = f"/direct/extract_and_merge?{urlencode(params)}"
        else:
            raise ValueError(f"Unexpected args type: [{type(task_args)}]")

        body = self._get_body_from_args(task_args)

        self._get_extract_and_merge_queue_manager(
            region,
            task_args.ingest_instance,
            is_bq_materialization_enabled=is_bq_materialization_enabled,
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
        ingest_instance = DirectIngestInstanceFactory.for_ingest_bucket(ingest_bucket)
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
        ingest_instance = DirectIngestInstanceFactory.for_ingest_bucket(ingest_bucket)
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

    def create_direct_ingest_view_materialization_task(
        self,
        region: Region,
        task_args: IngestViewMaterializationArgs,
        is_bq_materialization_enabled: bool,
    ) -> None:
        task_id = build_ingest_view_materialization_task_id(region, task_args)

        if isinstance(task_args, GcsfsIngestViewExportArgs):
            # TODO(#11424): Delete this block when BQ materialization has fully shipped.
            if is_bq_materialization_enabled:
                raise ValueError(
                    "Cannot pass GcsfsIngestViewExportArgs if "
                    "is_bq_materialization_enabled is True: [{task_args}]."
                )
            params = {
                "region": region.region_code.lower(),
                "output_bucket": task_args.output_bucket_name,
            }
            relative_uri = f"/direct/ingest_view_export?{urlencode(params)}"
        elif isinstance(task_args, BQIngestViewMaterializationArgs):
            if not is_bq_materialization_enabled:
                raise ValueError(
                    f"Cannot pass NewExtractAndMergeArgs if "
                    f"is_bq_materialization_enabled is False: [{task_args}]."
                )
            params = {
                "region": region.region_code.lower(),
                "ingest_instance": task_args.ingest_instance.value.lower(),
                "ingest_view_name": task_args.ingest_view_name,
            }
            relative_uri = f"/direct/materialize_ingest_view?{urlencode(params)}"
        else:
            raise ValueError(f"Unexpected args type: [{type(task_args)}]")

        body = self._get_body_from_args(task_args)

        self._get_ingest_view_export_queue_manager(
            region,
            task_args.ingest_instance,
            is_bq_materialization_enabled=is_bq_materialization_enabled,
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
