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
from typing import Optional, Dict, Union
from urllib.parse import urlencode

import attr
from google.cloud import tasks_v2

from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath
from recidiviz.common.google_cloud.cloud_task_queue_manager import (
    CloudTaskQueueInfo,
    CloudTaskQueueManager,
)
from recidiviz.common.google_cloud.google_cloud_tasks_shared_queues import (
    DIRECT_INGEST_JAILS_PROCESS_JOB_QUEUE_V2,
    DIRECT_INGEST_SCHEDULER_QUEUE_V2,
    DIRECT_INGEST_BQ_IMPORT_EXPORT_QUEUE_V2,
    DIRECT_INGEST_STATE_PROCESS_JOB_QUEUE_V2,
)
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.controllers.direct_ingest_instance import (
    DirectIngestInstance,
)
from recidiviz.ingest.direct.controllers.direct_ingest_types import (
    CloudTaskArgs,
)
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    GcsfsIngestArgs,
    GcsfsRawDataBQImportArgs,
    GcsfsIngestViewExportArgs,
)
from recidiviz.persistence.database.sqlalchemy_database_key import (
    SQLAlchemyStateDatabaseVersion,
)
from recidiviz.utils.regions import Region


def _build_task_id(
    region_code: str, task_id_tag: Optional[str], prefix_only: bool = False
) -> str:
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

    return "-".join(task_id_parts)


class DirectIngestQueueType(Enum):
    SFTP_QUEUE = "sftp-queue"
    SCHEDULER = "scheduler"
    PROCESS_JOB_QUEUE = "process-job-queue"
    BQ_IMPORT_EXPORT = "bq-import-export"


def _build_direct_ingest_queue_name(
    region_code: str, queue_type: DirectIngestQueueType
) -> str:
    """Creates a cloud task queue name for a specific task for a particular region.
    Names take the form: direct-ingest-<region_code>-<queue_type>.

    For example:
        _build_direct_ingest_queue_name('us_id', SFTP_QUEUE) ->
        'direct-ingest-state-us-id-sftp-queue'
    """
    return f"direct-ingest-state-{region_code.lower().replace('_', '-')}-{queue_type.value}"


@attr.s
class SchedulerCloudTaskQueueInfo(CloudTaskQueueInfo):
    pass


@attr.s
class SftpCloudTaskQueueInfo(CloudTaskQueueInfo):
    pass


@attr.s
class ProcessIngestJobCloudTaskQueueInfo(CloudTaskQueueInfo):
    def is_task_queued(self, region: Region, ingest_args: GcsfsIngestArgs) -> bool:
        """Returns true if the ingest_args correspond to a task currently in
        the queue.
        """

        task_id_prefix = _build_task_id(
            region.region_code, ingest_args.task_id_tag(), prefix_only=True
        )

        for task_name in self.task_names:
            _, task_id = os.path.split(task_name)
            if task_id.startswith(task_id_prefix):
                return True
        return False


@attr.s
class BQImportExportCloudTaskQueueInfo(CloudTaskQueueInfo):
    @staticmethod
    def _is_raw_data_export_task(task_name: str) -> bool:
        return "raw_data_import" in task_name

    @staticmethod
    def _is_ingest_view_export_job(task_name: str) -> bool:
        return "ingest_view_export" in task_name

    def has_task_already_scheduled(
        self, task_args: Union[GcsfsRawDataBQImportArgs, GcsfsIngestViewExportArgs]
    ) -> bool:
        return any(
            task_args.task_id_tag() in task_name for task_name in self.task_names
        )

    def has_raw_data_import_jobs_queued(self) -> bool:
        return any(
            self._is_raw_data_export_task(task_name) for task_name in self.task_names
        )

    def has_ingest_view_export_jobs_queued(self) -> bool:
        return any(
            self._is_ingest_view_export_job(task_name) for task_name in self.task_names
        )


class DirectIngestCloudTaskManager:
    """Abstract interface for a class that interacts with Cloud Task queues."""

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
    def get_bq_import_export_queue_info(
        self, region: Region, ingest_instance: DirectIngestInstance
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
        ingest_instance: DirectIngestInstance,
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
        ingest_instance: DirectIngestInstance,
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
        ingest_instance: DirectIngestInstance,
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
        ingest_instance: DirectIngestInstance,
        data_import_args: GcsfsRawDataBQImportArgs,
    ) -> None:
        pass

    @abc.abstractmethod
    def create_direct_ingest_ingest_view_export_task(
        self,
        region: Region,
        ingest_instance: DirectIngestInstance,
        ingest_view_export_args: GcsfsIngestViewExportArgs,
    ) -> None:
        pass

    @abc.abstractmethod
    def create_direct_ingest_sftp_download_task(self, region: Region) -> None:
        """Creates a sftp download task for direct ingest for a given region.

        Args:
            region: `Region` direct ingest region.
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


# TODO(#6226): Remove this and the shared queues when removing LEGACY
def _is_legacy_instance(region: Region, ingest_instance: DirectIngestInstance) -> bool:
    return (
        ingest_instance.database_version(SystemLevel.for_region(region))
        is SQLAlchemyStateDatabaseVersion.LEGACY
    )


class DirectIngestCloudTaskManagerImpl(DirectIngestCloudTaskManager):
    """Real implementation of the DirectIngestCloudTaskManager that interacts
    with actual GCP Cloud Task queues."""

    def __init__(self) -> None:
        self.cloud_tasks_client = tasks_v2.CloudTasksClient()

    def _get_scheduler_queue_manager(
        self, region: Region, ingest_instance: DirectIngestInstance
    ) -> CloudTaskQueueManager[SchedulerCloudTaskQueueInfo]:
        if _is_legacy_instance(region, ingest_instance):
            queue_name = DIRECT_INGEST_SCHEDULER_QUEUE_V2
        else:
            queue_name = _build_direct_ingest_queue_name(
                region.region_code, DirectIngestQueueType.SCHEDULER
            )

        return CloudTaskQueueManager(
            queue_info_cls=SchedulerCloudTaskQueueInfo,
            queue_name=queue_name,
            cloud_tasks_client=self.cloud_tasks_client,
        )

    def _get_bq_import_export_queue_manager(
        self, region: Region, ingest_instance: DirectIngestInstance
    ) -> CloudTaskQueueManager[BQImportExportCloudTaskQueueInfo]:
        if _is_legacy_instance(region, ingest_instance):
            queue_name = DIRECT_INGEST_BQ_IMPORT_EXPORT_QUEUE_V2
        else:
            queue_name = _build_direct_ingest_queue_name(
                region.region_code, DirectIngestQueueType.BQ_IMPORT_EXPORT
            )

        return CloudTaskQueueManager(
            queue_info_cls=BQImportExportCloudTaskQueueInfo,
            queue_name=queue_name,
            cloud_tasks_client=self.cloud_tasks_client,
        )

    def _get_process_job_queue_manager(
        self, region: Region, ingest_instance: DirectIngestInstance
    ) -> CloudTaskQueueManager[ProcessIngestJobCloudTaskQueueInfo]:
        if _is_legacy_instance(region, ingest_instance):
            system_level = SystemLevel.for_region(region)
            if system_level is SystemLevel.COUNTY:
                queue_name = DIRECT_INGEST_JAILS_PROCESS_JOB_QUEUE_V2
            elif system_level is SystemLevel.STATE:
                queue_name = DIRECT_INGEST_STATE_PROCESS_JOB_QUEUE_V2
            else:
                raise ValueError(f"Unexpected system level: {system_level}")
        else:
            queue_name = _build_direct_ingest_queue_name(
                region.region_code, DirectIngestQueueType.PROCESS_JOB_QUEUE
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
            queue_name=_build_direct_ingest_queue_name(
                region.region_code, DirectIngestQueueType.SFTP_QUEUE
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

    def get_bq_import_export_queue_info(
        self, region: Region, ingest_instance: DirectIngestInstance
    ) -> BQImportExportCloudTaskQueueInfo:
        return self._get_bq_import_export_queue_manager(
            region, ingest_instance
        ).get_queue_info(task_id_prefix=region.region_code)

    def get_sftp_queue_info(self, region: Region) -> SftpCloudTaskQueueInfo:
        return self._get_sftp_queue_manager(region).get_queue_info(
            task_id_prefix=region.region_code
        )

    def create_direct_ingest_process_job_task(
        self,
        region: Region,
        ingest_instance: DirectIngestInstance,
        ingest_args: GcsfsIngestArgs,
    ) -> None:
        task_id = _build_task_id(
            region.region_code, ingest_args.task_id_tag(), prefix_only=False
        )
        params = {
            "region": region.region_code.lower(),
        }
        relative_uri = f"/direct/process_job?{urlencode(params)}"
        body = self._get_body_from_args(ingest_args)

        self._get_process_job_queue_manager(region, ingest_instance).create_task(
            task_id=task_id,
            relative_uri=relative_uri,
            body=body,
        )

    def create_direct_ingest_scheduler_queue_task(
        self,
        region: Region,
        ingest_instance: DirectIngestInstance,
        ingest_bucket: GcsfsBucketPath,
        just_finished_job: bool,
    ) -> None:
        task_id = _build_task_id(
            region.region_code, task_id_tag="scheduler", prefix_only=False
        )

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
        ingest_instance: DirectIngestInstance,
        ingest_bucket: GcsfsBucketPath,
        can_start_ingest: bool,
    ) -> None:
        task_id = _build_task_id(
            region.region_code, task_id_tag="handle_new_files", prefix_only=False
        )

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
        ingest_instance: DirectIngestInstance,
        data_import_args: GcsfsRawDataBQImportArgs,
    ) -> None:
        task_id = _build_task_id(
            region.region_code,
            task_id_tag=data_import_args.task_id_tag(),
            prefix_only=False,
        )

        params = {
            "region": region.region_code.lower(),
        }
        relative_uri = f"/direct/raw_data_import?{urlencode(params)}"

        body = self._get_body_from_args(data_import_args)

        self._get_bq_import_export_queue_manager(region, ingest_instance).create_task(
            task_id=task_id,
            relative_uri=relative_uri,
            body=body,
        )

    def create_direct_ingest_ingest_view_export_task(
        self,
        region: Region,
        ingest_instance: DirectIngestInstance,
        ingest_view_export_args: GcsfsIngestViewExportArgs,
    ) -> None:
        task_id = _build_task_id(
            region.region_code,
            task_id_tag=ingest_view_export_args.task_id_tag(),
            prefix_only=False,
        )
        params = {
            "region": region.region_code.lower(),
        }
        relative_uri = f"/direct/ingest_view_export?{urlencode(params)}"

        body = self._get_body_from_args(ingest_view_export_args)

        self._get_bq_import_export_queue_manager(region, ingest_instance).create_task(
            task_id=task_id,
            relative_uri=relative_uri,
            body=body,
        )

    def create_direct_ingest_sftp_download_task(self, region: Region) -> None:
        task_id = _build_task_id(
            region.region_code, task_id_tag="handle_sftp_download", prefix_only=False
        )
        params = {
            "region": region.region_code.lower(),
        }
        relative_uri = f"/direct/upload_from_sftp?{urlencode(params)}"
        self._get_sftp_queue_manager(region).create_task(
            task_id=task_id, relative_uri=relative_uri, body={}
        )
