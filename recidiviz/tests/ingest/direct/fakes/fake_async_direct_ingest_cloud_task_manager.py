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
"""Test implementation of the DirectIngestCloudTaskManager that runs tasks
asynchronously on background threads."""
from typing import Any, Callable

from recidiviz.ingest.direct.direct_ingest_cloud_task_queue_manager import (
    DirectIngestQueueType,
    ExtractAndMergeCloudTaskQueueInfo,
    IngestViewMaterializationCloudTaskQueueInfo,
    RawDataImportCloudTaskQueueInfo,
    SchedulerCloudTaskQueueInfo,
    SftpCloudTaskQueueInfo,
    build_extract_and_merge_task_id,
    build_handle_new_files_task_id,
    build_ingest_view_materialization_task_id,
    build_raw_data_import_task_id,
    build_scheduler_task_id,
    build_sftp_download_task_id,
)
from recidiviz.ingest.direct.direct_ingest_regions import DirectIngestRegion
from recidiviz.ingest.direct.types.cloud_task_args import (
    ExtractAndMergeArgs,
    GcsfsRawDataBQImportArgs,
    IngestViewMaterializationArgs,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tests.ingest.direct.fakes.fake_direct_ingest_cloud_task_manager import (
    FakeDirectIngestCloudTaskQueueManager,
)
from recidiviz.utils import monitoring
from recidiviz.utils.single_thread_task_queue import SingleThreadTaskQueue


def with_monitoring(
    region_code: str, ingest_instance: DirectIngestInstance, fn: Callable
) -> Callable:
    def wrapped_fn(*args: Any, **kwargs: Any) -> None:
        with monitoring.push_region_tag(region_code, ingest_instance.value):
            fn(*args, **kwargs)

    return wrapped_fn


class FakeAsyncDirectIngestCloudTaskManager(FakeDirectIngestCloudTaskQueueManager):
    """Test implementation of the DirectIngestCloudTaskManager that runs tasks
    asynchronously on background threads."""

    def __init__(self) -> None:
        super().__init__()
        self.queues = {
            DirectIngestInstance.PRIMARY: {
                DirectIngestQueueType.SCHEDULER: SingleThreadTaskQueue(
                    name=self.queue_name_for_type(
                        DirectIngestQueueType.SCHEDULER, DirectIngestInstance.PRIMARY
                    )
                ),
                DirectIngestQueueType.EXTRACT_AND_MERGE: SingleThreadTaskQueue(
                    name=self.queue_name_for_type(
                        DirectIngestQueueType.EXTRACT_AND_MERGE,
                        DirectIngestInstance.PRIMARY,
                    )
                ),
                DirectIngestQueueType.MATERIALIZE_INGEST_VIEW: SingleThreadTaskQueue(
                    name=self.queue_name_for_type(
                        DirectIngestQueueType.MATERIALIZE_INGEST_VIEW,
                        DirectIngestInstance.PRIMARY,
                    )
                ),
                DirectIngestQueueType.RAW_DATA_IMPORT: SingleThreadTaskQueue(
                    name=self.queue_name_for_type(
                        DirectIngestQueueType.RAW_DATA_IMPORT,
                        DirectIngestInstance.PRIMARY,
                    )
                ),
                DirectIngestQueueType.SFTP_QUEUE: SingleThreadTaskQueue(
                    name=self.queue_name_for_type(
                        DirectIngestQueueType.SFTP_QUEUE, DirectIngestInstance.PRIMARY
                    )
                ),
            },
            DirectIngestInstance.SECONDARY: {
                DirectIngestQueueType.SCHEDULER: SingleThreadTaskQueue(
                    name=self.queue_name_for_type(
                        DirectIngestQueueType.SCHEDULER, DirectIngestInstance.SECONDARY
                    )
                ),
                DirectIngestQueueType.EXTRACT_AND_MERGE: SingleThreadTaskQueue(
                    name=self.queue_name_for_type(
                        DirectIngestQueueType.EXTRACT_AND_MERGE,
                        DirectIngestInstance.SECONDARY,
                    )
                ),
                DirectIngestQueueType.MATERIALIZE_INGEST_VIEW: SingleThreadTaskQueue(
                    name=self.queue_name_for_type(
                        DirectIngestQueueType.MATERIALIZE_INGEST_VIEW,
                        DirectIngestInstance.SECONDARY,
                    )
                ),
                DirectIngestQueueType.RAW_DATA_IMPORT: SingleThreadTaskQueue(
                    name=self.queue_name_for_type(
                        DirectIngestQueueType.RAW_DATA_IMPORT,
                        DirectIngestInstance.SECONDARY,
                    )
                ),
                # Note: There is no SFTP queue in SECONDARY
            },
        }

    def _get_queue(
        self, ingest_instance: DirectIngestInstance, queue_type: DirectIngestQueueType
    ) -> SingleThreadTaskQueue:
        return self.queues[ingest_instance][queue_type]

    def create_direct_ingest_extract_and_merge_task(
        self,
        region: DirectIngestRegion,
        task_args: ExtractAndMergeArgs,
    ) -> None:
        controller = self.controllers.get(task_args.ingest_instance)
        if not controller:
            raise ValueError(
                f"Controller for instance={task_args.ingest_instance} is null - did you call "
                "set_controller()?"
            )

        task_id = build_extract_and_merge_task_id(region, task_args)
        self._get_queue(
            task_args.ingest_instance, DirectIngestQueueType.EXTRACT_AND_MERGE
        ).add_task(
            f"projects/path/to/{task_id}",
            with_monitoring(
                region.region_code,
                task_args.ingest_instance,
                controller.run_extract_and_merge_job_and_kick_scheduler_on_completion,
            ),
            task_args,
        )

    def create_direct_ingest_scheduler_queue_task(
        self,
        region: DirectIngestRegion,
        ingest_instance: DirectIngestInstance,
        just_finished_job: bool,
    ) -> None:
        controller = self.controllers.get(ingest_instance)
        if not controller:
            raise ValueError(
                f"Controller for instance={ingest_instance} is null - did you call set_controller()?"
            )
        if not controller.ingest_instance == ingest_instance:
            raise ValueError(
                f"Task request for ingest instance [{ingest_instance}] that does not match "
                f"registered controller instance [{controller.ingest_instance}]."
            )

        task_id = build_scheduler_task_id(region, ingest_instance)
        self._get_queue(ingest_instance, DirectIngestQueueType.SCHEDULER).add_task(
            f"projects/path/to/{task_id}",
            with_monitoring(
                region.region_code,
                ingest_instance,
                controller.schedule_next_ingest_task,
            ),
            current_task_id=task_id,
            just_finished_job=just_finished_job,
        )

    def create_direct_ingest_handle_new_files_task(
        self,
        region: DirectIngestRegion,
        ingest_instance: DirectIngestInstance,
        can_start_ingest: bool,
    ) -> None:
        controller = self.controllers.get(ingest_instance)
        if not controller:
            raise ValueError(
                f"Controller for instance={ingest_instance} is null - did you call set_controller()?"
            )
        if (
            controller.ingest_instance == DirectIngestInstance.SECONDARY
            and ingest_instance == DirectIngestInstance.PRIMARY
        ):
            raise ValueError(
                f"Task request for instance [{ingest_instance}] that does not match "
                f"registered controller instance [{controller.ingest_instance}]. Only PRIMARY instances can "
                "schedule tasks in SECONDARY instances."
            )

        task_id = build_handle_new_files_task_id(region, ingest_instance)

        self._get_queue(ingest_instance, DirectIngestQueueType.SCHEDULER).add_task(
            f"projects/path/to/{task_id}",
            with_monitoring(
                region.region_code,
                ingest_instance,
                controller.handle_new_files,
            ),
            current_task_id=task_id,
            can_start_ingest=can_start_ingest,
        )

    def create_direct_ingest_raw_data_import_task(
        self,
        region: DirectIngestRegion,
        raw_data_source_instance: DirectIngestInstance,
        data_import_args: GcsfsRawDataBQImportArgs,
    ) -> None:
        controller = self.controllers.get(raw_data_source_instance)
        if not controller:
            raise ValueError(
                f"Controller for instance={raw_data_source_instance} is null - did you call "
                "set_controller()?"
            )

        task_id = build_raw_data_import_task_id(region, data_import_args)
        self._get_queue(
            raw_data_source_instance, DirectIngestQueueType.RAW_DATA_IMPORT
        ).add_task(
            f"projects/path/to/{task_id}",
            with_monitoring(
                region.region_code,
                data_import_args.ingest_instance(),
                controller.do_raw_data_import,
            ),
            data_import_args,
        )

    def create_direct_ingest_view_materialization_task(
        self,
        region: DirectIngestRegion,
        task_args: IngestViewMaterializationArgs,
    ) -> None:
        controller = self.controllers.get(task_args.ingest_instance)
        if not controller:
            raise ValueError(
                f"Controller for instance={task_args.ingest_instance} is null - did you call "
                f"set_controller()?"
            )

        task_id = build_ingest_view_materialization_task_id(region, task_args)
        self._get_queue(
            task_args.ingest_instance, DirectIngestQueueType.MATERIALIZE_INGEST_VIEW
        ).add_task(
            f"projects/path/to/{task_id}",
            with_monitoring(
                region.region_code,
                task_args.ingest_instance,
                controller.do_ingest_view_materialization,
            ),
            task_args,
        )

    def create_direct_ingest_sftp_download_task(
        self, region: DirectIngestRegion
    ) -> None:
        task_id = build_sftp_download_task_id(region)
        # SFTP only uses in the PRIMARY queue
        self._get_queue(
            DirectIngestInstance.PRIMARY, DirectIngestQueueType.SFTP_QUEUE
        ).add_task(f"projects/path/to/{task_id}", lambda _: None)

    def delete_scheduler_queue_task(
        self,
        region: DirectIngestRegion,
        ingest_instance: DirectIngestInstance,
        task_name: str,
    ) -> None:
        self._get_queue(ingest_instance, DirectIngestQueueType.SCHEDULER).delete_task(
            task_name
        )

    def get_extract_and_merge_queue_info(
        self,
        region: DirectIngestRegion,
        ingest_instance: DirectIngestInstance,
    ) -> ExtractAndMergeCloudTaskQueueInfo:
        extract_and_merge_queue = self._get_queue(
            ingest_instance, DirectIngestQueueType.EXTRACT_AND_MERGE
        )
        with extract_and_merge_queue.all_tasks_mutex:
            task_names = extract_and_merge_queue.get_unfinished_task_names_unsafe()

        return ExtractAndMergeCloudTaskQueueInfo(
            queue_name=extract_and_merge_queue.name, task_names=task_names
        )

    def get_scheduler_queue_info(
        self, region: DirectIngestRegion, ingest_instance: DirectIngestInstance
    ) -> SchedulerCloudTaskQueueInfo:
        scheduler_queue = self._get_queue(
            ingest_instance, DirectIngestQueueType.SCHEDULER
        )
        with scheduler_queue.all_tasks_mutex:
            task_names = scheduler_queue.get_unfinished_task_names_unsafe()

        return SchedulerCloudTaskQueueInfo(
            queue_name=scheduler_queue.name, task_names=task_names
        )

    def get_raw_data_import_queue_info(
        self, region: DirectIngestRegion, ingest_instance: DirectIngestInstance
    ) -> RawDataImportCloudTaskQueueInfo:
        raw_data_import_queue = self._get_queue(
            ingest_instance, DirectIngestQueueType.RAW_DATA_IMPORT
        )
        with raw_data_import_queue.all_tasks_mutex:
            task_names = raw_data_import_queue.get_unfinished_task_names_unsafe()

        return RawDataImportCloudTaskQueueInfo(
            queue_name=raw_data_import_queue.name, task_names=task_names
        )

    def get_ingest_view_materialization_queue_info(
        self,
        region: DirectIngestRegion,
        ingest_instance: DirectIngestInstance,
    ) -> IngestViewMaterializationCloudTaskQueueInfo:
        ingest_view_materialization_queue = self._get_queue(
            ingest_instance, DirectIngestQueueType.MATERIALIZE_INGEST_VIEW
        )
        with ingest_view_materialization_queue.all_tasks_mutex:
            task_names = (
                ingest_view_materialization_queue.get_unfinished_task_names_unsafe()
            )

        return IngestViewMaterializationCloudTaskQueueInfo(
            queue_name=ingest_view_materialization_queue.name,
            task_names=task_names,
        )

    def get_sftp_queue_info(self, region: DirectIngestRegion) -> SftpCloudTaskQueueInfo:
        # SFTP queues are only in PRIMARY
        sftp_queue = self._get_queue(
            DirectIngestInstance.PRIMARY, DirectIngestQueueType.SFTP_QUEUE
        )
        with sftp_queue.all_tasks_mutex:
            has_unfinished_tasks = sftp_queue.get_unfinished_task_names_unsafe()

        task_names = (
            [f"{region.region_code}-sftp-download"] if has_unfinished_tasks else []
        )
        return SftpCloudTaskQueueInfo(queue_name=sftp_queue.name, task_names=task_names)

    def wait_for_all_tasks_to_run(
        self,
        ingest_instance: DirectIngestInstance,
        raw_data_source_instance: DirectIngestInstance,
    ) -> None:
        """Waits for all tasks to finish running."""
        raw_data_import_done = False
        ingest_view_materialization_done = False
        scheduler_done = False
        extract_and_merge_queue_done = False
        while not (
            ingest_view_materialization_done
            and scheduler_done
            and extract_and_merge_queue_done
            and raw_data_import_done
        ):
            raw_data_import_queue = self._get_queue(
                raw_data_source_instance, DirectIngestQueueType.RAW_DATA_IMPORT
            )
            ingest_view_materialization_queue = self._get_queue(
                ingest_instance, DirectIngestQueueType.MATERIALIZE_INGEST_VIEW
            )
            scheduler_queue = self._get_queue(
                ingest_instance, DirectIngestQueueType.SCHEDULER
            )
            extract_and_merge_queue = self._get_queue(
                ingest_instance, DirectIngestQueueType.EXTRACT_AND_MERGE
            )

            raw_data_import_queue.join()
            ingest_view_materialization_queue.join()
            scheduler_queue.join()
            extract_and_merge_queue.join()

            with ingest_view_materialization_queue.all_tasks_mutex:
                with scheduler_queue.all_tasks_mutex:
                    with extract_and_merge_queue.all_tasks_mutex:
                        raw_data_import_done = (
                            not raw_data_import_queue.get_unfinished_task_names_unsafe()
                        )
                        ingest_view_materialization_done = (
                            not ingest_view_materialization_queue.get_unfinished_task_names_unsafe()
                        )
                        scheduler_done = (
                            not scheduler_queue.get_unfinished_task_names_unsafe()
                        )
                        extract_and_merge_queue_done = (
                            not extract_and_merge_queue.get_unfinished_task_names_unsafe()
                        )
