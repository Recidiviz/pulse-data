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
synchronously, when prompted."""
import os.path
from typing import List, Tuple

from recidiviz.ingest.direct.direct_ingest_cloud_task_manager import (
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
    FakeDirectIngestCloudTaskManager,
)
from recidiviz.utils import monitoring


class FakeSynchronousDirectIngestCloudTaskManager(FakeDirectIngestCloudTaskManager):
    """Test implementation of the DirectIngestCloudTaskManager that runs tasks
    synchronously, when prompted."""

    def __init__(self) -> None:
        super().__init__()
        self.extract_and_merge_tasks: List[Tuple[str, ExtractAndMergeArgs]] = []
        self.num_finished_extract_and_merge_tasks = 0
        self.scheduler_tasks: List[Tuple[str, DirectIngestInstance, bool]] = []
        self.num_finished_scheduler_tasks = 0

        self.raw_data_import_tasks: List[Tuple[str, GcsfsRawDataBQImportArgs]] = []
        self.num_finished_raw_data_import_tasks = 0

        self.ingest_view_materialization_tasks: List[
            Tuple[str, IngestViewMaterializationArgs]
        ] = []
        self.num_finished_ingest_view_materialization_tasks = 0

        self.sftp_tasks: List[str] = []

    def get_extract_and_merge_queue_info(
        self,
        region: DirectIngestRegion,
        ingest_instance: DirectIngestInstance,
    ) -> ExtractAndMergeCloudTaskQueueInfo:

        return ExtractAndMergeCloudTaskQueueInfo(
            queue_name="process",
            task_names=[t[0] for t in self.extract_and_merge_tasks],
        )

    def get_scheduler_queue_info(
        self, region: DirectIngestRegion, ingest_instance: DirectIngestInstance
    ) -> SchedulerCloudTaskQueueInfo:
        return SchedulerCloudTaskQueueInfo(
            queue_name="schedule", task_names=[t[0] for t in self.scheduler_tasks]
        )

    def get_raw_data_import_queue_info(
        self, region: DirectIngestRegion
    ) -> RawDataImportCloudTaskQueueInfo:
        return RawDataImportCloudTaskQueueInfo(
            queue_name="raw_data_import",
            task_names=[t[0] for t in self.raw_data_import_tasks],
        )

    def get_ingest_view_materialization_queue_info(
        self,
        region: DirectIngestRegion,
        ingest_instance: DirectIngestInstance,
    ) -> IngestViewMaterializationCloudTaskQueueInfo:
        return IngestViewMaterializationCloudTaskQueueInfo(
            queue_name="ingest_view_materialization",
            task_names=[t[0] for t in self.ingest_view_materialization_tasks],
        )

    def get_sftp_queue_info(self, region: DirectIngestRegion) -> SftpCloudTaskQueueInfo:
        return SftpCloudTaskQueueInfo(
            queue_name="sftp_download", task_names=[t[0] for t in self.sftp_tasks]
        )

    def create_direct_ingest_extract_and_merge_task(
        self,
        region: DirectIngestRegion,
        task_args: ExtractAndMergeArgs,
    ) -> None:
        """Queues *but does not run* a process job task."""
        if not self.controller:
            raise ValueError("Controller is null - did you call set_controller()?")

        task_id = build_extract_and_merge_task_id(region, task_args)
        self.extract_and_merge_tasks.append((f"projects/path/to/{task_id}", task_args))

    def create_direct_ingest_scheduler_queue_task(
        self,
        region: DirectIngestRegion,
        ingest_instance: DirectIngestInstance,
        just_finished_job: bool,
    ) -> None:
        """Queues *but does not run* a scheduler task."""
        if not self.controller:
            raise ValueError("Controller is null - did you call set_controller()?")

        task_id = build_scheduler_task_id(region, ingest_instance)
        self.scheduler_tasks.append(
            (f"projects/path/to/{task_id}", ingest_instance, just_finished_job)
        )

    def create_direct_ingest_handle_new_files_task(
        self,
        region: DirectIngestRegion,
        ingest_instance: DirectIngestInstance,
        can_start_ingest: bool,
    ) -> None:
        if not self.controller:
            raise ValueError("Controller is null - did you call set_controller()?")
        task_id = build_handle_new_files_task_id(region, ingest_instance)
        self.scheduler_tasks.append(
            (
                f"projects/path/to/{task_id}",
                ingest_instance,
                can_start_ingest,
            )
        )

    def create_direct_ingest_raw_data_import_task(
        self,
        region: DirectIngestRegion,
        data_import_args: GcsfsRawDataBQImportArgs,
    ) -> None:
        if not self.controller:
            raise ValueError("Controller is null - did you call set_controller()?")
        task_id = build_raw_data_import_task_id(region, data_import_args)
        self.raw_data_import_tasks.append(
            (f"projects/path/to/{task_id}", data_import_args)
        )

    def create_direct_ingest_view_materialization_task(
        self,
        region: DirectIngestRegion,
        task_args: IngestViewMaterializationArgs,
    ) -> None:
        if not self.controller:
            raise ValueError("Controller is null - did you call set_controller()?")
        task_id = build_ingest_view_materialization_task_id(region, task_args)
        self.ingest_view_materialization_tasks.append(
            (f"projects/path/to/{task_id}", task_args)
        )

    def create_direct_ingest_sftp_download_task(
        self, region: DirectIngestRegion
    ) -> None:
        if not self.controller:
            raise ValueError("Controller is null - did you call set_controller()?")
        task_id = build_sftp_download_task_id(region)
        self.sftp_tasks.append(task_id)

    def delete_scheduler_queue_task(
        self,
        region: DirectIngestRegion,
        ingest_instance: DirectIngestInstance,
        task_name: str,
    ) -> None:
        scheduler_tasks = []
        for task_info in self.scheduler_tasks:
            this_task_name = task_info[0]
            if this_task_name != task_name:
                scheduler_tasks.append(task_info)

        self.scheduler_tasks = scheduler_tasks

    def test_run_next_extract_and_merge_task(self) -> None:
        """Synchronously executes the next queued process job task, but *does
        not remove it from the queue*."""
        if not self.extract_and_merge_tasks:
            raise ValueError("Process job tasks should not be empty.")

        if self.num_finished_extract_and_merge_tasks:
            raise ValueError("Must first pop last finished task.")

        if not self.controller:
            raise ValueError("Controller is null - did you call set_controller()?")

        task = self.extract_and_merge_tasks[0]

        with monitoring.push_region_tag(
            self.controller.region.region_code, self.controller.ingest_instance.value
        ):
            self.controller.run_extract_and_merge_job_and_kick_scheduler_on_completion(
                task[1]
            )
        self.num_finished_extract_and_merge_tasks += 1

    def test_run_next_scheduler_task(self) -> None:
        """Synchronously executes the next queued scheduler task, but *does not
        remove it from the queue*."""

        if not self.scheduler_tasks:
            raise ValueError("Scheduler job tasks should not be empty.")

        if self.num_finished_scheduler_tasks:
            raise ValueError("Must first pop last finished task.")

        if not self.controller:
            raise ValueError("Controller is null - did you call set_controller()?")

        task = self.scheduler_tasks[0]
        task_name = task[0]
        _, task_id = os.path.split(task_name)

        with monitoring.push_region_tag(
            self.controller.region.region_code, self.controller.ingest_instance.value
        ):
            ingest_instance = task[1]
            if not self.controller.ingest_instance == ingest_instance:
                raise ValueError(
                    f"Task request [{task_name}] for instance [{ingest_instance}]"
                    f"that does not match registered controller instance"
                    f"[{self.controller.ingest_instance}]."
                )
            if task_id.startswith(
                build_scheduler_task_id(
                    self.controller.region,
                    self.controller.ingest_instance,
                    prefix_only=True,
                )
            ):
                self.controller.schedule_next_ingest_task(
                    current_task_id=task_id, just_finished_job=task[2]
                )
            elif task_id.startswith(
                build_handle_new_files_task_id(
                    self.controller.region,
                    self.controller.ingest_instance,
                    prefix_only=True,
                )
            ):
                self.controller.handle_new_files(
                    current_task_id=task_id, can_start_ingest=task[2]
                )
            else:
                raise ValueError(f"Unexpected task id [{task_name}]")
        self.num_finished_scheduler_tasks += 1

    def test_run_next_raw_data_import_task(self) -> None:
        """Synchronously executes the next queued raw data import task, but *does not
        remove it from the queue*."""
        if not self.raw_data_import_tasks:
            raise ValueError("Raw data import tasks should not be empty.")

        if self.num_finished_raw_data_import_tasks:
            raise ValueError("Must first pop last finished task.")

        if not self.controller:
            raise ValueError("Controller is null - did you call set_controller()?")

        _task_id, args = self.raw_data_import_tasks[0]

        with monitoring.push_region_tag(
            self.controller.region.region_code, self.controller.ingest_instance.value
        ):
            self.controller.do_raw_data_import(data_import_args=args)

        self.num_finished_raw_data_import_tasks += 1

    def test_run_next_ingest_view_materialization_task(self) -> None:
        """Synchronously executes the next queued ingest view materialization task, but
        *does not remove it from the queue*.
        """
        if not self.ingest_view_materialization_tasks:
            raise ValueError("Ingest view materialization tasks should not be empty.")

        if self.num_finished_ingest_view_materialization_tasks:
            raise ValueError("Must first pop last finished task.")

        if not self.controller:
            raise ValueError("Controller is null - did you call set_controller()?")

        _task_id, args = self.ingest_view_materialization_tasks[0]

        with monitoring.push_region_tag(
            self.controller.region.region_code, self.controller.ingest_instance.value
        ):
            self.controller.do_ingest_view_materialization(
                ingest_view_materialization_args=args
            )

        self.num_finished_ingest_view_materialization_tasks += 1

    def test_pop_finished_extract_and_merge_task(
        self,
    ) -> Tuple[str, ExtractAndMergeArgs]:
        """Removes most recently run process job task from the queue."""
        if self.num_finished_extract_and_merge_tasks == 0:
            raise ValueError("No finished tasks to pop.")

        task = self.extract_and_merge_tasks.pop(0)
        self.num_finished_extract_and_merge_tasks -= 1
        return task

    def test_pop_finished_scheduler_task(self) -> None:
        """Removes most recently run scheduler task from the queue."""
        if self.num_finished_scheduler_tasks == 0:
            raise ValueError("No finished tasks to pop.")

        self.scheduler_tasks.pop(0)
        self.num_finished_scheduler_tasks -= 1

    def test_pop_finished_raw_data_import_task(self) -> None:
        """Removes most recently run raw data import task from the queue."""
        if self.num_finished_raw_data_import_tasks == 0:
            raise ValueError("No finished tasks to pop.")

        self.raw_data_import_tasks.pop(0)
        self.num_finished_raw_data_import_tasks -= 1

    def test_pop_finished_ingest_view_materialization_task(self) -> None:
        """Removes most recently run ingest view materialization task from the queue."""
        if self.num_finished_ingest_view_materialization_tasks == 0:
            raise ValueError("No finished tasks to pop.")

        self.ingest_view_materialization_tasks.pop(0)
        self.num_finished_ingest_view_materialization_tasks -= 1
