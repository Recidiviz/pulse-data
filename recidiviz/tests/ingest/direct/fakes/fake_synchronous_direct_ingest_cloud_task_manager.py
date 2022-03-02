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

from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath
from recidiviz.ingest.direct.direct_ingest_cloud_task_manager import (
    IngestViewExportCloudTaskQueueInfo,
    ProcessIngestJobCloudTaskQueueInfo,
    RawDataImportCloudTaskQueueInfo,
    SchedulerCloudTaskQueueInfo,
    SftpCloudTaskQueueInfo,
    build_handle_new_files_task_id,
    build_ingest_view_export_task_id,
    build_process_job_task_id,
    build_raw_data_import_task_id,
    build_scheduler_task_id,
    build_sftp_download_task_id,
)
from recidiviz.ingest.direct.types.cloud_task_args import (
    GcsfsIngestViewExportArgs,
    GcsfsRawDataBQImportArgs,
    LegacyExtractAndMergeArgs,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.direct_ingest_instance_factory import (
    DirectIngestInstanceFactory,
)
from recidiviz.tests.ingest.direct.fakes.fake_direct_ingest_cloud_task_manager import (
    FakeDirectIngestCloudTaskManager,
)
from recidiviz.utils import monitoring
from recidiviz.utils.regions import Region


class FakeSynchronousDirectIngestCloudTaskManager(FakeDirectIngestCloudTaskManager):
    """Test implementation of the DirectIngestCloudTaskManager that runs tasks
    synchronously, when prompted."""

    def __init__(self) -> None:
        super().__init__()
        self.process_job_tasks: List[Tuple[str, LegacyExtractAndMergeArgs]] = []
        self.num_finished_process_job_tasks = 0
        self.scheduler_tasks: List[Tuple[str, GcsfsBucketPath, bool]] = []
        self.num_finished_scheduler_tasks = 0

        self.raw_data_import_tasks: List[Tuple[str, GcsfsRawDataBQImportArgs]] = []
        self.num_finished_raw_data_import_tasks = 0

        self.ingest_view_export_tasks: List[Tuple[str, GcsfsIngestViewExportArgs]] = []
        self.num_finished_ingest_view_export_tasks = 0

        self.sftp_tasks: List[str] = []

    def get_process_job_queue_info(
        self, region: Region, ingest_instance: DirectIngestInstance
    ) -> ProcessIngestJobCloudTaskQueueInfo:

        return ProcessIngestJobCloudTaskQueueInfo(
            queue_name="process", task_names=[t[0] for t in self.process_job_tasks]
        )

    def get_scheduler_queue_info(
        self, region: Region, ingest_instance: DirectIngestInstance
    ) -> SchedulerCloudTaskQueueInfo:
        return SchedulerCloudTaskQueueInfo(
            queue_name="schedule", task_names=[t[0] for t in self.scheduler_tasks]
        )

    def get_raw_data_import_queue_info(
        self, region: Region
    ) -> RawDataImportCloudTaskQueueInfo:
        return RawDataImportCloudTaskQueueInfo(
            queue_name="raw_data_import",
            task_names=[t[0] for t in self.raw_data_import_tasks],
        )

    def get_ingest_view_export_queue_info(
        self, region: Region, ingest_instance: DirectIngestInstance
    ) -> IngestViewExportCloudTaskQueueInfo:
        return IngestViewExportCloudTaskQueueInfo(
            queue_name="ingest_view_export",
            task_names=[t[0] for t in self.ingest_view_export_tasks],
        )

    def get_sftp_queue_info(self, region: Region) -> SftpCloudTaskQueueInfo:
        return SftpCloudTaskQueueInfo(
            queue_name="sftp_download", task_names=[t[0] for t in self.sftp_tasks]
        )

    def create_direct_ingest_process_job_task(
        self,
        region: Region,
        ingest_args: LegacyExtractAndMergeArgs,
    ) -> None:
        """Queues *but does not run* a process job task."""
        if not self.controller:
            raise ValueError("Controller is null - did you call set_controller()?")

        task_id = build_process_job_task_id(region, ingest_args)
        self.process_job_tasks.append((f"projects/path/to/{task_id}", ingest_args))

    def create_direct_ingest_scheduler_queue_task(
        self,
        region: Region,
        ingest_bucket: GcsfsBucketPath,
        just_finished_job: bool,
    ) -> None:
        """Queues *but does not run* a scheduler task."""
        if not self.controller:
            raise ValueError("Controller is null - did you call set_controller()?")

        task_id = build_scheduler_task_id(
            region, DirectIngestInstanceFactory.for_ingest_bucket(ingest_bucket)
        )
        self.scheduler_tasks.append(
            (f"projects/path/to/{task_id}", ingest_bucket, just_finished_job)
        )

    def create_direct_ingest_handle_new_files_task(
        self,
        region: Region,
        ingest_bucket: GcsfsBucketPath,
        can_start_ingest: bool,
    ) -> None:
        if not self.controller:
            raise ValueError("Controller is null - did you call set_controller()?")
        task_id = build_handle_new_files_task_id(
            region, DirectIngestInstanceFactory.for_ingest_bucket(ingest_bucket)
        )
        self.scheduler_tasks.append(
            (
                f"projects/path/to/{task_id}",
                ingest_bucket,
                can_start_ingest,
            )
        )

    def create_direct_ingest_raw_data_import_task(
        self,
        region: Region,
        data_import_args: GcsfsRawDataBQImportArgs,
    ) -> None:
        if not self.controller:
            raise ValueError("Controller is null - did you call set_controller()?")
        task_id = build_raw_data_import_task_id(region, data_import_args)
        self.raw_data_import_tasks.append(
            (f"projects/path/to/{task_id}", data_import_args)
        )

    def create_direct_ingest_ingest_view_export_task(
        self,
        region: Region,
        ingest_view_export_args: GcsfsIngestViewExportArgs,
    ) -> None:
        if not self.controller:
            raise ValueError("Controller is null - did you call set_controller()?")
        task_id = build_ingest_view_export_task_id(region, ingest_view_export_args)
        self.ingest_view_export_tasks.append(
            (f"projects/path/to/{task_id}", ingest_view_export_args)
        )

    def create_direct_ingest_sftp_download_task(self, region: Region) -> None:
        if not self.controller:
            raise ValueError("Controller is null - did you call set_controller()?")
        task_id = build_sftp_download_task_id(region)
        self.sftp_tasks.append(task_id)

    def delete_scheduler_queue_task(
        self, region: Region, ingest_instance: DirectIngestInstance, task_name: str
    ) -> None:
        scheduler_tasks = []
        for task_info in self.scheduler_tasks:
            this_task_name = task_info[0]
            if this_task_name != task_name:
                scheduler_tasks.append(task_info)

        self.scheduler_tasks = scheduler_tasks

    def test_run_next_process_job_task(self) -> None:
        """Synchronously executes the next queued process job task, but *does
        not remove it from the queue*."""
        if not self.process_job_tasks:
            raise ValueError("Process job tasks should not be empty.")

        if self.num_finished_process_job_tasks:
            raise ValueError("Must first pop last finished task.")

        if not self.controller:
            raise ValueError("Controller is null - did you call set_controller()?")

        task = self.process_job_tasks[0]

        with monitoring.push_region_tag(
            self.controller.region.region_code, self.controller.ingest_instance.value
        ):
            self.controller.run_extract_and_merge_job_and_kick_scheduler_on_completion(
                task[1]
            )
        self.num_finished_process_job_tasks += 1

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
            ingest_bucket_path = task[1]
            if not self.controller.ingest_bucket_path == ingest_bucket_path:
                raise ValueError(
                    f"Task request [{task_name}] for ingest bucket [{ingest_bucket_path}]"
                    f"that does not match registered controller ingest bucket"
                    f"[{self.controller.ingest_bucket_path}]."
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
            raise ValueError("BQ import/export job tasks should not be empty.")

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

    def test_run_next_ingest_view_export_task(self) -> None:
        """Synchronously executes the next queued ingest view export task, but *does not
        remove it from the queue*."""
        if not self.ingest_view_export_tasks:
            raise ValueError("BQ import/export job tasks should not be empty.")

        if self.num_finished_ingest_view_export_tasks:
            raise ValueError("Must first pop last finished task.")

        if not self.controller:
            raise ValueError("Controller is null - did you call set_controller()?")

        _task_id, args = self.ingest_view_export_tasks[0]

        with monitoring.push_region_tag(
            self.controller.region.region_code, self.controller.ingest_instance.value
        ):
            self.controller.do_ingest_view_export(ingest_view_export_args=args)

        self.num_finished_ingest_view_export_tasks += 1

    def test_pop_finished_process_job_task(
        self,
    ) -> Tuple[str, LegacyExtractAndMergeArgs]:
        """Removes most recently run process job task from the queue."""
        if self.num_finished_process_job_tasks == 0:
            raise ValueError("No finished tasks to pop.")

        task = self.process_job_tasks.pop(0)
        self.num_finished_process_job_tasks -= 1
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

    def test_pop_finished_ingest_view_export_task(self) -> None:
        """Removes most recently run ingest view export task from the queue."""
        if self.num_finished_ingest_view_export_tasks == 0:
            raise ValueError("No finished tasks to pop.")

        self.ingest_view_export_tasks.pop(0)
        self.num_finished_ingest_view_export_tasks -= 1
