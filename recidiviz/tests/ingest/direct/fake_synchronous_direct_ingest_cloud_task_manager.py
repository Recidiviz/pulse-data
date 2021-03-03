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

from typing import List, Tuple, Union

from recidiviz.ingest.direct.controllers.direct_ingest_types import IngestArgs
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import (
    GcsfsDirectIngestController,
)
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    GcsfsIngestViewExportArgs,
    GcsfsRawDataBQImportArgs,
)
from recidiviz.ingest.direct.direct_ingest_cloud_task_manager import (
    _build_task_id,
    ProcessIngestJobCloudTaskQueueInfo,
    BQImportExportCloudTaskQueueInfo,
    SchedulerCloudTaskQueueInfo,
    SftpCloudTaskQueueInfo,
)
from recidiviz.tests.ingest.direct.fake_direct_ingest_cloud_task_manager import (
    FakeDirectIngestCloudTaskManager,
)
from recidiviz.utils import monitoring
from recidiviz.utils.regions import Region


class FakeSynchronousDirectIngestCloudTaskManager(FakeDirectIngestCloudTaskManager):
    """Test implementation of the DirectIngestCloudTaskManager that runs tasks
    synchronously, when prompted."""

    def __init__(self) -> None:
        super().__init__()
        self.process_job_tasks: List[Tuple[str, IngestArgs]] = []
        self.num_finished_process_job_tasks = 0
        self.scheduler_tasks: List[Tuple[str, bool]] = []
        self.num_finished_scheduler_tasks = 0
        self.bq_import_export_tasks: List[
            Tuple[str, Union[GcsfsRawDataBQImportArgs, GcsfsIngestViewExportArgs]]
        ] = []
        self.num_finished_bq_import_export_tasks = 0
        self.sftp_tasks: List[str] = []

    def get_process_job_queue_info(
        self, region: Region
    ) -> ProcessIngestJobCloudTaskQueueInfo:

        return ProcessIngestJobCloudTaskQueueInfo(
            queue_name="process", task_names=[t[0] for t in self.process_job_tasks]
        )

    def get_scheduler_queue_info(self, region: Region) -> SchedulerCloudTaskQueueInfo:
        return SchedulerCloudTaskQueueInfo(
            queue_name="schedule", task_names=[t[0] for t in self.scheduler_tasks]
        )

    def get_bq_import_export_queue_info(
        self, region: Region
    ) -> BQImportExportCloudTaskQueueInfo:
        return BQImportExportCloudTaskQueueInfo(
            queue_name="bq_import_export",
            task_names=[t[0] for t in self.bq_import_export_tasks],
        )

    def get_sftp_download_queue_info(self, region: Region) -> SftpCloudTaskQueueInfo:
        return SftpCloudTaskQueueInfo(
            queue_name="sftp_download", task_names=[t[0] for t in self.sftp_tasks]
        )

    def create_direct_ingest_process_job_task(
        self, region: Region, ingest_args: IngestArgs
    ) -> None:
        """Queues *but does not run* a process job task."""
        if not self.controller:
            raise ValueError("Controller is null - did you call set_controller()?")

        task_id = _build_task_id(
            self.controller.region.region_code, ingest_args.task_id_tag()
        )
        self.process_job_tasks.append((f"projects/path/to/{task_id}", ingest_args))

    def create_direct_ingest_scheduler_queue_task(
        self, region: Region, just_finished_job: bool, delay_sec: int
    ) -> None:
        """Queues *but does not run* a scheduler task."""
        if not self.controller:
            raise ValueError("Controller is null - did you call set_controller()?")

        task_id = _build_task_id(self.controller.region.region_code, None)
        self.scheduler_tasks.append(
            (f"projects/path/to/{task_id}-schedule", just_finished_job)
        )

    def create_direct_ingest_handle_new_files_task(
        self, region: Region, can_start_ingest: bool
    ) -> None:
        if not self.controller:
            raise ValueError("Controller is null - did you call set_controller()?")
        task_id = _build_task_id(self.controller.region.region_code, None)
        self.scheduler_tasks.append(
            (f"projects/path/to/{task_id}-handle_new_files", can_start_ingest)
        )

    def create_direct_ingest_raw_data_import_task(
        self, region: Region, data_import_args: GcsfsRawDataBQImportArgs
    ) -> None:
        if not self.controller:
            raise ValueError("Controller is null - did you call set_controller()?")
        task_id = _build_task_id(self.controller.region.region_code, None)
        self.bq_import_export_tasks.append(
            (f"projects/path/to/{task_id}-raw_data_import", data_import_args)
        )

    def create_direct_ingest_ingest_view_export_task(
        self, region: Region, ingest_view_export_args: GcsfsIngestViewExportArgs
    ) -> None:
        if not self.controller:
            raise ValueError("Controller is null - did you call set_controller()?")
        task_id = _build_task_id(self.controller.region.region_code, None)
        self.bq_import_export_tasks.append(
            (f"projects/path/to/{task_id}-ingest_view_export", ingest_view_export_args)
        )

    def create_direct_ingest_sftp_download_task(self, region: Region) -> None:
        if not self.controller:
            raise ValueError("Controller is null - did you call set_controller()?")
        task_id = _build_task_id(self.controller.region.region_code, None)
        self.sftp_tasks.append(f"projects/path/to/{task_id}-handle_sftp_download")

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

        with monitoring.push_region_tag(self.controller.region.region_code):
            self.controller.run_ingest_job_and_kick_scheduler_on_completion(task[1])
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
        task_id = task[0]

        with monitoring.push_region_tag(self.controller.region.region_code):
            if task_id.endswith("schedule"):
                self.controller.schedule_next_ingest_job_or_wait_if_necessary(
                    just_finished_job=task[1]
                )
            elif task_id.endswith("handle_new_files"):
                if not isinstance(self.controller, GcsfsDirectIngestController):
                    raise ValueError(
                        f"Unexpected controller type {type(self.controller)}"
                    )
                self.controller.handle_new_files(can_start_ingest=task[1])
            else:
                raise ValueError(f"Unexpected task id [{task_id}]")
        self.num_finished_scheduler_tasks += 1

    def test_run_next_bq_import_export_task(self) -> None:
        """Synchronously executes the next queued BQ import/export task, but *does not
        remove it from the queue*."""
        if not self.bq_import_export_tasks:
            raise ValueError("Scheduler job tasks should not be empty.")

        if self.num_finished_bq_import_export_tasks:
            raise ValueError("Must first pop last finished task.")

        if not self.controller:
            raise ValueError("Controller is null - did you call set_controller()?")

        task = self.bq_import_export_tasks[0]
        task_id = task[0]
        args = task[1]

        with monitoring.push_region_tag(self.controller.region.region_code):
            if task_id.endswith("raw_data_import"):
                if not isinstance(self.controller, GcsfsDirectIngestController):
                    raise ValueError(
                        f"Unexpected controller type {type(self.controller)}"
                    )
                if not isinstance(args, GcsfsRawDataBQImportArgs):
                    raise ValueError(f"Unexpected args type {type(args)}")

                self.controller.do_raw_data_import(data_import_args=args)
            elif task_id.endswith("ingest_view_export"):
                if not isinstance(self.controller, GcsfsDirectIngestController):
                    raise ValueError(
                        f"Unexpected controller type {type(self.controller)}"
                    )
                if not isinstance(args, GcsfsIngestViewExportArgs):
                    raise ValueError(f"Unexpected args type {type(args)}")

                self.controller.do_ingest_view_export(ingest_view_export_args=args)
            else:
                raise ValueError(f"Unexpected task id [{task_id}]")
        self.num_finished_bq_import_export_tasks += 1

    def test_pop_finished_process_job_task(self) -> Tuple[str, IngestArgs]:
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

    def test_pop_finished_bq_import_export_task(self) -> None:
        """Removes most recently run import/export task from the queue."""

        if self.num_finished_bq_import_export_tasks == 0:
            raise ValueError("No finished tasks to pop.")

        self.bq_import_export_tasks.pop(0)
        self.num_finished_bq_import_export_tasks -= 1
