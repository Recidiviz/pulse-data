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
from queue import Queue
from threading import Thread
from typing import Callable, List

from recidiviz.ingest.direct.controllers.direct_ingest_types import \
    IngestArgsType
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import \
    GcsfsDirectIngestController
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import GcsfsRawDataBQImportArgs, \
    GcsfsIngestViewExportArgs
from recidiviz.ingest.direct.direct_ingest_cloud_task_manager import \
    ProcessIngestJobCloudTaskQueueInfo, BQImportExportCloudTaskQueueInfo, \
    SchedulerCloudTaskQueueInfo
from recidiviz.tests.ingest.direct.fake_direct_ingest_cloud_task_manager \
    import FakeDirectIngestCloudTaskManager
from recidiviz.utils import monitoring
from recidiviz.utils.regions import Region


class SingleThreadTaskQueue(Queue):
    """Simple class for running tasks on a single background thread."""

    def __init__(self, name: str):
        super().__init__()
        self.name = name

        t = Thread(target=self.worker)
        t.daemon = True
        t.start()

    def add_task(self, task_name: str, task: Callable, *args, **kwargs):
        args = args or ()
        kwargs = kwargs or {}
        self.put((task_name, task, args, kwargs))

    def worker(self):
        while True:
            _task_name, task, args, kwargs = self.get()
            task(*args, **kwargs)
            self.task_done()

    def get_queued_task_names_unsafe(self) -> List[str]:
        """Returns the names of all tasks in this queue.

        NOTE: You must be holding the mutex for this queue before calling this
        to avoid races. For example:
            queue = SingleThreadTaskQueue(name='my_queue')
            with queue.mutex:
                task_names = queue.get_queued_task_names_unsafe()

            if task_names:
                ...
        """
        _queue = self.queue.copy()
        task_names = []
        while _queue:
            task_name, _task, _args, _kwargs = _queue.pop()
            task_names.append(task_name)

        return task_names


def with_monitoring(region_code: str, fn: Callable) -> Callable:
    def wrapped_fn(*args, **kwargs):
        with monitoring.push_region_tag(region_code):
            fn(*args, **kwargs)
    return wrapped_fn


class FakeAsyncDirectIngestCloudTaskManager(FakeDirectIngestCloudTaskManager):
    """Test implementation of the DirectIngestCloudTaskManager that runs tasks
    asynchronously on background threads."""

    def __init__(self):
        super().__init__()
        self.scheduler_queue = SingleThreadTaskQueue(name='scheduler')
        self.process_job_queue = SingleThreadTaskQueue(name='process_job')
        self.bq_import_export_queue = SingleThreadTaskQueue(name='bq_import_export')

    def create_direct_ingest_process_job_task(self,
                                              region: Region,
                                              ingest_args: IngestArgsType):
        if not self.controller:
            raise ValueError(
                "Controller is null - did you call set_controller()?")

        self.process_job_queue.add_task(
            f'{region.region_code}-process_job-{ingest_args.task_id_tag()}',
            with_monitoring(region.region_code,
                            self.controller.
                            run_ingest_job_and_kick_scheduler_on_completion),
            ingest_args,
        )

    def create_direct_ingest_scheduler_queue_task(
            self,
            region: Region,
            just_finished_job: bool,
            delay_sec: int):
        if not self.controller:
            raise ValueError(
                "Controller is null - did you call set_controller()?")

        self.scheduler_queue.add_task(
            f'{region.region_code}-scheduler',
            with_monitoring(region.region_code,
                            self.controller.
                            schedule_next_ingest_job_or_wait_if_necessary),
            just_finished_job)

    def create_direct_ingest_handle_new_files_task(self,
                                                   region: Region,
                                                   can_start_ingest: bool):
        if not self.controller:
            raise ValueError(
                "Controller is null - did you call set_controller()?")

        if not isinstance(self.controller, GcsfsDirectIngestController):
            raise ValueError(
                f'Unexpected controller type {type(self.controller)}')

        self.scheduler_queue.add_task(
            f'{region.region_code}-handle_new_files',
            with_monitoring(region.region_code,
                            self.controller.handle_new_files),
            can_start_ingest)

    def create_direct_ingest_raw_data_import_task(self,
                                                  region: Region,
                                                  data_import_args: GcsfsRawDataBQImportArgs):
        if not self.controller:
            raise ValueError(
                "Controller is null - did you call set_controller()?")

        if not isinstance(self.controller, GcsfsDirectIngestController):
            raise ValueError(
                f'Unexpected controller type {type(self.controller)}')

        self.bq_import_export_queue.add_task(
            f'{region.region_code}-raw_data_import-{data_import_args.task_id_tag()}',
            with_monitoring(region.region_code,
                            self.controller.do_raw_data_import),
            data_import_args
        )

    def create_direct_ingest_ingest_view_export_task(self,
                                                     region: Region,
                                                     ingest_view_export_args: GcsfsIngestViewExportArgs):
        if not self.controller:
            raise ValueError(
                "Controller is null - did you call set_controller()?")

        if not isinstance(self.controller, GcsfsDirectIngestController):
            raise ValueError(
                f'Unexpected controller type {type(self.controller)}')

        self.bq_import_export_queue.add_task(
            f'{region.region_code}-ingest_view_export-{ingest_view_export_args.task_id_tag()}',
            with_monitoring(region.region_code,
                            self.controller.do_ingest_view_export),
            ingest_view_export_args
        )

    def get_process_job_queue_info(self, region: Region) -> ProcessIngestJobCloudTaskQueueInfo:
        with self.process_job_queue.mutex:
            task_names = self.process_job_queue.get_queued_task_names_unsafe()

        return ProcessIngestJobCloudTaskQueueInfo(queue_name=self.process_job_queue.name,
                                                  task_names=task_names)

    def get_scheduler_queue_info(self, region: Region) -> SchedulerCloudTaskQueueInfo:
        with self.scheduler_queue.mutex:
            task_names = self.scheduler_queue.get_queued_task_names_unsafe()

        return SchedulerCloudTaskQueueInfo(queue_name=self.scheduler_queue.name,
                                           task_names=task_names)

    def get_bq_import_export_queue_info(self, region: Region) -> BQImportExportCloudTaskQueueInfo:
        with self.bq_import_export_queue.mutex:
            has_unfinished_tasks = \
                self.bq_import_export_queue.get_queued_task_names_unsafe()

        task_names = [f'{region.region_code}-schedule-job'] \
            if has_unfinished_tasks else []
        return BQImportExportCloudTaskQueueInfo(queue_name=self.bq_import_export_queue.name,
                                                task_names=task_names)

    def wait_for_all_tasks_to_run(self):
        while True:
            with self.bq_import_export_queue.mutex:
                with self.scheduler_queue.mutex:
                    with self.process_job_queue.mutex:
                        bq_import_export_done = not self.bq_import_export_queue.get_queued_task_names_unsafe()
                        scheduler_done = not self.scheduler_queue.get_queued_task_names_unsafe()
                        process_job_queue_done = not self.process_job_queue.get_queued_task_names_unsafe()

            if scheduler_done and process_job_queue_done and bq_import_export_done:
                break

            self.bq_import_export_queue.join()
            self.scheduler_queue.join()
            self.process_job_queue.join()
