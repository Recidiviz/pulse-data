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
import logging
from queue import Queue, Empty
from threading import Thread, Lock, Condition
from typing import Callable, List, Tuple, Optional, Any

from recidiviz.ingest.direct.controllers.direct_ingest_types import IngestArgsType
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import (
    GcsfsDirectIngestController,
)
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    GcsfsRawDataBQImportArgs,
    GcsfsIngestViewExportArgs,
)
from recidiviz.ingest.direct.direct_ingest_cloud_task_manager import (
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


class TooManyTasksError(ValueError):
    pass


class SingleThreadTaskQueue(Queue):
    """Simple class for running tasks on a single background thread."""

    MAX_TASKS = 100

    def __init__(self, name: str, max_tasks: int = MAX_TASKS):
        super().__init__()
        self.name = name
        self.max_tasks = max_tasks

        self.all_tasks_mutex = Lock()
        self.has_unfinished_tasks_condition = Condition(self.all_tasks_mutex)

        # These variables all protected by all_tasks_mutex
        self.all_task_names: List[str] = []
        self.all_executed_tasks: List[str] = []
        self.running_task_name: Optional[str] = None
        self.terminating_exception: Optional[Exception] = None

        t = Thread(target=self.worker)
        t.daemon = True
        t.start()

    def add_task(
        self, task_name: str, task: Callable, *args: Any, **kwargs: Any
    ) -> None:
        args = args or ()
        kwargs = kwargs or {}
        with self.all_tasks_mutex:
            if self.terminating_exception:
                return
            self.put_nowait((task_name, task, args, kwargs))

            self.all_task_names = self._get_queued_task_names()
            if self.running_task_name is not None:
                self.all_task_names.append(self.running_task_name)
            self.has_unfinished_tasks_condition.notify()

    def get_unfinished_task_names_unsafe(self) -> List[str]:
        """Returns the names of all unfinished tasks in this queue, including the task that is currently running, if
        there is one.

        NOTE: You must be holding the all_tasks_mutex for this queue before calling this
        to avoid races. For example:
            queue = SingleThreadTaskQueue(name='my_queue')
            with queue.all_tasks_mutex:
                task_names = queue.get_unfinished_task_names_unsafe()

            if task_names:
                ...
        """
        return self.all_task_names

    def join(self) -> None:
        """Waits until all queued tasks are complete. Raises any exceptions that were raised on the worker thread."""
        super().join()
        with self.all_tasks_mutex:
            if self.terminating_exception:
                if isinstance(self.terminating_exception, TooManyTasksError):
                    logging.warning("Too many tasks run: [%s]", self.all_executed_tasks)
                raise self.terminating_exception

    def worker(self) -> None:
        """Runs tasks indefinitely until a task raises an exception, waiting for new tasks if the queue is empty."""
        while True:
            _task_name, task, args, kwargs = self._worker_pop_task()
            try:

                task(*args, **kwargs)
            except Exception as e:
                self._worker_mark_task_done()
                self._worker_handle_exception(e)
                return

            self._worker_mark_task_done()

            with self.all_tasks_mutex:
                too_many_tasks = len(self.all_executed_tasks) > self.max_tasks
            if too_many_tasks:
                self._worker_handle_exception(
                    TooManyTasksError(f"Ran too many tasks on queue [{self.name}]")
                )
                return

    def _get_queued_task_names(self) -> List[str]:
        """Returns the names of all queued tasks in this queue. This does NOT include
        tasks that are currently running."""
        with self.mutex:
            _queue = self.queue.copy()
            task_names = []
            while _queue:
                task_name, _task, _args, _kwargs = _queue.pop()
                task_names.append(task_name)

            return task_names

    def _worker_pop_task(self) -> Tuple:
        """Helper for the worker thread. Waits until there is a task in the queue, marks it as the running task, then
        returns the task and args."""
        while True:
            with self.all_tasks_mutex:
                try:
                    task_name, task, args, kwargs = self.get_nowait()
                    self.running_task_name = task_name
                    self.all_task_names = self._get_queued_task_names() + [task_name]
                    return task_name, task, args, kwargs
                except Empty:
                    self.has_unfinished_tasks_condition.wait()

    def _worker_mark_task_done(self) -> None:
        """Helper for the worker thread. Marks the current task as complete in the running thread and updates the list
        of all task names."""
        with self.all_tasks_mutex:
            self.task_done()
            if not self.running_task_name:
                raise ValueError("Expected nonnull running_task_name, found None.")
            self.all_executed_tasks.append(self.running_task_name)
            self.running_task_name = None
            self.all_task_names = self._get_queued_task_names()

    def _worker_handle_exception(self, e: Exception) -> None:
        """Helper for the worker thread. Clears the queue and sets the terminating exception field so that any callers
        to join() will terminate and raise on the join thread."""
        with self.all_tasks_mutex:
            self.terminating_exception = e
            try:
                while True:
                    _ = self.get_nowait()
                    self.task_done()
            except Empty:
                self.running_task_name = None
                self.all_task_names = []


def with_monitoring(region_code: str, fn: Callable) -> Callable:
    def wrapped_fn(*args: Any, **kwargs: Any) -> None:
        with monitoring.push_region_tag(region_code):
            fn(*args, **kwargs)

    return wrapped_fn


class FakeAsyncDirectIngestCloudTaskManager(FakeDirectIngestCloudTaskManager):
    """Test implementation of the DirectIngestCloudTaskManager that runs tasks
    asynchronously on background threads."""

    def __init__(self) -> None:
        super().__init__()
        self.scheduler_queue = SingleThreadTaskQueue(name="scheduler")
        self.process_job_queue = SingleThreadTaskQueue(name="process_job")
        self.bq_import_export_queue = SingleThreadTaskQueue(name="bq_import_export")
        self.sftp_queue = SingleThreadTaskQueue(name="sftp")

    def create_direct_ingest_process_job_task(
        self, region: Region, ingest_args: IngestArgsType
    ) -> None:
        if not self.controller:
            raise ValueError("Controller is null - did you call set_controller()?")

        self.process_job_queue.add_task(
            f"{region.region_code}-process_job-{ingest_args.task_id_tag()}",
            with_monitoring(
                region.region_code,
                self.controller.run_ingest_job_and_kick_scheduler_on_completion,
            ),
            ingest_args,
        )

    def create_direct_ingest_scheduler_queue_task(
        self, region: Region, just_finished_job: bool, delay_sec: int
    ) -> None:
        if not self.controller:
            raise ValueError("Controller is null - did you call set_controller()?")

        self.scheduler_queue.add_task(
            f"{region.region_code}-scheduler",
            with_monitoring(
                region.region_code,
                self.controller.schedule_next_ingest_job_or_wait_if_necessary,
            ),
            just_finished_job,
        )

    def create_direct_ingest_handle_new_files_task(
        self, region: Region, can_start_ingest: bool
    ) -> None:
        if not self.controller:
            raise ValueError("Controller is null - did you call set_controller()?")

        if not isinstance(self.controller, GcsfsDirectIngestController):
            raise ValueError(f"Unexpected controller type {type(self.controller)}")

        self.scheduler_queue.add_task(
            f"{region.region_code}-handle_new_files",
            with_monitoring(region.region_code, self.controller.handle_new_files),
            can_start_ingest,
        )

    def create_direct_ingest_raw_data_import_task(
        self, region: Region, data_import_args: GcsfsRawDataBQImportArgs
    ) -> None:
        if not self.controller:
            raise ValueError("Controller is null - did you call set_controller()?")

        if not isinstance(self.controller, GcsfsDirectIngestController):
            raise ValueError(f"Unexpected controller type {type(self.controller)}")

        self.bq_import_export_queue.add_task(
            f"{region.region_code}-raw_data_import-{data_import_args.task_id_tag()}",
            with_monitoring(region.region_code, self.controller.do_raw_data_import),
            data_import_args,
        )

    def create_direct_ingest_ingest_view_export_task(
        self, region: Region, ingest_view_export_args: GcsfsIngestViewExportArgs
    ) -> None:
        if not self.controller:
            raise ValueError("Controller is null - did you call set_controller()?")

        if not isinstance(self.controller, GcsfsDirectIngestController):
            raise ValueError(f"Unexpected controller type {type(self.controller)}")

        self.bq_import_export_queue.add_task(
            f"{region.region_code}-ingest_view_export-{ingest_view_export_args.task_id_tag()}",
            with_monitoring(region.region_code, self.controller.do_ingest_view_export),
            ingest_view_export_args,
        )

    def create_direct_ingest_sftp_download_task(self, region: Region) -> None:
        self.sftp_queue.add_task(
            f"{region.region_code}-handle_sftp_download", lambda _: None
        )

    def get_process_job_queue_info(
        self, region: Region
    ) -> ProcessIngestJobCloudTaskQueueInfo:
        with self.process_job_queue.all_tasks_mutex:
            task_names = self.process_job_queue.get_unfinished_task_names_unsafe()

        return ProcessIngestJobCloudTaskQueueInfo(
            queue_name=self.process_job_queue.name, task_names=task_names
        )

    def get_scheduler_queue_info(self, region: Region) -> SchedulerCloudTaskQueueInfo:
        with self.scheduler_queue.all_tasks_mutex:
            task_names = self.scheduler_queue.get_unfinished_task_names_unsafe()

        return SchedulerCloudTaskQueueInfo(
            queue_name=self.scheduler_queue.name, task_names=task_names
        )

    def get_bq_import_export_queue_info(
        self, region: Region
    ) -> BQImportExportCloudTaskQueueInfo:
        with self.bq_import_export_queue.all_tasks_mutex:
            has_unfinished_tasks = (
                self.bq_import_export_queue.get_unfinished_task_names_unsafe()
            )

        task_names = (
            [f"{region.region_code}-schedule-job"] if has_unfinished_tasks else []
        )
        return BQImportExportCloudTaskQueueInfo(
            queue_name=self.bq_import_export_queue.name, task_names=task_names
        )

    def get_sftp_queue_info(self, region: Region) -> SftpCloudTaskQueueInfo:
        with self.sftp_queue.all_tasks_mutex:
            has_unfinished_tasks = self.sftp_queue.get_unfinished_task_names_unsafe()

        task_names = (
            [f"{region.region_code}-sftp-download"] if has_unfinished_tasks else []
        )
        return SftpCloudTaskQueueInfo(
            queue_name=self.sftp_queue.name, task_names=task_names
        )

    def wait_for_all_tasks_to_run(self) -> None:
        bq_import_export_done = False
        scheduler_done = False
        process_job_queue_done = False
        while not (bq_import_export_done and scheduler_done and process_job_queue_done):
            self.bq_import_export_queue.join()
            self.scheduler_queue.join()
            self.process_job_queue.join()

            with self.bq_import_export_queue.all_tasks_mutex:
                with self.scheduler_queue.all_tasks_mutex:
                    with self.process_job_queue.all_tasks_mutex:
                        bq_import_export_done = (
                            not self.bq_import_export_queue.get_unfinished_task_names_unsafe()
                        )
                        scheduler_done = (
                            not self.scheduler_queue.get_unfinished_task_names_unsafe()
                        )
                        process_job_queue_done = (
                            not self.process_job_queue.get_unfinished_task_names_unsafe()
                        )
