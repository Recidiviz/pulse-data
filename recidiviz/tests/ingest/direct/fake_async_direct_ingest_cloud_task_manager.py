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

from recidiviz.ingest.direct.controllers.direct_ingest_types import \
    IngestArgsType
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import \
    GcsfsDirectIngestController
from recidiviz.ingest.direct.direct_ingest_cloud_task_manager import \
    CloudTaskQueueInfo
from recidiviz.tests.ingest.direct.fake_direct_ingest_cloud_task_manager \
    import FakeDirectIngestCloudTaskManager
from recidiviz.utils.regions import Region


class SingleThreadTaskQueue(Queue):
    """Simple class for running tasks on a single background thread."""

    def __init__(self, name: str):
        super().__init__()
        self.name = name

        t = Thread(target=self.worker)
        t.daemon = True
        t.start()

    def add_task(self, task, *args, **kwargs):
        args = args or ()
        kwargs = kwargs or {}
        self.put((task, args, kwargs))

    def worker(self):
        while True:
            item, args, kwargs = self.get()
            item(*args, **kwargs)
            self.task_done()

    def has_unfinished_tasks_unsafe(self) -> bool:
        """Returns true if there are unfinished tasks in this queue.

        NOTE: You must be holding the mutex for this queue before calling this
        to avoid races. For example:
            queue = SingleThreadTaskQueue(name='my_queue')
            with queue.mutex:
                is_running = queue.has_unfinished_tasks_unsafe()

            if is_running:
                ...
        """
        return self.unfinished_tasks > 0  # type: ignore


class FakeAsyncDirectIngestCloudTaskManager(FakeDirectIngestCloudTaskManager):
    """Test implementation of the DirectIngestCloudTaskManager that runs tasks
    asynchronously on background threads."""

    def __init__(self):
        super().__init__()
        self.scheduler_queue = SingleThreadTaskQueue(name='scheduler')
        self.process_job_queue = SingleThreadTaskQueue(name='process_job')

    def create_direct_ingest_process_job_task(self,
                                              region: Region,
                                              ingest_args: IngestArgsType):
        if not self.controller:
            raise ValueError(
                "Controller is null - did you call set_controller()?")

        self.process_job_queue.add_task(
            self.controller.run_ingest_job_and_kick_scheduler_on_completion,
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
            self.controller.schedule_next_ingest_job_or_wait_if_necessary,
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
            self.controller.handle_new_files,
            can_start_ingest)

    def get_process_job_queue_info(self, region: Region) -> CloudTaskQueueInfo:
        with self.process_job_queue.mutex:
            has_unfinished_tasks = \
                self.process_job_queue.has_unfinished_tasks_unsafe()

        task_names = [f'{region.region_code}-process-job'] \
            if has_unfinished_tasks else []
        return CloudTaskQueueInfo(queue_name=self.scheduler_queue.name,
                                  task_names=task_names)

    def get_scheduler_queue_info(self, region: Region) -> CloudTaskQueueInfo:
        with self.scheduler_queue.mutex:
            has_unfinished_tasks = \
                self.scheduler_queue.has_unfinished_tasks_unsafe()

        task_names = [f'{region.region_code}-schedule-job'] \
            if has_unfinished_tasks else []
        return CloudTaskQueueInfo(queue_name=self.scheduler_queue.name,
                                  task_names=task_names)

    def wait_for_all_tasks_to_run(self):
        while True:
            with self.scheduler_queue.mutex:
                with self.process_job_queue.mutex:
                    scheduler_done = \
                        not self.scheduler_queue.has_unfinished_tasks_unsafe()
                    process_job_queue_done = \
                        not self.process_job_queue.has_unfinished_tasks_unsafe()

            if scheduler_done and process_job_queue_done:
                break

            self.scheduler_queue.join()
            self.process_job_queue.join()
