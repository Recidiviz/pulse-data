# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Simple class for running tasks on a single background thread."""

import logging
from queue import Empty, Queue
from threading import Condition, Lock, Thread
from typing import Any, Callable, List, Optional, Tuple


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

    def delete_task(self, task_name: str) -> None:
        with self.all_tasks_mutex:
            self.all_task_names.remove(task_name)

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
