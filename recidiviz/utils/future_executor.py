# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
""" Class for awaiting results of futures """

from concurrent import futures
from concurrent.futures import Future
from contextlib import contextmanager
from typing import Any, Callable, Dict, Generator, List, Optional

import attr
from progress.bar import Bar

from recidiviz.tools.deploy.logging import redirect_logging_to_file
from recidiviz.utils import structured_logging


@attr.s(frozen=True)
class FutureExecutorProgress:
    """Data class for future execution progress"""

    running: int = attr.ib()
    completed: int = attr.ib()
    total: int = attr.ib()

    def format(self) -> str:
        """Format to progress string"""
        return f"{self.running} running - {self.completed} / {self.total}"


class FutureExecutor:
    """Given a list of task futures, exposes a progress generator, and the results of futures"""

    PROGRESS_TIMEOUT = 120  # 2 minutes

    def __init__(self, task_futures: List[Future]):
        self.task_futures = task_futures
        self.futures_count = len(self.task_futures)

    def progress(
        self, timeout: Optional[int] = PROGRESS_TIMEOUT
    ) -> Generator[FutureExecutorProgress, None, None]:
        """Publishes progress of the futures until all futures are completed
        Raises a timeout error if futures have not completed after `timeout` seconds
        Raises exceptions that the futures may have encountered
        """

        completed = 0

        for _ in futures.as_completed(self.task_futures, timeout=timeout):
            completed += 1
            running = len([future for future in self.task_futures if future.running()])

            yield FutureExecutorProgress(
                running=running, completed=completed, total=self.futures_count
            )

        # Aggregate results, raising any exceptions that a future may have encountered
        self.results()

    def wait_with_progress_bar(
        self, message: str, timeout: Optional[int] = PROGRESS_TIMEOUT
    ) -> None:
        progress_bar = Bar(message, max=self.futures_count, check_tty=False)
        progress_bar.start()

        for progress in self.progress(timeout=timeout):
            progress_bar.goto(progress.completed)

        progress_bar.finish()

    def results(self) -> List:
        return [task_future.result() for task_future in self.task_futures]

    @staticmethod
    @contextmanager
    def build(
        func: Callable,
        kwargs_list: List[Dict[str, Any]],
        max_workers: Optional[int] = None,
    ) -> Generator["FutureExecutor", None, None]:
        """Creates a ThreadPoolExecutor and corresponding FutureExecutor"""
        with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_execution = FutureExecutor(
                [
                    executor.submit(structured_logging.with_context(func), **kwargs)
                    for kwargs in kwargs_list
                ]
            )

            yield future_execution


def map_fn_with_progress_bar(
    fn: Callable,
    kwargs_list: List[Dict[str, Any]],
    progress_bar_message: str,
    max_workers: int,
    timeout_sec: int,
    logging_redirect_filename: str,
) -> None:
    """This will call `fn` with each set of args in `kwargs_list`, distributing the
    work across `max_workers` threads.

    Additionally, it will write out a progress bar to keep track of how many of the
    calls have completed, and redirect logging for the individual calls to
    `logging_redirect_filename`.
    """
    with redirect_logging_to_file(logging_redirect_filename), FutureExecutor.build(
        fn,
        kwargs_list,
        max_workers,
    ) as execution:

        execution.wait_with_progress_bar(
            progress_bar_message,
            timeout=timeout_sec,
        )
