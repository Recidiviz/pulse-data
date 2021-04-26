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
from typing import List, Generator, Dict, Callable, Optional

import attr

from recidiviz.utils import structured_logging


@attr.s(frozen=True)
class FutureExecutorProgress:
    """ Data class for future execution progress"""

    running: int = attr.ib()
    completed: int = attr.ib()
    total: int = attr.ib()

    def format(self) -> str:
        """ Format to progress string """
        return f"{self.running} running - {self.completed} / {self.total}"


class FutureExecutor:
    """ Given a list of task futures, exposes a progress generator, and the results of futures """

    PROGRESS_TIMEOUT = 120  # 2 minutes

    def __init__(self, task_futures: List[Future]):
        self.task_futures = task_futures
        self.futures_count = len(self.task_futures)

    def progress(
        self, timeout: Optional[int] = PROGRESS_TIMEOUT
    ) -> Generator[FutureExecutorProgress, None, None]:
        """Publishes progress of the futures until all futures are completed
        Raises a timeout error if futures have not completed after `timeout` seconds
        """

        completed = 0

        for _ in futures.as_completed(self.task_futures, timeout=timeout):
            completed += 1
            running = len([future for future in self.task_futures if future.running()])

            yield FutureExecutorProgress(
                running=running, completed=completed, total=self.futures_count
            )

    def results(self) -> List:
        return [task_future.result() for task_future in self.task_futures]

    @staticmethod
    @contextmanager
    def build(
        func: Callable, kwargs_list: List[Dict], max_workers: Optional[int] = None
    ) -> Generator["FutureExecutor", None, None]:
        """ Creates a ThreadPoolExecutor and corresponding FutureExecutor """
        with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_execution = FutureExecutor(
                [
                    executor.submit(structured_logging.with_context(func), **kwargs)
                    for kwargs in kwargs_list
                ]
            )

            yield future_execution
