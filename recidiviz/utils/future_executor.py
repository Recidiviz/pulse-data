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
"""Function for awaiting results of futures with progress bar, tracking exceptions"""

import sys
from concurrent import futures
from typing import Callable, Generic

import attr
from progress.bar import Bar

from recidiviz.utils import structured_logging
from recidiviz.utils.types import T, U


@attr.define(kw_only=True)
class ThreadPoolExecutorResult(Generic[T, U]):
    # The items that the operation succeeded for
    successes: list[tuple[T, U]]

    # The items with an operation that raised an exception
    exceptions: list[tuple[T, Exception]]


def map_fn_with_progress_bar_results(
    *,
    work_items: list[T],
    work_fn: Callable[[T], U],
    progress_bar_message: str,
    overall_timeout_sec: int,
    single_work_item_timeout_sec: int,
    max_workers: int = 32,
) -> ThreadPoolExecutorResult[T, U]:
    """Processes all items in |work_items| using the provided |work_fn| in parallel,
    showing a progress bar as work progresses.

    Args:
        work_items: The list of itemps to process, passed as a single arg to |work_fn|
        work_fn: The function used to process items
        progress_bar_message: The message on the progress bar
        overall_timeout_sec: The timeout for ALL items to complete
        single_work_item_timeout_sec: The timeout for any single work item
        max_workers: The maximum number of ThreadPoolExecutor workers to use
    """
    successes = []
    exceptions = []
    with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        with Bar(
            progress_bar_message, file=sys.stderr, max=len(work_items), check_tty=False
        ) as progress_bar:
            future_to_work_item = {
                executor.submit(
                    structured_logging.with_context(work_fn), work_item
                ): work_item
                for work_item in work_items
            }
            for future in futures.as_completed(
                future_to_work_item, timeout=overall_timeout_sec
            ):
                work_item = future_to_work_item[future]
                try:
                    result = future.result(timeout=single_work_item_timeout_sec)
                    successes.append((work_item, result))
                except Exception as e:
                    exceptions.append((work_item, e))
                finally:
                    progress_bar.next()
    return ThreadPoolExecutorResult(successes=successes, exceptions=exceptions)


def map_fn_with_results(
    *,
    work_items: list[T],
    work_fn: Callable[[T], U],
    overall_timeout_sec: int,
    single_work_item_timeout_sec: int,
    max_workers: int = 32,
) -> ThreadPoolExecutorResult[T, U]:
    """Processes all items in |work_items| using the provided |work_fn| in parallel

    Args:
        work_items: The list of itemps to process, passed as a single arg to |work_fn|
        work_fn: The function used to process items
        overall_timeout_sec: The timeout for ALL items to complete
        single_work_item_timeout_sec: The timeout for any single work item
        max_workers: The maximum number of ThreadPoolExecutor workers to use
    """
    successes = []
    exceptions = []
    with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_work_item = {
            executor.submit(
                structured_logging.with_context(work_fn), work_item
            ): work_item
            for work_item in work_items
        }
        for future in futures.as_completed(
            future_to_work_item, timeout=overall_timeout_sec
        ):
            work_item = future_to_work_item[future]
            try:
                result = future.result(timeout=single_work_item_timeout_sec)
                successes.append((work_item, result))
            except Exception as e:
                exceptions.append((work_item, e))
    return ThreadPoolExecutorResult(successes=successes, exceptions=exceptions)
