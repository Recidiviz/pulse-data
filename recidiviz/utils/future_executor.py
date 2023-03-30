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
from typing import Any, Callable, Dict, List, Tuple

from progress.bar import Bar

from recidiviz.utils import structured_logging


def map_fn_with_progress_bar_results(
    fn: Callable,
    kwargs_list: List[Dict[str, Any]],
    max_workers: int,
    timeout: int,
    progress_bar_message: str,
) -> Tuple[List[Tuple[Any, Dict[str, Any]]], List[Tuple[Exception, Dict[str, Any]]]]:
    successes = []
    exceptions = []
    with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        with Bar(
            progress_bar_message, file=sys.stderr, max=len(kwargs_list), check_tty=False
        ) as progress_bar:
            future_to_kwargs_map = {
                executor.submit(structured_logging.with_context(fn), **kwargs): kwargs
                for kwargs in kwargs_list
            }
            for future in futures.as_completed(future_to_kwargs_map, timeout=timeout):
                task_kwargs = future_to_kwargs_map[future]
                try:
                    data = future.result()
                except Exception as ex:
                    exceptions.append((ex, task_kwargs))
                else:
                    successes.append((data, task_kwargs))
                progress_bar.next()
    return (successes, exceptions)
