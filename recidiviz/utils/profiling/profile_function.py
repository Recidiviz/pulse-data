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
"""Helpers for profiling functions in our code."""

import cProfile
import io
import pstats
from functools import wraps
from typing import Any, Callable


def profile_function(func: Callable, num_profile_lines: int = 25) -> Callable:
    """Annotation that can be used to profile and print profiling information
    for a given function after each run of that function.

    Example:

    @profile_function
    def my_expensive_function():
        ...
    """

    @wraps(func)
    def wrap_with_profile(*args: Any, **kwargs: Any) -> Any:
        profiler = cProfile.Profile()
        profiler.enable()
        return_value = func(*args, **kwargs)
        profiler.disable()
        stats_stream = io.StringIO()
        ps = pstats.Stats(profiler, stream=stats_stream).sort_stats(
            pstats.SortKey.CUMULATIVE
        )
        ps.print_stats(num_profile_lines)
        print(stats_stream.getvalue())
        return return_value

    return wrap_with_profile
