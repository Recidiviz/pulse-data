# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""A set of helpful patching utils."""
from contextlib import contextmanager
from functools import wraps
from typing import Any, Callable, ContextManager, Iterator, Optional
from unittest.mock import patch


def before(target: str, fn: Callable, **kwargs: Any) -> ContextManager:
    """Patches the |target| function so that |fn| runs immediately before the original
    |target|. Example usage:

    def function_to_run_before(...):
        ...

    with before("path.to.patch.target", function_to_run_before):
        ...
    """
    return before_after(target, before_fn=fn, **kwargs)


def after(target: str, fn: Callable, **kwargs: Any) -> ContextManager:
    """Patches the |target| function so that |fn| runs immediately after the original
    |target|. Example usage:

    def function_to_run_after(...):
        ...

    with after("path.to.patch.target", function_to_run_after):
        ...
    """

    return before_after(target, after_fn=fn, **kwargs)


@contextmanager
def before_after(
    target: str,
    before_fn: Optional[Callable] = None,
    after_fn: Optional[Callable] = None,
    once: bool = True,
    **kwargs: Any
) -> Iterator:
    """TAKEN FROM https://pypi.org/project/before_after/ as the package is not
    compatible with python 3.11

    Small wrapper around pytest's patch to simulate events that might happen around
    a function call.
    """

    def before_after_wrap(fn: Callable) -> Callable:
        called = False

        @wraps(fn)
        def inner(*args: Any, **kwargs: Any) -> Any:
            # If once is True, then don't call if this function has already been called
            nonlocal called
            if once:
                if called:
                    return fn(*args, **kwargs)
                called = True

            if before_fn:
                before_fn(*args, **kwargs)
            ret = fn(*args, **kwargs)
            if after_fn:
                after_fn(*args, **kwargs)
            return ret

        return inner

    patcher = patch(target, **kwargs)
    original, _ = patcher.get_original()
    patcher.new = before_after_wrap(original)
    with patcher:
        yield
