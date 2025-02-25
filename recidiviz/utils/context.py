# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Context manager based utilities"""

from types import TracebackType
from typing import Any, Callable, Optional, Type

from typing_extensions import ParamSpec

P = ParamSpec("P")


class on_exit:
    """Runs the provided function whenever we exit this context."""

    def __init__(self, runnable: Callable[P, Any], *args: P.args, **kwargs: P.kwargs):
        self.runnable = runnable
        self.args = args
        self.kwargs = kwargs

    def __enter__(self) -> None:
        pass

    def __exit__(
        self,
        exctype: Optional[Type[BaseException]],
        excinst: Optional[BaseException],
        exctb: Optional[TracebackType],
    ) -> None:
        self.runnable(*self.args, **self.kwargs)
