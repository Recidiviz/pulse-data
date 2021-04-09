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
from typing import Callable, Optional, Type

def if_transient_error(exception: Exception) -> Callable[[Exception], bool]: ...
def if_exception_type(
    *exception_types: Type[Exception],
) -> Callable[[Exception], bool]: ...

class Retry:
    def __init__(
        self, predicate: Callable[[Exception], Callable[[Exception], bool]]
    ) -> None: ...
    def __call__(
        self, func: Callable, on_error: Optional[Callable] = None
    ) -> Callable: ...
