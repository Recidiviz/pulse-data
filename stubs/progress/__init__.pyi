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

from types import TracebackType
from typing import Optional, TextIO

class Infinite:
    def __enter__(self) -> Infinite: ...
    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: TracebackType,
    ) -> None: ...
    def finish(self) -> None: ...
    def next(self, n: int = 1) -> None: ...

class Progress(Infinite):
    def __init__(
        self,
        message: str,
        max: int,
        check_tty: bool = True,
        file: Optional[TextIO] = None,
    ): ...
    def start(self) -> None: ...
    def goto(self, index: int) -> None: ...
