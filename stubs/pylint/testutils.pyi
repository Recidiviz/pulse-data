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

from contextlib import contextmanager
from typing import Any, Iterator, Optional, Type

from astroid import nodes

from .checkers.base_checker import BaseChecker

class MessageTest:
    def __init__(
        self,
        msg_id: str,
        line: Optional[int] = None,
        node: Optional[nodes.NodeNG] = None,
        args: Optional[Any] = None,
        confidence: Optional[Any] = None,
        col_offset: Optional[int] = None,
        end_line: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ): ...

class CheckerTestCase:
    CHECKER_CLASS: Optional[Type[BaseChecker]]
    checker: BaseChecker
    def setup_method(self) -> None: ...
    @contextmanager
    def assertAddsMessages(self, *messages: MessageTest) -> Iterator[None]: ...
    @contextmanager
    def assertNoMessages(self) -> Iterator[None]: ...
