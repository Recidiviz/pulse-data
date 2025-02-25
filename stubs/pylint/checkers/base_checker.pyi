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

from typing import Any, Optional

from astroid import nodes

from ..lint import PyLinter

class BaseChecker:
    def __init__(self, linter: Optional[PyLinter] = None) -> None: ...
    def add_message(
        self,
        msgid: str,
        line: Optional[Any] = None,
        node: Optional[nodes.NodeNG] = None,
        args: Optional[Any] = None,
        confidence: Optional[Any] = None,
        col_offset: Optional[Any] = None,
    ) -> None: ...
