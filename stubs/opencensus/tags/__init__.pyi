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
from contextvars import ContextVar
from collections import OrderedDict
from typing import Optional, List

from opencensus.common.runtime_context import _AsyncRuntimeContext
from opencensus.tags.tag import Tag

TagContext: reveal_type[_AsyncRuntimeContext().Slot]

class TagMap:
    map: OrderedDict
    def __init__(self, tags: Optional[List[Tag]]) -> None: ...
    def insert(self, key: str, value: str) -> None: ...
    def delete(self, key: str) -> None: ...
