# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Defines types used for direct ingest."""
import abc
import datetime
import heapq
from typing import TypeVar, Generic, Callable, List, Tuple, Optional, Type, Dict, Any

import attr
import cattr

from recidiviz.cloud_storage.content_types import FileContentsHandle
from recidiviz.utils.types import ClsT


@attr.s(frozen=True)
class CloudTaskArgs:
    @abc.abstractmethod
    def task_id_tag(self) -> Optional[str]:
        """Tag to add to the name of an associated cloud task."""

    def to_serializable(self) -> Dict[str, Any]:
        return cattr.unstructure(self)

    @classmethod
    def from_serializable(cls: Type[ClsT], serializable: Dict[str, Any]) -> ClsT:
        return cattr.structure(serializable, cls)


@attr.s(frozen=True)
class IngestArgs(CloudTaskArgs):
    ingest_time: datetime.datetime = attr.ib()

    def task_id_tag(self) -> Optional[str]:
        return None


IngestArgsType = TypeVar('IngestArgsType', bound=IngestArgs)


ContentsHandleType = TypeVar('ContentsHandleType', bound=FileContentsHandle)


class ArgsPriorityQueue(Generic[IngestArgsType]):
    def __init__(self, sort_key_gen: Callable[[IngestArgsType], str]):
        self._sort_key_gen = sort_key_gen
        self._heap: List[Tuple[str, IngestArgsType]] = []

    def push(self, item: IngestArgsType) -> None:
        heapq.heappush(self._heap, (self._sort_key_gen(item), item))

    def pop(self) -> Optional[IngestArgsType]:
        if self.size() == 0:
            return None

        return heapq.heappop(self._heap)[1]

    def peek(self) -> Optional[IngestArgsType]:
        if self.size() == 0:
            return None
        return heapq.nsmallest(1, self._heap)[0][1]

    def size(self) -> int:
        return len(self._heap)
