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
"""Container that allows for querying by range of a given attribute."""


import abc
import bisect
import sys
from typing import Callable, Generic, List, Optional, Protocol, Sequence, TypeVar, cast


class Comparable(Protocol):
    """Protocol for annotating comparable types."""

    @abc.abstractmethod
    def __lt__(self: "KeyT", other: "KeyT", /) -> bool:
        pass


KeyT = TypeVar("KeyT", bound=Comparable)
ItemT = TypeVar("ItemT")


class RangeQuerier(Generic[KeyT, ItemT]):
    """Container that allows for querying by range of a given attribute.

    For example, the container could hold items with dates, and when provided a date
    range will return the list of items that fall within that range.

    Note, any items with `None` for the specified key attribute are skipped."""

    def __init__(
        self,
        items: Sequence[ItemT],
        key: Callable[[ItemT], Optional[KeyT]],
    ):
        def key_not_optional(item: ItemT) -> KeyT:
            return cast(KeyT, key(item))

        self.key = key_not_optional

        self.items = sorted(
            (item for item in items if self.key(item) is not None), key=self.key
        )

    def get_sorted_items_in_range(
        self, start_inclusive: Optional[KeyT], end_inclusive: Optional[KeyT]
    ) -> List[ItemT]:
        start_index = (
            0
            if start_inclusive is None
            else bisect.bisect_left(self.items, start_inclusive, key=self.key)
        )
        end_index = (
            sys.maxsize
            if end_inclusive is None
            else bisect.bisect_right(self.items, end_inclusive, key=self.key)
        )
        return self.items[start_index:end_index]
