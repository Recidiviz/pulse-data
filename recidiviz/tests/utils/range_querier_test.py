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
"""Tests for range querying functionality"""

from typing import List, Optional

import attr
from parameterized import parameterized

from recidiviz.utils.range_querier import RangeQuerier


@parameterized.expand(
    [
        ("zeros", 0, 0, []),
        ("positive", 0, 100, [3, 4, 7]),
        ("single digit", 3, 3, [3]),
        ("single empty", 8, 8, []),
        ("reversed", 9, 0, []),
        ("negative", -20, -10, [-12]),
    ]
)
def test_range_int(_name: str, start: int, end: int, expected: List[int]) -> None:
    rq = RangeQuerier([4, 3, 7, None, 200, -1, -12], lambda i: i)

    assert rq.get_sorted_items_in_range(start, end) == expected


def test_range_empty() -> None:
    rq = RangeQuerier[int, int]([], lambda i: i)

    assert rq.get_sorted_items_in_range(0, 0) == []
    assert rq.get_sorted_items_in_range(0, 10) == []


@attr.s
class SillyObject:
    name: str = attr.ib()
    rank: Optional[int] = attr.ib()


def test_range_object() -> None:
    rq = RangeQuerier(
        [
            SillyObject("foo", 8),
            SillyObject("bar", 1),
            SillyObject("baz", 17),
            SillyObject("qux", None),
        ],
        lambda s: s.rank,
    )

    assert rq.get_sorted_items_in_range(1, 10) == [
        SillyObject("bar", 1),
        SillyObject("foo", 8),
    ]


def test_range_object_all_none() -> None:
    rq = RangeQuerier(
        [
            SillyObject("foo", None),
            SillyObject("bar", None),
            SillyObject("baz", None),
            SillyObject("qux", None),
        ],
        lambda s: s.rank,
    )

    assert rq.get_sorted_items_in_range(1, 10) == []
