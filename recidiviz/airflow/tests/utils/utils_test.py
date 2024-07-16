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
"""Tests for raw data utils"""
import re
from unittest import TestCase

from recidiviz.airflow.dags.raw_data.utils import n_evenly_weighted_buckets


class NBucketsTest(TestCase):
    """Unit tests for n_evenly_weighted_buckets"""

    def test_none(self) -> None:
        assert n_evenly_weighted_buckets([], 1000) == []

    def test_n_is_0(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape("Expected n to be greater than or equal to 0; got 0"),
        ):
            n_evenly_weighted_buckets([(1, 1)], 0)

    def test_fewer_than_n(self) -> None:
        assert n_evenly_weighted_buckets([(1, 1), (2, 1), (3, 1)], 10) == [
            [1],
            [2],
            [3],
        ]

    def test_equal_weights(self) -> None:
        assert n_evenly_weighted_buckets([(i, 1) for i in range(10)], 3) == [
            list(range(0, 10, 3)),
            list(range(1, 10, 3)),
            list(range(2, 10, 3)),
        ]

    def test_different_weights(self) -> None:
        assert n_evenly_weighted_buckets([(i, i) for i in range(10)], 3) == [
            [9, 4, 3],
            [8, 5, 2],
            [7, 6, 1, 0],
        ]
