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

from recidiviz.airflow.dags.raw_data.utils import (
    evenly_weighted_buckets_with_max,
    max_number_of_buckets_with_target,
    n_evenly_weighted_buckets,
)


class BucketingTests(TestCase):
    """Unit tests for bucketing tests"""

    def test_n_evenly_weighted_buckets(self) -> None:
        # none
        assert n_evenly_weighted_buckets([], 1000) == []

        # n is 0
        with self.assertRaisesRegex(
            ValueError,
            re.escape("Expected n to be greater than or equal to 0; got 0"),
        ):
            n_evenly_weighted_buckets([(1, 1)], 0)

        # fewer than n
        assert n_evenly_weighted_buckets([(1, 1), (2, 1), (3, 1)], 10) == [
            [1],
            [2],
            [3],
        ]

        # equal weights
        assert n_evenly_weighted_buckets([(i, 1) for i in range(10)], 3) == [
            list(range(0, 10, 3)),
            list(range(1, 10, 3)),
            list(range(2, 10, 3)),
        ]

        # different weights
        assert n_evenly_weighted_buckets([(i, i) for i in range(10)], 3) == [
            [9, 4, 3],
            [8, 5, 2],
            [7, 6, 1, 0],
        ]

    def test_max_number_of_buckets_with_target(self) -> None:
        # less than max
        batches = max_number_of_buckets_with_target(
            ["" for _ in range(9)], max_per_bucket=500, target_number_of_buckets=10
        )

        assert len(batches) == 9
        for batch in batches:
            assert len(batch) == 1

        # still fits within max
        batches = max_number_of_buckets_with_target(
            ["" for _ in range(10 * 499)],
            max_per_bucket=500,
            target_number_of_buckets=10,
        )

        assert len(batches) == 10
        for batch in batches:
            assert len(batch) == 499

        # overflows max
        batches = max_number_of_buckets_with_target(
            ["" for _ in range(15 * 501)],
            max_per_bucket=500,
            target_number_of_buckets=10,
        )

        assert len(batches) == 16

    def test_evenly_weighted_buckets_with_max(self) -> None:
        # none
        assert (
            evenly_weighted_buckets_with_max(
                [], target_n=1000, max_weight_per_bucket=100
            )
            == []
        )

        # n is 0
        with self.assertRaisesRegex(
            ValueError,
            re.escape("Expected target_n to be greater than or equal to 0; got 0"),
        ):
            evenly_weighted_buckets_with_max(
                [(1, 1)], target_n=0, max_weight_per_bucket=10
            )

        # fewer than n
        assert evenly_weighted_buckets_with_max(
            [(1, 1), (2, 1), (3, 1)], target_n=10, max_weight_per_bucket=2
        ) == [
            [1],
            [2],
            [3],
        ]

        # equal weights
        assert evenly_weighted_buckets_with_max(
            [(i, 1) for i in range(10)], target_n=3, max_weight_per_bucket=10
        ) == [
            list(range(0, 10, 3)),
            list(range(1, 10, 3)),
            list(range(2, 10, 3)),
        ]

        # different weights
        assert evenly_weighted_buckets_with_max(
            [(i, i) for i in range(10)], target_n=3, max_weight_per_bucket=10
        ) == [
            [9, 0],
            [8, 1],
            [7, 2],
            [6, 3],
            [5, 4],
        ]

        # constrain max weight to 3 which is less than max, so 1 per bucket
        assert evenly_weighted_buckets_with_max(
            [(i, i) for i in range(10)], target_n=3, max_weight_per_bucket=3
        ) == [[9], [8], [7], [6], [5], [4], [3], [2], [1], [0]]

        # 12 max
        assert evenly_weighted_buckets_with_max(
            [(i, i) for i in range(20)], target_n=3, max_weight_per_bucket=12
        ) == [
            [19],
            [18],
            [17],
            [16],
            [15],
            [14],
            [13],
            [12],
            [11],
            [10],
            [9],
            [8],
            [7, 0],
            [6, 1],
            [5, 2],
            [4, 3],
        ]
