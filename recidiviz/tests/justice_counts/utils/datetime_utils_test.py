# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Tests for Justice Counts datetime utils."""

import datetime
import unittest

from recidiviz.justice_counts.utils.datetime_utils import (
    convert_date_range_to_year_month,
)


class TestJusticeCountsDatetimeUtils(unittest.TestCase):
    def test_convert_date_range(self) -> None:
        year, month = convert_date_range_to_year_month(
            datetime.datetime(2021, 1, 2), datetime.datetime(2021, 2, 2)
        )
        self.assertEqual((year, month), (2021, 1))

        year, month = convert_date_range_to_year_month(
            datetime.datetime(2021, 12, 30), datetime.datetime(2022, 1, 30)
        )
        self.assertEqual((year, month), (2021, 12))

        year, month = convert_date_range_to_year_month(
            datetime.datetime(2021, 1, 30), datetime.datetime(2022, 1, 30)
        )
        self.assertEqual((year, month), (2021, None))

        with self.assertRaisesRegex(ValueError, "Invalid report start and end:"):
            convert_date_range_to_year_month(
                datetime.datetime(2021, 1, 30), datetime.datetime(2021, 1, 30)
            )
