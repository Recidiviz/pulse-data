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
"""Custom parser tests for US_ME direct ingest."""

import unittest

from recidiviz.ingest.direct.regions.us_me.us_me_custom_parsers import (
    compute_earned_time,
    total_days_from_ymd,
)


class UsMeCustomParsersTest(unittest.TestCase):
    """Parser unit tests for the various US_ME custom parsers."""

    def test_total_days_from_ymd(self) -> None:
        result = total_days_from_ymd("1", "4", "0", None)
        self.assertEqual("485", result)

    def test_total_days_from_ymd_with_start_date_not_including_leap_year(self) -> None:
        result = total_days_from_ymd("1", "4", "0", "2018-01-01")
        self.assertEqual("485", result)

    def test_total_days_from_ymd_with_start_date_including_leap_year(self) -> None:
        result = total_days_from_ymd("1", "4", "0", "2020-01-01")
        self.assertEqual("486", result)

    def test_total_days_from_ymd_all_none(self) -> None:
        result = total_days_from_ymd(None, None, None, "2020-04-03")
        self.assertEqual("0", result)

    def test_compute_earned_time(self) -> None:
        result = compute_earned_time("60", "30", "15")
        self.assertEqual("45", result)

    def test_compute_earned_time_all_none(self) -> None:
        result = compute_earned_time(None, None, None)
        self.assertEqual("0", result)

    def test_compute_earned_time_negative(self) -> None:
        result = compute_earned_time("30", "60", "0")
        self.assertEqual("-30", result)
