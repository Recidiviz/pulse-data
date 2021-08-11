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
"""Tests for date.py"""
import datetime
import unittest

from recidiviz.common.date import (
    DateRange,
    DateRangeDiff,
    NonNegativeDateRange,
    munge_date_string,
)


def test_mungeDateString_munges() -> None:
    assert munge_date_string("1y 1m 1d") == "1year 1month 1day"


def test_mungeDateString_doesntMunge() -> None:
    assert munge_date_string("1year 1month 1day") == "1year 1month 1day"


def test_mungeDateString_caseInsensitive() -> None:
    assert munge_date_string("1Y 1M 1D") == "1year 1month 1day"


def test_mungeDateString_noYear() -> None:
    assert munge_date_string("10M 12D") == "10month 12day"


def test_mungeDateString_noMonth() -> None:
    assert munge_date_string("9Y 28D") == "9year 28day"


def test_mungeDateString_noDay() -> None:
    assert munge_date_string("4y 3m") == "4year 3month"


def test_mungeDateString_ZeroAm() -> None:
    assert munge_date_string("Jan 1, 2018 00:00 AM") == "Jan 1, 2018 12:00 AM"


class TestDateRange(unittest.TestCase):
    """Tests for DateRange"""

    def setUp(self) -> None:
        self.negative_day_range = DateRange(
            lower_bound_inclusive_date=datetime.date(2019, 2, 3),
            upper_bound_exclusive_date=datetime.date(2019, 2, 2),
        )

        self.zero_day_range = DateRange(
            lower_bound_inclusive_date=datetime.date(2019, 2, 3),
            upper_bound_exclusive_date=datetime.date(2019, 2, 3),
        )

        self.one_day_range = DateRange(
            lower_bound_inclusive_date=datetime.date(2019, 2, 3),
            upper_bound_exclusive_date=datetime.date(2019, 2, 4),
        )

        self.single_month_range = DateRange(
            lower_bound_inclusive_date=datetime.date(2019, 2, 1),
            upper_bound_exclusive_date=datetime.date(2019, 3, 1),
        )

        self.multi_month_range = DateRange(
            lower_bound_inclusive_date=datetime.date(2019, 2, 3),
            upper_bound_exclusive_date=datetime.date(2019, 4, 10),
        )

    def test_get_months_range_overlaps_at_all(self) -> None:
        self.assertEqual([], self.negative_day_range.get_months_range_overlaps_at_all())
        self.assertEqual([], self.zero_day_range.get_months_range_overlaps_at_all())
        self.assertEqual(
            [(2019, 2)], self.one_day_range.get_months_range_overlaps_at_all()
        )
        self.assertEqual(
            [(2019, 2)], self.single_month_range.get_months_range_overlaps_at_all()
        )
        self.assertEqual(
            [(2019, 2), (2019, 3), (2019, 4)],
            self.multi_month_range.get_months_range_overlaps_at_all(),
        )

    def test_portion_overlapping_with_month(self) -> None:

        self.assertEqual(
            None, self.negative_day_range.portion_overlapping_with_month(2019, 2)
        )

        self.assertEqual(
            None, self.zero_day_range.portion_overlapping_with_month(2019, 2)
        )

        self.assertEqual(
            self.one_day_range,
            self.one_day_range.portion_overlapping_with_month(2019, 2),
        )

        self.assertEqual(
            self.single_month_range,
            self.single_month_range.portion_overlapping_with_month(2019, 2),
        )

        self.assertEqual(
            DateRange(
                lower_bound_inclusive_date=datetime.date(2019, 2, 3),
                upper_bound_exclusive_date=datetime.date(2019, 3, 1),
            ),
            self.multi_month_range.portion_overlapping_with_month(2019, 2),
        )

        self.assertEqual(
            DateRange(
                lower_bound_inclusive_date=datetime.date(2019, 3, 1),
                upper_bound_exclusive_date=datetime.date(2019, 4, 1),
            ),
            self.multi_month_range.portion_overlapping_with_month(2019, 3),
        )

        self.assertEqual(
            DateRange(
                lower_bound_inclusive_date=datetime.date(2019, 4, 1),
                upper_bound_exclusive_date=datetime.date(2019, 4, 10),
            ),
            self.multi_month_range.portion_overlapping_with_month(2019, 4),
        )

    def test_for_year_of_date(self) -> None:
        year_range = DateRange.for_year_of_date(datetime.date(2019, 12, 4))
        self.assertEqual(
            (datetime.date(2019, 1, 1), datetime.date(2020, 1, 1)),
            (
                year_range.lower_bound_inclusive_date,
                year_range.upper_bound_exclusive_date,
            ),
        )

    def test_for_year_of_date_early_month(self) -> None:
        year_range = DateRange.for_year_of_date(datetime.date(2019, 1, 25))
        self.assertEqual(
            (datetime.date(2019, 1, 1), datetime.date(2020, 1, 1)),
            (
                year_range.lower_bound_inclusive_date,
                year_range.upper_bound_exclusive_date,
            ),
        )


class TestNonNegativeDateRange(unittest.TestCase):
    """Tests for NonNegativeDateRange"""

    def test_negative_raises_value_error(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "Parsed date has to be in chronological order"
        ):
            NonNegativeDateRange(
                lower_bound_inclusive_date=datetime.date(2019, 2, 3),
                upper_bound_exclusive_date=datetime.date(2019, 2, 2),
            )


class TestDateRangeDiff(unittest.TestCase):
    """Tests for DateRangeDiff"""

    def test_non_overlapping_ranges(self) -> None:
        range_1 = DateRange.for_month(2019, 2)
        range_2 = DateRange.for_month(2019, 3)

        time_range_diff = DateRangeDiff(range_1, range_2)

        self.assertEqual(None, time_range_diff.overlapping_range)
        self.assertEqual([range_1], time_range_diff.range_1_non_overlapping_parts)
        self.assertEqual([range_2], time_range_diff.range_2_non_overlapping_parts)

    def test_exactly_overlapping_ranges(self) -> None:
        range_1 = DateRange.for_month(2019, 2)
        range_2 = DateRange.for_month(2019, 2)

        time_range_diff = DateRangeDiff(range_1, range_2)

        self.assertEqual(range_1, time_range_diff.overlapping_range)
        self.assertEqual([], time_range_diff.range_1_non_overlapping_parts)
        self.assertEqual([], time_range_diff.range_2_non_overlapping_parts)

    def test_range_fully_overlaps_other(self) -> None:
        range_1 = DateRange(datetime.date(2019, 2, 5), datetime.date(2019, 5, 2))
        range_2 = DateRange(datetime.date(2019, 3, 1), datetime.date(2019, 3, 5))

        time_range_diff = DateRangeDiff(range_1, range_2)

        self.assertEqual(range_2, time_range_diff.overlapping_range)
        self.assertEqual(
            [
                DateRange(datetime.date(2019, 2, 5), datetime.date(2019, 3, 1)),
                DateRange(datetime.date(2019, 3, 5), datetime.date(2019, 5, 2)),
            ],
            time_range_diff.range_1_non_overlapping_parts,
        )
        self.assertEqual([], time_range_diff.range_2_non_overlapping_parts)

        time_range_diff = DateRangeDiff(range_2, range_1)
        self.assertEqual(range_2, time_range_diff.overlapping_range)
        self.assertEqual([], time_range_diff.range_1_non_overlapping_parts)
        self.assertEqual(
            [
                DateRange(datetime.date(2019, 2, 5), datetime.date(2019, 3, 1)),
                DateRange(datetime.date(2019, 3, 5), datetime.date(2019, 5, 2)),
            ],
            time_range_diff.range_2_non_overlapping_parts,
        )

    def test_partially_overlapping_ranges(self) -> None:
        range_1 = DateRange(datetime.date(2019, 2, 5), datetime.date(2019, 5, 2))
        range_2 = DateRange(datetime.date(2019, 3, 1), datetime.date(2019, 6, 5))

        time_range_diff = DateRangeDiff(range_1, range_2)

        self.assertEqual(
            DateRange(datetime.date(2019, 3, 1), datetime.date(2019, 5, 2)),
            time_range_diff.overlapping_range,
        )
        self.assertEqual(
            [DateRange(datetime.date(2019, 2, 5), datetime.date(2019, 3, 1))],
            time_range_diff.range_1_non_overlapping_parts,
        )
        self.assertEqual(
            [DateRange(datetime.date(2019, 5, 2), datetime.date(2019, 6, 5))],
            time_range_diff.range_2_non_overlapping_parts,
        )

        time_range_diff = DateRangeDiff(range_2, range_1)
        self.assertEqual(
            DateRange(datetime.date(2019, 3, 1), datetime.date(2019, 5, 2)),
            time_range_diff.overlapping_range,
        )
        self.assertEqual(
            [DateRange(datetime.date(2019, 5, 2), datetime.date(2019, 6, 5))],
            time_range_diff.range_1_non_overlapping_parts,
        )
        self.assertEqual(
            [DateRange(datetime.date(2019, 2, 5), datetime.date(2019, 3, 1))],
            time_range_diff.range_2_non_overlapping_parts,
        )
