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

from freezegun import freeze_time
from parameterized import parameterized

from recidiviz.common.date import (
    DateRange,
    DateRangeDiff,
    NonNegativeDateRange,
    PotentiallyOpenDateRange,
    convert_critical_dates_to_time_spans,
    is_date_str,
    merge_sorted_date_ranges,
    munge_date_string,
    safe_strptime,
    split_range_by_birthdate,
    today_in_iso,
)


class TestMungeDateString(unittest.TestCase):
    """Tests for munge_date_string"""

    @parameterized.expand(
        [
            ("normal", "1y 1m 1d", "1year 1month 1day"),
            ("no_munge", "1year 1month 1day", "1year 1month 1day"),
            ("no_year", "10M 12D", "10month 12day"),
            ("no_month", "9Y 28D", "9year 28day"),
            ("no_day", "4y 3m", "4year 3month"),
            ("zero_am", "Jan 1, 2018 00:00AM", "Jan 1, 2018 12:00 AM"),
        ]
    )
    def test_munge_date_string(
        self, _name: str, given_string: str, expected_result: str
    ) -> None:
        self.assertEqual(munge_date_string(given_string), expected_result)


def test_today_in_iso() -> None:
    assert is_date_str(today_in_iso())


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

    def test_safe_strptime_valid_date(self) -> None:
        date_string = "2022-03-14"
        date_format = "%Y-%m-%d"
        self.assertEqual(
            datetime.datetime(2022, 3, 14), safe_strptime(date_string, date_format)
        )

    def test_safe_strptime_invalid_date(self) -> None:
        date_string = "None"
        date_format = "%Y-%m-%d"
        self.assertEqual(None, safe_strptime(date_string, date_format))

    def test_safe_strptime_none_date(self) -> None:
        date_string = None
        date_format = "%Y-%m-%d"
        self.assertEqual(None, safe_strptime(date_string, date_format))

    def test_safe_strptime_invalid_format(self) -> None:
        date_string = "2022-03-14"
        date_format = "%Y-%m-%d %HH:%MM:%SS"
        self.assertEqual(None, safe_strptime(date_string, date_format))


class TestSplitRangeByBirthdate(unittest.TestCase):
    """Tests for split_range_by_birthdate"""

    def test_split_range_by_birthdate(self) -> None:
        expected_results = [
            (datetime.date(2000, 1, 1), datetime.date(2001, 1, 1)),
            (datetime.date(2001, 1, 1), datetime.date(2002, 1, 1)),
            (datetime.date(2002, 1, 1), datetime.date(2003, 1, 1)),
            (datetime.date(2003, 1, 1), datetime.date(2004, 1, 1)),
            (datetime.date(2004, 1, 1), datetime.date(2005, 1, 1)),
        ]
        results = list(
            split_range_by_birthdate(
                (datetime.date(2000, 1, 1), datetime.date(2005, 1, 1)),
                datetime.date(2000, 1, 1),
            )
        )
        self.assertListEqual(expected_results, results)

    @freeze_time("2020-05-01")
    def test_split_range_by_birthdate_open_today(self) -> None:
        expected_results = [
            (datetime.date(2019, 1, 1), datetime.date(2019, 5, 1)),
            (datetime.date(2019, 5, 1), datetime.date(2020, 5, 1)),
            (datetime.date(2020, 5, 1), None),
        ]
        results = list(
            split_range_by_birthdate(
                (datetime.date(2019, 1, 1), None),
                datetime.date(1971, 5, 1),
            )
        )
        self.assertListEqual(expected_results, results)

    def test_split_range_by_birthdate_handles_leap_day(self) -> None:
        expected_results = [
            (datetime.date(2000, 1, 1), datetime.date(2000, 2, 29)),
            (datetime.date(2000, 2, 29), datetime.date(2001, 3, 1)),
            (datetime.date(2001, 3, 1), datetime.date(2002, 3, 1)),
            (datetime.date(2002, 3, 1), datetime.date(2003, 3, 1)),
            (datetime.date(2003, 3, 1), datetime.date(2004, 2, 29)),
            (datetime.date(2004, 2, 29), datetime.date(2005, 1, 1)),
        ]
        results = list(
            split_range_by_birthdate(
                (datetime.date(2000, 1, 1), datetime.date(2005, 1, 1)),
                datetime.date(2000, 2, 29),
            )
        )
        self.assertListEqual(expected_results, results)


class TestMergeSortedDateRange(unittest.TestCase):
    """Tests for merge_sorted_date_ranges"""

    def test_merge_sorted_date_ranges(self) -> None:
        expected_results = [
            NonNegativeDateRange(datetime.date(2000, 1, 1), datetime.date(2001, 1, 1)),
            NonNegativeDateRange(datetime.date(2001, 3, 1), datetime.date(2001, 9, 1)),
            NonNegativeDateRange(datetime.date(2002, 1, 1), datetime.date(2002, 10, 1)),
        ]
        results = merge_sorted_date_ranges(
            [
                NonNegativeDateRange(
                    datetime.date(2000, 1, 1), datetime.date(2001, 1, 1)
                ),
                NonNegativeDateRange(
                    datetime.date(2001, 3, 1), datetime.date(2001, 6, 1)
                ),
                NonNegativeDateRange(
                    datetime.date(2001, 6, 1), datetime.date(2001, 9, 1)
                ),
                NonNegativeDateRange(
                    datetime.date(2002, 1, 1), datetime.date(2002, 5, 1)
                ),
                NonNegativeDateRange(
                    datetime.date(2002, 5, 1), datetime.date(2002, 10, 1)
                ),
            ]
        )
        self.assertListEqual(expected_results, results)


class TestConvertCriticalDatesToTimeSpans(unittest.TestCase):
    """Tests for convert_critical_dates_to_time_spans"""

    def test_convert_critical_dates_to_time_spans(self) -> None:
        expected_results = [
            DateRange(datetime.date(2000, 1, 1), datetime.date(2001, 1, 1)),
            DateRange(datetime.date(2001, 1, 1), datetime.date(2001, 3, 1)),
            DateRange(datetime.date(2001, 3, 1), datetime.date(2001, 5, 1)),
        ]
        results = convert_critical_dates_to_time_spans(
            critical_dates=[
                datetime.date(2000, 1, 1),
                datetime.date(2001, 1, 1),
                datetime.date(2001, 3, 1),
                datetime.date(2001, 5, 1),
            ],
            has_open_end_date=False,
        )
        self.assertListEqual(expected_results, results)

    def test_convert_critical_dates_to_time_spans_with_open_end_date(self) -> None:
        expected_results = [
            DateRange(datetime.date(2000, 1, 1), datetime.date(2001, 1, 1)),
            DateRange(datetime.date(2001, 1, 1), datetime.date(2001, 3, 1)),
            DateRange(datetime.date(2001, 3, 1), datetime.date(2001, 5, 1)),
            PotentiallyOpenDateRange(datetime.date(2001, 5, 1), None),
        ]
        results = convert_critical_dates_to_time_spans(
            critical_dates=[
                datetime.date(2000, 1, 1),
                datetime.date(2001, 1, 1),
                datetime.date(2001, 3, 1),
                datetime.date(2001, 5, 1),
            ],
            has_open_end_date=True,
        )
        self.assertListEqual(expected_results, results)

    def test_convert_critical_dates_to_time_spans_unsorted(self) -> None:
        expected_results = [
            DateRange(datetime.date(2000, 1, 1), datetime.date(2001, 1, 1)),
            DateRange(datetime.date(2001, 1, 1), datetime.date(2001, 3, 1)),
            DateRange(datetime.date(2001, 3, 1), datetime.date(2001, 5, 1)),
        ]
        results = convert_critical_dates_to_time_spans(
            critical_dates=[
                datetime.date(2000, 1, 1),
                datetime.date(2001, 5, 1),
                datetime.date(2001, 1, 1),
                datetime.date(2001, 3, 1),
            ],
            has_open_end_date=False,
        )
        self.assertListEqual(expected_results, results)
