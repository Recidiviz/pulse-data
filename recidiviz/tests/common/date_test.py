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
from typing import Optional

import attr
import pandas as pd
from freezegun import freeze_time
from parameterized import parameterized

from recidiviz.common.date import (
    CriticalRangesBuilder,
    DateRange,
    DateRangeDiff,
    DurationMixin,
    NonNegativeDateRange,
    PotentiallyOpenDateRange,
    calendar_unit_date_diff,
    convert_critical_dates_to_time_spans,
    is_date_str,
    merge_sorted_date_ranges,
    munge_date_string,
    safe_strptime,
    split_range_by_birthdate,
    today_in_iso,
)


class TestCalendarUnitDateDiff(unittest.TestCase):
    """Tests for calendar_unit_date_diff"""

    # add list of accepted units for use in test cases
    accepted_units = ["days", "months", "years"]

    # common start and end date test cases
    start_date = datetime.date(2000, 1, 1)
    end_date = datetime.date(2001, 2, 2)
    year_diff = 1
    month_diff = 13
    day_diff = (end_date - start_date).days
    accepted_diffs = [day_diff, month_diff, year_diff]

    def test_valid_units(self) -> None:
        "Ensures that calendar_unit_date_diff accepts valid units"

        for unit in self.accepted_units:
            calendar_unit_date_diff(self.start_date, self.end_date, unit)
        with self.assertRaises(ValueError):
            calendar_unit_date_diff(self.start_date, self.end_date, "not_a_unit")

    def test_valid_date_types(self) -> None:
        """Ensures that calendar_unit_date_diff accepts valid date types and returns
        the same value regardless of type provided."""

        for unit in self.accepted_units:
            datetime_date_result = calendar_unit_date_diff(
                self.start_date, self.end_date, unit
            )
            datetime_datetime_result = calendar_unit_date_diff(
                pd.to_datetime(self.start_date), pd.to_datetime(self.end_date), unit
            )
            datetime_str_result = calendar_unit_date_diff(
                self.start_date.isoformat(), self.end_date.isoformat(), unit
            )
            assert (
                datetime_date_result == datetime_datetime_result == datetime_str_result
            )

    def test_valid_dates(self) -> None:
        "Ensure error raised if date is not valid"

        with self.assertRaises(ValueError):
            calendar_unit_date_diff("not_a_date", "not_a_date_either", "days")

    def test_start_date_precede_end_date(self) -> None:
        "Ensure error raised if start date is after end date"

        with self.assertRaises(ValueError):
            calendar_unit_date_diff(self.end_date, self.start_date, "days")

    def test_calendar_unit_date_diff(self) -> None:
        "Ensures that calendar_unit_date_diff returns the correct values"

        # same day
        for unit in self.accepted_units:
            self.assertEqual(
                calendar_unit_date_diff(self.start_date, self.start_date, unit),
                0,
            )

        # same day with not same time
        for unit in self.accepted_units:
            self.assertEqual(
                calendar_unit_date_diff(
                    self.start_date,
                    self.start_date + datetime.timedelta(hours=1),
                    unit,
                ),
                0,
            )

        # not same day
        for unit, value in zip(self.accepted_units, self.accepted_diffs):
            self.assertEqual(
                calendar_unit_date_diff(self.start_date, self.end_date, unit),
                value,
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


@attr.define(kw_only=True)
class _SimpleDurationObject(DurationMixin):
    id: str
    start_date_inclusive: datetime.date
    end_date_exclusive: Optional[datetime.date]

    @property
    def duration(self) -> DateRange:
        return DateRange.from_maybe_open_range(
            self.start_date_inclusive, self.end_date_exclusive
        )


@attr.define(kw_only=True)
class _SimpleDurationObjectAnother(DurationMixin):
    another_id: str
    start_date_inclusive: datetime.date
    end_date_exclusive: Optional[datetime.date]

    @property
    def duration(self) -> DateRange:
        return DateRange.from_maybe_open_range(
            self.start_date_inclusive, self.end_date_exclusive
        )


class TestCriticalRangesBuilder(unittest.TestCase):
    """Tests for CriticalRangesBuilder."""

    DATE_1 = datetime.date(2021, 1, 1)
    DATE_2 = datetime.date(2022, 2, 2)
    DATE_3 = datetime.date(2023, 3, 3)
    DATE_4 = datetime.date(2024, 4, 4)

    def test_empty(self) -> None:
        ranges_builder = CriticalRangesBuilder([])
        self.assertEqual([], ranges_builder.get_sorted_critical_ranges())

    def test_single_zero_day_range(self) -> None:
        obj_a = _SimpleDurationObject(
            id="A",
            start_date_inclusive=self.DATE_1,
            end_date_exclusive=self.DATE_1,
        )
        ranges_builder = CriticalRangesBuilder([obj_a])

        # Even though there are input ranges, the only input data is zero-day
        # ranges which effectively exist for zero time. Since there are no non-zero
        # ranges of time we can attribute objects to, we return no ranges.
        self.assertEqual([], ranges_builder.get_sorted_critical_ranges())

    def test_zero_day_range_fully_overlapped(self) -> None:
        obj_a = _SimpleDurationObject(
            id="A",
            start_date_inclusive=self.DATE_1,
            end_date_exclusive=self.DATE_3,
        )
        # Zero-day span
        obj_b = _SimpleDurationObject(
            id="B",
            start_date_inclusive=self.DATE_2,
            end_date_exclusive=self.DATE_2,
        )
        ranges_builder = CriticalRangesBuilder([obj_a, obj_b])

        expected_ranges = [
            DateRange(
                lower_bound_inclusive_date=self.DATE_1,
                upper_bound_exclusive_date=self.DATE_2,
            ),
            DateRange(
                lower_bound_inclusive_date=self.DATE_2,
                upper_bound_exclusive_date=self.DATE_3,
            ),
        ]
        self.assertEqual(expected_ranges, ranges_builder.get_sorted_critical_ranges())

        # Test objects overlapping with range - zero-day object doesn't overlap with
        # either.
        self.assertEqual(
            [obj_a],
            ranges_builder.get_objects_overlapping_with_critical_range(
                expected_ranges[0], _SimpleDurationObject
            ),
        )
        self.assertEqual(
            [obj_a],
            ranges_builder.get_objects_overlapping_with_critical_range(
                expected_ranges[1], _SimpleDurationObject
            ),
        )

    def test_zero_day_range_adjacent_to_others(self) -> None:
        obj_a = _SimpleDurationObject(
            id="A",
            start_date_inclusive=self.DATE_1,
            end_date_exclusive=self.DATE_2,
        )
        # Zero-day span
        obj_b = _SimpleDurationObject(
            id="B",
            start_date_inclusive=self.DATE_2,
            end_date_exclusive=self.DATE_2,
        )
        obj_c = _SimpleDurationObject(
            id="B",
            start_date_inclusive=self.DATE_2,
            end_date_exclusive=self.DATE_3,
        )
        ranges_builder = CriticalRangesBuilder([obj_c, obj_a, obj_b])

        expected_ranges = [
            DateRange(
                lower_bound_inclusive_date=self.DATE_1,
                upper_bound_exclusive_date=self.DATE_2,
            ),
            DateRange(
                lower_bound_inclusive_date=self.DATE_2,
                upper_bound_exclusive_date=self.DATE_3,
            ),
        ]
        self.assertEqual(expected_ranges, ranges_builder.get_sorted_critical_ranges())

        # Test objects overlapping with range - zero-day object doesn't overlap with
        # either.
        self.assertEqual(
            [obj_a],
            ranges_builder.get_objects_overlapping_with_critical_range(
                expected_ranges[0], _SimpleDurationObject
            ),
        )
        self.assertEqual(
            [obj_c],
            ranges_builder.get_objects_overlapping_with_critical_range(
                expected_ranges[1], _SimpleDurationObject
            ),
        )

    def test_single_open_range(self) -> None:
        obj_a = _SimpleDurationObject(
            id="A",
            start_date_inclusive=self.DATE_1,
            end_date_exclusive=None,
        )
        ranges_builder = CriticalRangesBuilder([obj_a])

        expected_range = PotentiallyOpenDateRange(
            lower_bound_inclusive_date=self.DATE_1,
            upper_bound_exclusive_date=None,
        )

        self.assertEqual(
            [expected_range],
            ranges_builder.get_sorted_critical_ranges(),
        )

        # Test objects overlapping with range
        self.assertEqual(
            [obj_a],
            ranges_builder.get_objects_overlapping_with_critical_range(
                expected_range, _SimpleDurationObject
            ),
        )

        # Test objects preceding critical range
        self.assertEqual(
            [],
            ranges_builder.get_objects_directly_preceding_range(
                expected_range, _SimpleDurationObject
            ),
        )

        # Test objects following critical range
        self.assertEqual(
            [],
            ranges_builder.get_objects_directly_preceding_range(
                expected_range, _SimpleDurationObject
            ),
        )

    def test_single_closed_range(self) -> None:
        obj_a = _SimpleDurationObject(
            id="A",
            start_date_inclusive=self.DATE_1,
            end_date_exclusive=self.DATE_2,
        )
        ranges_builder = CriticalRangesBuilder([obj_a])

        expected_range = DateRange(
            lower_bound_inclusive_date=self.DATE_1,
            upper_bound_exclusive_date=self.DATE_2,
        )

        self.assertEqual(
            [expected_range],
            ranges_builder.get_sorted_critical_ranges(),
        )

        # Test objects overlapping with range
        self.assertEqual(
            [obj_a],
            ranges_builder.get_objects_overlapping_with_critical_range(
                expected_range, _SimpleDurationObject
            ),
        )

        # Test objects preceding critical range
        self.assertEqual(
            [],
            ranges_builder.get_objects_directly_preceding_range(
                expected_range, _SimpleDurationObject
            ),
        )

        # Test objects following critical range
        self.assertEqual(
            [],
            ranges_builder.get_objects_directly_preceding_range(
                expected_range, _SimpleDurationObject
            ),
        )

    def test_several_contiguous(self) -> None:
        obj_a = _SimpleDurationObject(
            id="A",
            start_date_inclusive=self.DATE_1,
            end_date_exclusive=self.DATE_2,
        )
        obj_b = _SimpleDurationObject(
            id="B",
            start_date_inclusive=self.DATE_2,
            end_date_exclusive=self.DATE_3,
        )
        obj_c = _SimpleDurationObject(
            id="C",
            start_date_inclusive=self.DATE_3,
            end_date_exclusive=None,
        )
        # This overlaps with first two objects
        obj_d = _SimpleDurationObject(
            id="D",
            start_date_inclusive=self.DATE_1,
            end_date_exclusive=self.DATE_3,
        )
        ranges_builder = CriticalRangesBuilder([obj_a, obj_b, obj_c, obj_d])

        expected_ranges = [
            DateRange(
                lower_bound_inclusive_date=self.DATE_1,
                upper_bound_exclusive_date=self.DATE_2,
            ),
            DateRange(
                lower_bound_inclusive_date=self.DATE_2,
                upper_bound_exclusive_date=self.DATE_3,
            ),
            PotentiallyOpenDateRange(
                lower_bound_inclusive_date=self.DATE_3,
                upper_bound_exclusive_date=None,
            ),
        ]
        self.assertEqual(expected_ranges, ranges_builder.get_sorted_critical_ranges())

        # Test objects overlapping with range
        self.assertEqual(
            [obj_a, obj_d],
            ranges_builder.get_objects_overlapping_with_critical_range(
                expected_ranges[0], _SimpleDurationObject
            ),
        )
        self.assertEqual(
            [obj_d, obj_b],
            ranges_builder.get_objects_overlapping_with_critical_range(
                expected_ranges[1], _SimpleDurationObject
            ),
        )
        self.assertEqual(
            [obj_c],
            ranges_builder.get_objects_overlapping_with_critical_range(
                expected_ranges[2], _SimpleDurationObject
            ),
        )
        self.assertEqual(
            [],
            ranges_builder.get_objects_overlapping_with_critical_range(
                expected_ranges[0], _SimpleDurationObjectAnother
            ),
        )

        # Test objects preceding critical range
        self.assertEqual(
            [],
            ranges_builder.get_objects_directly_preceding_range(
                expected_ranges[0], _SimpleDurationObject
            ),
        )
        self.assertEqual(
            [obj_a],
            ranges_builder.get_objects_directly_preceding_range(
                expected_ranges[1], _SimpleDurationObject
            ),
        )
        self.assertEqual(
            [obj_d, obj_b],
            ranges_builder.get_objects_directly_preceding_range(
                expected_ranges[2], _SimpleDurationObject
            ),
        )
        self.assertEqual(
            [],
            ranges_builder.get_objects_directly_preceding_range(
                expected_ranges[2], _SimpleDurationObjectAnother
            ),
        )

        # Test objects following critical range
        self.assertEqual(
            [obj_b],
            ranges_builder.get_objects_directly_following_range(
                expected_ranges[0], _SimpleDurationObject
            ),
        )
        self.assertEqual(
            [obj_c],
            ranges_builder.get_objects_directly_following_range(
                expected_ranges[1], _SimpleDurationObject
            ),
        )
        self.assertEqual(
            [],
            ranges_builder.get_objects_directly_following_range(
                expected_ranges[2], _SimpleDurationObject
            ),
        )
        self.assertEqual(
            [],
            ranges_builder.get_objects_directly_following_range(
                expected_ranges[0], _SimpleDurationObjectAnother
            ),
        )

    def test_several_with_gap(self) -> None:
        obj_a = _SimpleDurationObject(
            id="A",
            start_date_inclusive=self.DATE_1,
            end_date_exclusive=self.DATE_2,
        )
        obj_b = _SimpleDurationObject(
            id="B",
            start_date_inclusive=self.DATE_3,
            end_date_exclusive=self.DATE_4,
        )
        obj_c = _SimpleDurationObject(
            id="C",
            start_date_inclusive=self.DATE_4,
            end_date_exclusive=None,
        )
        ranges_builder = CriticalRangesBuilder([obj_c, obj_b, obj_a])

        expected_ranges = [
            DateRange(
                lower_bound_inclusive_date=self.DATE_1,
                upper_bound_exclusive_date=self.DATE_2,
            ),
            DateRange(
                lower_bound_inclusive_date=self.DATE_2,
                upper_bound_exclusive_date=self.DATE_3,
            ),
            DateRange(
                lower_bound_inclusive_date=self.DATE_3,
                upper_bound_exclusive_date=self.DATE_4,
            ),
            PotentiallyOpenDateRange(
                lower_bound_inclusive_date=self.DATE_4,
                upper_bound_exclusive_date=None,
            ),
        ]
        self.assertEqual(expected_ranges, ranges_builder.get_sorted_critical_ranges())

        # Test objects overlapping with range
        self.assertEqual(
            [obj_a],
            ranges_builder.get_objects_overlapping_with_critical_range(
                expected_ranges[0], _SimpleDurationObject
            ),
        )
        self.assertEqual(
            [],
            ranges_builder.get_objects_overlapping_with_critical_range(
                expected_ranges[1], _SimpleDurationObject
            ),
        )
        self.assertEqual(
            [obj_b],
            ranges_builder.get_objects_overlapping_with_critical_range(
                expected_ranges[2], _SimpleDurationObject
            ),
        )
        self.assertEqual(
            [obj_c],
            ranges_builder.get_objects_overlapping_with_critical_range(
                expected_ranges[3], _SimpleDurationObject
            ),
        )

        gap_range = expected_ranges[1]
        # Test objects preceding / following gap range
        self.assertEqual(
            [obj_a],
            ranges_builder.get_objects_directly_preceding_range(
                gap_range, _SimpleDurationObject
            ),
        )
        self.assertEqual(
            [obj_b],
            ranges_builder.get_objects_directly_following_range(
                gap_range, _SimpleDurationObject
            ),
        )

    def test_multiple_open_ranges(self) -> None:
        obj_a = _SimpleDurationObject(
            id="A",
            start_date_inclusive=self.DATE_1,
            end_date_exclusive=self.DATE_2,
        )
        obj_b = _SimpleDurationObject(
            id="B",
            start_date_inclusive=self.DATE_3,
            end_date_exclusive=self.DATE_4,
        )
        obj_c = _SimpleDurationObject(
            id="C",
            start_date_inclusive=self.DATE_2,
            end_date_exclusive=None,
        )
        # This overlaps with first two objects
        obj_d = _SimpleDurationObject(
            id="D",
            start_date_inclusive=self.DATE_3,
            end_date_exclusive=None,
        )
        ranges_builder = CriticalRangesBuilder([obj_d, obj_b, obj_c, obj_a])

        expected_ranges = [
            DateRange(
                lower_bound_inclusive_date=self.DATE_1,
                upper_bound_exclusive_date=self.DATE_2,
            ),
            DateRange(
                lower_bound_inclusive_date=self.DATE_2,
                upper_bound_exclusive_date=self.DATE_3,
            ),
            DateRange(
                lower_bound_inclusive_date=self.DATE_3,
                upper_bound_exclusive_date=self.DATE_4,
            ),
            PotentiallyOpenDateRange(
                lower_bound_inclusive_date=self.DATE_4,
                upper_bound_exclusive_date=None,
            ),
        ]
        self.assertEqual(expected_ranges, ranges_builder.get_sorted_critical_ranges())

        # Test objects overlapping with range
        self.assertEqual(
            [obj_a],
            ranges_builder.get_objects_overlapping_with_critical_range(
                expected_ranges[0], _SimpleDurationObject
            ),
        )
        self.assertEqual(
            [obj_c],
            ranges_builder.get_objects_overlapping_with_critical_range(
                expected_ranges[1], _SimpleDurationObject
            ),
        )
        self.assertEqual(
            [obj_c, obj_b, obj_d],
            ranges_builder.get_objects_overlapping_with_critical_range(
                expected_ranges[2], _SimpleDurationObject
            ),
        )
        self.assertEqual(
            [obj_c, obj_d],
            ranges_builder.get_objects_overlapping_with_critical_range(
                expected_ranges[3], _SimpleDurationObject
            ),
        )

        # Test objects preceding critical range
        self.assertEqual(
            [],
            ranges_builder.get_objects_directly_preceding_range(
                expected_ranges[0], _SimpleDurationObject
            ),
        )
        self.assertEqual(
            [obj_a],
            ranges_builder.get_objects_directly_preceding_range(
                expected_ranges[1], _SimpleDurationObject
            ),
        )
        self.assertEqual(
            [],
            ranges_builder.get_objects_directly_preceding_range(
                expected_ranges[2], _SimpleDurationObject
            ),
        )
        self.assertEqual(
            [obj_b],
            ranges_builder.get_objects_directly_preceding_range(
                expected_ranges[3], _SimpleDurationObject
            ),
        )

        # Test objects following critical range
        self.assertEqual(
            [obj_c],
            ranges_builder.get_objects_directly_following_range(
                expected_ranges[0], _SimpleDurationObject
            ),
        )
        self.assertEqual(
            [obj_b, obj_d],
            ranges_builder.get_objects_directly_following_range(
                expected_ranges[1], _SimpleDurationObject
            ),
        )
        self.assertEqual(
            [],
            ranges_builder.get_objects_directly_following_range(
                expected_ranges[2], _SimpleDurationObject
            ),
        )
        self.assertEqual(
            [],
            ranges_builder.get_objects_directly_following_range(
                expected_ranges[3], _SimpleDurationObject
            ),
        )

    def test_mixed_types(self) -> None:
        obj_a = _SimpleDurationObject(
            id="A",
            start_date_inclusive=self.DATE_1,
            end_date_exclusive=self.DATE_3,
        )
        obj_b = _SimpleDurationObjectAnother(
            another_id="B",
            start_date_inclusive=self.DATE_2,
            end_date_exclusive=self.DATE_4,
        )
        ranges_builder = CriticalRangesBuilder([obj_a, obj_b])

        expected_ranges = [
            DateRange(
                lower_bound_inclusive_date=self.DATE_1,
                upper_bound_exclusive_date=self.DATE_2,
            ),
            DateRange(
                lower_bound_inclusive_date=self.DATE_2,
                upper_bound_exclusive_date=self.DATE_3,
            ),
            DateRange(
                lower_bound_inclusive_date=self.DATE_3,
                upper_bound_exclusive_date=self.DATE_4,
            ),
        ]
        self.assertEqual(expected_ranges, ranges_builder.get_sorted_critical_ranges())

        # Test objects overlapping with range
        self.assertEqual(
            [obj_a],
            ranges_builder.get_objects_overlapping_with_critical_range(
                expected_ranges[0], _SimpleDurationObject
            ),
        )
        self.assertEqual(
            [],
            ranges_builder.get_objects_overlapping_with_critical_range(
                expected_ranges[0], _SimpleDurationObjectAnother
            ),
        )
        self.assertEqual(
            [obj_a],
            ranges_builder.get_objects_overlapping_with_critical_range(
                expected_ranges[1], _SimpleDurationObject
            ),
        )
        self.assertEqual(
            [obj_b],
            ranges_builder.get_objects_overlapping_with_critical_range(
                expected_ranges[1], _SimpleDurationObjectAnother
            ),
        )
        self.assertEqual(
            [],
            ranges_builder.get_objects_overlapping_with_critical_range(
                expected_ranges[2], _SimpleDurationObject
            ),
        )
        self.assertEqual(
            [obj_b],
            ranges_builder.get_objects_overlapping_with_critical_range(
                expected_ranges[2], _SimpleDurationObjectAnother
            ),
        )
