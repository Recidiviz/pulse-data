# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for the release-day shift."""

import unittest
from datetime import date

from recidiviz.common.constants.states import StateCode
from recidiviz.sentence_calculator.calendars.state_holiday_calendar import (
    StateHolidayCalendar,
)
from recidiviz.sentence_calculator.engine.release_day_shift import (
    ReleaseDayShiftDirection,
    is_workday,
    shift_release_date,
)
from recidiviz.sentence_calculator.models.tracked import TrackedValue

# Independence Day 2026 falls on a Saturday and is observed on Friday 2026-07-03.
_CALENDAR = StateHolidayCalendar(
    state_code=StateCode.US_XX,
    first_year=2026,
    last_year=2026,
    holidays=frozenset({date(2026, 7, 3), date(2026, 9, 7)}),
)


def _release_date(day: date) -> TrackedValue[date]:
    return TrackedValue.input(day, "earned_release_credit_date")


class TestIsWorkday(unittest.TestCase):
    """Tests for is_workday."""

    def test_weekday_that_is_not_a_holiday(self) -> None:
        self.assertTrue(is_workday(date(2026, 7, 2), calendar=_CALENDAR))

    def test_saturday_and_sunday(self) -> None:
        self.assertFalse(is_workday(date(2026, 7, 4), calendar=_CALENDAR))
        self.assertFalse(is_workday(date(2026, 7, 5), calendar=_CALENDAR))

    def test_weekday_holiday(self) -> None:
        self.assertFalse(is_workday(date(2026, 7, 3), calendar=_CALENDAR))

    def test_date_outside_the_covered_years_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"^Date \[2027-01-04\] is outside the holiday calendar"
        ):
            is_workday(date(2027, 1, 4), calendar=_CALENDAR)


class TestShiftReleaseDate(unittest.TestCase):
    """Tests for shift_release_date."""

    def test_workday_is_not_moved(self) -> None:
        release_date = _release_date(date(2026, 7, 2))

        self.assertIs(
            release_date,
            shift_release_date(
                release_date=release_date,
                direction=ReleaseDayShiftDirection.NEXT_WORKDAY,
                calendar=_CALENDAR,
            ),
        )

    def test_direction_none_never_moves_a_weekend_date(self) -> None:
        release_date = _release_date(date(2026, 7, 4))

        self.assertIs(
            release_date,
            shift_release_date(
                release_date=release_date,
                direction=ReleaseDayShiftDirection.NONE,
                calendar=_CALENDAR,
            ),
        )

    def test_saturday_before_a_friday_holiday_moves_back_to_thursday(self) -> None:
        shifted = shift_release_date(
            release_date=_release_date(date(2026, 7, 4)),
            direction=ReleaseDayShiftDirection.PREVIOUS_WORKDAY,
            calendar=_CALENDAR,
        )

        self.assertEqual(date(2026, 7, 2), shifted.value)
        self.assertEqual("earned_release_credit_date_shifted", shifted.label)
        self.assertEqual(
            "2026-07-04 moved to the previous workday 2026-07-02; skipped "
            "2026-07-04 (Saturday), 2026-07-03 (holiday)",
            shifted.formula,
        )

    def test_sunday_moves_forward_to_monday(self) -> None:
        shifted = shift_release_date(
            release_date=_release_date(date(2026, 7, 5)),
            direction=ReleaseDayShiftDirection.NEXT_WORKDAY,
            calendar=_CALENDAR,
        )

        self.assertEqual(date(2026, 7, 6), shifted.value)
        self.assertEqual(
            "2026-07-05 moved to the next workday 2026-07-06; skipped "
            "2026-07-05 (Sunday)",
            shifted.formula,
        )

    def test_monday_holiday_moves_back_over_the_weekend(self) -> None:
        """Labor Day 2026 is Monday 2026-09-07, so the previous workday is
        Friday 2026-09-04."""
        shifted = shift_release_date(
            release_date=_release_date(date(2026, 9, 7)),
            direction=ReleaseDayShiftDirection.PREVIOUS_WORKDAY,
            calendar=_CALENDAR,
        )

        self.assertEqual(date(2026, 9, 4), shifted.value)
        self.assertEqual(
            "2026-09-07 moved to the previous workday 2026-09-04; skipped "
            "2026-09-07 (holiday), 2026-09-06 (Sunday), 2026-09-05 (Saturday)",
            shifted.formula,
        )

    def test_shifted_date_derives_from_the_release_date(self) -> None:
        release_date = _release_date(date(2026, 7, 4))

        shifted = shift_release_date(
            release_date=release_date,
            direction=ReleaseDayShiftDirection.NEXT_WORKDAY,
            calendar=_CALENDAR,
        )

        self.assertEqual([release_date], list(shifted.sources))

    def test_shift_that_leaves_the_covered_years_raises(self) -> None:
        """A shift walks day by day, so a date near the end of the covered years
        can step into a year the calendar is silent about."""
        year_end_holiday = StateHolidayCalendar(
            state_code=StateCode.US_XX,
            first_year=2026,
            last_year=2026,
            holidays=frozenset({date(2026, 12, 31)}),
        )

        with self.assertRaisesRegex(
            ValueError, r"^Date \[2027-01-01\] is outside the holiday calendar"
        ):
            shift_release_date(
                release_date=_release_date(date(2026, 12, 31)),
                direction=ReleaseDayShiftDirection.NEXT_WORKDAY,
                calendar=year_end_holiday,
            )
