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
"""Tests for recidiviz/sentence_calculator/calendars/state_holiday_calendar.py."""

import unittest
from datetime import date

from recidiviz.common.constants.states import StateCode
from recidiviz.sentence_calculator.calendars.state_holiday_calendar import (
    StateHolidayCalendar,
    load_state_holiday_calendar,
    state_holidays_yaml_path,
)


class TestLoadStateHolidayCalendar(unittest.TestCase):
    """Tests for load_state_holiday_calendar and the Arizona calendar file."""

    def test_arizona_2026_holidays(self) -> None:
        """Arizona observes ten state holidays. In 2026 Independence Day falls
        on a Saturday and is observed on the Friday before."""
        holidays_2026 = {
            d
            for d in load_state_holiday_calendar(StateCode.US_AZ).holidays
            if d.year == 2026
        }

        self.assertEqual(
            {
                date(2026, 1, 1),
                date(2026, 1, 19),
                date(2026, 2, 16),
                date(2026, 5, 25),
                date(2026, 7, 3),
                date(2026, 9, 7),
                date(2026, 10, 12),
                date(2026, 11, 11),
                date(2026, 11, 26),
                date(2026, 12, 25),
            },
            holidays_2026,
        )

    def test_arizona_calendar_covers_2024_through_2030(self) -> None:
        calendar = load_state_holiday_calendar(StateCode.US_AZ)

        self.assertEqual(2024, calendar.first_year)
        self.assertEqual(2030, calendar.last_year)
        self.assertEqual(70, len(calendar.holidays))
        self.assertEqual(date(2024, 1, 1), min(calendar.holidays))
        self.assertEqual(date(2030, 12, 25), max(calendar.holidays))

    def test_state_without_a_calendar_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^No holiday calendar for state \[US_XX\]\. Expected a file at "
            r"\[.*/calendars/us_xx_holidays\.yaml\]\.$",
        ):
            load_state_holiday_calendar(StateCode.US_XX)

    def test_yaml_path_is_lowercased(self) -> None:
        self.assertRegex(
            state_holidays_yaml_path(StateCode.US_XX),
            r"/recidiviz/sentence_calculator/calendars/us_xx_holidays\.yaml$",
        )


class TestStateHolidayCalendar(unittest.TestCase):
    """Tests for the covered-years checks on StateHolidayCalendar."""

    def test_covers_only_the_listed_years(self) -> None:
        calendar = StateHolidayCalendar(
            state_code=StateCode.US_XX,
            first_year=2026,
            last_year=2027,
            holidays=frozenset({date(2026, 7, 3)}),
        )

        self.assertTrue(calendar.covers(date(2026, 1, 1)))
        self.assertTrue(calendar.covers(date(2027, 12, 31)))
        self.assertFalse(calendar.covers(date(2025, 12, 31)))
        self.assertFalse(calendar.covers(date(2028, 1, 1)))

    def test_check_covers_names_the_date_and_the_file(self) -> None:
        calendar = StateHolidayCalendar(
            state_code=StateCode.US_XX,
            first_year=2026,
            last_year=2027,
            holidays=frozenset(),
        )

        with self.assertRaisesRegex(
            ValueError,
            r"^Date \[2033-05-02\] is outside the holiday calendar for \[US_XX\], "
            r"which covers \[2026\] through \[2027\]\. Add that year's holidays to "
            r"\[.*/calendars/us_xx_holidays\.yaml\] and extend its covered years\.$",
        ):
            calendar.check_covers(date(2033, 5, 2))

    def test_holiday_outside_the_covered_years_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"covers \[2026\] through \[2026\] but lists \['2027-01-01'\]",
        ):
            StateHolidayCalendar(
                state_code=StateCode.US_XX,
                first_year=2026,
                last_year=2026,
                holidays=frozenset({date(2026, 7, 3), date(2027, 1, 1)}),
            )

    def test_empty_year_range_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, r"which is empty"):
            StateHolidayCalendar(
                state_code=StateCode.US_XX,
                first_year=2027,
                last_year=2026,
                holidays=frozenset(),
            )
