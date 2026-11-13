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
"""Moves a release date that falls on a weekend or a holiday to a workday.

For example, with 2026-07-03 observed as Independence Day, a release date of
Saturday 2026-07-04 shifted to the previous workday lands on Thursday
2026-07-02, because Friday is the holiday. Shifted to the next workday it lands
on Monday 2026-07-06. A release date already on a workday is returned as it is,
and a shift direction of NONE never moves a date. A date in a year the
calendar does not cover raises, because treating it as holiday-free would move
it to the wrong day without a trace.
"""

import datetime
from enum import Enum

from recidiviz.sentence_calculator.calendars.state_holiday_calendar import (
    StateHolidayCalendar,
)
from recidiviz.sentence_calculator.models.tracked import TrackedValue

# Monday is 0 and Sunday is 6 in datetime.date.weekday().
_WEEKEND_WEEKDAYS = {5, 6}


class ReleaseDayShiftDirection(Enum):
    """Which way a release date moves off a weekend or holiday."""

    NEXT_WORKDAY = "NEXT_WORKDAY"
    """Forward to the first workday on or after the date."""

    PREVIOUS_WORKDAY = "PREVIOUS_WORKDAY"
    """Back to the last workday on or before the date."""

    NONE = "NONE"
    """The date is never moved."""


def is_workday(day: datetime.date, *, calendar: StateHolidayCalendar) -> bool:
    """Returns whether day is a weekday that is not a holiday."""
    calendar.check_covers(day)
    return day.weekday() not in _WEEKEND_WEEKDAYS and day not in calendar.holidays


def shift_release_date(
    *,
    release_date: TrackedValue[datetime.date],
    direction: ReleaseDayShiftDirection,
    calendar: StateHolidayCalendar,
) -> TrackedValue[datetime.date]:
    """Returns release_date moved in direction until it lands on a workday,
    with the move recorded as a derivation from release_date. A date already on
    a workday, or a direction of NONE, returns release_date unchanged."""
    if direction is ReleaseDayShiftDirection.NONE:
        return release_date
    if is_workday(release_date.value, calendar=calendar):
        return release_date
    step = _step_for_direction(direction)
    shifted = release_date.value
    skipped: list[str] = []
    while not is_workday(shifted, calendar=calendar):
        skipped.append(_describe_non_workday(shifted, calendar=calendar))
        shifted += step
    return release_date.derive(
        new_value=shifted,
        label=f"{release_date.label}_shifted",
        formula=(
            f"{release_date.value} moved to the {direction.value.lower().replace('_', ' ')} "
            f"{shifted}; skipped {', '.join(skipped)}"
        ),
    )


def _step_for_direction(direction: ReleaseDayShiftDirection) -> datetime.timedelta:
    """Returns the one-day step that moves a date in direction."""
    if direction is ReleaseDayShiftDirection.NEXT_WORKDAY:
        return datetime.timedelta(days=1)
    if direction is ReleaseDayShiftDirection.PREVIOUS_WORKDAY:
        return datetime.timedelta(days=-1)
    raise ValueError(f"Unexpected shift direction: [{direction}]")


def _describe_non_workday(day: datetime.date, *, calendar: StateHolidayCalendar) -> str:
    """Returns why day is not a workday, e.g. "2026-07-04 (Saturday)" or
    "2026-07-03 (holiday)"."""
    if day in calendar.holidays:
        return f"{day} (holiday)"
    return f"{day} ({day.strftime('%A')})"
