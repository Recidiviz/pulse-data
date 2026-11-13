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
"""Loads the holidays a state observes from its calendar file, for the
release-day shift, together with the years the file covers.

For example, with calendars/us_az_holidays.yaml holding

    covers:
      first_year: 2026
      last_year: 2026
    holidays:
      - date: 2026-07-03
        name: Independence Day (observed; falls on a Saturday)
      - date: 2026-09-07
        name: Labor Day

load_state_holiday_calendar(StateCode.US_AZ) returns a StateHolidayCalendar
whose holidays hold 2026-07-03 and 2026-09-07 and which covers 2026 only. A
release date outside the covered years is a loud error, because a calendar
that is silent about a year would let a holiday pass as a workday.
"""

import datetime
import os

import attr

from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.sentence_calculator import calendars
from recidiviz.sentence_calculator.models.rules import (
    yaml_dict_from_path_with_unique_keys,
)


def state_holidays_yaml_path(state_code: StateCode) -> str:
    """Returns the path of the calendar file for state_code, e.g.
    .../calendars/us_az_holidays.yaml for US_AZ."""
    return os.path.join(
        os.path.dirname(calendars.__file__),
        f"{state_code.value.lower()}_holidays.yaml",
    )


@attr.define(frozen=True, kw_only=True)
class StateHolidayCalendar:
    """The holidays a state observes and the years its calendar file covers."""

    state_code: StateCode = attr.ib(validator=attr.validators.instance_of(StateCode))
    """The state whose holidays these are."""

    first_year: int = attr.ib(validator=attr_validators.is_positive_int)
    """The first year the calendar file lists holidays for."""

    last_year: int = attr.ib(validator=attr_validators.is_positive_int)
    """The last year the calendar file lists holidays for."""

    holidays: frozenset[datetime.date] = attr.ib(
        validator=attr_validators.is_frozenset_of(datetime.date)
    )
    """Every observed holiday in the covered years."""

    def __attrs_post_init__(self) -> None:
        if self.first_year > self.last_year:
            raise ValueError(
                f"The holiday calendar for [{self.state_code.value}] covers "
                f"[{self.first_year}] through [{self.last_year}], which is empty."
            )
        stray_dates = sorted(d for d in self.holidays if not self.covers(d))
        if stray_dates:
            raise ValueError(
                f"The holiday calendar for [{self.state_code.value}] covers "
                f"[{self.first_year}] through [{self.last_year}] but lists "
                f"{[d.isoformat() for d in stray_dates]}. Extend the covered years "
                f"or drop those dates."
            )

    def covers(self, day: datetime.date) -> bool:
        """Returns whether day falls in a year the calendar lists holidays for."""
        return self.first_year <= day.year <= self.last_year

    def check_covers(self, day: datetime.date) -> None:
        """Raises if day falls outside the covered years, naming the file to
        extend."""
        if not self.covers(day):
            raise ValueError(
                f"Date [{day.isoformat()}] is outside the holiday calendar for "
                f"[{self.state_code.value}], which covers [{self.first_year}] "
                f"through [{self.last_year}]. Add that year's holidays to "
                f"[{state_holidays_yaml_path(self.state_code)}] and extend its "
                f"covered years."
            )


def load_state_holiday_calendar(state_code: StateCode) -> StateHolidayCalendar:
    """Returns the holidays state_code observes and the years its file covers,
    raising if the state has no calendar file, the file repeats a key, the file
    lists a date twice, or a date falls outside the covered years."""
    yaml_path = state_holidays_yaml_path(state_code)
    if not os.path.exists(yaml_path):
        raise ValueError(
            f"No holiday calendar for state [{state_code.value}]. Expected a file "
            f"at [{yaml_path}]."
        )
    file_dict = yaml_dict_from_path_with_unique_keys(yaml_path)
    covers_dict = file_dict.pop_dict("covers")
    first_year = covers_dict.pop("first_year", int)
    last_year = covers_dict.pop("last_year", int)
    if covers_dict:
        raise ValueError(
            f"Found unexpected keys under covers in [{yaml_path}]: "
            f"{repr(covers_dict.get())}"
        )
    holidays: list[datetime.date] = []
    for holiday_dict in file_dict.pop_dicts("holidays"):
        holidays.append(holiday_dict.pop("date", datetime.date))
        holiday_dict.pop("name", str)
        if holiday_dict:
            raise ValueError(
                f"Found unexpected keys in a holiday in [{yaml_path}]: "
                f"{repr(holiday_dict.get())}"
            )
    if file_dict:
        raise ValueError(
            f"Found unexpected keys in [{yaml_path}]: {repr(file_dict.get())}"
        )
    duplicate_dates = sorted({d for d in holidays if holidays.count(d) > 1})
    if duplicate_dates:
        raise ValueError(
            f"The holiday calendar [{yaml_path}] lists these dates more than once: "
            f"{[d.isoformat() for d in duplicate_dates]}. List each date once."
        )
    return StateHolidayCalendar(
        state_code=state_code,
        first_year=first_year,
        last_year=last_year,
        holidays=frozenset(holidays),
    )
