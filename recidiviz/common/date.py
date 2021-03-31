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
# ============================================================================
"""Utils for parsing dates."""
from abc import ABCMeta, abstractmethod
import datetime
import re
from typing import List, Optional, Tuple

import attr

# Date Parsing


def snake_case_datetime(dt: datetime.datetime) -> str:
    """Converts a datetime to snake case format, e.g '2020_05_17_10_31_08_693498'. Friendly for BQ table names or cloud
    task ids."""
    return dt.strftime("%Y_%m_%d_%H_%M_%S_%f")


def munge_date_string(date_string: str) -> str:
    """Transforms the input date string so it can be parsed, if necessary"""
    date_string = re.sub(r"\b00:00\s*[Aa][Mm]\b", "12:00 AM", date_string)
    return re.sub(
        r"^((?P<year>-?\d+)y)?\s*((?P<month>-?\d+)m)?\s*((?P<day>-?\d+)d)?$",
        _date_component_match,
        date_string,
        flags=re.IGNORECASE,
    )


def _date_component_match(match: re.Match) -> str:
    components = []

    if match.group("year"):
        components.append("{year}year".format(year=match.group("year")))
    if match.group("month"):
        components.append("{month}month".format(month=match.group("month")))
    if match.group("day"):
        components.append("{day}day".format(day=match.group("day")))

    return " ".join(components)


# Date Manipulation


def year_and_month_for_today() -> Tuple[int, int]:
    """Returns the year and month of today's date."""
    today = datetime.date.today()

    return today.year, today.month


def date_or_tomorrow(date: Optional[datetime.date]) -> datetime.date:
    """Returns the date if set, otherwise tomorrow"""
    return date if date else datetime.date.today() + datetime.timedelta(days=1)


def first_day_of_month(date: datetime.date) -> datetime.date:
    """Returns the date corresponding to the first day of the month for the given date."""
    year = date.year
    month = date.month

    return datetime.date(year, month, 1)


def last_day_of_month(date: datetime.date) -> datetime.date:
    """Returns the date corresponding to the last day of the month for the given date."""
    first_of_next_month = first_day_of_next_month(date)
    return first_of_next_month - datetime.timedelta(days=1)


def first_day_of_next_month(date: datetime.date) -> datetime.date:
    """Returns the date corresponding to the first day of the next month for the given date."""
    next_month_date = date.replace(day=28) + datetime.timedelta(days=4)
    return next_month_date.replace(day=1)


def first_day_of_next_year(date: datetime.date) -> datetime.date:
    """Returns the date corresponding to the first day of the first month of the next year for the given date."""
    return first_day_of_next_month(date.replace(month=12))


@attr.s
class DateRange:
    """Object representing a range of dates."""

    lower_bound_inclusive_date: datetime.date = attr.ib()
    upper_bound_exclusive_date: datetime.date = attr.ib()

    def get_months_range_overlaps_at_all(self) -> List[Tuple[int, int]]:
        """Returns a list of (year, month) pairs where any portion of the month overlaps the date range."""

        months_range_overlaps: List[Tuple[int, int]] = []
        month_date = self.lower_bound_inclusive_date

        while month_date < self.upper_bound_exclusive_date:
            months_range_overlaps.append((month_date.year, month_date.month))
            month_date = first_day_of_next_month(month_date)

        return months_range_overlaps

    @classmethod
    def for_year_of_date(cls, date: datetime.date) -> "DateRange":
        return cls.for_year(date.year)

    @classmethod
    def for_year(cls, year: int) -> "DateRange":
        start_of_year = datetime.date(year, 1, 1)
        start_of_next_year = first_day_of_next_year(start_of_year)

        return cls(
            lower_bound_inclusive_date=start_of_year,
            upper_bound_exclusive_date=start_of_next_year,
        )

    @classmethod
    def for_month_of_date(cls, date: datetime.date) -> "DateRange":
        return cls.for_month(date.year, date.month)

    @classmethod
    def for_month(cls, year: int, month: int) -> "DateRange":
        start_of_month = datetime.date(year, month, 1)
        start_of_next_month = first_day_of_next_month(start_of_month)

        return cls(
            lower_bound_inclusive_date=start_of_month,
            upper_bound_exclusive_date=start_of_next_month,
        )

    @classmethod
    def for_day(cls, date: datetime.date) -> "DateRange":
        return cls(date, date + datetime.timedelta(days=1))

    @classmethod
    def from_maybe_open_range(
        cls, start_date: datetime.date, end_date: Optional[datetime.date]
    ) -> "DateRange":
        return cls(start_date, date_or_tomorrow(end_date))

    def portion_overlapping_with_month(
        self, year: int, month: int
    ) -> Optional["DateRange"]:
        month_range = DateRange.for_month(year, month)
        return DateRangeDiff(range_1=self, range_2=month_range).overlapping_range

    def contains_day(self, day: datetime.date) -> bool:
        day_range = self.for_day(day)
        overlapping_range = DateRangeDiff(
            range_1=day_range, range_2=self
        ).overlapping_range
        return overlapping_range is not None


@attr.s
class NonNegativeDateRange(DateRange):
    def __attrs_post_init__(self) -> None:
        if self.lower_bound_inclusive_date > self.upper_bound_exclusive_date:
            raise ValueError(
                f"Parsed date has to be in chronological order. "
                f"Current order: {self.lower_bound_inclusive_date}, {self.upper_bound_exclusive_date}"
            )


@attr.s
class DateRangeDiff:
    """Utility class for representing the difference between two date ranges."""

    range_1: DateRange = attr.ib()
    range_2: DateRange = attr.ib()

    # Date range that is shared between the two ranges
    @property
    def overlapping_range(self) -> Optional[DateRange]:
        lower_bound_inclusive_date = max(
            self.range_1.lower_bound_inclusive_date,
            self.range_2.lower_bound_inclusive_date,
        )
        upper_bound_exclusive_date = min(
            self.range_1.upper_bound_exclusive_date,
            self.range_2.upper_bound_exclusive_date,
        )

        if upper_bound_exclusive_date <= lower_bound_inclusive_date:
            return None

        return DateRange(
            lower_bound_inclusive_date=lower_bound_inclusive_date,
            upper_bound_exclusive_date=upper_bound_exclusive_date,
        )

    # Date ranges in range_1 that do not overlap with range_2
    @property
    def range_1_non_overlapping_parts(self) -> List[DateRange]:
        parts = []
        if (
            self.range_1.lower_bound_inclusive_date
            < self.range_2.lower_bound_inclusive_date
        ):
            parts.append(
                DateRange(
                    lower_bound_inclusive_date=self.range_1.lower_bound_inclusive_date,
                    upper_bound_exclusive_date=self.range_2.lower_bound_inclusive_date,
                )
            )

        if (
            self.range_1.upper_bound_exclusive_date
            > self.range_2.upper_bound_exclusive_date
        ):
            parts.append(
                DateRange(
                    lower_bound_inclusive_date=self.range_2.upper_bound_exclusive_date,
                    upper_bound_exclusive_date=self.range_1.upper_bound_exclusive_date,
                )
            )
        return parts

    # Date ranges in range_2 that do not overlap with range_1
    @property
    def range_2_non_overlapping_parts(self) -> List[DateRange]:
        parts = []
        if (
            self.range_2.lower_bound_inclusive_date
            < self.range_1.lower_bound_inclusive_date
        ):
            parts.append(
                DateRange(
                    lower_bound_inclusive_date=self.range_2.lower_bound_inclusive_date,
                    upper_bound_exclusive_date=self.range_1.lower_bound_inclusive_date,
                )
            )

        if (
            self.range_2.upper_bound_exclusive_date
            > self.range_1.upper_bound_exclusive_date
        ):
            parts.append(
                DateRange(
                    lower_bound_inclusive_date=self.range_1.upper_bound_exclusive_date,
                    upper_bound_exclusive_date=self.range_2.upper_bound_exclusive_date,
                )
            )
        return parts


class DurationMixin(metaclass=ABCMeta):
    """Mixin to use if the given object has a duration"""

    @property
    @abstractmethod
    def start_date_inclusive(self) -> Optional[datetime.date]:
        """The object's duration start date, if set."""

    @property
    @abstractmethod
    def end_date_exclusive(self) -> Optional[datetime.date]:
        """The object's duration end date, if set."""

    @property
    @abstractmethod
    def duration(self) -> DateRange:
        """The object's duration, returned as a DateRange"""


def is_date_str(potential_date_str: str) -> bool:
    """Returns True if the string is an ISO-formatted date, (e.g. '2019-09-25'), False otherwise."""
    try:
        datetime.datetime.strptime(potential_date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def is_between_date_strs_inclusive(
    *,
    upper_bound_date: Optional[str],
    lower_bound_date: Optional[str],
    date_of_interest: str,
) -> bool:
    """Returns true if the provided |date_of_interest| is between the provided |upper_bound_date| and
    |lower_bound_date|.
    """

    if (lower_bound_date is None or date_of_interest >= lower_bound_date) and (
        upper_bound_date is None or date_of_interest <= upper_bound_date
    ):
        return True
    return False
