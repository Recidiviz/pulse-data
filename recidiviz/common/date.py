# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
import datetime
import itertools
import re
from abc import ABCMeta, abstractmethod
from calendar import isleap
from typing import (
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import attr
import pandas as pd
import pytz

from recidiviz.common import attr_validators
from recidiviz.utils.types import assert_type

DateOrDateTime = Union[datetime.date, datetime.datetime]

DateOrDateTimeT = TypeVar("DateOrDateTimeT", datetime.datetime, datetime.date)

# Date Parsing


def as_datetime(value: DateOrDateTime) -> datetime.datetime:
    """Returns the given value as a datetime to be compared against other datetimes."""
    if isinstance(value, datetime.datetime):
        return value
    return datetime.datetime(value.year, value.month, value.day)


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
        components.append(f"{match.group('year')}year")
    if match.group("month"):
        components.append(f"{match.group('month')}month")
    if match.group("day"):
        components.append(f"{match.group('day')}day")

    return " ".join(components)


# Date Manipulation


def year_and_month_for_today_us_eastern() -> Tuple[int, int]:
    """Returns the year and month of today's date."""
    today = current_date_us_eastern()

    return today.year, today.month


def tomorrow_us_eastern() -> datetime.date:
    """Returns tomorrow's date in the US/Eastern timezone."""
    return current_date_us_eastern() + datetime.timedelta(days=1)


def date_or_tomorrow_us_eastern(date: Optional[datetime.date]) -> datetime.date:
    """Returns the date (in US/Eastern timezone) if set, otherwise tomorrow."""
    return date if date else tomorrow_us_eastern()


def reset_to_midnight(dt: datetime.datetime) -> datetime.datetime:
    """Zeroes out the time info on the given datetime object, retaining timezone info."""
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def first_day_of_week(date: datetime.date) -> datetime.date:
    """Returns the first day of the week the provided date falls in (i.e. the Monday).
    Returns the same day if it is a Monday.
    """
    if isinstance(date, datetime.datetime):
        date = reset_to_midnight(date)

    # The weekday() returns the day of the week as an integer value, with Monday = 0,
    # Tuesday = 1, etc.
    return date - datetime.timedelta(days=date.weekday())


def first_day_of_month(date: DateOrDateTimeT) -> DateOrDateTimeT:
    """Returns the date corresponding to the first day of the month for the given date."""
    if isinstance(date, datetime.datetime):
        date = reset_to_midnight(date)

    return date.replace(day=1)


def last_day_of_month(date: DateOrDateTimeT) -> DateOrDateTimeT:
    """Returns the date corresponding to the last day of the month for the given date.

    If a datetime is provided, returns a datetime representing midnight on the desired
    day, with timezone information retained.
    """
    first_of_next_month = first_day_of_next_month(date)
    return first_of_next_month - datetime.timedelta(days=1)


def first_day_of_next_month(date: DateOrDateTimeT) -> DateOrDateTimeT:
    """Returns the date corresponding to the first day of the next month for the given
    date.

    If a datetime is provided, returns a datetime representing midnight on the desired
    day, with timezone information retained.
    """
    if isinstance(date, datetime.datetime):
        date = reset_to_midnight(date)
    next_month_date = date.replace(day=28) + datetime.timedelta(days=4)
    return next_month_date.replace(day=1)


def first_day_of_next_year(date: DateOrDateTimeT) -> DateOrDateTimeT:
    """Returns the date corresponding to the first day of the first month of the next
    year for the given date.

    If a datetime is provided, returns a datetime representing midnight on the desired
    day, with timezone information retained.
    """
    return first_day_of_next_month(date.replace(month=12))


def current_datetime_us_eastern() -> datetime.datetime:
    """Returns the current datetime in the US/Eastern timezone."""
    return datetime.datetime.now(tz=pytz.timezone("US/Eastern"))


def current_date_us_eastern() -> datetime.date:
    """Returns the current date in the US/Eastern timezone."""
    return datetime.datetime.now(tz=pytz.timezone("US/Eastern")).date()


def current_date_utc() -> datetime.date:
    """Returns the current date in the UTC timezone."""
    return datetime.datetime.now(tz=pytz.UTC).date()


def current_date_us_eastern_in_iso() -> str:
    """Returns the current date in the US/Eastern timezone, formatted as a string in
    ISO format.
    """
    return current_date_us_eastern().isoformat()


# helper function for getting days, months, or years between dates
def calendar_unit_date_diff(
    start_date: Union[datetime.date, datetime.datetime, str],
    end_date: Union[datetime.date, datetime.datetime, str],
    time_unit: str,
) -> int:
    """
    Returns an integer representing the number of full calendar units (specified with
    `time_unit`) that have passed between the `start_date` and `end_date`. Supported
    unit options are "days", "months", "years". As an example, with a start_date of
    "2018-02-15" and an end_date of "2018-04-14", 60 days have passed, but only 1 full
    calendar month has passed. With an end_date value of "2018-04-16" the output would
    change to "2" because two full calendar months have passed at that point.

    Params:
    ------
    start_date: string
        Date in string format representing the start date

    end_date: string
        Date in string format representing the end date

    time_unit: string
        Unit of time that date difference calculation outputs. Can be "years", "months",
        or "days"
    """

    # Check that time unit option is in supported list
    time_unit_options = ["days", "months", "years"]
    if time_unit not in time_unit_options:
        raise ValueError(f"Invalid time unit. Expected one of: {time_unit_options}")

    # Convert strings/dates to datetime objects
    if isinstance(start_date, (datetime.date, str)):
        start_date = pd.to_datetime(start_date)
    if isinstance(end_date, (datetime.date, str)):
        end_date = pd.to_datetime(end_date)

    # check dates formatted correctly, otherwise throw error
    # this is required for mypy since pd.to_datetime doesn't necessarily return
    # a datetime object.
    if not (
        isinstance(start_date, datetime.datetime)
        and isinstance(end_date, datetime.datetime)
    ):
        raise ValueError(
            "Could not format `start_date` or `end_date` as datetime objects."
        )

    # Check that start date precedes the end date
    if end_date < start_date:
        raise ValueError("`end_date` must be >= `start date`")

    if time_unit == "days":
        # get number of (complete) days between dates
        if end_date == start_date:
            # the timedelta between two identical datetimes is -1, so use zero here
            diff_result = 0
        else:
            diff_result = (end_date - start_date).days

    elif time_unit == "months":
        # get number of (complete) months between dates
        diff_result = (
            (end_date.year - start_date.year) * 12
            + (end_date.month - start_date.month)
            - (1 if end_date.day < start_date.day else 0)
        )

    else:  # time_unit == "years"
        # get number of (complete) years between dates
        diff_result = end_date.year - start_date.year
        if end_date.month < start_date.month:
            diff_result -= 1
        elif end_date.month == start_date.month and end_date.day < start_date.day:
            diff_result -= 1

    return diff_result


@attr.s(frozen=True)
class PotentiallyOpenDateTimeRange:
    """Object representing a range of time where the end datetime could be null (open)."""

    lower_bound_inclusive: datetime.datetime = attr.ib(
        validator=attr_validators.is_datetime
    )
    upper_bound_exclusive: Optional[datetime.datetime] = attr.ib(
        validator=attr_validators.is_opt_datetime
    )

    @property
    def is_open(self) -> bool:
        return self.upper_bound_exclusive is None

    @property
    def non_optional_upperbound_exclusive(self) -> datetime.datetime:
        return assert_type(self.upper_bound_exclusive, datetime.datetime)

    def __contains__(
        self,
        value: Union[datetime.date, datetime.datetime, "PotentiallyOpenDateTimeRange"],
    ) -> bool:
        if isinstance(value, PotentiallyOpenDateTimeRange):
            return self._contains_other_range(value)
        dt_value = as_datetime(value)
        # If the value is before the span begins, it is NOT in the span
        if dt_value < self.lower_bound_inclusive:
            return False
        # If the span never ends, the value is in the span
        if self.is_open:
            return True
        # If the value is *less* than the upperbound, it is in the span.
        # Note a value equal to upperbound is NOT in the span because it is
        # exclusive.
        return dt_value < self.non_optional_upperbound_exclusive

    def _contains_other_range(self, other: "PotentiallyOpenDateTimeRange") -> bool:
        if self.is_open:
            return self.lower_bound_inclusive <= other.lower_bound_inclusive
        if other.is_open:
            return False
        return (
            self.lower_bound_inclusive <= other.lower_bound_inclusive
            and self.non_optional_upperbound_exclusive
            > other.non_optional_upperbound_exclusive
        )

    def __attrs_post_init__(self) -> None:
        if (
            not self.is_open
            and self.lower_bound_inclusive > self.non_optional_upperbound_exclusive
        ):
            raise ValueError(
                f"Parsed datetimes must be in chronological order. "
                f"Current order: {self.lower_bound_inclusive}, {self.non_optional_upperbound_exclusive}"
            )


@attr.s(frozen=True)
class PotentiallyOpenDateRange:
    """Object representing a range of dates where the end date could be null (open)."""

    lower_bound_inclusive_date: datetime.date = attr.ib()
    upper_bound_exclusive_date: Optional[datetime.date] = attr.ib()

    # TODO(#37512) Consolidate properties and checks with other classes in this file, where appropriate

    @property
    def is_open(self) -> bool:
        return self.upper_bound_exclusive_date is None

    @property
    def non_optional_upperbound_exclusive_date(self) -> datetime.date:
        return assert_type(self.upper_bound_exclusive_date, datetime.date)

    def _contains_other_range(self, other: "PotentiallyOpenDateRange") -> bool:
        if self.is_open:
            return self.lower_bound_inclusive_date <= other.lower_bound_inclusive_date
        if other.is_open:
            return False
        return (
            self.lower_bound_inclusive_date <= other.lower_bound_inclusive_date
            and self.non_optional_upperbound_exclusive_date
            > other.non_optional_upperbound_exclusive_date
        )

    def __contains__(
        self,
        value: Union[datetime.date, "PotentiallyOpenDateRange"],
    ) -> bool:
        if isinstance(value, PotentiallyOpenDateRange):
            return self._contains_other_range(value)
        if value < self.lower_bound_inclusive_date:
            return False
        if self.is_open:
            return True
        return value < self.non_optional_upperbound_exclusive_date


@attr.s
class DateRange(PotentiallyOpenDateRange):
    """Object representing a range of dates."""

    upper_bound_exclusive_date: datetime.date = attr.ib()

    def get_months_range_overlaps_at_all(self) -> List[Tuple[int, int]]:
        """Returns a list of (year, month) pairs where any portion of the month overlaps the date range."""

        months_range_overlaps: List[Tuple[int, int]] = []
        month_date = self.lower_bound_inclusive_date

        while month_date < self.upper_bound_exclusive_date:
            months_range_overlaps.append((month_date.year, month_date.month))
            month_date = first_day_of_next_month(month_date)

        return months_range_overlaps

    def timedelta(self) -> datetime.timedelta:
        return self.upper_bound_exclusive_date - self.lower_bound_inclusive_date

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
        return cls(start_date, date_or_tomorrow_us_eastern(end_date))

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

    # Date range portion of range_1 that comes before range_2
    @property
    def range_1_non_overlapping_before_part(self) -> Optional[DateRange]:
        return (
            DateRange(
                lower_bound_inclusive_date=self.range_1.lower_bound_inclusive_date,
                upper_bound_exclusive_date=self.range_2.lower_bound_inclusive_date,
            )
            if (
                self.range_1.lower_bound_inclusive_date
                < self.range_2.lower_bound_inclusive_date
            )
            else None
        )

    # Date range portion of range_1 that comes after range_2
    @property
    def range_1_non_overlapping_after_part(self) -> Optional[DateRange]:
        return (
            DateRange(
                lower_bound_inclusive_date=self.range_2.upper_bound_exclusive_date,
                upper_bound_exclusive_date=self.range_1.upper_bound_exclusive_date,
            )
            if (
                self.range_1.upper_bound_exclusive_date
                > self.range_2.upper_bound_exclusive_date
            )
            else None
        )

    # Date ranges in range_1 that do not overlap with range_2
    @property
    def range_1_non_overlapping_parts(self) -> List[DateRange]:
        parts = [
            self.range_1_non_overlapping_before_part,
            self.range_1_non_overlapping_after_part,
        ]
        return [p for p in parts if p is not None]

    # Date range portion of range_2 that comes before range_1
    @property
    def range_2_non_overlapping_before_part(self) -> Optional[DateRange]:
        return (
            DateRange(
                lower_bound_inclusive_date=self.range_2.lower_bound_inclusive_date,
                upper_bound_exclusive_date=self.range_1.lower_bound_inclusive_date,
            )
            if (
                self.range_2.lower_bound_inclusive_date
                < self.range_1.lower_bound_inclusive_date
            )
            else None
        )

    # Date range portion or range_2 that comes after range_1
    @property
    def range_2_non_overlapping_after_part(self) -> Optional[DateRange]:
        return (
            DateRange(
                lower_bound_inclusive_date=self.range_1.upper_bound_exclusive_date,
                upper_bound_exclusive_date=self.range_2.upper_bound_exclusive_date,
            )
            if (
                self.range_2.upper_bound_exclusive_date
                > self.range_1.upper_bound_exclusive_date
            )
            else None
        )

    # Date ranges in range_2 that do not overlap with range_1
    @property
    def range_2_non_overlapping_parts(self) -> List[DateRange]:
        parts = [
            self.range_2_non_overlapping_before_part,
            self.range_2_non_overlapping_after_part,
        ]
        return [p for p in parts if p is not None]


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


DurationMixinT = TypeVar("DurationMixinT", bound=DurationMixin)


class CriticalRangesBuilder:
    """Class that can be used to construct a list of critical date ranges associated
    with a list of objects that each represent a time span.

    A "critical range" can be defined as a period of time when the set of overlapping
    input objects remains exactly the same.

    The list of critical ranges and associated metadata is built at instantiation time
    in O(N) time, where N is the number of input duration objects. All public methods on
    this class have O(M) runtime, where M is the number of returned objects.
    """

    def __init__(self, duration_objects: Sequence[DurationMixin]) -> None:
        for o in duration_objects:
            if not o.start_date_inclusive:
                raise ValueError(
                    f"Cannot build critical ranges from objects with a null "
                    f"start_date_inclusive. Found: {o}"
                )

        # DurationMixin objects sorted by start date (earliest to latest), then end date
        # (earliest to latest, with open ranges sorted last).
        sorted_duration_objects = sorted(
            duration_objects,
            key=lambda o: (
                o.start_date_inclusive,
                o.end_date_exclusive or datetime.date.max,
            ),
        )
        self._sorted_critical_ranges = self._build_sorted_critical_ranges(
            sorted_duration_objects
        )

        # Maps each critical range to its index in self._sorted_critical_ranges
        self._critical_range_to_index: Dict[PotentiallyOpenDateRange, int] = {
            cr: i for i, cr in enumerate(self._sorted_critical_ranges)
        }

        # Maps each critical range to the objects that overlap that range
        self._critical_range_to_overlapping_objects: Dict[
            PotentiallyOpenDateRange, List[DurationMixin]
        ] = self._build_overlapping_objects_by_critical_range(
            sorted_duration_objects, self._sorted_critical_ranges
        )

    @staticmethod
    def _build_sorted_critical_ranges(
        duration_objects: List[DurationMixin],
    ) -> List[PotentiallyOpenDateRange]:
        """Derives a set of date ranges that can be built using all dates associated
        with the given input duration objects.
        """
        has_open_end_date = any(o.end_date_exclusive is None for o in duration_objects)
        critical_dates = {
            assert_type(o.start_date_inclusive, datetime.date) for o in duration_objects
        } | {
            o.end_date_exclusive
            for o in duration_objects
            if o.end_date_exclusive is not None
        }

        return convert_critical_dates_to_time_spans(
            critical_dates, has_open_end_date=has_open_end_date
        )

    @staticmethod
    def _build_overlapping_objects_by_critical_range(
        sorted_duration_objects: List[DurationMixin],
        sorted_critical_ranges: List[PotentiallyOpenDateRange],
    ) -> Dict[PotentiallyOpenDateRange, List[DurationMixin]]:
        """Returns a dictionary mapping critical range to the list of ALL objects that
        overlap with that date range.
        """
        critical_range_to_overlapping_objects = {}

        next_duration_index = 0
        open_ranges: List[DurationMixin] = []
        for critical_range in sorted_critical_ranges:
            # LOOP PRECONDITIONS:
            # 1) All objs that were already open BEFORE the start of this range are in
            #   open_ranges.
            # 2) The next object to look at is at index next_duration_index

            # Discard all objects that ended before the start of this range.
            open_ranges = [
                o
                for o in open_ranges
                if not o.end_date_exclusive
                or o.end_date_exclusive > critical_range.lower_bound_inclusive_date
            ]

            # Add any objects that have started with this new range.
            while next_duration_index < len(sorted_duration_objects):
                next_duration = sorted_duration_objects[next_duration_index]

                if (
                    assert_type(next_duration.start_date_inclusive, datetime.date)
                    > critical_range.lower_bound_inclusive_date
                ):
                    # This object's time window hasn't started yet
                    break
                next_duration_index += 1

                if (
                    next_duration.start_date_inclusive
                    == next_duration.end_date_exclusive
                ):
                    # Zero-day periods don't overlap with ranges
                    continue
                open_ranges.append(next_duration)
            critical_range_to_overlapping_objects[critical_range] = open_ranges
        return critical_range_to_overlapping_objects

    def _get_preceding_range(
        self, critical_range: PotentiallyOpenDateRange
    ) -> Optional[PotentiallyOpenDateRange]:
        """Get the range directly preceding the given input range."""
        index = self._critical_range_to_index[critical_range]
        if index == 0:
            return None
        return self._sorted_critical_ranges[index - 1]

    def _get_following_range(
        self, critical_range: PotentiallyOpenDateRange
    ) -> Optional[PotentiallyOpenDateRange]:
        """Get the range directly following the given input range."""
        index = self._critical_range_to_index[critical_range]
        if index >= len(self._sorted_critical_ranges) - 1:
            return None
        return self._sorted_critical_ranges[index + 1]

    def get_sorted_critical_ranges(self) -> List[PotentiallyOpenDateRange]:
        """Returns the list of ranges that can be built from the set of start and end
        dates associated with the list of input duration objects.
        """
        return self._sorted_critical_ranges

    def get_objects_overlapping_with_critical_range(
        self,
        critical_range: PotentiallyOpenDateRange,
        type_filter: Type[DurationMixinT],
    ) -> List[DurationMixinT]:
        """Returns all input objects of type |type_filter| overlapping with the
        provided |critical_range|. The |critical_range| must be one of the ranges
        returned by get_sorted_critical_ranges().
        """
        return [
            o
            for o in self._critical_range_to_overlapping_objects[critical_range]
            if isinstance(o, type_filter)
        ]

    def get_objects_directly_preceding_range(
        self,
        critical_range: PotentiallyOpenDateRange,
        type_filter: Type[DurationMixinT],
    ) -> List[DurationMixinT]:
        """Returns objects of type |type_filter| that end on the day |critical_range|
        starts. The |critical_range| must be one of the ranges returned by
        get_sorted_critical_ranges().
        """
        preceding_range = self._get_preceding_range(critical_range)
        if not preceding_range:
            return []
        return [
            o
            for o in self.get_objects_overlapping_with_critical_range(
                preceding_range, type_filter
            )
            if o.end_date_exclusive == critical_range.lower_bound_inclusive_date
        ]

    def get_objects_directly_following_range(
        self,
        critical_range: PotentiallyOpenDateRange,
        type_filter: Type[DurationMixinT],
    ) -> List[DurationMixinT]:
        """Returns objects of type |type_filter| that start on the day |critical_range|
        ends. The |critical_range| must be one of the ranges returned by
        get_sorted_critical_ranges().
        """
        following_range = self._get_following_range(critical_range)
        if not following_range:
            return []

        return [
            o
            for o in self.get_objects_overlapping_with_critical_range(
                following_range, type_filter
            )
            if critical_range.upper_bound_exclusive_date
            and o.start_date_inclusive == critical_range.upper_bound_exclusive_date
        ]


def is_date_str(potential_date_str: str) -> bool:
    """Returns True if the string is an ISO-formatted date, (e.g. '2019-09-25'), False otherwise."""
    try:
        datetime.datetime.strptime(potential_date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False


# TODO(#37517) deprecate once not used
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


def safe_strptime(
    date_string: Optional[str], date_format: str
) -> Optional[datetime.datetime]:
    """Returns the parsed date string per the provided date format, or None if the date_string is not a valid
    date string"""
    try:
        return datetime.datetime.strptime(str(date_string), date_format)
    except (ValueError, TypeError):
        return None


def safe_year_replace(date: datetime.date, year: int) -> datetime.date:
    if date.month == 2 and date.day == 29 and not isleap(year):
        return datetime.date(year, 3, 1)
    return date.replace(year=year)


def split_range_by_birthdate(
    date_range: Tuple[datetime.date, Optional[datetime.date]], birthdate: datetime.date
) -> Iterator[Tuple[datetime.date, Optional[datetime.date]]]:
    """Splits a date range into smaller ranges given a birthdate. Preserves the ending
    date being None if not given. The birthdate is to set the age of the person at the
    time of the date range."""
    start_date, end_date = date_range

    # If the birthdate is earlier in the year than the start of the range, the first
    # split will be in the following year.
    same_year_birthdate = safe_year_replace(birthdate, year=start_date.year)
    if same_year_birthdate > start_date:
        first_birthdate_after_start = same_year_birthdate
    else:
        first_birthdate_after_start = safe_year_replace(
            birthdate, year=(start_date.year + 1)
        )

    split_date = min(first_birthdate_after_start, date_or_tomorrow_us_eastern(end_date))

    # Keep splitting until we get to the end of the range
    while split_date < date_or_tomorrow_us_eastern(end_date):
        yield (start_date, split_date)

        start_date = split_date
        split_date = safe_year_replace(birthdate, year=split_date.year + 1)

    yield (start_date, end_date)


def merge_sorted_date_ranges(
    sorted_date_ranges: List[NonNegativeDateRange],
) -> List[NonNegativeDateRange]:
    """Given a sorted list of date ranges, returns a list where all consecutive ranges
    have been merged into a single range."""
    merged_ranges: List[NonNegativeDateRange] = []
    for date_range in sorted_date_ranges:
        if merged_ranges:
            prev_duration = merged_ranges[-1]
            if (
                date_range.lower_bound_inclusive_date
                == prev_duration.upper_bound_exclusive_date
            ):
                merged_ranges[-1] = NonNegativeDateRange(
                    merged_ranges[-1].lower_bound_inclusive_date,
                    date_range.upper_bound_exclusive_date,
                )
                continue
        merged_ranges.append(date_range)
    return merged_ranges


def convert_critical_dates_to_time_spans(
    critical_dates: Iterable[datetime.date], has_open_end_date: bool
) -> List[PotentiallyOpenDateRange]:
    """Given a list of critical dates, returns a series of date ranges that represent
    the time spans between these dates."""

    critical_dates_sorted = sorted(list(critical_dates))

    time_spans: List[PotentiallyOpenDateRange] = []
    for index, critical_date in enumerate(critical_dates_sorted):
        if index == len(critical_dates_sorted) - 1 and has_open_end_date:
            time_spans.append(
                PotentiallyOpenDateRange(
                    lower_bound_inclusive_date=critical_date,
                    upper_bound_exclusive_date=None,
                )
            )
        elif index < len(critical_dates_sorted) - 1:
            time_spans.append(
                DateRange(
                    lower_bound_inclusive_date=critical_date,
                    upper_bound_exclusive_date=critical_dates_sorted[index + 1],
                )
            )
    return time_spans


def date_ranges_overlap(date_ranges: list[PotentiallyOpenDateRange]) -> bool:
    """Returns True if any of the provided date ranges overlap."""
    return any(get_overlapping_date_ranges(date_ranges))


# TODO(#37512) Generalize or allow use for other range classes in this file
def get_overlapping_date_ranges(
    date_ranges: list[PotentiallyOpenDateRange],
) -> set[PotentiallyOpenDateRange]:
    overlaps = set()
    for a, b in itertools.combinations(date_ranges, 2):
        if (a.lower_bound_inclusive_date in b) or (b.lower_bound_inclusive_date in a):
            overlaps.add(a)
            overlaps.add(b)
    return overlaps


def parse_datetime_maybe_add_tz(
    datetime_str: str, *, tz_to_add: datetime.timezone | None = None
) -> datetime.datetime:
    """Parses an ISO-formatted date string, optionally adding a timezone if one is not
    present.
    """
    new_datetime = datetime.datetime.fromisoformat(datetime_str)
    return (
        new_datetime.replace(tzinfo=tz_to_add)
        if tz_to_add is not None and new_datetime.tzinfo is None
        else new_datetime
    )


def parse_opt_datetime_maybe_add_tz(
    opt_datetime_str: str | None, *, tz_to_add: datetime.timezone | None = None
) -> datetime.datetime | None:
    """Parses an ISO-formatted date string if it is not None, optionally adding a
    timezone if one is not present.
    """
    if opt_datetime_str is None:
        return None
    return parse_datetime_maybe_add_tz(
        datetime_str=opt_datetime_str, tz_to_add=tz_to_add
    )


def is_datetime_in_opt_range(
    value: datetime.datetime,
    *,
    start_datetime_inclusive: datetime.datetime | None,
    end_datetime_exclusive: datetime.datetime | None,
) -> bool:
    """Returns True if |value| is greater than or equal to |start_datetime_inclusive| and
    less than |end_datetime_exclusive|, if they are provided.
    """
    if (
        start_datetime_inclusive
        and end_datetime_exclusive
        and start_datetime_inclusive >= end_datetime_exclusive
    ):
        raise ValueError(
            f"Invalid date range: range start [{start_datetime_inclusive}] cannot be before or on range end [{end_datetime_exclusive}]"
        )

    if start_datetime_inclusive and start_datetime_inclusive > value:
        return False
    if end_datetime_exclusive and end_datetime_exclusive <= value:
        return False
    return True
