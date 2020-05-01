# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Utility classes for handling date range abstractions."""

from datetime import date, timedelta
from typing import List, Tuple, Optional

import attr

from recidiviz.calculator.pipeline.utils.calculator_utils import first_day_of_next_month
from recidiviz.common.constants.state.state_incarceration_period import StateIncarcerationPeriodStatus
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod, StateSupervisionPeriod


@attr.s
class TimeRange:
    """Object representing a range in time."""

    lower_bound_inclusive_date: date = attr.ib()
    upper_bound_exclusive_date: date = attr.ib()

    def get_months_range_overlaps_at_all(self) -> List[Tuple[int, int]]:
        """Returns a list of (year, month) pairs where any portion of the month overlaps the time range."""

        months_range_overlaps: List[Tuple[int, int]] = []
        month_date = self.lower_bound_inclusive_date

        while month_date < self.upper_bound_exclusive_date:
            months_range_overlaps.append((month_date.year, month_date.month))
            month_date = first_day_of_next_month(month_date)

        return months_range_overlaps

    @classmethod
    def for_incarceration_period(cls, incarceration_period: StateIncarcerationPeriod) -> 'TimeRange':
        """Generates a TimeRange for the time covered by the incarceration period.  Since TimeRange is never open,
        if the incarceration period is still active, then the exclusive upper bound of the range is set to tomorrow.
        """
        if not incarceration_period.admission_date:
            raise ValueError(
                f'Expected start date for period {incarceration_period.incarceration_period_id}, found None')

        if (not incarceration_period.release_date and
                incarceration_period.status != StateIncarcerationPeriodStatus.IN_CUSTODY):
            raise ValueError("Unexpected missing release date. _infer_missing_dates_and_statuses is not properly"
                             " setting missing dates.")

        ip_upper_bound_exclusive_date = incarceration_period.release_date \
            if incarceration_period.release_date else (date.today() + timedelta(days=1))

        return TimeRange(lower_bound_inclusive_date=incarceration_period.admission_date,
                         upper_bound_exclusive_date=ip_upper_bound_exclusive_date)

    @classmethod
    def for_supervision_period(cls, supervision_period: StateSupervisionPeriod) -> 'TimeRange':
        """Generates a TimeRange for the time covered by the supervision period.  Since TimeRange is never open,
        if the supervision period is still active, then the exclusive upper bound of the range is set to tomorrow.
        """
        ip_upper_bound_exclusive_date = supervision_period.termination_date \
            if supervision_period.termination_date else (date.today() + timedelta(days=1))

        if not supervision_period.start_date:
            raise ValueError(
                f'Expected start date for period {supervision_period.supervision_period_id}, found None')

        return TimeRange(lower_bound_inclusive_date=supervision_period.start_date,
                         upper_bound_exclusive_date=ip_upper_bound_exclusive_date)

    @classmethod
    def for_month(cls, year: int, month: int) -> 'TimeRange':
        start_of_month = date(year, month, 1)
        start_of_next_month = first_day_of_next_month(start_of_month)

        return TimeRange(lower_bound_inclusive_date=start_of_month,
                         upper_bound_exclusive_date=start_of_next_month)

    def portion_overlapping_with_month(self, year: int, month: int) -> Optional['TimeRange']:
        month_range = TimeRange.for_month(year, month)
        return TimeRangeDiff(range_1=self, range_2=month_range).overlapping_range


@attr.s
class TimeRangeDiff:
    """Utility class for representing the difference between two time ranges."""

    range_1: TimeRange = attr.ib()
    range_2: TimeRange = attr.ib()

    # Time range that is shared between the two ranges
    overlapping_range: Optional[TimeRange] = attr.ib()

    @overlapping_range.default
    def _overlapping_range(self) -> Optional[TimeRange]:
        lower_bound_inclusive_date = max(self.range_1.lower_bound_inclusive_date,
                                         self.range_2.lower_bound_inclusive_date)
        upper_bound_exclusive_date = min(self.range_1.upper_bound_exclusive_date,
                                         self.range_2.upper_bound_exclusive_date)

        if upper_bound_exclusive_date <= lower_bound_inclusive_date:
            return None

        return TimeRange(
            lower_bound_inclusive_date=lower_bound_inclusive_date,
            upper_bound_exclusive_date=upper_bound_exclusive_date
        )

    # Time ranges in range_1 that do not overlap with range_2
    range_1_non_overlapping_parts: List[TimeRange] = attr.ib()

    @range_1_non_overlapping_parts.default
    def _range_1_non_overlapping_parts(self):
        parts = []
        if self.range_1.lower_bound_inclusive_date < self.range_2.lower_bound_inclusive_date:
            parts.append(
                TimeRange(
                    lower_bound_inclusive_date=self.range_1.lower_bound_inclusive_date,
                    upper_bound_exclusive_date=self.range_2.lower_bound_inclusive_date
                )
            )

        if self.range_1.upper_bound_exclusive_date > self.range_2.upper_bound_exclusive_date:
            parts.append(
                TimeRange(
                    lower_bound_inclusive_date=self.range_2.upper_bound_exclusive_date,
                    upper_bound_exclusive_date=self.range_1.upper_bound_exclusive_date
                )
            )
        return parts

    # Time ranges in range_2 that do not overlap with range_1
    range_2_non_overlapping_parts = attr.ib()
    @range_2_non_overlapping_parts.default
    def _range_2_non_overlapping_parts(self):
        parts = []
        if self.range_2.lower_bound_inclusive_date < self.range_1.lower_bound_inclusive_date:
            parts.append(
                TimeRange(
                    lower_bound_inclusive_date=self.range_2.lower_bound_inclusive_date,
                    upper_bound_exclusive_date=self.range_1.lower_bound_inclusive_date
                )
            )

        if self.range_2.upper_bound_exclusive_date > self.range_1.upper_bound_exclusive_date:
            parts.append(
                TimeRange(
                    lower_bound_inclusive_date=self.range_1.upper_bound_exclusive_date,
                    upper_bound_exclusive_date=self.range_2.upper_bound_exclusive_date
                )
            )
        return parts
