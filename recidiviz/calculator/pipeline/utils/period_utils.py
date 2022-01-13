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
# =============================================================================
"""Shared utils for dealing with PeriodType entities (StateIncarcerationPeriod and
StateSupervisionPeriod)."""
from datetime import date
from functools import cmp_to_key
from typing import Callable, List, Optional

from dateutil.relativedelta import relativedelta

from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.state.entities import (
    PeriodType,
    StateIncarcerationPeriod,
    StateSupervisionPeriod,
)


def sort_period_by_external_id(p_a: PeriodType, p_b: PeriodType) -> int:
    """Sorts two periods alphabetically by external_id."""
    if p_a.external_id is None or p_b.external_id is None:
        raise ValueError("Expect no placeholder periods in this function.")

    # Alphabetic sort by external_id
    return -1 if p_a.external_id < p_b.external_id else 1


def sort_periods_by_set_dates_and_statuses(
    periods: List[PeriodType],
    is_transfer_start_function: Callable[[PeriodType], int],
    is_transfer_end_function: Callable[[PeriodType], int],
) -> None:
    """Sorts periods chronologically by the start and end dates according to this logic:
    - Sorts by start_date_inclusive, if set, else by end_date_exclusive
    - For periods with the same start_date_inclusive:
        - If neither have a end_date_exclusive, sorts by the external_id
        - Else, sorts by end_date_exclusive, with unset end_date_exclusives before set
            end_date_exclusives
    """

    def _sort_period_by_external_id(p_a: PeriodType, p_b: PeriodType) -> int:
        """Sorts two periods alphabetically by external_id. This is not a reliable
        way to sort periods chronologically, but is used to sort periods
        deterministically when we cannot determine the chronological order of the
        two."""
        if p_a.external_id is None or p_b.external_id is None:
            raise ValueError("Expect no placeholder periods in this function.")

        # Alphabetic sort by external_id
        return -1 if p_a.external_id < p_b.external_id else 1

    def _sort_by_transfer_end(period_a: PeriodType, period_b: PeriodType) -> int:
        """Sorts two periods with the same start and end dates that both started with a
        transfer."""
        period_a_transfer_end = is_transfer_end_function(period_a)
        period_b_transfer_end = is_transfer_end_function(period_b)

        if period_a_transfer_end == period_b_transfer_end:
            # Either both or neither periods were ended because of a transfer. Sort
            # by external_id
            return _sort_period_by_external_id(period_a, period_b)

        if period_a_transfer_end:
            return -1
        if period_b_transfer_end:
            return 1
        raise ValueError("One period should have a transfer start at this point")

    def _sort_by_transfer_start(period_a: PeriodType, period_b: PeriodType) -> int:
        if not (
            period_a.start_date_inclusive == period_b.start_date_inclusive
            and period_a.end_date_exclusive == period_b.end_date_exclusive
        ):
            raise ValueError("Expected equal start and end dates.")

        period_a_transfer_start = is_transfer_start_function(period_a)
        period_b_transfer_start = is_transfer_start_function(period_b)

        if period_a_transfer_start and period_b_transfer_start:
            # Both periods were started because of a transfer. Sort by transfer end
            # edges.
            return _sort_by_transfer_end(period_a, period_b)

        if period_a_transfer_start == period_b_transfer_start:
            # Neither periods were started because of a transfer. Sort by external_id.
            return _sort_period_by_external_id(period_a, period_b)

        if period_a_transfer_start:
            return 1
        if period_b_transfer_start:
            return -1
        raise ValueError("One period should have a transfer start at this point")

    def _sort_by_nonnull_end_dates(period_a: PeriodType, period_b: PeriodType) -> int:
        if not period_a.end_date_exclusive or not period_b.end_date_exclusive:
            raise ValueError("Expected nonnull ending dates")
        if period_a.end_date_exclusive != period_b.end_date_exclusive:
            return (period_a.end_date_exclusive - period_b.end_date_exclusive).days

        # They have the same start and end dates. Sort by transfer starts.
        return _sort_by_transfer_start(period_a, period_b)

    def _sort_equal_start_date(period_a: PeriodType, period_b: PeriodType) -> int:
        if period_a.start_date_inclusive != period_b.start_date_inclusive:
            raise ValueError("Expected equal start dates")
        if period_a.end_date_exclusive and period_b.end_date_exclusive:
            return _sort_by_nonnull_end_dates(period_a, period_b)
        if (
            period_a.start_date_inclusive is None
            or period_b.start_date_inclusive is None
        ):
            raise ValueError(
                "Start dates expected to be equal and nonnull at this point otherwise we would have a "
                "period that has a null ending and null starting reason."
            )
        if period_a.end_date_exclusive is None and period_b.end_date_exclusive is None:
            return sort_period_by_external_id(period_a, period_b)

        # Sort by end dates, with unset end dates coming first if the following period
        # is greater than 0 days long (we assume in this case that we forgot to close
        # this open period).
        if period_a.end_date_exclusive:
            return (
                1
                if (period_a.end_date_exclusive - period_a.start_date_inclusive).days
                else -1
            )
        if period_b.end_date_exclusive:
            return (
                -1
                if (period_b.end_date_exclusive - period_b.start_date_inclusive).days
                else 1
            )
        raise ValueError(
            "At least one of the periods is expected to have a end_date_exclusive at this point."
        )

    def _sort_share_date_not_starting(
        period_a: PeriodType, period_b: PeriodType
    ) -> int:
        both_a_set = (
            period_a.start_date_inclusive is not None
            and period_a.end_date_exclusive is not None
        )
        both_b_set = (
            period_b.start_date_inclusive is not None
            and period_b.end_date_exclusive is not None
        )

        if not both_a_set and not both_b_set:
            # One has an start date and the other has an end date on the same day. Order the start before the end.
            return -1 if period_a.start_date_inclusive else 1

        # One period has both a start date and an end date, and the other has only an end date.
        if not period_a.start_date_inclusive:
            if period_a.end_date_exclusive == period_b.start_date_inclusive:
                # period_a is missing a start_date_inclusive, and its end_date_exclusive matches period_b's
                # start_date_inclusive. We want to order the end before the start that has a later ending.
                return -1
            # These share an end date, and period_a does not have a start date. Order the period with the set, earlier
            # start date first.
            return 1
        if not period_b.start_date_inclusive:
            if period_b.end_date_exclusive == period_a.start_date_inclusive:
                # period_b is missing a start date, and its end date matches period_a's start date. We want to order the
                # end date before the start date that has a later end date.
                return 1
            # These share an end date, and period_b does not have a start date. Order the period with the set, earlier
            # start date first.
            return -1
        raise ValueError(
            "It should not be possible to reach this point. If either, but not both, period_a or period_b"
            " only have one date set, and they don't have equal None start dates, then we expect either"
            "period_a or period_b to have a missing start_date_inclusive here."
        )

    def _sort_function(period_a: PeriodType, period_b: PeriodType) -> int:
        if period_a.start_date_inclusive == period_b.start_date_inclusive:
            return _sort_equal_start_date(period_a, period_b)

        # Sort by start_date_inclusive, if set, or end_date_exclusive if not set
        date_a = (
            period_a.start_date_inclusive
            if period_a.start_date_inclusive
            else period_a.end_date_exclusive
        )
        date_b = (
            period_b.start_date_inclusive
            if period_b.start_date_inclusive
            else period_b.end_date_exclusive
        )
        if not date_a:
            raise ValueError(f"Found period with no starting or ending date {period_a}")
        if not date_b:
            raise ValueError(f"Found period with no starting or ending date {period_b}")
        if date_a == date_b:
            return _sort_share_date_not_starting(period_a, period_b)

        return (date_a - date_b).days

    periods.sort(key=cmp_to_key(_sort_function))


def find_last_terminated_period_on_or_before_date(
    upper_bound_date_inclusive: date,
    periods: Optional[List[PeriodType]],
    maximum_months_proximity: int,
    same_date_sort_fn: Callable[
        [PeriodType, PeriodType], int
    ] = sort_period_by_external_id,
) -> Optional[PeriodType]:
    """Looks for the incarceration or supervision period that ended most recently before the upper_bound_date, within
    the month window defined by |maximum_months_proximity|.

    If no terminated period is found before the upper_bound_date, returns None.
    """
    if not periods:
        return None

    termination_date_cutoff = upper_bound_date_inclusive - relativedelta(
        months=maximum_months_proximity
    )

    previous_periods = [
        period
        for period in periods
        if period.start_date_inclusive is not None
        and period.end_date_exclusive is not None
        and period.end_date_exclusive >= termination_date_cutoff
        and period.start_date_inclusive
        <= period.end_date_exclusive
        <= upper_bound_date_inclusive
    ]

    def _sort_function(period_a: PeriodType, period_b: PeriodType) -> int:
        p_a_date = period_a.end_date_exclusive
        p_b_date = period_b.end_date_exclusive

        if not p_a_date or not p_b_date:
            raise ValueError(
                "end_date_exclusive should be set on all periods by this point."
            )

        if p_a_date == p_b_date:
            return same_date_sort_fn(period_a, period_b)
        # Sort the more recent termination date first
        return (p_b_date - p_a_date).days

    if previous_periods:
        # Sort in date descending order, and take the first one
        return min(previous_periods, key=cmp_to_key(_sort_function))

    return None


def find_earliest_date_of_period_ending_in_death(
    periods: Optional[List[PeriodType]],
) -> Optional[date]:
    """Looks for the incarceration or supervision period that ended in death and a set end_date_exclusive,
    and returns the earliest end_date_exclusive of the periods ending in death.

    If no terminated period is ending in death, returns None.
    """

    def _is_period_ending_in_death(period: PeriodType) -> bool:
        """Whether or not the period terminates in death."""
        if isinstance(period, StateIncarcerationPeriod) and period.release_reason:
            return period.release_reason == StateIncarcerationPeriodReleaseReason.DEATH
        if isinstance(period, StateSupervisionPeriod) and period.termination_reason:
            return (
                period.termination_reason
                == StateSupervisionPeriodTerminationReason.DEATH
            )
        return False

    if not periods:
        return None

    dates_of_death: List[date] = []
    for pd in periods:
        if _is_period_ending_in_death(pd) and pd.end_date_exclusive:
            # We should only return a date for a period that has a set end_date_exclusive
            dates_of_death.append(pd.end_date_exclusive)

    if dates_of_death:
        return min(dates_of_death)

    return None
