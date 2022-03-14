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
"""Utils for validating and manipulating incarceration periods for use in
calculations."""
import datetime
from typing import List, Optional

from recidiviz.calculator.pipeline.utils.period_utils import (
    sort_periods_by_set_dates_and_statuses,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.date import DateRangeDiff
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod

DEFAULT_VALID_TRANSFER_THRESHOLD_DAYS: int = 1


def _is_transfer_start(period: StateIncarcerationPeriod) -> bool:
    return period.admission_reason == StateIncarcerationPeriodAdmissionReason.TRANSFER


def _is_transfer_end(period: StateIncarcerationPeriod) -> bool:
    return period.release_reason == StateIncarcerationPeriodReleaseReason.TRANSFER


def standard_date_sort_for_incarceration_periods(
    incarceration_periods: List[StateIncarcerationPeriod],
) -> List[StateIncarcerationPeriod]:
    """Sorts incarceration periods chronologically by dates and statuses."""
    sort_periods_by_set_dates_and_statuses(
        incarceration_periods, _is_transfer_start, _is_transfer_end
    )

    return incarceration_periods


def dates_are_temporally_adjacent(
    date_1: Optional[datetime.date],
    date_2: Optional[datetime.date],
    valid_adjacency_threshold_override: Optional[int] = None,
) -> bool:
    """Returns whether two dates are temporally adjacent. Two dates periods are
    temporally adjacent if date_1 is within VALID_TRANSFER_THRESHOLD days of date_2."""
    adjacency_threshold_days = (
        valid_adjacency_threshold_override or DEFAULT_VALID_TRANSFER_THRESHOLD_DAYS
    )

    if not date_1 or not date_2:
        # If there are missing dates, then this is not a valid date edge
        return False

    days_between_periods = (date_2 - date_1).days

    return days_between_periods <= adjacency_threshold_days


def periods_are_temporally_adjacent(
    first_incarceration_period: StateIncarcerationPeriod,
    second_incarceration_period: StateIncarcerationPeriod,
    valid_adjacency_threshold_override: Optional[int] = None,
) -> bool:
    """Returns whether two incarceration periods are temporally adjacent, meaning they
    can be treated as a continuous stint of incarceration, even if their edges do not
    line up perfectly. Two consecutive periods are temporally adjacent if the
    release_date on the |first_incarceration_period| is within
    VALID_TRANSFER_THRESHOLD days of the admission_date on the
    |second_incarceration_period|."""
    release_date = first_incarceration_period.release_date
    admission_date = second_incarceration_period.admission_date

    return dates_are_temporally_adjacent(
        date_1=release_date,
        date_2=admission_date,
        valid_adjacency_threshold_override=valid_adjacency_threshold_override,
    )


def period_edges_are_valid_transfer(
    first_incarceration_period: Optional[StateIncarcerationPeriod] = None,
    second_incarceration_period: Optional[StateIncarcerationPeriod] = None,
) -> bool:
    """Returns whether the edge between two incarceration periods is a valid transfer.
    Valid transfer means:
       - The two periods are adjacent
       - The adjacent release reason and admission reason between two consecutive
         periods is TRANSFER
    """
    if not first_incarceration_period or not second_incarceration_period:
        return False

    release_reason = first_incarceration_period.release_reason
    admission_reason = second_incarceration_period.admission_reason

    if not release_reason or not admission_reason:
        # If there is no release reason or admission reason, then this is not a valid
        # period edge
        return False

    return (
        periods_are_temporally_adjacent(
            first_incarceration_period, second_incarceration_period
        )
        and admission_reason == StateIncarcerationPeriodAdmissionReason.TRANSFER
        and release_reason == StateIncarcerationPeriodReleaseReason.TRANSFER
    )


def ip_is_nested_in_previous_period(
    ip: StateIncarcerationPeriod, previous_ip: StateIncarcerationPeriod
) -> bool:
    """Returns whether the StateIncarcerationPeriod |ip| is entirely nested within the
    |previous_ip|. Both periods must have set admission and release dates.
    A nested period is defined as an incarceration period that overlaps with the
    previous_ip and has no parts that are non-overlapping with the previous_ip.
    Single-day periods (admission_date = release_date) by definition do not have
    overlapping ranges with another period because the ranges are end date exclusive.
    If a single-day period falls within the admission and release of the previous_ip,
    then it is nested within that period. If a single-day period falls on the
    previous_ip.release_date, then it is not nested within that period.
    """
    ip_range_diff = DateRangeDiff(ip.duration, previous_ip.duration)

    if not ip.admission_date or not ip.release_date:
        raise ValueError(f"ip cannot have unset dates: {ip}")

    if not previous_ip.admission_date or not previous_ip.release_date:
        raise ValueError(f"previous_ip cannot have unset dates: {previous_ip}")

    if ip.admission_date < previous_ip.admission_date:
        raise ValueError(
            "previous_ip should be sorted after ip. Error in _sort_ips_by_set_dates_and_statuses. "
            f"ip: {ip}, previous_ip: {previous_ip}"
        )

    return (
        ip_range_diff.overlapping_range
        and not ip_range_diff.range_1_non_overlapping_parts
    ) or (
        ip.admission_date == ip.release_date
        and ip.release_date < previous_ip.release_date
    )


def period_is_commitment_from_supervision_admission_from_parole_board_hold(
    incarceration_period: StateIncarcerationPeriod,
    preceding_incarceration_period: StateIncarcerationPeriod,
) -> bool:
    """Determines whether the transition from the preceding_incarceration_period to
    the incarceration_period is a commitment from supervision admission after being
    held for a parole board hold."""
    if not periods_are_temporally_adjacent(
        first_incarceration_period=preceding_incarceration_period,
        second_incarceration_period=incarceration_period,
    ):
        return False

    return (
        incarceration_period.admission_reason
        # Valid commitment from supervision admission reasons following a parole board
        # hold
        in (
            StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
            StateIncarcerationPeriodAdmissionReason.REVOCATION,
        )
        # Commitment admissions from a parole board hold should happen on the same day
        # as the release from the parole board hold
        and preceding_incarceration_period.specialized_purpose_for_incarceration
        == StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD
    )


def incarceration_periods_with_admissions_between_dates(
    incarceration_periods: List[StateIncarcerationPeriod],
    start_date_inclusive: datetime.date,
    end_date_exclusive: datetime.date,
) -> List[StateIncarcerationPeriod]:
    """Returns any incarceration periods with admissions between the start_date and
    end_date, not inclusive of the end date."""
    return [
        ip
        for ip in incarceration_periods
        if ip.admission_date
        and start_date_inclusive <= ip.admission_date < end_date_exclusive
    ]
