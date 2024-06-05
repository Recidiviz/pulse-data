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

from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.common.date import CriticalRangesBuilder, DateRangeDiff
from recidiviz.persistence.entity.normalized_entities_utils import (
    update_normalized_entity_with_globally_unique_id,
)
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSupervisionPeriod,
)
from recidiviz.pipelines.utils.entity_normalization.normalized_supervision_period_index import (
    NormalizedSupervisionPeriodIndex,
)
from recidiviz.pipelines.utils.period_utils import (
    sort_periods_by_set_dates_and_statuses,
)

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


def infer_incarceration_periods_from_in_custody_sps(
    person_id: int,
    state_code: StateCode,
    incarceration_periods: List[StateIncarcerationPeriod],
    supervision_period_index: NormalizedSupervisionPeriodIndex,
    temp_custody_custodial_authority: StateCustodialAuthority,
) -> List[StateIncarcerationPeriod]:
    """Returns a set of incarceration periods that includes any new inferred incarceration periods based off of
    supervision periods with a supervision_level of 'IN_CUSTODY'. We use the CriticalRangesBuilder to create a set
    of key spans that either have no periods, an IP, an SP, or overlapping IPs/SPs. Then, we get all the SPs for a given span,
    check if they have a supervision_level of 'IN_CUSTODY', and then create a new inferred IP with
    PFI of 'TEMPORARY_CUSTODY' during this time. Given the use of the CriticalRangesBuilder, the IP created will not
    overlap with any current IPs. The release_date for the new inferred IP will either be NULL (if the period is still
    open), the same as the end_date of the IN_CUSTODY SP (if there is not an overlapping IP at any point in the span),
    or the admission_date of the next IP that starts while the IN_CUSTODY SP is ongoing."""

    critical_range_builder = CriticalRangesBuilder(
        [*incarceration_periods, *supervision_period_index.sorted_supervision_periods]
    )

    inferred_incarceration_periods = []

    # The current SP that we are inferring periods for. May persist across multiple
    # ranges, e.g. if there is an IP that only partially overlaps this SP.
    in_custody_sp_external_id = None

    # The number of incarceration periods that have been inferred from the
    # SP with external_id=in_custody_sp_external_id.
    inferred_period_count = 0

    for critical_range in critical_range_builder.get_sorted_critical_ranges():
        ips_overlapping_with_range = (
            critical_range_builder.get_objects_overlapping_with_critical_range(
                critical_range, StateIncarcerationPeriod
            )
        )
        if ips_overlapping_with_range:
            # If there is already an ingested IP overlapping with this range, do not
            # create a new inferred one.
            continue

        sps_overlapping_with_range = (
            critical_range_builder.get_objects_overlapping_with_critical_range(
                critical_range, NormalizedStateSupervisionPeriod
            )
        )

        in_custody_sps_overlapping_with_range = [
            sp
            for sp in sps_overlapping_with_range
            if sp.supervision_level == StateSupervisionLevel.IN_CUSTODY
        ]

        if not in_custody_sps_overlapping_with_range:
            in_custody_sp_external_id = None
            continue

        # If there wasn't an in custody IP for the previous time range or the newly
        # identified in custody SP for this range is different from the in custody SP
        # for the previous range, reset the inferred period count because this is the
        # first time we're inferring periods from this SP.
        if (
            in_custody_sp_external_id
            != in_custody_sps_overlapping_with_range[0].external_id
        ):
            in_custody_sp_external_id = in_custody_sps_overlapping_with_range[
                0
            ].external_id
            inferred_period_count = 0

        if critical_range.upper_bound_exclusive_date:
            release_reason = (
                StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY
            )
        else:
            release_reason = None

        new_incarceration_period = StateIncarcerationPeriod(
            state_code=state_code.value,
            external_id=f"{in_custody_sp_external_id}-{inferred_period_count}-IN-CUSTODY",
            admission_date=critical_range.lower_bound_inclusive_date,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=critical_range.upper_bound_exclusive_date,
            release_reason=release_reason,
            custodial_authority=temp_custody_custodial_authority,
            incarceration_type=StateIncarcerationType.INTERNAL_UNKNOWN,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
        )
        inferred_period_count += 1

        # Add a unique id to the new IP
        update_normalized_entity_with_globally_unique_id(
            person_id=person_id,
            entity=new_incarceration_period,
            state_code=state_code,
        )
        inferred_incarceration_periods.append(new_incarceration_period)

    return incarceration_periods + inferred_incarceration_periods
