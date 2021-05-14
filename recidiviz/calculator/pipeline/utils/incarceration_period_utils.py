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

import logging
from copy import deepcopy
from datetime import date

from typing import List, Optional

import attr

from recidiviz.calculator.pipeline.utils.period_utils import (
    sort_periods_by_set_dates_and_statuses,
)
from recidiviz.common.attr_mixins import BuildableAttr
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason as AdmissionReason,
    StateIncarcerationPeriodStatus,
    is_official_admission,
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodReleaseReason as ReleaseReason,
)
from recidiviz.common.date import DateRangeDiff
from recidiviz.persistence.entity.entity_utils import is_placeholder


VALID_TRANSFER_THRESHOLD_DAYS: int = 1


@attr.s
class IncarcerationPreProcessingConfig(BuildableAttr):
    """Defines how the incarceration periods should be prepared for the calculations."""

    # Whether or not to drop periods with an admission_reason of TEMPORARY_CUSTODY
    drop_temporary_custody_periods: bool = attr.ib()

    # Whether or not to drop periods with an incarceration_type that is not STATE_PRISON
    drop_non_state_prison_incarceration_type_periods: bool = attr.ib()

    # Whether or not to collapse chronologically adjacent periods that are connected
    # by a transfer release and transfer admission
    collapse_transfers: bool = attr.ib()

    # Whether or not to collapse periods of temporary custody that are followed by
    # admissions to prison for a supervision revocation
    collapse_temporary_custody_periods_with_revocation: bool = attr.ib()

    # Whether or not to collapse chronologically adjacent periods that are connected
    # by a transfer release and transfer admission but have different
    # specialized_purpose_for_incarceration values
    collapse_transfers_with_different_pfi: bool = attr.ib()

    # Whether or not to overwrite facility information when collapsing transfer edges
    overwrite_facility_information_in_transfers: bool = attr.ib()

    # The end date of the earliest period ending in death. None if no periods end in death.
    earliest_death_date: Optional[date] = attr.ib(default=None)


def drop_placeholder_periods(
    incarceration_periods: List[StateIncarcerationPeriod],
) -> List[StateIncarcerationPeriod]:
    """Removes any incarceration periods that are placeholders. Returns the valid incarceration periods."""
    return [ip for ip in incarceration_periods if not is_placeholder(ip)]


def collapse_incarceration_period_transfers(
    sorted_incarceration_periods: List[StateIncarcerationPeriod],
    overwrite_facility_information_in_transfers: bool = True,
    collapse_transfers_with_different_pfi: bool = True,
) -> List[StateIncarcerationPeriod]:
    """Collapses any incarceration periods that are connected by transfers.
    Loops through all of the StateIncarcerationPeriods and combines adjacent
    periods that are connected by a transfer. Only connects two periods if the
    release reason of the first is `TRANSFER` and the admission reason for the
    second is also `TRANSFER`.
    Args:
        sorted_incarceration_periods: list of StateIncarcerationPeriods for a StatePerson, sorted by ascending
            admission_date
        overwrite_facility_information_in_transfers: Whether or not to overwrite facility information when
            collapsing transfers.
        collapse_transfers_with_different_pfi: Whether or not to collapse two periods connected by a transfer
            if their overwrite_facility_information_in_transfers values are different
    Returns:
        A list of collapsed StateIncarcerationPeriods.
    """

    new_incarceration_periods: List[StateIncarcerationPeriod] = []
    open_transfer = False

    # TODO(#1782): Check to see if back to back incarceration periods are related
    #  to the same StateIncarcerationSentence or SentenceGroup to be sure we
    #  aren't counting stacked sentences or related periods as recidivism.
    for incarceration_period in sorted_incarceration_periods:
        if open_transfer:
            admission_reason = incarceration_period.admission_reason

            # Do not collapse any period with an official admission reason
            if (
                not is_official_admission(admission_reason)
                and admission_reason == AdmissionReason.TRANSFER
            ):
                # If there is an open transfer period and they were
                # transferred into this incarceration period, then combine this
                # period with the open transfer period.
                start_period = new_incarceration_periods.pop(-1)

                if (
                    not collapse_transfers_with_different_pfi
                    and start_period.specialized_purpose_for_incarceration
                    != incarceration_period.specialized_purpose_for_incarceration
                ):
                    # If periods with different specialized_purpose_for_incarceration values should not be collapsed,
                    # and this period has a different specialized_purpose_for_incarceration value than the one before
                    # it, add the two period separately
                    new_incarceration_periods.append(start_period)
                    new_incarceration_periods.append(incarceration_period)
                else:
                    combined_period = combine_incarceration_periods(
                        start_period,
                        incarceration_period,
                        overwrite_facility_information=overwrite_facility_information_in_transfers,
                    )
                    new_incarceration_periods.append(combined_period)
            else:
                # They weren't transferred here. Add this as a new
                # incarceration period.
                # TODO(#1790): Analyze how often a transfer out is followed by an
                #  admission type that isn't a transfer to ensure we aren't
                #  making bad assumptions with this transfer logic.
                new_incarceration_periods.append(incarceration_period)
        else:
            # TODO(#1790): Analyze how often an incarceration period that starts
            #  with a transfer in is not preceded by a transfer out of a
            #  different facility.
            new_incarceration_periods.append(incarceration_period)

        # If this incarceration period ended in a transfer, then flag
        # that there's an open transfer period.
        open_transfer = incarceration_period.release_reason == ReleaseReason.TRANSFER

    return new_incarceration_periods


def collapse_temporary_custody_and_revocation_periods(
    sorted_incarceration_periods: List[StateIncarcerationPeriod],
) -> List[StateIncarcerationPeriod]:
    """Looks through the |sorted_incarceration_periods| and collapses consecutive periods only if the first period
    has a temporary custody admission reason and the subsequent period has a revocation admission reason. When the
    periods are collapsed, the revocation admission reason is kept over the temporary custody admission reason.
    This method assumes the input |sorted_incarceration_periods| are sorted by ascending admission reason.
    """

    previous_period = None
    collapsed_ips = []
    for incarceration_period in sorted_incarceration_periods:
        if not previous_period:
            previous_period = incarceration_period
            continue

        if (
            previous_period.release_date == incarceration_period.admission_date
            and previous_period.admission_reason == AdmissionReason.TEMPORARY_CUSTODY
            and incarceration_period.admission_reason
            in [
                AdmissionReason.DUAL_REVOCATION,
                AdmissionReason.PAROLE_REVOCATION,
                AdmissionReason.PROBATION_REVOCATION,
            ]
        ):
            merged_period = combine_incarceration_periods(
                previous_period,
                incarceration_period,
                overwrite_admission_reason=True,
                overwrite_facility_information=True,
            )
            collapsed_ips.append(merged_period)
            previous_period = None
        else:
            collapsed_ips.append(previous_period)
            previous_period = incarceration_period

    if previous_period:
        collapsed_ips.append(previous_period)

    return collapsed_ips


def combine_incarceration_periods(
    start: StateIncarcerationPeriod,
    end: StateIncarcerationPeriod,
    overwrite_admission_reason: bool = False,
    overwrite_facility_information: bool = False,
) -> StateIncarcerationPeriod:
    """Combines two StateIncarcerationPeriods.
    Brings together two StateIncarcerationPeriods by setting the following
    fields on a deep copy of the |start| StateIncarcerationPeriod to the values
    on the |end| StateIncarcerationPeriod:
        [status, release_date, facility, housing_unit, facility_security_level,
        facility_security_level_raw_text, projected_release_reason,
        projected_release_reason_raw_text, release_reason,
        release_reason_raw_text]
        Args:
            start: The starting StateIncarcerationPeriod.
            end: The ending StateIncarcerationPeriod.
            overwrite_admission_reason: Whether to use the end admission reason instead of the start admission reason.
            overwrite_facility_information: Whether to use the facility, housing, and purpose for incarceration
                information on the end period instead of on the start period.
    """

    collapsed_incarceration_period = deepcopy(start)

    if overwrite_admission_reason:
        collapsed_incarceration_period.admission_reason = end.admission_reason
        collapsed_incarceration_period.admission_reason_raw_text = (
            end.admission_reason_raw_text
        )

    if overwrite_facility_information:
        collapsed_incarceration_period.facility = end.facility
        collapsed_incarceration_period.facility_security_level = (
            end.facility_security_level
        )
        collapsed_incarceration_period.facility_security_level_raw_text = (
            end.facility_security_level_raw_text
        )
        collapsed_incarceration_period.housing_unit = end.housing_unit
        # We want the latest non-null specialized_purpose_for_incarceration
        if end.specialized_purpose_for_incarceration is not None:
            collapsed_incarceration_period.specialized_purpose_for_incarceration = (
                end.specialized_purpose_for_incarceration
            )
            collapsed_incarceration_period.specialized_purpose_for_incarceration_raw_text = (
                end.specialized_purpose_for_incarceration_raw_text
            )

    collapsed_incarceration_period.status = end.status
    collapsed_incarceration_period.release_date = end.release_date
    collapsed_incarceration_period.projected_release_reason = (
        end.projected_release_reason
    )
    collapsed_incarceration_period.projected_release_reason_raw_text = (
        end.projected_release_reason_raw_text
    )
    collapsed_incarceration_period.release_reason = end.release_reason
    collapsed_incarceration_period.release_reason_raw_text = end.release_reason_raw_text

    return collapsed_incarceration_period


def _is_active_period(period: StateIncarcerationPeriod) -> bool:
    return period.status == StateIncarcerationPeriodStatus.IN_CUSTODY


def _is_transfer_start(period: StateIncarcerationPeriod) -> bool:
    return period.admission_reason == AdmissionReason.TRANSFER


def standard_date_sort_for_incarceration_periods(
    incarceration_periods: List[StateIncarcerationPeriod],
) -> List[StateIncarcerationPeriod]:
    """Sorts incarceration periods chronologically by dates and statuses."""
    sort_periods_by_set_dates_and_statuses(
        incarceration_periods, _is_active_period, _is_transfer_start
    )

    return incarceration_periods


def prepare_incarceration_periods_for_calculations(
    incarceration_periods: List[StateIncarcerationPeriod],
    ip_preprocessing_config: IncarcerationPreProcessingConfig,
) -> List[StateIncarcerationPeriod]:
    """Validates, sorts, and collapses the incarceration period inputs.
    Ensures the necessary dates and fields are set on each incarceration period. If an incarceration period is found
    with missing data, drops the incarceration period from the calculations. Then, sorts the list of valid
    StateIncarcerationPeriods by admission_date, and collapses the ones connected by a transfer.
    """

    updated_periods = _filter_and_update_incarceration_periods_for_calculations(
        incarceration_periods, ip_preprocessing_config
    )

    sorted_periods = standard_date_sort_for_incarceration_periods(updated_periods)

    collapsed_periods = _collapse_incarceration_periods_for_calculations(
        sorted_periods,
        collapse_transfers=ip_preprocessing_config.collapse_transfers,
        collapse_temporary_custody_periods_with_revocation=ip_preprocessing_config.collapse_temporary_custody_periods_with_revocation,
        collapse_transfers_with_different_pfi=ip_preprocessing_config.collapse_transfers_with_different_pfi,
        overwrite_facility_information_in_transfers=ip_preprocessing_config.overwrite_facility_information_in_transfers,
    )
    return collapsed_periods


def _filter_and_update_incarceration_periods_for_calculations(
    incarceration_periods: List[StateIncarcerationPeriod],
    ip_preprocessing_config: IncarcerationPreProcessingConfig,
) -> List[StateIncarcerationPeriod]:
    """Returns a modified and filtered subset of the provided |incarceration_periods| list so that all remaining
    periods have the the fields necessary for calculations.
    """
    if not incarceration_periods:
        return []

    filtered_incarceration_periods = drop_placeholder_periods(incarceration_periods)

    filtered_incarceration_periods = _infer_missing_dates_and_statuses(
        filtered_incarceration_periods, ip_preprocessing_config.earliest_death_date
    )

    if ip_preprocessing_config.drop_temporary_custody_periods:
        filtered_incarceration_periods = drop_temporary_custody_periods(
            filtered_incarceration_periods
        )

    if ip_preprocessing_config.drop_non_state_prison_incarceration_type_periods:
        filtered_incarceration_periods = _drop_non_prison_periods(
            filtered_incarceration_periods
        )

    filtered_incarceration_periods = _drop_zero_day_erroneous_periods(
        filtered_incarceration_periods
    )

    updated_incarceration_periods = _infer_missing_dates_and_statuses(
        filtered_incarceration_periods
    )

    return updated_incarceration_periods


def _infer_missing_dates_and_statuses(
    incarceration_periods: List[StateIncarcerationPeriod],
    earliest_death_date: Optional[date] = None,
) -> List[StateIncarcerationPeriod]:
    """First, sorts the incarceration_periods in chronological order of the admission and release dates. Then, for any
    periods missing dates and statuses, infers this information given the other incarceration periods.
    """
    standard_date_sort_for_incarceration_periods(incarceration_periods)

    updated_periods: List[StateIncarcerationPeriod] = []

    for index, ip in enumerate(incarceration_periods):
        previous_ip = incarceration_periods[index - 1] if index > 0 else None
        next_ip = (
            incarceration_periods[index + 1]
            if index < len(incarceration_periods) - 1
            else None
        )

        if earliest_death_date:
            if ip.admission_date and earliest_death_date <= ip.admission_date:
                # If a period starts after the earliest_death_date, drop the period.
                logging.info(
                    "Dropping incarceration period with with an admission_date after a release due to death: [%s]",
                    ip,
                )
                continue
            if (
                ip.release_date and ip.release_date > earliest_death_date
            ) or ip.release_date is None:
                # If the incarceration period duration exceeds the earliest_death_date or is not terminated,
                # set the release date to earliest_death_date, change release_reason to DEATH, update status
                ip.release_date = earliest_death_date
                ip.release_reason = StateIncarcerationPeriodReleaseReason.DEATH
                ip.status = StateIncarcerationPeriodStatus.NOT_IN_CUSTODY

        if ip.release_date is None:
            if next_ip:
                # This is not the last incarceration period in the list. Set the release date to the next admission or
                # release date.
                ip.release_date = (
                    next_ip.admission_date
                    if next_ip.admission_date
                    else next_ip.release_date
                )

                if ip.release_reason is None:
                    if next_ip.admission_reason == AdmissionReason.TRANSFER:
                        # If they were transferred into the next period, infer that this release was a transfer
                        ip.release_reason = ReleaseReason.TRANSFER

                ip.status = StateIncarcerationPeriodStatus.NOT_IN_CUSTODY
            else:
                # This is the last incarceration period in the list.
                if ip.status != StateIncarcerationPeriodStatus.IN_CUSTODY:
                    # If the person is no longer in custody on this period, set the release date to the admission date.
                    ip.release_date = ip.admission_date
                    ip.release_reason = ReleaseReason.INTERNAL_UNKNOWN
                elif ip.release_reason or ip.release_reason_raw_text:
                    # There is no release date on this period, but the set release_reason indicates that the person
                    # is no longer in custody. Set the release date to the admission date.
                    ip.release_date = ip.admission_date
                    ip.status = StateIncarcerationPeriodStatus.NOT_IN_CUSTODY

                    logging.warning(
                        "No release_date for incarceration period (%d) with nonnull release_reason (%s) or "
                        "release_reason_raw_text (%s)",
                        ip.incarceration_period_id,
                        ip.release_reason,
                        ip.release_reason_raw_text,
                    )
        elif ip.release_date > date.today():
            # This is an erroneous release_date in the future. For the purpose of calculations, clear the release_date
            # and the release_reason.
            ip.release_date = None
            ip.release_reason = None
            ip.status = StateIncarcerationPeriodStatus.IN_CUSTODY

        if ip.admission_date is None:
            if previous_ip:
                # If the admission date is not set, and this is not the first incarceration period, then set the
                # admission_date to be the same as the release_date or admission_date of the preceding period
                ip.admission_date = (
                    previous_ip.release_date
                    if previous_ip.release_date
                    else previous_ip.admission_date
                )

                if ip.admission_reason is None:
                    if previous_ip.release_reason == ReleaseReason.TRANSFER:
                        # If they were transferred out of the previous period, infer that this admission was a transfer
                        ip.admission_reason = AdmissionReason.TRANSFER
            else:
                # If the admission date is not set, and this is the first incarceration period, then set the
                # admission_date to be the same as the release_date
                ip.admission_date = ip.release_date
                ip.admission_reason = AdmissionReason.INTERNAL_UNKNOWN
        elif ip.admission_date > date.today():
            logging.info(
                "Dropping incarceration period with admission_date in the future: [%s]",
                ip,
            )
            continue

        if ip.admission_reason is None:
            # We have no idea what this admission reason was. Set as INTERNAL_UNKNOWN.
            ip.admission_reason = AdmissionReason.INTERNAL_UNKNOWN
        if ip.release_date is not None and ip.release_reason is None:
            # We have no idea what this release reason was. Set as INTERNAL_UNKNOWN.
            ip.release_reason = ReleaseReason.INTERNAL_UNKNOWN

        if ip.admission_date and ip.release_date:
            if ip.release_date < ip.admission_date:
                logging.info(
                    "Dropping incarceration period with release before admission: [%s]",
                    ip,
                )
                continue

            if updated_periods:
                most_recent_valid_period = updated_periods[-1]

                if _ip_is_nested_in_previous_period(ip, most_recent_valid_period):
                    # This period is entirely nested within the period before it. Do not include in the list of periods.
                    logging.info(
                        "Dropping incarceration period [%s] that is nested in period [%s]",
                        ip,
                        most_recent_valid_period,
                    )
                    continue

        updated_periods.append(ip)

    return updated_periods


def _ip_is_nested_in_previous_period(
    ip: StateIncarcerationPeriod, previous_ip: StateIncarcerationPeriod
) -> bool:
    """Returns whether the StateIncarcerationPeriod |ip| is entirely nested within the |previous_ip|. Both periods
    must have set admission and release dates.
    A nested period is defined as an incarceration period that overlaps with the previous_ip and has no parts that are
    non-overlapping with the previous_ip. Single-day periods (admission_date = release_date) by definition do not have
    overlapping ranges with another period because the ranges are end date exclusive. If a single-day period falls
    within the admission and release of the previous_ip, then it is nested within that period. If a single-day period
    falls on the previous_ip.release_date, then it is not nested within that period.
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


# TODO(#4661): Update this logic once we have standardized our representation of parole board holds and periods of
#  temporary custody
def drop_temporary_custody_periods(
    incarceration_periods: List[StateIncarcerationPeriod],
) -> List[StateIncarcerationPeriod]:
    """Removes any incarceration periods that denote an admission to a temporary custody.
    Returns the filtered incarceration periods.
    """

    return [
        ip
        for ip in incarceration_periods
        if ip.admission_reason != AdmissionReason.TEMPORARY_CUSTODY
    ]


def _drop_non_prison_periods(
    incarceration_periods: List[StateIncarcerationPeriod],
) -> List[StateIncarcerationPeriod]:
    """Removes any incarceration periods where the incarceration type isn't STATE_PRISON.
    Returns the filtered incarceration periods.
    """
    return [
        ip
        for ip in incarceration_periods
        if ip.incarceration_type == StateIncarcerationType.STATE_PRISON
    ]


def _collapse_incarceration_periods_for_calculations(
    sorted_incarceration_periods: List[StateIncarcerationPeriod],
    collapse_temporary_custody_periods_with_revocation: bool,
    collapse_transfers: bool,
    collapse_transfers_with_different_pfi: bool,
    overwrite_facility_information_in_transfers: bool,
) -> List[StateIncarcerationPeriod]:
    """Collapses the provided |sorted_incarceration_periods| based on the input params
    |collapse_temporary_custody_periods_with_revocation| and |collapse_transfers_with_different_pfi|. Assumes the
    |sorted_incarceration_periods| are sorted based on ascending admission_date.
    """
    collapsed_periods = sorted_incarceration_periods

    if collapse_transfers:
        collapsed_periods = collapse_incarceration_period_transfers(
            collapsed_periods,
            overwrite_facility_information_in_transfers,
            collapse_transfers_with_different_pfi,
        )

    if collapse_temporary_custody_periods_with_revocation:
        collapsed_periods = collapse_temporary_custody_and_revocation_periods(
            collapsed_periods
        )

    return collapsed_periods


def _drop_zero_day_erroneous_periods(
    incarceration_periods: List[StateIncarcerationPeriod],
) -> List[StateIncarcerationPeriod]:
    """Removes any incarceration periods where the admission_date is the same as the
    release_date, where one of the following is true:
    - The release_reason is RELEASED_FROM_ERRONEOUS_ADMISSION, and
        where the admission_reason is not TRANSFER.
    - The admission_reason is ADMITTED_FROM_SUPERVISION and the release_reason is
        CONDITIONAL_RELEASE.
    It is reasonable to assume that these periods are erroneous and should not be
    considered in any metrics involving incarceration.
    Returns the filtered incarceration periods.
    """
    periods_to_keep: List[StateIncarcerationPeriod] = []

    for ip in incarceration_periods:
        if (
            ip.admission_date
            and ip.release_date
            and ip.admission_date == ip.release_date
        ):
            # This is a zero-day period
            if (
                ip.release_reason == ReleaseReason.RELEASED_FROM_ERRONEOUS_ADMISSION
                and ip.admission_reason != AdmissionReason.TRANSFER
            ):
                # A release from an erroneous admission on a non-transfer zero-day
                # period is reliably an entirely erroneous period
                continue
            if (
                ip.admission_reason == AdmissionReason.ADMITTED_FROM_SUPERVISION
                and ip.release_reason == ReleaseReason.CONDITIONAL_RELEASE
            ):
                # A zero-day return from supervision and then immediate conditional
                # release is reliably an entirely erroneous period
                continue

        periods_to_keep.append(ip)

    return periods_to_keep


def period_edges_are_valid_transfer(
    first_incarceration_period: Optional[StateIncarcerationPeriod] = None,
    second_incarceration_period: Optional[StateIncarcerationPeriod] = None,
) -> bool:
    """Returns whether the edge between two incarceration periods is a valid transfer.
    Valid transfer means:
       - The adjacent release reason and admission reason between two consecutive periods is TRANSFER
       - The adjacent release date and admission date between two consecutive periods is less than or
       equal to the VALID_TRANSFER_THRESHOLD
    """
    if not first_incarceration_period or not second_incarceration_period:
        return False

    release_reason = first_incarceration_period.release_reason
    release_date = first_incarceration_period.release_date

    admission_reason = second_incarceration_period.admission_reason
    admission_date = second_incarceration_period.admission_date

    if (
        not release_reason
        or not release_date
        or not admission_reason
        or not admission_date
    ):
        # If there is no release reason or admission reason, then this is not a valid period edge
        return False

    days_between_periods = (admission_date - release_date).days

    return (
        days_between_periods <= VALID_TRANSFER_THRESHOLD_DAYS
        and admission_reason == StateIncarcerationPeriodAdmissionReason.TRANSFER
        and release_reason == StateIncarcerationPeriodReleaseReason.TRANSFER
    )
