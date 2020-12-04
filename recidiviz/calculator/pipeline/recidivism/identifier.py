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

"""Identifies instances of recidivism and non-recidivism for calculation.

This contains the core logic for identifying recidivism events on a
person-by-person basis, transforming incarceration periods for a given person
into instances of recidivism or non-recidivism as appropriate.


"""
from datetime import date
from typing import Dict, List, Optional, Any

from collections import defaultdict

from recidiviz.calculator.pipeline.recidivism.release_event import \
    ReincarcerationReturnType, ReleaseEvent, RecidivismReleaseEvent, NonRecidivismReleaseEvent
from recidiviz.calculator.pipeline.utils.execution_utils import extract_county_of_residence_from_rows
from recidiviz.calculator.pipeline.utils.incarceration_period_utils import \
    prepare_incarceration_periods_for_calculations, drop_temporary_custody_periods
from recidiviz.calculator.pipeline.utils.violation_utils import identify_most_severe_violation_type_and_subtype
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodStatus, is_revocation_admission
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodAdmissionReason as AdmissionReason
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodReleaseReason as ReleaseReason
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodSupervisionType
from recidiviz.common.constants.state.state_supervision_violation import \
    StateSupervisionViolationType
from recidiviz.common.date import DateRange, DateRangeDiff
from recidiviz.persistence.entity.entity_utils import get_single_state_code
from recidiviz.persistence.entity.state.entities import \
    StateIncarcerationPeriod, StateSupervisionViolationResponse


def find_release_events_by_cohort_year(
        incarceration_periods: List[StateIncarcerationPeriod],
        persons_to_recent_county_of_residence: List[Dict[str, Any]]) -> Dict[int, List[ReleaseEvent]]:
    """Finds instances of release and determines if they resulted in recidivism.

    Transforms each StateIncarcerationPeriod from which the person has been released into a mapping from its release
    cohort to the details of the event. The release cohort is an integer for the year, e.g. 2006. The event details
    are a ReleaseEvent object, which can represent events of both recidivism and non-recidivism. That is, each
    StateIncarcerationPeriod is transformed into a recidivism event unless it is the most recent period of
    incarceration and they are still incarcerated, or it is connected to a subsequent StateIncarcerationPeriod by a
    transfer.

    Example output for someone who went to prison in 2006, was released in 2008, went back in 2010, was released in
    2012, and never returned:
    {
      2008: [RecidivismReleaseEvent(original_admission_date="2006-04-05", ...)],
      2012: [NonRecidivismReleaseEvent(original_admission_date="2010-09-17", ...)]
    }

    Args:
        incarceration_periods: list of StateIncarcerationPeriods for a person
        persons_to_recent_county_of_residence: Reference table rows containing the county that the incarcerated person
            lives in (prior to incarceration).

    Returns:
        A dictionary mapping release cohorts to a list of ReleaseEvents for the given person in that cohort.
    """
    release_events: Dict[int, List[ReleaseEvent]] = defaultdict(list)

    if not incarceration_periods:
        return release_events

    state_code = get_single_state_code(incarceration_periods)

    county_of_residence = extract_county_of_residence_from_rows(persons_to_recent_county_of_residence)

    incarceration_periods = prepare_incarceration_periods_for_recidivism_calculations(
        state_code, incarceration_periods)

    for index, incarceration_period in enumerate(incarceration_periods):
        state_code = incarceration_period.state_code
        admission_date = incarceration_period.admission_date
        status = incarceration_period.status
        release_date = incarceration_period.release_date
        release_reason = incarceration_period.release_reason
        release_facility = incarceration_period.facility

        event = None
        next_incarceration_period = (incarceration_periods[index + 1]
                                     if index <= len(incarceration_periods) - 2 else None)

        if not should_include_in_release_cohort(status, release_date, release_reason, next_incarceration_period):
            # If this release should not be included in a release cohort, then we do not need to produce any events
            # for this period of incarceration
            continue

        if not release_date or not release_reason:
            raise ValueError("Incarceration_period must have valid release_date and release_reason to be included in"
                             " the release_cohort. Error in should_include_in_release_cohort.")

        # Admission data and status have been validated already
        if admission_date and status:
            if index == len(incarceration_periods) - 1:
                event = for_incarceration_period_no_return(
                    state_code, admission_date, release_date, release_facility, county_of_residence)
            else:
                reincarceration_period = find_valid_reincarceration_period(incarceration_periods, index, release_date)

                if not reincarceration_period:
                    # We were unable to identify a reincarceration for this period
                    event = for_incarceration_period_no_return(
                        state_code, admission_date, release_date, release_facility, county_of_residence)
                else:
                    reincarceration_date = reincarceration_period.admission_date
                    reincarceration_facility = reincarceration_period.facility
                    reincarceration_admission_reason = reincarceration_period.admission_reason

                    if is_revocation_admission(reincarceration_admission_reason):
                        source_supervision_violation_response = \
                            reincarceration_period.source_supervision_violation_response
                    else:
                        source_supervision_violation_response = None

                    # These fields have been validated already
                    if not reincarceration_date or not reincarceration_admission_reason:
                        raise ValueError("Incarceration period pre-processing should have set admission_dates and"
                                         "admission_reasons on all periods.")

                    event = for_intermediate_incarceration_period(
                        state_code=state_code,
                        admission_date=admission_date,
                        release_date=release_date,
                        release_facility=release_facility,
                        county_of_residence=county_of_residence,
                        reincarceration_date=reincarceration_date,
                        reincarceration_facility=reincarceration_facility,
                        reincarceration_admission_reason=reincarceration_admission_reason,
                        source_supervision_violation_response=source_supervision_violation_response)

        if event:
            if release_date:
                release_cohort = release_date.year
                release_events[release_cohort].append(event)

    return release_events


def prepare_incarceration_periods_for_recidivism_calculations(
        state_code: str, incarceration_periods: List[StateIncarcerationPeriod]) -> List[StateIncarcerationPeriod]:
    """Returns a filtered list of the provided |incarceration_periods| to be used for recidivism calculation."""

    incarceration_periods = prepare_incarceration_periods_for_calculations(
        state_code,
        incarceration_periods,
        collapse_transfers=True,
        collapse_temporary_custody_periods_with_revocation=True,
        collapse_transfers_with_different_pfi=True,
        overwrite_facility_information_in_transfers=True)

    # TODO(#2936): Consider not dropping temporary custody periods when we want to use the recidivism output for states
    #  that may have temporary custody periods at this point (currently just US_MO).
    incarceration_periods = drop_temporary_custody_periods(incarceration_periods)

    return incarceration_periods


def find_valid_reincarceration_period(incarceration_periods: List[StateIncarcerationPeriod],
                                      index: int, release_date: date) -> Optional[StateIncarcerationPeriod]:
    """Finds a StateIncarcerationPeriod representing an instance of reincarceration following a release from prison,
    where the admission_date on the incarceration period occurred on or after the release_date."""

    for i in range(index + 1, len(incarceration_periods)):
        ip = incarceration_periods[i]

        if not ip.admission_date or not ip.admission_reason:
            raise ValueError("All incarceration periods should have set a admission_date and admission_reason after"
                             "pre-processing.")

        if ip.admission_date < release_date:
            raise ValueError(f"Release from incarceration on date {release_date} overlaps with another period of"
                             " incarceration. Failure in IP pre-processing or should_include_in_release_cohort"
                             f" function: {incarceration_periods}")

        if ip.admission_reason in (
                AdmissionReason.ADMITTED_IN_ERROR,
                AdmissionReason.RETURN_FROM_ESCAPE,
                AdmissionReason.RETURN_FROM_ERRONEOUS_RELEASE,
                AdmissionReason.TEMPORARY_CUSTODY,
                AdmissionReason.TRANSFERRED_FROM_OUT_OF_STATE
        ):
            continue
        if ip.admission_reason in (
                AdmissionReason.EXTERNAL_UNKNOWN,
                AdmissionReason.INTERNAL_UNKNOWN,
                AdmissionReason.NEW_ADMISSION,
                AdmissionReason.PAROLE_REVOCATION,
                AdmissionReason.PROBATION_REVOCATION,
                AdmissionReason.DUAL_REVOCATION,
                AdmissionReason.RETURN_FROM_SUPERVISION,
                # This should be a rare case, but we are considering this a valid reincarceration admission
                # because this person became reincarcerated at some point after being released.
                AdmissionReason.TRANSFER
        ):
            return ip

        raise ValueError("Enum case not handled for StateIncarcerationPeriodAdmissionReason of type:"
                         f" {ip.admission_reason}.")

    return None


def for_incarceration_period_no_return(
        state_code: str,
        admission_date: date,
        release_date: date,
        release_facility: Optional[str],
        county_of_residence: Optional[str]) \
        -> Optional[ReleaseEvent]:
    """Builds a NonRecidivismReleaseEvent from the attributes of the release from incarceration.

    Returns any non-recidivism event relevant to the person's StateIncarcerationPeriod.

        Args:
            state_code: the state code on the StateIncarcerationPeriod
            admission_date: when this StateIncarcerationPeriod started
            release_date: when they were released from this StateIncarcerationPeriod
            release_facility: the facility they were released from on this StateIncarcerationPeriod
            county_of_residence: the county that the incarcerated person lives in (prior to incarceration).

        Returns:
            A non-recidivism event.
    """
    return NonRecidivismReleaseEvent(
        state_code=state_code,
        original_admission_date=admission_date,
        release_date=release_date,
        release_facility=release_facility,
        county_of_residence=county_of_residence)


def for_intermediate_incarceration_period(
        state_code: str,
        admission_date: date,
        release_date: date,
        release_facility: Optional[str],
        county_of_residence: Optional[str],
        reincarceration_date: date,
        reincarceration_facility: Optional[str],
        reincarceration_admission_reason: AdmissionReason,
        source_supervision_violation_response:
        Optional[StateSupervisionViolationResponse]) -> \
        Optional[ReleaseEvent]:
    """Returns the ReleaseEvent relevant to an intermediate
    StateIncarcerationPeriod.

    If this is not the person's last StateIncarcerationPeriod and they have been
    released, there is probably an instance of recidivism to count.

    Args:
        state_code: state where the incarceration takes place
        admission_date: when the StateIncarcerationPeriod started
        release_date: when they were released from the StateIncarcerationPeriod
        release_facility: the facility they were released from on the
            StateIncarcerationPeriod
        reincarceration_date: date they were admitted to the subsequent
            StateIncarcerationPeriod
        county_of_residence: the county that the incarcerated person lives in
            (prior to incarceration).
        reincarceration_facility: facility in which the subsequent
            StateIncarcerationPeriod started
        reincarceration_admission_reason: reason they were admitted to the
            subsequent StateIncarcerationPeriod
        source_supervision_violation_response: the response to a supervision
            violation that resulted in the reincarceration

    Returns:
        A ReleaseEvent.
    """
    return_type = get_return_type(reincarceration_admission_reason)
    from_supervision_type = get_from_supervision_type(reincarceration_admission_reason)
    source_violation_type = get_source_violation_type(source_supervision_violation_response)

    # This is a new admission recidivism event. Return it.
    return RecidivismReleaseEvent(
        state_code=state_code,
        original_admission_date=admission_date,
        release_date=release_date,
        release_facility=release_facility,
        county_of_residence=county_of_residence,
        reincarceration_date=reincarceration_date,
        reincarceration_facility=reincarceration_facility,
        return_type=return_type,
        from_supervision_type=from_supervision_type,
        source_violation_type=source_violation_type)


def should_include_in_release_cohort(
        status: StateIncarcerationPeriodStatus,
        release_date: Optional[date],
        release_reason: Optional[ReleaseReason],
        next_incarceration_period: Optional[StateIncarcerationPeriod]) -> bool:
    """Identifies whether a period of incarceration with the given features should be included in the release
    cohort."""
    # If the person is still in custody, there is no release to include in a cohort.
    if status == StateIncarcerationPeriodStatus.IN_CUSTODY:
        return False

    if not release_date:
        # If the person is not in custody, there should be a release_date.
        # This should not happen after validation. Throw error.
        raise ValueError("release_date is not set where it should be.")

    if not release_reason:
        # If there is no recorded release reason, then we cannot classify this as a valid release for the cohort
        return False

    if next_incarceration_period:
        time_range_release = DateRange.for_day(release_date)
        if DateRangeDiff(time_range_release, next_incarceration_period.duration).overlapping_range:
            # If the release overlaps with the following incarceration period, this is not an actual release from
            # incarceration
            return False

        if next_incarceration_period.release_date and release_date == next_incarceration_period.release_date:
            # This release shares a release_date with the next incarceration period. Do not include this release.
            return False

    if release_reason in [ReleaseReason.DEATH, ReleaseReason.EXECUTION]:
        # If the person was released from this incarceration period because they died or were executed, do not include
        # them in the release cohort.
        return False
    if release_reason == ReleaseReason.ESCAPE:
        # If the person was released from this incarceration period because they escaped, do not include them in the
        # release cohort.
        return False
    if release_reason == ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY:
        # If the person was released from a period of temporary custody, do not include them in the release_cohort.
        return False
    if release_reason == ReleaseReason.RELEASED_IN_ERROR:
        # If the person was released from this incarceration period due to an error, do not include them in the
        # release cohort.
        return False
    if release_reason == ReleaseReason.TRANSFER:
        # If the person was released from this incarceration period because they were transferred elsewhere, do not
        # include them in the release cohort.
        return False
    if release_reason == ReleaseReason.TRANSFERRED_OUT_OF_STATE:
        # Releases where the person has been transferred out of state don't really count as true releases.
        return False
    if release_reason == ReleaseReason.COURT_ORDER:
        # If the person was released from this incarceration period due to a court order, do not include them in the
        # release cohort.
        return False
    if release_reason in (ReleaseReason.EXTERNAL_UNKNOWN, ReleaseReason.INTERNAL_UNKNOWN):
        # We do not have enough information to determine whether this release qualifies for inclusion in the release
        # cohort.
        return False
    if release_reason in (ReleaseReason.COMMUTED,
                          ReleaseReason.COMPASSIONATE,
                          ReleaseReason.CONDITIONAL_RELEASE,
                          ReleaseReason.PARDONED,
                          ReleaseReason.RELEASED_FROM_ERRONEOUS_ADMISSION,
                          ReleaseReason.SENTENCE_SERVED,
                          ReleaseReason.VACATED):
        return True

    raise ValueError("Enum case not handled for "
                     "StateIncarcerationPeriodReleaseReason of type:"
                     f" {release_reason}.")


def get_return_type(reincarceration_admission_reason: AdmissionReason) -> ReincarcerationReturnType:
    """Returns the return type for the reincarceration admission reason."""
    if reincarceration_admission_reason == AdmissionReason.EXTERNAL_UNKNOWN:
        return ReincarcerationReturnType.NEW_ADMISSION
    if reincarceration_admission_reason == AdmissionReason.INTERNAL_UNKNOWN:
        return ReincarcerationReturnType.NEW_ADMISSION
    if reincarceration_admission_reason == AdmissionReason.NEW_ADMISSION:
        return ReincarcerationReturnType.NEW_ADMISSION
    if is_revocation_admission(reincarceration_admission_reason):
        return ReincarcerationReturnType.REVOCATION
    if reincarceration_admission_reason == AdmissionReason.TRANSFER:
        # This should be a rare case, but we are considering this a type of new admission recidivism because this person
        # became reincarcerated at some point after being released, and this is the most likely return type in most
        # cases in the absence of further information.
        return ReincarcerationReturnType.NEW_ADMISSION
    if reincarceration_admission_reason in (AdmissionReason.ADMITTED_IN_ERROR,
                                            AdmissionReason.RETURN_FROM_ESCAPE,
                                            AdmissionReason.RETURN_FROM_ERRONEOUS_RELEASE,
                                            AdmissionReason.TEMPORARY_CUSTODY,
                                            AdmissionReason.TRANSFERRED_FROM_OUT_OF_STATE):
        # This should never happen. Should have been filtered by find_valid_reincarceration_period function.
        raise ValueError(f"find_valid_reincarceration_period is not excluding invalid returns. Found unexpected "
                         f"admission_reason of: {reincarceration_admission_reason}")

    raise ValueError(f"Enum case not handled for StateIncarcerationPeriodAdmissionReason of type:"
                     f" {reincarceration_admission_reason}.")


def get_from_supervision_type(
        reincarceration_admission_reason: AdmissionReason) -> Optional[StateSupervisionPeriodSupervisionType]:
    """If the person returned from supervision, returns the type."""

    if reincarceration_admission_reason in [
        AdmissionReason.EXTERNAL_UNKNOWN,
        AdmissionReason.INTERNAL_UNKNOWN,
        AdmissionReason.NEW_ADMISSION,
        AdmissionReason.TRANSFER
    ]:
        return None
    if reincarceration_admission_reason == AdmissionReason.RETURN_FROM_SUPERVISION:
        return StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN
    if reincarceration_admission_reason == AdmissionReason.PAROLE_REVOCATION:
        return StateSupervisionPeriodSupervisionType.PAROLE
    if reincarceration_admission_reason == AdmissionReason.DUAL_REVOCATION:
        return StateSupervisionPeriodSupervisionType.DUAL
    if reincarceration_admission_reason == AdmissionReason.PROBATION_REVOCATION:
        return StateSupervisionPeriodSupervisionType.PROBATION
    if reincarceration_admission_reason in (AdmissionReason.ADMITTED_IN_ERROR,
                                            AdmissionReason.RETURN_FROM_ESCAPE,
                                            AdmissionReason.RETURN_FROM_ERRONEOUS_RELEASE,
                                            AdmissionReason.TEMPORARY_CUSTODY,
                                            AdmissionReason.TRANSFERRED_FROM_OUT_OF_STATE):
        # This should never happen. Should have been filtered by find_valid_reincarceration_period function.
        raise ValueError(f"find_valid_reincarceration_period is not excluding invalid returns. Found unexpected "
                         f"admission_reason of: {reincarceration_admission_reason}")

    raise ValueError("Enum case not handled for StateIncarcerationPeriodAdmissionReason of type:"
                     f" {reincarceration_admission_reason}.")


def get_source_violation_type(source_supervision_violation_response: Optional[StateSupervisionViolationResponse]) \
        -> Optional[StateSupervisionViolationType]:
    """Returns, where applicable, the type of violation that caused the period of incarceration.

    If the person returned from supervision, and we know the supervision violation response that caused the return, then
    this returns the type of violation that prompted this revocation.
    """

    if source_supervision_violation_response:
        supervision_violation = source_supervision_violation_response.supervision_violation
        if supervision_violation:
            violation_type, _ = identify_most_severe_violation_type_and_subtype([supervision_violation])
            return violation_type

    return None
