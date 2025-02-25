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
import logging
from typing import Dict, List, Optional

from collections import defaultdict
from recidiviz.calculator.pipeline.recidivism.release_event import \
    ReincarcerationReturnType, ReincarcerationReturnFromSupervisionType, \
    ReleaseEvent, RecidivismReleaseEvent, NonRecidivismReleaseEvent
from recidiviz.calculator.pipeline.utils.calculator_utils import \
    identify_most_severe_violation_type_and_subtype
from recidiviz.calculator.pipeline.utils.incarceration_period_utils import \
    prepare_incarceration_periods_for_calculations
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodStatus
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodAdmissionReason as AdmissionReason
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodReleaseReason as ReleaseReason
from recidiviz.common.constants.state.state_supervision_violation import \
    StateSupervisionViolationType
from recidiviz.persistence.entity.state.entities import \
    StateIncarcerationPeriod, StateSupervisionViolationResponse


def find_release_events_by_cohort_year(
        incarceration_periods: List[StateIncarcerationPeriod],
        county_of_residence: Optional[str]) \
        -> Dict[int, List[ReleaseEvent]]:
    """Finds instances of release and determines if they resulted in recidivism.

    Transforms each StateIncarcerationPeriod from which the person has been
    released into a mapping from its release cohort to the details of the event.
    The release cohort is an integer for the year, e.g. 2006. The event details
    are a ReleaseEvent object, which can represent events of both recidivism and
    non-recidivism. That is, each StateIncarcerationPeriod is transformed into a
    recidivism event unless it is the most recent period of incarceration and
    they are still incarcerated, or it is connected to a subsequent
    StateIncarcerationPeriod by a transfer.

    Example output for someone who went to prison in 2006, was released in 2008,
    went back in 2010, was released in 2012, and never returned:
    {
      2008: [RecidivismReleaseEvent(original_admission_date="2006-04-05", ...)],
      2012: [NonRecidivismReleaseEvent(original_admission_date="2010-09-17",
                ...)]
    }

    Args:
        incarceration_periods: list of StateIncarcerationPeriods for a person
        county_of_residence: the county that the incarcerated person lives in
            (prior to incarceration).

    Returns:
        A dictionary mapping release cohorts to a list of ReleaseEvents
            for the given person in that cohort.
    """
    release_events: Dict[int, List[ReleaseEvent]] = defaultdict(list)

    incarceration_periods = \
        prepare_incarceration_periods_for_calculations(incarceration_periods)

    for index, incarceration_period in enumerate(incarceration_periods):
        state_code = incarceration_period.state_code
        admission_date = incarceration_period.admission_date
        status = incarceration_period.status
        release_date = incarceration_period.release_date
        release_reason = incarceration_period.release_reason
        release_facility = incarceration_period.facility

        event = None

        # Admission data and status have been validated already
        if admission_date and status:
            if index == len(incarceration_periods) - 1:
                event = for_last_incarceration_period(state_code,
                                                      admission_date,
                                                      status,
                                                      release_date,
                                                      release_reason,
                                                      release_facility,
                                                      county_of_residence)
            else:
                reincarceration_period = \
                    incarceration_periods[index + 1]
                reincarceration_date = reincarceration_period.admission_date
                reincarceration_facility = reincarceration_period.facility
                reincarceration_admission_reason = \
                    reincarceration_period.admission_reason

                if reincarceration_admission_reason in \
                        [AdmissionReason.PROBATION_REVOCATION,
                         AdmissionReason.PAROLE_REVOCATION]:
                    source_supervision_violation_response = \
                        reincarceration_period. \
                        source_supervision_violation_response
                else:
                    source_supervision_violation_response = None

                # These fields have been validated already
                if release_date and release_reason and \
                        reincarceration_date and \
                        reincarceration_admission_reason:

                    event = for_intermediate_incarceration_period(
                        state_code=state_code,
                        admission_date=admission_date,
                        release_date=release_date,
                        release_reason=release_reason,
                        release_facility=release_facility,
                        county_of_residence=county_of_residence,
                        reincarceration_date=reincarceration_date,
                        reincarceration_facility=reincarceration_facility,
                        reincarceration_admission_reason=
                        reincarceration_admission_reason,
                        source_supervision_violation_response=
                        source_supervision_violation_response)

        if event:
            if release_date:
                release_cohort = release_date.year
                release_events[release_cohort].append(event)

    return release_events


def for_last_incarceration_period(
        state_code: str,
        admission_date: date,
        status: StateIncarcerationPeriodStatus,
        release_date: Optional[date],
        release_reason: Optional[ReleaseReason],
        release_facility: Optional[str],
        county_of_residence: Optional[str]) \
        -> Optional[ReleaseEvent]:
    """Looks at the last StateIncarcerationPeriod for a non-recidivism event.

    If the person has been released from their last StateIncarcerationPeriod,
    there is an instance of non-recidivism to count. If they are still
    incarcerated, or they were released for some circumstances, e.g. because
    they died, there is nothing to count. Returns any non-recidivism event
    relevant to the person's last StateIncarcerationPeriod.

        Args:
            state_code: the state code on the StateIncarcerationPeriod
            admission_date: when this StateIncarcerationPeriod started
            status: status of this StateIncarcerationPeriod
            release_date: when they were released from this
                 StateIncarcerationPeriod
            release_reason: reason they were released from this
                StateIncarcerationPeriod
            release_facility: the facility they were released from on this
                StateIncarcerationPeriod
            county_of_residence: the county that the incarcerated person lives
                in (prior to incarceration).


        Returns:
            A non-recidivism event if released legitimately from this
                StateIncarcerationPeriod. None otherwise.
    """

    # If the person is still in custody, there is nothing to track.
    if status == StateIncarcerationPeriodStatus.IN_CUSTODY:
        return None

    if not release_date:
        # If the person is not in custody, there should be a release_date.
        # This should not happen after validation. Throw error.
        raise ValueError("release_date is not set where it should be.")

    if release_reason in [ReleaseReason.DEATH, ReleaseReason.EXECUTION]:
        # If the person was released from this incarceration period because they
        # died or were executed, do not include them in the release cohort.
        return None
    if release_reason in (ReleaseReason.ESCAPE,
                          ReleaseReason.RELEASED_IN_ERROR):
        # If the person was released from this incarceration period but was not
        # meant to be released, do not include them in the release cohort.
        return None
    if release_reason == ReleaseReason.TRANSFER:
        # If the person was released from this incarceration period because they
        # were transferred elsewhere, do not include them in the release cohort.
        return None
    if release_reason == ReleaseReason.COURT_ORDER:
        # If the person was released from this incarceration period on a court
        # order, do not include them in the release cohort.
        return None
    if release_reason in [ReleaseReason.COMMUTED,
                          ReleaseReason.COMPASSIONATE,
                          ReleaseReason.CONDITIONAL_RELEASE,
                          ReleaseReason.EXTERNAL_UNKNOWN,
                          ReleaseReason.SENTENCE_SERVED]:

        return NonRecidivismReleaseEvent(
            state_code=state_code,
            original_admission_date=admission_date,
            release_date=release_date,
            release_facility=release_facility,
            county_of_residence=county_of_residence)

    if release_reason == ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY:
        # This should never happen. Should have been filtered by
        # validate_sort_and_collapse_incarceration_periods function. Throw error
        raise ValueError("validate_sort_and_collapse_incarceration_periods is "
                         "not effectively filtering. Found unexpected "
                         f"release_reason of: {release_reason}")

    raise ValueError("Enum case not handled for "
                     "StateIncarcerationPeriodReleaseReason of type:"
                     f" {release_reason}.")


def for_intermediate_incarceration_period(
        state_code: str,
        admission_date: date,
        release_date: date,
        release_reason: ReleaseReason,
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
        release_reason: the reason they were released from the
            StateIncarcerationPeriod
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

    if not should_include_in_release_cohort(release_reason,
                                            reincarceration_admission_reason):
        # Don't include this release in the release cohort
        return None

    return_type = get_return_type(reincarceration_admission_reason)
    from_supervision_type = \
        get_from_supervision_type(reincarceration_admission_reason)
    source_violation_type = \
        get_source_violation_type(source_supervision_violation_response)

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
        release_reason: ReleaseReason,
        reincarceration_admission_reason: AdmissionReason) -> bool:
    """Identifies whether a pair of release reason and admission reason should
    be included in the release cohort."""

    if release_reason in [ReleaseReason.DEATH, ReleaseReason.EXECUTION]:
        # If there is an intermediate incarceration period with a release reason
        # of death/execution, log this unexpected situation.
        logging.info("StateIncarcerationPeriod following a release for death.")
        return False
    if release_reason == ReleaseReason.ESCAPE:
        # If this escape is followed by a reincarceration_admission_reason that
        # is not RETURN_FROM_ESCAPE, log this unexpected situation.
        if reincarceration_admission_reason != \
                AdmissionReason.RETURN_FROM_ESCAPE:
            logging.info("%s following a release for ESCAPE.",
                         reincarceration_admission_reason)

        # If the person was released from this incarceration period because they
        # escaped, do not include them in the release cohort.
        return False
    if release_reason == ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY:
        # This should never happen. Should have been filtered by
        # validate_sort_and_collapse_incarceration_periods function. Throw error
        raise ValueError("validate_sort_and_collapse_incarceration_periods is "
                         "not effectively filtering. Found unexpected "
                         f"release_reason of: {release_reason}")
    if release_reason == ReleaseReason.RELEASED_IN_ERROR:
        if reincarceration_admission_reason != \
                AdmissionReason.RETURN_FROM_ERRONEOUS_RELEASE:
            logging.info("%s following an erroneous release.",
                         reincarceration_admission_reason)
        # If the person was released from this incarceration period due to an
        # error, do not include them in the release cohort.
        return False
    if release_reason == ReleaseReason.TRANSFER:
        # The fact that this transfer release reason isn't collapsed with a
        # transfer admission reason means there is something unexpected going
        # on. Log it.
        logging.warning("Unexpected reincarceration_admission_reason: %s "
                        "following a release for a transfer.",
                        reincarceration_admission_reason)

        # If the person was released from this incarceration period because they
        # were transferred elsewhere, do not include them in the release cohort.
        return False
    if release_reason == ReleaseReason.COURT_ORDER:
        # If the person was released from this incarceration period due to a
        # court order, do not include them in the release cohort.
        return False

    if reincarceration_admission_reason == AdmissionReason.TEMPORARY_CUSTODY:
        # This should never happen. Should have been filtered by
        # validate_sort_and_collapse_incarceration_periods function. Throw error
        raise ValueError("validate_sort_and_collapse_incarceration_periods is "
                         "not effectively filtering. Found unexpected "
                         "admission_reason of: "
                         f"{reincarceration_admission_reason}")

    if release_reason in (ReleaseReason.COMMUTED,
                          ReleaseReason.COMPASSIONATE,
                          ReleaseReason.CONDITIONAL_RELEASE,
                          ReleaseReason.EXTERNAL_UNKNOWN,
                          ReleaseReason.SENTENCE_SERVED):
        if reincarceration_admission_reason in (
                AdmissionReason.RETURN_FROM_ESCAPE,
                AdmissionReason.RETURN_FROM_ERRONEOUS_RELEASE):
            # Log this unexpected situation and exclude from the release cohort.
            logging.info("Unexpected reincarceration_admission_reason of "
                         "%s following a release reason of %s.",
                         reincarceration_admission_reason, release_reason)
            return False

        return True

    raise ValueError("Enum case not handled for "
                     "StateIncarcerationPeriodReleaseReason of type:"
                     f" {release_reason}.")


def get_return_type(
        reincarceration_admission_reason: AdmissionReason) -> \
        ReincarcerationReturnType:
    """Returns the return type for the reincarceration admission reason."""

    if reincarceration_admission_reason == AdmissionReason.ADMITTED_IN_ERROR:
        return ReincarcerationReturnType.NEW_ADMISSION
    if reincarceration_admission_reason == AdmissionReason.EXTERNAL_UNKNOWN:
        return ReincarcerationReturnType.NEW_ADMISSION
    if reincarceration_admission_reason == AdmissionReason.INTERNAL_UNKNOWN:
        return ReincarcerationReturnType.NEW_ADMISSION
    if reincarceration_admission_reason == AdmissionReason.NEW_ADMISSION:
        return ReincarcerationReturnType.NEW_ADMISSION
    if reincarceration_admission_reason == AdmissionReason.PAROLE_REVOCATION:
        return ReincarcerationReturnType.REVOCATION
    if reincarceration_admission_reason == AdmissionReason.PROBATION_REVOCATION:
        return ReincarcerationReturnType.REVOCATION
    if reincarceration_admission_reason == AdmissionReason.TRANSFER:
        # This should be a rare case, but we are considering this a type of
        # new admission recidivism because this person became reincarcerated at
        # some point after being released, and this is the most likely return
        # type in most cases in the absence of further information."
        return ReincarcerationReturnType.NEW_ADMISSION
    if reincarceration_admission_reason in (
            AdmissionReason.RETURN_FROM_ESCAPE,
            AdmissionReason.RETURN_FROM_ERRONEOUS_RELEASE):
        # This should never happen. Should have been filtered by
        # should_include_in_release_cohort function. Throw error.
        raise ValueError("should_include_in_release_cohort is not effectively"
                         " filtering. Found unexpected admission_reason of:"
                         f" {reincarceration_admission_reason}")
    if reincarceration_admission_reason == AdmissionReason.TEMPORARY_CUSTODY:
        # This should never happen. Should have been filtered by
        # validate_sort_and_collapse_incarceration_periods function. Throw error
        raise ValueError("validate_sort_and_collapse_incarceration_periods is "
                         "not effectively filtering. Found unexpected "
                         "admission_reason of: "
                         f"{reincarceration_admission_reason}")

    raise ValueError("Enum case not handled for "
                     "StateIncarcerationPeriodAdmissionReason of type:"
                     f" {reincarceration_admission_reason}.")


def get_from_supervision_type(
        reincarceration_admission_reason: AdmissionReason) \
        -> Optional[ReincarcerationReturnFromSupervisionType]:
    """If the person returned from supervision, returns the type."""

    if reincarceration_admission_reason in [AdmissionReason.ADMITTED_IN_ERROR,
                                            AdmissionReason.EXTERNAL_UNKNOWN,
                                            AdmissionReason.INTERNAL_UNKNOWN,
                                            AdmissionReason.NEW_ADMISSION,
                                            AdmissionReason.TRANSFER]:
        return None
    if reincarceration_admission_reason == AdmissionReason.PAROLE_REVOCATION:
        return ReincarcerationReturnFromSupervisionType.PAROLE
    if reincarceration_admission_reason == AdmissionReason.PROBATION_REVOCATION:
        return ReincarcerationReturnFromSupervisionType.PROBATION
    if reincarceration_admission_reason in (
            AdmissionReason.RETURN_FROM_ESCAPE,
            AdmissionReason.RETURN_FROM_ERRONEOUS_RELEASE):
        # This should never happen. Should have been filtered by
        # should_include_in_release_cohort function. Throw error.
        raise ValueError("should_include_in_release_cohort is not effectively"
                         " filtering. Found unexpected admission_reason of:"
                         f" {reincarceration_admission_reason}")
    if reincarceration_admission_reason == AdmissionReason.TEMPORARY_CUSTODY:
        # This should never happen. Should have been filtered by
        # validate_sort_and_collapse_incarceration_periods function. Throw error
        raise ValueError("validate_sort_and_collapse_incarceration_periods is "
                         "not effectively filtering. Found unexpected "
                         "admission_reason of: "
                         f"{reincarceration_admission_reason}")

    raise ValueError("Enum case not handled for "
                     "StateIncarcerationPeriodAdmissionReason of type:"
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
