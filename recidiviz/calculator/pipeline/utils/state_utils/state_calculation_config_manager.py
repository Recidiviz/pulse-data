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
"""Manages state-specific methodology decisions made throughout the calculation pipelines."""
# TODO(#2995): Make a state config file for every state and every one of these state-specific calculation methodologies
from datetime import date
import logging
from typing import List, Optional

from recidiviz.calculator.pipeline.supervision.supervision_case_compliance import SupervisionCaseCompliance
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_revocation_identification import \
    us_id_filter_supervision_periods_for_revocation_identification, us_id_get_pre_revocation_supervision_type, \
    us_id_is_revocation_admission
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_supervision_compliance import \
    us_id_case_compliance_on_date
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_supervision_type_identification import \
    us_id_get_pre_incarceration_supervision_type, us_id_get_post_incarceration_supervision_type
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_supervision_type_identification import \
    us_nd_get_post_incarceration_supervision_type
from recidiviz.calculator.pipeline.utils.supervision_type_identification import get_month_supervision_type_default, \
    get_pre_incarceration_supervision_type_from_incarceration_period
from recidiviz.calculator.pipeline.utils.time_range_utils import TimeRange, TimeRangeDiff
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_supervision_type_identification import \
    us_mo_get_month_supervision_type, us_mo_get_pre_incarceration_supervision_type, \
    us_mo_get_most_recent_supervision_period_supervision_type_before_upper_bound_day, \
    us_mo_get_post_incarceration_supervision_type
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_violation_utils import us_mo_filter_violation_responses
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_incarceration_period import is_revocation_admission
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodSupervisionType
from recidiviz.persistence.entity.state.entities import StateSupervisionSentence, StateIncarcerationSentence, \
    StateSupervisionPeriod, StateIncarcerationPeriod, StateSupervisionViolationResponse, StateAssessment, \
    StateSupervisionContact


def supervision_types_distinct_for_state(state_code: str) -> bool:
    """For some states, we want to track DUAL supervision as distinct from both PAROLE and PROBATION. For others, a
    person can be on multiple types of supervision simultaneously and contribute to counts for both types.

        - US_ID: True
        - US_MO: True
        - US_ND: False

    Returns whether our calculations should consider supervision types as distinct for the given state_code.
    """
    return state_code.upper() in ('US_ID', 'US_MO')


def default_to_supervision_period_officer_for_revocation_details_for_state(state_code: str) -> bool:
    """For some states, if there's no officer information coming from the source_supervision_violation_response,
    we should default to the officer information on the overlapping supervision period for the revocation details.

        - US_ID: True
        - US_MO: True
        - US_ND: False

    Returns whether our calculations should use supervising officer information for revocation details.
    """
    return state_code.upper() in ('US_MO', 'US_ID')


def temporary_custody_periods_under_state_authority(state_code: str) -> bool:
    """Whether or not periods of temporary custody are considered a part of state authority.

        - US_ID: True
        - US_MO: True
        - US_ND: False
    """
    return state_code.upper() != 'US_ND'


def non_prison_periods_under_state_authority(state_code: str) -> bool:
    """Whether or not incarceration periods that aren't in a STATE_PRISON are considered under the state authority.
        - US_ID: True
        - US_MO: False
        - US_ND: True
    """
    return state_code.upper() in ('US_ID', 'US_ND')


def investigation_periods_in_supervision_population(_state_code: str) -> bool:
    """Whether or not supervision periods that have a supervision_period_supervision_type of INVESTIGATION should be
    counted in the supervision calculations.

    - US_ID: False
    - US_MO: False
    - US_ND: False
    """
    return False


def only_state_custodial_authority_in_supervision_population(state_code: str) -> bool:
    """Whether or not only supervision periods that are under the state DOC's custodial authority should be counted in
    the supervision calculations.

    - US_ID: True
    - US_MO: False
    - US_ND: False
    """
    return state_code.upper() == 'US_ID'


def should_collapse_transfers_different_purpose_for_incarceration(state_code: str) -> bool:
    """Whether or not incarceration periods that are connected by a TRANSFER release and a TRANSFER admission should
    be collapsed into one period if they have different specialized_purpose_for_incarceration values.
        - US_ID: False
            We need the dates of transfers from parole board holds and treatment custody to identify revocations
            in US_ID.
        - US_MO: True
        - US_ND: True
    """
    return state_code.upper() != 'US_ID'


def include_decisions_on_follow_up_responses(state_code: str) -> bool:
    """Some StateSupervisionViolationResponses are a 'follow-up' type of reponse, which is a state-defined response
    that is related to a previously submitted response. This returns whether or not the decision entries on
    follow-up responses should be considered in the calculations.

        - US_ID: False
        - US_MO: True
        - US_ND: False
    """
    return state_code.upper() == 'US_MO'


def second_assessment_on_supervision_is_more_reliable(state_code: str) -> bool:
    """Some states rely on the first-reassessment (the second assessment) instead of the first assessment when comparing
    terminating assessment scores to a score at the beginning of someone's supervision.

        - US_ID: True
        - US_MO: True
        - US_ND: True
    """
    # TODO(#2782): Investigate whether to update this logic
    return state_code in ('US_ID', 'US_MO', 'US_ND')


def get_month_supervision_type(
        any_date_in_month: date,
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_period: StateSupervisionPeriod
) -> StateSupervisionPeriodSupervisionType:
    """Supervision type can change over time even if the period does not change. This function calculates the
    supervision type that a given supervision period represents during the month that |any_date_in_month| falls in. The
    objects / info we use to determine supervision type may be state-specific.

    Args:
    any_date_in_month: (date) Any day in the month to consider
    supervision_period: (StateSupervisionPeriod) The supervision period we want to associate a supervision type with
    supervision_sentences: (List[StateSupervisionSentence]) All supervision sentences for a given person.
    """

    if supervision_period.state_code == 'US_MO':
        return us_mo_get_month_supervision_type(any_date_in_month,
                                                supervision_sentences,
                                                incarceration_sentences,
                                                supervision_period)

    if supervision_period.state_code == 'US_ID':
        return (supervision_period.supervision_period_supervision_type
                if supervision_period.supervision_period_supervision_type
                else StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN)

    return get_month_supervision_type_default(
        any_date_in_month, supervision_sentences, incarceration_sentences, supervision_period)


def get_pre_incarceration_supervision_type(
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_period: StateIncarcerationPeriod) -> Optional[StateSupervisionPeriodSupervisionType]:
    """If the person was reincarcerated after a period of supervision, returns the type of supervision they were on
    right before the reincarceration.

    Args:
        incarceration_sentences: (List[StateIncarcerationSentence]) All IncarcerationSentences associated with this
            person.
        supervision_sentences: (List[StateSupervisionSentence]) All SupervisionSentences associated with this person.
        incarceration_period: (StateIncarcerationPeriod) The incarceration period where the person was first
            reincarcerated.
    """

    state_code = incarceration_period.state_code

    if state_code == 'US_MO':
        return us_mo_get_pre_incarceration_supervision_type(incarceration_sentences,
                                                            supervision_sentences,
                                                            incarceration_period)

    if state_code == 'US_ID':
        return us_id_get_pre_incarceration_supervision_type(incarceration_sentences,
                                                            supervision_sentences,
                                                            incarceration_period)

    # TODO(#2938): Decide if we want date matching/supervision period lookback logic for US_ND
    return get_pre_incarceration_supervision_type_from_incarceration_period(incarceration_period)


def get_post_incarceration_supervision_type(
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_period: StateIncarcerationPeriod) -> Optional[StateSupervisionPeriodSupervisionType]:
    """If the person was released from incarceration onto some form of supervision, returns the type of supervision
    they were released to. This function must be implemented for each state for which we need this output. There is not
    a default way to determine the supervision type someone is released onto.

    Args:
        incarceration_sentences: (List[StateIncarcerationSentence]) All IncarcerationSentences associated with this
            person.
        supervision_sentences: (List[StateSupervisionSentence]) All SupervisionSentences associated with this person.
        incarceration_period: (StateIncarcerationPeriod) The incarceration period the person was released from.
    """
    state_code = incarceration_period.state_code

    if state_code == 'US_ID':
        return us_id_get_post_incarceration_supervision_type(incarceration_sentences,
                                                             supervision_sentences,
                                                             incarceration_period)
    if state_code == 'US_MO':
        return us_mo_get_post_incarceration_supervision_type(incarceration_sentences,
                                                             supervision_sentences,
                                                             incarceration_period)
    if state_code == 'US_ND':
        return us_nd_get_post_incarceration_supervision_type(incarceration_period)

    logging.warning("get_post_incarceration_supervision_type not implemented for state: %s", state_code)
    return None


def get_pre_revocation_supervision_type(
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_period: StateIncarcerationPeriod,
        revoked_supervision_period: Optional[StateSupervisionPeriod]) -> \
        Optional[StateSupervisionPeriodSupervisionType]:
    """Returns the supervision type the person was on before they had their supervision revoked."""
    if incarceration_period.state_code == 'US_ID':
        return us_id_get_pre_revocation_supervision_type(revoked_supervision_period)

    return get_pre_incarceration_supervision_type(
        incarceration_sentences,
        supervision_sentences,
        incarceration_period
    )


def supervision_period_counts_towards_supervision_population_in_date_range_state_specific(
        date_range: TimeRange,
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_period: StateSupervisionPeriod) -> bool:
    """ Returns False if there is state-specific information to indicate that the supervision period should not count
    towards the supervision population in a range. Returns True if either there is a state-specific check that indicates
    that the supervision period should count or if there is no state-specific check to perform.
    """

    if supervision_period.state_code == 'US_MO':
        sp_range = TimeRange.for_supervision_period(supervision_period)
        overlapping_range = TimeRangeDiff(range_1=date_range, range_2=sp_range).overlapping_range

        if not overlapping_range:
            return False

        return us_mo_get_most_recent_supervision_period_supervision_type_before_upper_bound_day(
            upper_bound_exclusive_date=overlapping_range.upper_bound_exclusive_date,
            lower_bound_inclusive_date=overlapping_range.lower_bound_inclusive_date,
            incarceration_sentences=incarceration_sentences,
            supervision_sentences=supervision_sentences
        ) is not None

    return True


def terminating_supervision_period_supervision_type(
        supervision_period: StateSupervisionPeriod,
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_sentences: List[StateIncarcerationSentence],
) -> StateSupervisionPeriodSupervisionType:
    """Calculates the supervision type that should be associated with a terminated supervision period. In some cases,
    the supervision period will be terminated long after the person has been incarcerated (e.g. in the case of a board
    hold, someone might remain assigned to a PO until their parole is revoked), so we do a lookback to see the most
    recent supervision period supervision type we can associate with this termination.
    """

    if not supervision_period.termination_date:
        raise ValueError(f'Expected a terminated supervision period for period '
                         f'[{supervision_period.supervision_period_id}]')

    if supervision_period.state_code == 'US_MO':
        supervision_type = us_mo_get_most_recent_supervision_period_supervision_type_before_upper_bound_day(
            upper_bound_exclusive_date=supervision_period.termination_date,
            lower_bound_inclusive_date=supervision_period.start_date,
            incarceration_sentences=incarceration_sentences,
            supervision_sentences=supervision_sentences
        )

        return supervision_type if supervision_type else StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN

    if supervision_period.state_code == 'US_ID':
        return (supervision_period.supervision_period_supervision_type
                if supervision_period.supervision_period_supervision_type
                else StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN)

    return get_month_supervision_type_default(
        supervision_period.termination_date, supervision_sentences, incarceration_sentences, supervision_period)


def filter_violation_responses_before_revocation(violation_responses: List[StateSupervisionViolationResponse],
                                                 include_follow_up_responses: bool) -> \
        List[StateSupervisionViolationResponse]:
    """State-specific filtering of the violation responses that should be included in pre-revocation analysis."""
    if violation_responses:
        state_code = violation_responses[0].state_code
        if state_code == 'US_MO':
            return us_mo_filter_violation_responses(violation_responses, include_follow_up_responses)
    return violation_responses


def filter_supervision_periods_for_revocation_identification(supervision_periods: List[StateSupervisionPeriod]) -> \
        List[StateSupervisionPeriod]:
    """State-specific filtering of supervision periods that should be included in pre-revocation analysis."""
    if supervision_periods:
        if supervision_periods[0].state_code == 'US_ID':
            return us_id_filter_supervision_periods_for_revocation_identification(supervision_periods)
    return supervision_periods


def incarceration_period_is_from_revocation(
        incarceration_period: StateIncarcerationPeriod,
        preceding_incarceration_period: Optional[StateIncarcerationPeriod]) \
        -> bool:
    """Determines if the sequence of incarceration periods represents a revocation."""
    if incarceration_period.state_code == 'US_ID':
        return us_id_is_revocation_admission(incarceration_period, preceding_incarceration_period)
    return is_revocation_admission(incarceration_period.admission_reason)


def produce_supervision_time_bucket_for_period(supervision_period: StateSupervisionPeriod):
    """Whether or not any SupervisionTimeBuckets should be created using the supervision_period. In some cases, we do
    not want to drop periods entirely because we need them for context in some of the calculations, but we do not want
    to create metrics using the periods."""
    if ((supervision_period.supervision_period_supervision_type == StateSupervisionPeriodSupervisionType.INVESTIGATION
         # TODO(#2891): Remove this check when we remove supervision_type from StateSupervisionPeriods
         or supervision_period.supervision_type == StateSupervisionType.PRE_CONFINEMENT)
            and not investigation_periods_in_supervision_population(supervision_period.state_code)):
        return False
    return True


def get_case_compliance_on_date(supervision_period: StateSupervisionPeriod,
                                case_type: StateSupervisionCaseType,
                                start_of_supervision: date,
                                compliance_evaluation_date: date,
                                assessments: List[StateAssessment],
                                supervision_contacts: List[StateSupervisionContact]) -> \
        Optional[SupervisionCaseCompliance]:
    """Returns the SupervisionCaseCompliance object containing information about whether the given supervision case is
    in compliance with state-specific standards on the compliance_evaluation_date. If the state of the
    supervision_period does not have state-specific compliance calculations, returns None."""
    if supervision_period.state_code == 'US_ID':
        return us_id_case_compliance_on_date(supervision_period,
                                             case_type,
                                             start_of_supervision,
                                             compliance_evaluation_date,
                                             assessments,
                                             supervision_contacts)

    return None


# TODO(#3829): Determine if we want a supervision district / supervision site distinction in our schema and/or metrics.
def get_supervision_district_from_supervision_period(supervision_period: StateSupervisionPeriod) -> Optional[str]:
    """Given |supervision_period| returns the relevant supervision site abiding by state-specific logic."""

    # In some states we have squashed the notion of district and site into one field, so all filled in supervision sites
    # are in the format "{supervision district}|{location/office within district}". This separates out the district
    # info.
    if supervision_period.state_code in ('US_ID', 'US_PA') and supervision_period.supervision_site:
        return supervision_period.supervision_site.split('|')[0]
    return supervision_period.supervision_site
