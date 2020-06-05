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
# TODO(2995): Make a state config file for every state and every one of these state-specific calculation methodologies
import datetime
from typing import List, Optional

from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_supervision_type_identification import \
    us_id_get_pre_incarceration_supervision_type
from recidiviz.calculator.pipeline.utils.supervision_type_identification import get_month_supervision_type_default, \
    get_pre_incarceration_supervision_type_from_incarceration_period
from recidiviz.calculator.pipeline.utils.time_range_utils import TimeRange, TimeRangeDiff
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_supervision_type_identification import \
    us_mo_get_month_supervision_type, us_mo_get_pre_incarceration_supervision_type, \
    us_mo_get_most_recent_supervision_period_supervision_type_before_upper_bound_day
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_violation_utils import us_mo_filter_violation_responses
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodSupervisionType
from recidiviz.persistence.entity.state.entities import StateSupervisionSentence, StateIncarcerationSentence, \
    StateSupervisionPeriod, StateIncarcerationPeriod, StateSupervisionViolationResponse


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


def investigation_periods_in_supervision_population(state_code: str) -> bool:
    """Whether or not supervision periods that have a supervision_period_supervision_type of INVESTIGATION should be
    counted in the supervision calculations.

    - US_ID: False
    - US_MO: False
    - US_ND: True
    """
    return state_code.upper() == 'US_ND'


def non_state_custodial_authority_in_supervision_population(state_code: str) -> bool:
    """Whether or not supervision periods that are not under the state DOC's custodial authority should be counted in
    the supervision calculations.

    - US_ID: False
    - US_MO: True
    - US_ND: True
    """
    return state_code.upper() != 'US_ID'


def get_month_supervision_type(
        any_date_in_month: datetime.date,
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

    # TODO(2938): Decide if we want date matching/supervision period lookback logic for US_ND
    return get_pre_incarceration_supervision_type_from_incarceration_period(incarceration_period)


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


def filter_violation_responses_before_revocation(violation_responses: List[StateSupervisionViolationResponse]) -> \
        List[StateSupervisionViolationResponse]:
    """State-specific filtering of the violation responses that should be included in pre-revocation analysis."""
    if violation_responses:
        state_code = violation_responses[0].state_code
        if state_code == 'US_MO':
            return us_mo_filter_violation_responses(violation_responses)
    return violation_responses
