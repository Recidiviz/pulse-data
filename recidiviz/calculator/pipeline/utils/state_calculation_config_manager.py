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

from recidiviz.calculator.pipeline.utils.supervision_type_identification import get_month_supervision_type_default, \
    get_pre_incarceration_supervision_type_from_incarceration_period
from recidiviz.calculator.pipeline.utils.us_mo_supervision_type_identification import \
    us_mo_get_month_supervision_type, us_mo_get_pre_incarceration_supervision_type
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodSupervisionType
from recidiviz.persistence.entity.state.entities import StateSupervisionSentence, StateIncarcerationSentence, \
    StateSupervisionPeriod, StateIncarcerationPeriod


def supervision_types_distinct_for_state(state_code: str) -> bool:
    """For some states, we want to track DUAL supervision as distinct from both PAROLE and PROBATION. For others, a
    person can be on multiple types of supervision simultaneously and contribute to counts for both types.

        - US_MO: True
        - US_ND: False

    Returns whether our calculations should consider supervision types as distinct for the given state_code.
    """
    return state_code.upper() == 'US_MO'


def default_to_supervision_period_officer_for_revocation_details_for_state(state_code: str) -> bool:
    """For some states, if there's no officer information coming from the source_supervision_violation_response,
    we should default to the officer information on the overlapping supervision period for the revocation details.

        - US_MO: True
        - US_ND: False

    Returns whether our calculations should use supervising officer information for revocation details.
    """
    return state_code.upper() == 'US_MO'


def drop_temporary_custody_incarceration_periods_for_state(state_code: str) -> bool:
    """For some states, we should always disregard incarceration periods of temporary custody in the calculation
    pipelines.

        - US_MO: False
        - US_ND: True

    Returns whether our calculations should drop temporary custody periods for the given state.
    """
    return state_code.upper() == 'US_ND'


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

    return get_month_supervision_type_default(
        any_date_in_month, supervision_sentences, incarceration_sentences, supervision_period)


# TODO(2647): Write full coverage unit tests for this function
def get_pre_incarceration_supervision_type(
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_period: StateIncarcerationPeriod) -> Optional[StateSupervisionPeriodSupervisionType]:
    """If the person was reincarcerated after a period of supervision, returns the type of supervision they were on
    right before the reincarceration.

    Args:
        incarceration_sentences: (List[StateIncarcerationSentence]) All IncarcerationSentences associated with this
            person.
        supervision_sentences: (List[StateSupervisionSentence]) All SupervionSentences associated with this person.
        incarceration_period: (StateIncarcerationPeriod) The incarceration period where the person was first
            reincarcerated.
    """

    state_code = incarceration_period.state_code

    if state_code == 'US_MO':
        return us_mo_get_pre_incarceration_supervision_type(incarceration_sentences,
                                                            supervision_sentences,
                                                            incarceration_period)

    # TODO(2938): Decide if we want date matching/supervision period lookback logic for US_ND
    return get_pre_incarceration_supervision_type_from_incarceration_period(incarceration_period)
