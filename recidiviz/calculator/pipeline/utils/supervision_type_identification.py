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
"""Helpers for determining supervision types at different points in time."""
import datetime
from typing import Optional, Set, List, Dict

from recidiviz.calculator.pipeline.utils.calculator_utils import last_day_of_month
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodAdmissionReason as AdmissionReason
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodSupervisionType
from recidiviz.persistence.entity.entity_utils import is_placeholder, get_ids
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod, StateSupervisionPeriod, \
    StateSupervisionSentence

# TODO(2647): Write unit tests for the public helpers in this file


def get_pre_incarceration_supervision_type(
        state_code: str,
        incarceration_period: StateIncarcerationPeriod) -> Optional[StateSupervisionPeriodSupervisionType]:
    """If the person was reincarcerated after a period of supervision, returns the type of supervision they were on
    right before the reincarceration.

    Args:
        state_code: (str) The state where this data originates from
        incarceration_period: (StateIncarcerationPeriod) The incarceration period where the person was first
            reincarcerated.
    """

    # TODO(2647): Move US_MO to use sentence and date-based determination once it's implemented
    if state_code in ('US_MO', 'US_ND'):
        return _get_pre_incarceration_supervision_type_from_incarceration_period(incarceration_period)

    if not incarceration_period.admission_date:
        raise ValueError(
            f'Admission date for period [{incarceration_period.incarceration_period_id}] unexpectedly null.')

    # TODO(2647): Update this function to take in supervision sentences so we can determine which sentences overlap with
    #  this admission date and use that to determine the supervision type pre-reincarceration.
    return _get_pre_incarceration_supervision_type_on_date(incarceration_period.admission_date)


def _get_pre_incarceration_supervision_type_from_incarceration_period(
        incarceration_period: StateIncarcerationPeriod
) -> Optional[StateSupervisionPeriodSupervisionType]:
    """Derives the supervision type the person was serving prior to incarceration from the admission reason to an
    incarceration period."""

    if incarceration_period.admission_reason in [AdmissionReason.ADMITTED_IN_ERROR,
                                                 AdmissionReason.EXTERNAL_UNKNOWN,
                                                 AdmissionReason.NEW_ADMISSION,
                                                 AdmissionReason.TRANSFER,
                                                 AdmissionReason.RETURN_FROM_ESCAPE,
                                                 AdmissionReason.RETURN_FROM_ERRONEOUS_RELEASE]:
        return None

    if incarceration_period.admission_reason == AdmissionReason.DUAL_REVOCATION:
        return StateSupervisionPeriodSupervisionType.DUAL
    if incarceration_period.admission_reason == AdmissionReason.PAROLE_REVOCATION:
        return StateSupervisionPeriodSupervisionType.PAROLE
    if incarceration_period.admission_reason == AdmissionReason.PROBATION_REVOCATION:
        return StateSupervisionPeriodSupervisionType.PROBATION

    if incarceration_period.admission_reason == AdmissionReason.TEMPORARY_CUSTODY:
        # This should never happen. Should have been filtered before calling this function. Throw error.
        raise ValueError(f"Cannot do admission-reason-based determination of pre-incarceration supervision type for "
                         f"periods with admission reason [{incarceration_period.admission_reason}] - if pre-processing "
                         f"isn't dropping / correcting these periods, must use the get_supervision_type_at_date() "
                         f"helper instead.")

    raise ValueError(f"Enum case not handled for StateIncarcerationPeriodAdmissionReason of type: "
                     f"{incarceration_period.admission_reason}.")


# TODO(2855): Bring in StateIncarcerationSentences for better supervision type classification
# TODO(2647): Refactor this into a helper function that accepts an arbitrary lookback window, then use that helper here
#  (calculate # days to beginning of month) and in _get_pre_incarceration_supervision_type_on_date.
def get_month_supervision_type(
        any_date_in_month: datetime.date,
        supervision_period: StateSupervisionPeriod,
        supervision_sentences: List[StateSupervisionSentence]
) -> StateSupervisionPeriodSupervisionType:
    """Supervision type can change over time even if the period does not change. This function calculates the
    supervision type that a given supervision period represents during the month that |any_date_in_month| falls in. We
    do this by looking at all sentences attached to this supervision period, then determining which ones overlap with
    any day in the month, and using the sentence supervision types to determine the period supervision type at this
    point in time.

    Args:
    any_date_in_month: (date) Any day in the month to consider
    supervision_period: (StateSupervisionPeriod) The supervision period we want to associate a supervision type with
    supervision_sentences: (List[StateSupervisionSentence]) All supervision sentences for a given person.
    """

    end_of_month = last_day_of_month(any_date_in_month)

    if not supervision_period.supervision_period_id:
        raise ValueError('All objects should have database ids.')

    if is_placeholder(supervision_period):
        raise ValueError('Do not expect placeholder periods!')

    # Find sentences that are attached to the period and overlap with the month
    valid_attached_supervision_sentences: Dict[int, StateSupervisionSentence] = {}
    for supervision_sentence in supervision_sentences:
        sentence_supervision_period_ids = get_ids(supervision_sentence.supervision_periods)

        completion_date_end_of_month: Optional[datetime.date] = \
            last_day_of_month(supervision_sentence.completion_date) if supervision_sentence.completion_date else None

        if (supervision_period.supervision_period_id in sentence_supervision_period_ids
                and (completion_date_end_of_month is None or completion_date_end_of_month >= end_of_month)):

            if not supervision_sentence.supervision_sentence_id:
                raise ValueError('All objects should have database ids.')

            valid_attached_supervision_sentences[supervision_sentence.supervision_sentence_id] = supervision_sentence

    # Get all the sentence types from the valid sentences
    supervision_sentence_types: Set[Optional[StateSupervisionType]] = set()
    for supervision_sentence in valid_attached_supervision_sentences.values():
        supervision_sentence_types.add(supervision_sentence.supervision_type)

    if not supervision_sentence_types:
        # Assume these are hanging off of StateIncarcerationSentences
        # TODO(2855): Bring in StateIncarcerationSentences for better supervision type classification
        return StateSupervisionPeriodSupervisionType.PAROLE

    if (StateSupervisionType.PROBATION in supervision_sentence_types
            and StateSupervisionType.PAROLE in supervision_sentence_types):
        return StateSupervisionPeriodSupervisionType.DUAL
    if StateSupervisionType.PROBATION in supervision_sentence_types:
        return StateSupervisionPeriodSupervisionType.PROBATION
    if StateSupervisionType.PAROLE in supervision_sentence_types:
        return StateSupervisionPeriodSupervisionType.PAROLE
    if supervision_sentence_types.intersection(
            {StateSupervisionType.PRE_CONFINEMENT,
             StateSupervisionType.POST_CONFINEMENT,
             StateSupervisionType.HALFWAY_HOUSE,
             StateSupervisionType.CIVIL_COMMITMENT}):
        # These are all types that should be deprecated, but for now, just assume probation - the numbers in ND are very
        # small for these.
        return StateSupervisionPeriodSupervisionType.PROBATION

    if None in supervision_sentence_types:
        # If there is a supervision sentence with no type set, assume probation
        return StateSupervisionPeriodSupervisionType.PROBATION

    if StateSupervisionType.INTERNAL_UNKNOWN in supervision_sentence_types:
        # TODO(2647): Make sure this test case is covered
        return StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN

    raise ValueError(f'Unexpected supervision_sentence_types {supervision_sentence_types}.')


# TODO(2647): Update this function to take in supervision sentences so we can determine which sentences overlap with
#  this admission date and use that to determine the supervision type pre-reincarceration.
def _get_pre_incarceration_supervision_type_on_date(_supervision_date: datetime.date):
    raise ValueError('Not yet implemented')
