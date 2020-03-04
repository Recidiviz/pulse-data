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
from typing import Optional, Set, List, Dict, Union

from recidiviz.calculator.pipeline.utils.calculator_utils import last_day_of_month
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodAdmissionReason as AdmissionReason
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodSupervisionType
from recidiviz.persistence.entity.entity_utils import is_placeholder, get_ids
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod, StateSupervisionPeriod, \
    StateSupervisionSentence, StateIncarcerationSentence


# TODO(2647): Write full coverage unit tests for this function
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
                                                 AdmissionReason.INTERNAL_UNKNOWN,
                                                 AdmissionReason.NEW_ADMISSION,
                                                 AdmissionReason.TRANSFER,
                                                 AdmissionReason.TRANSFERRED_FROM_OUT_OF_STATE,
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


# TODO(2647): Refactor this into a helper function that accepts arbitrary start and end dates, then uses that helper
#  here and in _get_pre_incarceration_supervision_type_on_date.
def get_month_supervision_type(
        any_date_in_month: datetime.date,
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_period: StateSupervisionPeriod
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
    if not supervision_period.supervision_period_id:
        raise ValueError('All objects should have database ids.')

    if is_placeholder(supervision_period):
        raise ValueError('Do not expect placeholder periods!')

    # Find sentences that are attached to the period and overlap with the month
    valid_attached_supervision_sentences = _valid_attached_sentences_in_month(
        any_date_in_month, supervision_sentences, supervision_period)

    valid_attached_incarceration_sentences = _valid_attached_sentences_in_month(
        any_date_in_month, incarceration_sentences, supervision_period
    )

    # Get all the sentence types from the valid sentences
    supervision_types: Set[Optional[StateSupervisionType]] = set()
    for supervision_sentence in valid_attached_supervision_sentences.values():
        if isinstance(supervision_sentence, StateSupervisionSentence) and supervision_sentence.supervision_type:
            supervision_types.add(supervision_sentence.supervision_type)

    # If it's hanging off of any StateIncarcerationSentences, assume this is a parole period
    if valid_attached_incarceration_sentences:
        supervision_types.add(StateSupervisionType.PAROLE)

    if (StateSupervisionType.PROBATION in supervision_types
            and StateSupervisionType.PAROLE in supervision_types):
        return StateSupervisionPeriodSupervisionType.DUAL
    if StateSupervisionType.PROBATION in supervision_types:
        return StateSupervisionPeriodSupervisionType.PROBATION
    if StateSupervisionType.PAROLE in supervision_types:
        return StateSupervisionPeriodSupervisionType.PAROLE
    if supervision_types.intersection(
            {StateSupervisionType.PRE_CONFINEMENT,
             StateSupervisionType.POST_CONFINEMENT,
             StateSupervisionType.HALFWAY_HOUSE,
             StateSupervisionType.CIVIL_COMMITMENT}):
        # These are all types that should be deprecated, but for now, just assume probation - the numbers in ND are very
        # small for these.
        return StateSupervisionPeriodSupervisionType.PROBATION
    if StateSupervisionType.EXTERNAL_UNKNOWN in supervision_types:
        return StateSupervisionPeriodSupervisionType.EXTERNAL_UNKNOWN

    if not supervision_types or StateSupervisionType.INTERNAL_UNKNOWN in supervision_types:
        return StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN

    raise ValueError(f'Unexpected supervision_types {supervision_types}.')


def _valid_attached_sentences_in_month(
        any_date_in_month: datetime.date,
        sentences: Union[List[StateIncarcerationSentence], List[StateSupervisionSentence]],
        supervision_period: StateSupervisionPeriod) -> \
        Dict[int, Union[StateIncarcerationSentence, StateSupervisionSentence]]:
    """This function returns valid sentences that were active during the month that |any_date_in_month| falls in. This
    identifies which sentences are attached to the supervision_period, are not placeholders, and overlap with any day
    in the month of |any_date_in_month|.
    """
    valid_attached_sentences: Dict[int, Union[StateIncarcerationSentence, StateSupervisionSentence]] = {}

    valid_sentences = _filter_invalid_sentences(sentences)

    end_of_month = last_day_of_month(any_date_in_month)

    for sentence in valid_sentences:
        sentence_supervision_period_ids = get_ids(sentence.supervision_periods)

        # TODO(2647): Don't allow null start_date at this point once US_ND supervision sentences have start_dates
        # Assume a valid sentence started before this month if there's no start date on it
        sentence_start_date = sentence.start_date if sentence.start_date else datetime.date.min
        completion_date_end_of_month: Optional[datetime.date] = last_day_of_month(
            sentence.completion_date) if sentence.completion_date else None

        if (supervision_period.supervision_period_id in sentence_supervision_period_ids
                and sentence_start_date <= end_of_month
                and (completion_date_end_of_month is None
                     or completion_date_end_of_month >= end_of_month)):
            if not sentence.get_id():
                raise ValueError('All objects should have database ids.')

            sentence_id = sentence.get_id()
            valid_attached_sentences[sentence_id] = sentence

    return valid_attached_sentences


def _filter_invalid_sentences(sentences: Union[List[StateIncarcerationSentence], List[StateSupervisionSentence]]) -> \
        List[Union[StateIncarcerationSentence, StateSupervisionSentence]]:
    """Drops any sentences that are placeholders or has a null start_date field."""

    # TODO(2647): Remove this once start_date is set on US_ND supervision sentences
    ignore_null_start_dates_for_state_codes = ['US_ND']

    valid_sentences = [
        sentence for sentence in sentences
        # Remove placeholder sentences
        if not is_placeholder(sentence)
        # Assert the start_date is set, that it's an acceptable state-specific inclusion of a null start_date,
        and (sentence.start_date or sentence.state_code in ignore_null_start_dates_for_state_codes)
    ]

    return valid_sentences


# TODO(2647): Update this function to take in supervision sentences so we can determine which sentences overlap with
#  this admission date and use that to determine the supervision type pre-reincarceration.
def _get_pre_incarceration_supervision_type_on_date(_supervision_date: datetime.date):
    raise ValueError('Not yet implemented')
