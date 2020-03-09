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
import logging
from typing import Optional, Set, List, Dict, Union

from recidiviz.calculator.pipeline.utils.calculator_utils import last_day_of_month, first_day_of_month
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

    # TODO(2932): Don't rely only on the incarceration period in ND
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
        any_date_in_month, incarceration_sentences, supervision_period)

    # Get all the sentence types from the valid sentences
    supervision_types: Set[Optional[StateSupervisionType]] = set()
    for supervision_sentence in valid_attached_supervision_sentences.values():
        if not isinstance(supervision_sentence, StateSupervisionSentence):
            continue
        if supervision_sentence.supervision_type:
            supervision_types.add(supervision_sentence.supervision_type)
        else:
            logging.warning('Unexpectedly found supervision_sentence [%d] without supervision_type. Defaulting to '
                            'StateSupervisionType.PROBATION', supervision_sentence.supervision_sentence_id)
            supervision_types.add(StateSupervisionType.PROBATION)

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


def _get_attached_sentences_by_db_id(
        sentences: Union[List[StateIncarcerationSentence], List[StateSupervisionSentence]],
        supervision_period: StateSupervisionPeriod
) -> Dict[int, Union[StateIncarcerationSentence, StateSupervisionSentence]]:
    """Returns sentences that are attached to the given period, in a map indexed by the sentence primary key id."""
    attached_sentences_by_db_id: Dict[int, Union[StateIncarcerationSentence, StateSupervisionSentence]] = {}

    for sentence in sentences:
        if not sentence.get_id():
            raise ValueError('All objects should have database ids.')

        sentence_supervision_period_ids = get_ids(sentence.supervision_periods)

        if supervision_period.supervision_period_id in sentence_supervision_period_ids:
            sentence_id = sentence.get_id()
            attached_sentences_by_db_id[sentence_id] = sentence

    return attached_sentences_by_db_id


def _valid_attached_sentences_in_month(
        any_date_in_month: datetime.date,
        sentences: Union[List[StateIncarcerationSentence], List[StateSupervisionSentence]],
        supervision_period: StateSupervisionPeriod) -> \
        Dict[int, Union[StateIncarcerationSentence, StateSupervisionSentence]]:
    """This function returns valid sentences that were active during the month that |any_date_in_month| falls in. This
    identifies which sentences are attached to the supervision_period, are not placeholders, and overlap with any day
    in the month of |any_date_in_month|.
    """
    attached_sentences_by_db_id = _get_attached_sentences_by_db_id(sentences, supervision_period)

    start_of_month = first_day_of_month(any_date_in_month)
    end_of_month = last_day_of_month(any_date_in_month)

    valid_attached_sentences: Dict[int, Union[StateIncarcerationSentence, StateSupervisionSentence]] = {}
    for sentence_id, sentence in attached_sentences_by_db_id.items():
        has_missing_fields = _sentence_has_missing_fields(sentence)

        if has_missing_fields:
            continue

        if not sentence.start_date:
            raise ValueError(
                f"Expected non-null start date on sentence [{sentence.external_id}] for state [{sentence.state_code}]")

        if (sentence.start_date <= end_of_month
                and (sentence.completion_date is None
                     or sentence.completion_date >= start_of_month)):
            valid_attached_sentences[sentence_id] = sentence

    return valid_attached_sentences


def _sentence_has_missing_fields(
        sentence: Union[StateIncarcerationSentence, StateSupervisionSentence]) -> bool:
    """Returns true for any sentences that are placeholders or have a null start_date field."""

    if is_placeholder(sentence):
        return True

    if not sentence.start_date:
        logging.error("Non-placeholder sentence [%s] for state [%s] has no start date - ignoring.",
                      sentence.external_id, sentence.state_code)
        return True

    return False


# TODO(2647): Update this function to take in supervision sentences so we can determine which sentences overlap with
#  this admission date and use that to determine the supervision type pre-reincarceration.
def _get_pre_incarceration_supervision_type_on_date(_supervision_date: datetime.date):
    raise ValueError('Not yet implemented')
