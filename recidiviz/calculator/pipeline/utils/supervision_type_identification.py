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
from typing import List, Optional, Set

from recidiviz.common.common_utils import date_spans_overlap_inclusive
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.common.constants.state.state_supervision_sentence import (
    StateSupervisionSentenceSupervisionType,
)
from recidiviz.common.date import first_day_of_month, last_day_of_month
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    get_ids,
    is_placeholder,
)
from recidiviz.persistence.entity.state.entities import (
    SentenceType,
    StateIncarcerationSentence,
    StateSupervisionPeriod,
    StateSupervisionSentence,
)

SUPERVISION_TYPE_PRECEDENCE_ORDER = [
    StateSupervisionPeriodSupervisionType.PROBATION,
    StateSupervisionPeriodSupervisionType.PAROLE,
    StateSupervisionPeriodSupervisionType.EXTERNAL_UNKNOWN,
    StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN,
]


def get_month_supervision_type_default(
    any_date_in_month: datetime.date,
    supervision_sentences: List[StateSupervisionSentence],
    incarceration_sentences: List[StateIncarcerationSentence],
    supervision_period: StateSupervisionPeriod,
    field_index: CoreEntityFieldIndex,
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
        raise ValueError("All objects should have database ids.")

    if is_placeholder(supervision_period, field_index):
        raise ValueError("Do not expect placeholder periods!")

    start_of_month = first_day_of_month(any_date_in_month)
    end_of_month = last_day_of_month(any_date_in_month)

    # Find sentences that are attached to the period and overlap with the month
    incarceration_sentences = _get_valid_attached_sentences(
        incarceration_sentences, supervision_period, field_index=field_index
    )
    incarceration_sentences = _get_sentences_overlapping_with_dates(
        start_of_month, end_of_month, incarceration_sentences
    )

    supervision_sentences = _get_valid_attached_sentences(
        supervision_sentences, supervision_period, field_index=field_index
    )
    supervision_sentences = _get_sentences_overlapping_with_dates(
        start_of_month, end_of_month, supervision_sentences
    )

    return get_supervision_type_from_sentences(
        incarceration_sentences, supervision_sentences
    )


def get_supervision_type_from_sentences(
    incarceration_sentences: List[StateIncarcerationSentence],
    supervision_sentences: List[StateSupervisionSentence],
) -> StateSupervisionPeriodSupervisionType:
    """Based on the provided |incarceration_sentences| and |supervision_sentences| determines and returns a
    supervision type.
    """
    # Get all the sentence types from the valid sentences
    supervision_types: Set[Optional[StateSupervisionSentenceSupervisionType]] = set()
    for supervision_sentence in supervision_sentences:
        if not isinstance(supervision_sentence, StateSupervisionSentence):
            continue

        if supervision_sentence.supervision_type:
            supervision_types.add(
                _get_sentence_supervision_type_from_sentence(supervision_sentence)
            )
        else:
            logging.warning(
                "Unexpectedly found supervision_sentence [%s] without supervision_type. Defaulting to "
                "PROBATION",
                supervision_sentence.supervision_sentence_id,
            )
            supervision_types.add(StateSupervisionSentenceSupervisionType.PROBATION)

    for incarceration_sentence in incarceration_sentences:
        supervision_types.add(
            _get_sentence_supervision_type_from_sentence(incarceration_sentence)
        )
    supervision_type = (
        sentence_supervision_types_to_supervision_period_supervision_type(
            supervision_types
        )
    )

    if not supervision_type:
        return StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN

    return supervision_type


def sentence_supervision_types_to_supervision_period_supervision_type(
    supervision_types: Set[Optional[StateSupervisionSentenceSupervisionType]],
) -> Optional[StateSupervisionPeriodSupervisionType]:

    supervision_period_supervision_types = [
        sentence_supervision_type_to_supervision_periods_supervision_type(
            supervision_type
        )
        for supervision_type in supervision_types
    ]
    if (
        StateSupervisionPeriodSupervisionType.PROBATION
        in supervision_period_supervision_types
        and StateSupervisionPeriodSupervisionType.PAROLE
        in supervision_period_supervision_types
    ):
        return StateSupervisionPeriodSupervisionType.DUAL

    # Return the supervision type that takes highest precedence
    return next(
        (
            supervision_period_supervision_type
            for supervision_period_supervision_type in SUPERVISION_TYPE_PRECEDENCE_ORDER
            if supervision_period_supervision_type
            in supervision_period_supervision_types
        ),
        None,
    )


def _get_sentence_supervision_type_from_sentence(
    sentence: SentenceType,
) -> StateSupervisionSentenceSupervisionType:
    """Classifies the StateSupervisionPeriodSupervisionType that should correspond to
    the supervision_type of the given sentence. This is a mapping of
    StateSupervisionSentenceSupervisionType to StateSupervisionPeriodSupervisionType for
    StateSupervisionSentences. For StateIncarcerationSentences, assumes the
    supervision_type is PAROLE."""
    if isinstance(sentence, StateIncarcerationSentence):
        # Assume supervision types from StateIncarcerationSentences are of type PAROLE
        return StateSupervisionSentenceSupervisionType.PAROLE

    return (
        sentence.supervision_type
        if sentence.supervision_type
        else StateSupervisionSentenceSupervisionType.INTERNAL_UNKNOWN
    )


def sentence_supervision_type_to_supervision_periods_supervision_type(
    sentence_supervision_type: Optional[StateSupervisionSentenceSupervisionType],
) -> Optional[StateSupervisionPeriodSupervisionType]:
    """Returns the StateSupervisionPeriodSupervisionType value that most closely
    matches the provided StateSupervisionSentenceSupervisionType."""
    if not sentence_supervision_type:
        return None

    if sentence_supervision_type == StateSupervisionSentenceSupervisionType.PROBATION:
        return StateSupervisionPeriodSupervisionType.PROBATION
    if sentence_supervision_type == StateSupervisionSentenceSupervisionType.PAROLE:
        return StateSupervisionPeriodSupervisionType.PAROLE
    if sentence_supervision_type in (
        StateSupervisionSentenceSupervisionType.PRE_CONFINEMENT,
        StateSupervisionSentenceSupervisionType.POST_CONFINEMENT,
        StateSupervisionSentenceSupervisionType.HALFWAY_HOUSE,
        StateSupervisionSentenceSupervisionType.CIVIL_COMMITMENT,
    ):
        # These are all types that should be deprecated, but for now, just assume probation - the numbers in ND are very
        # small for these.
        return StateSupervisionPeriodSupervisionType.PROBATION
    if (
        sentence_supervision_type
        == StateSupervisionSentenceSupervisionType.EXTERNAL_UNKNOWN
    ):
        return StateSupervisionPeriodSupervisionType.EXTERNAL_UNKNOWN

    if (
        sentence_supervision_type
        == StateSupervisionSentenceSupervisionType.INTERNAL_UNKNOWN
    ):
        return StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN

    raise ValueError(f"Unexpected supervision_type {sentence_supervision_type}.")


def _get_valid_attached_sentences(
    sentences: List[SentenceType],
    supervision_period: StateSupervisionPeriod,
    field_index: CoreEntityFieldIndex,
) -> List[SentenceType]:
    valid_sentences = _filter_sentences_with_missing_fields(sentences, field_index)
    attached_sentences = _filter_attached_sentences(valid_sentences, supervision_period)
    return attached_sentences


def _filter_attached_sentences(
    sentences: List[SentenceType], supervision_period: StateSupervisionPeriod
) -> List[SentenceType]:
    """Returns sentences that are attached to the given period, in a map indexed by the sentence primary key id."""
    attached_sentences: List[SentenceType] = []

    for sentence in sentences:
        if not sentence.get_id():
            raise ValueError("All objects should have database ids.")

        sentence_supervision_period_ids = get_ids(sentence.supervision_periods)

        if supervision_period.supervision_period_id in sentence_supervision_period_ids:
            attached_sentences.append(sentence)

    return attached_sentences


def _filter_sentences_with_missing_fields(
    sentences: List[SentenceType], field_index: CoreEntityFieldIndex
) -> List[SentenceType]:
    valid_sentences: List[SentenceType] = []
    for sentence in sentences:
        if is_placeholder(sentence, field_index):
            continue

        if not sentence.start_date:
            logging.error(
                "Non-placeholder sentence [%s] for state [%s] has no start date - ignoring.",
                sentence.external_id,
                sentence.state_code,
            )
            continue
        valid_sentences.append(sentence)
    return valid_sentences


def _get_sentences_overlapping_with_dates(
    begin_date: datetime.date, end_date: datetime.date, sentences: List[SentenceType]
) -> List[SentenceType]:
    sentences_within_dates: List[SentenceType] = []
    for sentence in sentences:
        if not sentence.start_date:
            raise ValueError(
                f"Expected non-null start date on sentence [{sentence.external_id}] for state [{sentence.state_code}]"
            )

        sentence_start = sentence.start_date
        sentence_completion = (
            sentence.completion_date if sentence.completion_date else datetime.date.max
        )

        if date_spans_overlap_inclusive(
            start_1=begin_date,
            end_1=end_date,
            start_2=sentence_start,
            end_2=sentence_completion,
        ):
            sentences_within_dates.append(sentence)

    return sentences_within_dates
