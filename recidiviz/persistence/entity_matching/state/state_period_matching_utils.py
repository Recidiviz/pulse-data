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
# ============================================================================
"""Specific entity matching utils for StateIncarcerationPeriod/StateSupervisionPeriod entities."""
import datetime
import logging
from typing import List, Optional, Type

from recidiviz.common.common_utils import date_spans_overlap_exclusive
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.entity_utils import is_placeholder
from recidiviz.persistence.entity_matching.state.state_matching_utils import get_all_entities_of_cls


def add_supervising_officer_to_open_supervision_periods(persons: List[schema.StatePerson]):
    """For each person in the provided |persons|, adds the supervising_officer from the person entity onto all open
    StateSupervisionPeriods.
    """
    for person in persons:
        if not person.supervising_officer:
            continue

        supervision_periods = get_all_entities_of_cls([person], schema.StateSupervisionPeriod)
        for supervision_period in supervision_periods:
            # Skip placeholders
            if is_placeholder(supervision_period):
                continue

            if not supervision_period.termination_date:
                supervision_period.supervising_officer = person.supervising_officer


def move_periods_onto_sentences_by_date(
        matched_persons: List[schema.StatePerson], period_filter: Optional[Type[schema.SchemaPeriodType]] = None):
    """Given a list of |matched_persons|, for each SentenceGroup associates all periods (incarceration or supervision)
    in that sentence group with the corresponding Sentence (incarceration or supervision) based on date. If
    |period_filter| is not None, this method will only move periods whose type matches |period_filter|.
    """
    for person in matched_persons:
        for sentence_group in person.sentence_groups:
            if period_filter:
                _move_periods_onto_sentences_for_sentence_group(sentence_group, period_filter)
            else:
                _move_periods_onto_sentences_for_sentence_group(sentence_group, schema.StateSupervisionPeriod)
                _move_periods_onto_sentences_for_sentence_group(sentence_group, schema.StateIncarcerationPeriod)


def _get_period_start_date(period: schema.SchemaPeriodType, default=datetime.date.min) -> datetime.date:
    if isinstance(period, schema.StateSupervisionPeriod):
        start_date = period.start_date
    else:
        start_date = period.admission_date
    return start_date if start_date else default


def _get_period_end_date(period: schema.SchemaPeriodType, default=datetime.date.max) -> datetime.date:
    if isinstance(period, schema.StateSupervisionPeriod):
        end_date = period.termination_date
    else:
        end_date = period.release_date
    return end_date if end_date else default


def _add_period_to_sentence(period: schema.SchemaPeriodType, sentence: schema.SchemaSentenceType):
    if isinstance(period, schema.StateSupervisionPeriod):
        sentence.supervision_periods.append(period)
    else:
        sentence.incarceration_periods.append(period)


def _only_keep_placeholder_periods_on_sentence(
        sentence: schema.SchemaSentenceType, period_type: Type[schema.SchemaPeriodType]):
    """Removes all non placeholder periods of type |period_type| from the provided |sentence|."""
    sentence_periods = sentence.supervision_periods \
        if period_type == schema.StateSupervisionPeriod \
        else sentence.incarceration_periods

    placeholder_periods = [p for p in sentence_periods if is_placeholder(p)]

    if period_type == schema.StateSupervisionPeriod:
        sentence.supervision_periods = placeholder_periods
    else:
        sentence.incarceration_periods = placeholder_periods


def _is_sentence_ended_by_status(sentence: schema.SchemaSentenceType):
    """Returns True if the provided |sentence| has a status that indicates the sentence has been ended."""
    if sentence.status is None or sentence.status in (StateSentenceStatus.EXTERNAL_UNKNOWN.value,
                                                      StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
                                                      StateSentenceStatus.SERVING.value,
                                                      StateSentenceStatus.SUSPENDED.value):
        return False
    if sentence.status in (StateSentenceStatus.COMPLETED.value,
                           StateSentenceStatus.COMMUTED.value,
                           StateSentenceStatus.REVOKED.value):
        return True
    raise ValueError(f'Unexpected sentence type [{sentence.status}] in _is_sentence_ended_by_status')


def _get_date_matchable_sentences(sentences: List[schema.SchemaSentenceType]) -> List[schema.SchemaSentenceType]:
    """Filters the provided list of |sentences| to only include sentences which are able to be matched to periods based
    on date. Returns this filtered list.
    """
    valid_sentences = []

    for sentence in sentences:
        if is_placeholder(sentence):
            continue
        # If we have sentences which we know are complete, but we have no completion date, exclude them from date
        # matching.
        if _is_sentence_ended_by_status(sentence) and not sentence.completion_date:
            continue
        valid_sentences.append(sentence)

    return valid_sentences


def _move_periods_onto_sentences_for_sentence_group(
        sentence_group: schema.StateSentenceGroup, period_type: Type[schema.SchemaPeriodType]):
    """Looks at all SupervisionPeriods in the provided |sentence_group|, and attempts to match them to any
    corresponding sentences, based on date.
    """
    sentences = sentence_group.supervision_sentences + sentence_group.incarceration_sentences

    # Get all periods from sentence group
    periods = get_all_entities_of_cls([sentence_group], period_type)

    # Clear non-placeholder links from sentence to period. We will re-add/update these relationships below.
    for sentence in sentences:
        _only_keep_placeholder_periods_on_sentence(sentence, period_type)

    unmatched_periods = []
    matchable_sentences = _get_date_matchable_sentences(sentences)

    non_placeholder_periods = [p for p in periods if not is_placeholder(p)]

    # Match periods to non_placeholder_sentences by date.
    for p in non_placeholder_periods:
        matched = False
        p_start_date = _get_period_start_date(p)
        p_end_date = _get_period_end_date(p)

        for s in matchable_sentences:
            s_start_date = s.start_date
            if not s_start_date:
                continue

            s_completion_date = s.completion_date if s.completion_date else datetime.date.max

            if date_spans_overlap_exclusive(
                    start_1=p_start_date, end_1=p_end_date, start_2=s_start_date, end_2=s_completion_date):
                matched = True
                _add_period_to_sentence(p, s)

        # Unmatched periods will be re-added to a placeholder sentence at the end.
        if not matched:
            unmatched_periods.append(p)

    # Add unmatched periods to a placeholder sentence
    if unmatched_periods:
        placeholder_sentences = [s for s in sentences if is_placeholder(s)]
        if not placeholder_sentences:
            # We may hit this case if an entity that has already been committed to the DB has a date updated in a later
            # run such that the dates of the existing sentences no longer line up with one of the existing periods.
            logging.info(
                'No placeholder sentences exist on sentence group [%s]([%s]), creating a new placeholder sentence.',
                sentence_group.external_id, sentence_group.sentence_group_id)
            new_placeholder_sentence = schema.StateSupervisionSentence(
                state_code=sentence_group.state_code,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
                person=sentence_group.person)
            placeholder_sentences.append(new_placeholder_sentence)
            sentence_group.supervision_sentences.append(new_placeholder_sentence)
        placeholder_sentence = placeholder_sentences[0]
        for unmatched_period in unmatched_periods:
            _add_period_to_sentence(unmatched_period, placeholder_sentence)
