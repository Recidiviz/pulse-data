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
"""Contains util methods for UsMoMatchingDelegate."""
import datetime
import logging
from typing import List, Union, Optional

from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodStatus
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.schema.state.schema import StateSupervisionViolation, StateSupervisionSentence, \
    StateIncarcerationSentence
from recidiviz.persistence.entity.entity_utils import is_placeholder
from recidiviz.persistence.entity_matching.state.state_matching_utils import get_all_entities_of_cls
from recidiviz.persistence.errors import EntityMatchingError


# TODO(1883): Remove this once our proto converter and data extractor can handle the presence of multiple paths to
#  entities with the same id
def remove_suffix_from_violation_ids(
        ingested_persons: List[schema.StatePerson]):
    """Removes SEO (sentence sequence numbers) and FSO (field sequence numbers) from the end of
    StateSupervisionViolation external_ids. This allows violations across sentences to be merged correctly by
    entity matching.
    """
    ssvs = get_all_entities_of_cls(ingested_persons, schema.StateSupervisionViolation)
    ssvrs = get_all_entities_of_cls(ingested_persons, schema.StateSupervisionViolationResponse)
    _remove_suffix_from_violation_entity(ssvs)
    _remove_suffix_from_violation_entity(ssvrs)


def _remove_suffix_from_violation_entity(
        violation_entities:
        List[Union[schema.StateSupervisionViolation, schema.StateSupervisionViolationResponse]]):
    for entity in violation_entities:
        if not entity.external_id:
            continue
        splits = entity.external_id.rsplit('-', 2)
        if len(splits) != 3:
            raise EntityMatchingError(f'Unexpected id format for {entity.get_entity_name()}{entity.external_id}',
                                      entity.get_entity_name())
        entity.external_id = splits[0]


def set_current_supervising_officer_from_supervision_periods(
        matched_persons: List[schema.StatePerson]):
    """For every matched person, update the supervising_officer field to pull in the supervising_officer from the latest
    supervision period (sorted by termination date).
    """
    for person in matched_persons:

        sps = get_all_entities_of_cls(person.sentence_groups, schema.StateSupervisionPeriod)

        non_placeholder_sps = [sp for sp in sps if not is_placeholder(sp)]

        if not non_placeholder_sps:
            continue

        non_placeholder_sps.sort(key=lambda sp: sp.termination_date if sp.termination_date else datetime.date.max)

        latest_supervision_period = non_placeholder_sps[-1]
        person.supervising_officer = latest_supervision_period.supervising_officer


def move_supervision_periods_onto_sentences_by_date(
        matched_persons: List[schema.StatePerson]):
    """Given a list of |matched_persons|, for each SentenceGroup associates all SupervisionPeriods in that sentence
    group with the corresponding Sentence (incarceration or supervision) based on date.
    """
    for person in matched_persons:
        for sentence_group in person.sentence_groups:
            _move_supervision_periods_onto_sentences_for_sentence_group(sentence_group)


# TODO(2647): Connect incarceration sentences so that we can look only at connected sentences whe determining which
#  overlapping sentences contribute to the supervision period supervision period.
# TODO(2798): Update this to also associate incarceration periods to sentences by date as a fast follow.
def _move_supervision_periods_onto_sentences_for_sentence_group(
        sentence_group: schema.StateSentenceGroup):
    """Looks at all SupervisionPeriods in the provided |sentence_group|, and attempts to match them to any
    corresponding sentences, based on date.
    """
    sentences = sentence_group.supervision_sentences + sentence_group.incarceration_sentences

    # Get all supervision periods from sentence group
    supervision_periods = get_all_entities_of_cls([sentence_group], schema.StateSupervisionPeriod)

    # Clear non-placeholder links from sentence to supervision period. We will
    # re-add/update these relationships below.
    for sentence in sentences:
        placeholder_supervision_periods = [sp for sp in sentence.supervision_periods if is_placeholder(sp)]
        sentence.supervision_periods = placeholder_supervision_periods

    unmatched_sps = []
    non_placeholder_sentences = [s for s in sentences if not is_placeholder(s)]

    non_placeholder_supervision_periods = [sp for sp in supervision_periods if not is_placeholder(sp)]

    # Match SVs to non_placeholder_periods by date.
    for sp in non_placeholder_supervision_periods:
        matched = False
        sp_start_date = sp.start_date if sp.start_date else datetime.date.min
        sp_termination_date = sp.termination_date if sp.termination_date else datetime.date.max

        for s in non_placeholder_sentences:
            if not s.start_date:
                continue

            s_completion_date = s.completion_date if s.completion_date else datetime.date.max

            if (s.start_date <= sp_start_date < s_completion_date) \
                    or (s.start_date <= sp_termination_date < s_completion_date):
                matched = True
                s.supervision_periods.append(sp)

        # Unmatched SPs will be re-added to a placeholder sentence at the end.
        if not matched:
            unmatched_sps.append(sp)

    # Add unmatched supervision periods to a placeholder sentence
    if unmatched_sps:
        placeholder_sentences = [s for s in sentences if is_placeholder(s)]
        if not placeholder_sentences:
            # We may hit this case if an entity that has already been committed to the DB has a date updated in a later
            # run such that the dates of the existing sentences no longer line up with one of the existing supervision
            # periods.
            logging.info(
                'No placeholder sentences exist on sentence group [%s]([%s]), creating a new placeholder sentence.',
                sentence_group.external_id, sentence_group.sentence_group_id)
            new_placeholder_sentence = schema.StateSupervisionSentence(
                state_code=sentence_group.state_code,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
                person=sentence_group.person)
            placeholder_sentences.append(new_placeholder_sentence)
            sentence_group.supervision_sentences.append(new_placeholder_sentence)
        placeholder_sentences[0].supervision_periods = unmatched_sps


def move_violations_onto_supervision_periods_by_date(
        matched_persons: List[schema.StatePerson]):
    """Given a list of |matched_persons|, for each sentence (either Incarceration or Supervision) associates all
    violations in that sentence with the corresponding SupervisionPeriod(s) based on date.
    """
    for person in matched_persons:
        for sentence_group in person.sentence_groups:
            for sentence in sentence_group.supervision_sentences \
                            + sentence_group.incarceration_sentences:
                _move_violations_onto_supervision_periods_for_sentence(sentence)


def _move_violations_onto_supervision_periods_for_sentence(
        sentence: Union[StateSupervisionSentence, StateIncarcerationSentence]):
    """Looks at all SupervisionViolations in the provided |sentence|, and attempts to match them to the corresponding
    SupervisionPeriod, based on date.
    """
    supervision_periods = sentence.supervision_periods

    # Get all supervision violations from supervision periods
    supervision_violations = get_all_entities_of_cls(supervision_periods, StateSupervisionViolation)

    # Clear the links from supervision period to supervision violations. We will
    # re-add/update these relationships below.
    for supervision_period in supervision_periods:
        supervision_period.supervision_violation_entries = []

    unmatched_svs = []
    non_placeholder_periods = [sp for sp in supervision_periods if not is_placeholder(sp)]

    # Match SVs to non_placeholder_periods by date.
    for sv in supervision_violations:
        matched = False
        violation_date = _get_approximate_violation_date(sv)
        if violation_date:
            for sp in non_placeholder_periods:
                sp_end_date = sp.termination_date if sp.termination_date else datetime.date.max
                sp_start_date = sp.start_date
                if sp_start_date <= violation_date <= sp_end_date:
                    matched = True
                    sp.supervision_violation_entries.append(sv)

        # Unmatched SVs will be re-added to a placeholder period at the end.
        if not matched:
            unmatched_svs.append(sv)

    # Add unmatched supervision violations to a placeholder period
    if unmatched_svs:
        placeholder_periods = [sp for sp in supervision_periods if is_placeholder(sp)]

        if not placeholder_periods:
            # We may hit this case if an entity that has already been committed to the DB has a date updated in a later
            # run such that the dates of the existing supervision periods no longer line up with one of the existing
            # supervision violations.
            logging.info(
                'No placeholder supervision periods exist on sentence [%s]([%s]), creating a new placeholder '
                'supervision period.',
                sentence.external_id, sentence.get_id())
            new_placeholder_supervision_period = schema.StateSupervisionPeriod(
                state_code=sentence.state_code,
                status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO.value,
                person=sentence.person
            )
            placeholder_periods.append(new_placeholder_supervision_period)
            sentence.supervision_periods.append(new_placeholder_supervision_period)

        placeholder_periods[0].supervision_violation_entries = unmatched_svs


def _get_approximate_violation_date(violation: StateSupervisionViolation) -> Optional[datetime.date]:
    """For the provided |violation|, returns the violation date (if present), otherwise relies on the earliest
    violation response date."""
    if violation.violation_date:
        return violation.violation_date

    response_dates = [svr.response_date for svr in violation.supervision_violation_responses if svr.response_date]
    if not response_dates:
        return None
    return min(response_dates)
