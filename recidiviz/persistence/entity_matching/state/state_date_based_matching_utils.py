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
"""Specific entity matching utils for date based matching of entities."""
import datetime
from typing import List, Optional, Type

from recidiviz.common.common_utils import date_spans_overlap_exclusive
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.schema.state.schema import (
    StateSupervisionViolation,
    StateSupervisionContact,
)
from recidiviz.persistence.entity.entity_utils import is_placeholder
from recidiviz.persistence.entity_matching.state.state_matching_utils import (
    get_all_entities_of_cls,
    get_or_create_placeholder_child,
)


def move_periods_onto_sentences_by_date(
    matched_persons: List[schema.StatePerson],
    period_filter: Optional[Type[schema.SchemaPeriodType]] = None,
) -> None:
    """Given a list of |matched_persons|, for each SentenceGroup associates all periods (incarceration or supervision)
    in that sentence group with the corresponding Sentence (incarceration or supervision) based on date. If
    |period_filter| is not None, this method will only move periods whose type matches |period_filter|.
    """
    for person in matched_persons:
        for sentence_group in person.sentence_groups:
            if period_filter:
                _move_periods_onto_sentences_for_sentence_group(
                    sentence_group, period_filter
                )
            else:
                _move_periods_onto_sentences_for_sentence_group(
                    sentence_group, schema.StateSupervisionPeriod
                )
                _move_periods_onto_sentences_for_sentence_group(
                    sentence_group, schema.StateIncarcerationPeriod
                )


def _get_period_start_date(
    period: schema.SchemaPeriodType, default=datetime.date.min
) -> datetime.date:
    if isinstance(period, schema.StateSupervisionPeriod):
        start_date = period.start_date
    else:
        start_date = period.admission_date
    return start_date if start_date else default


def _get_period_end_date(
    period: schema.SchemaPeriodType, default=datetime.date.max
) -> datetime.date:
    if isinstance(period, schema.StateSupervisionPeriod):
        end_date = period.termination_date
    else:
        end_date = period.release_date
    return end_date if end_date else default


def _add_period_to_sentence(
    period: schema.SchemaPeriodType, sentence: schema.SchemaSentenceType
) -> None:
    if isinstance(period, schema.StateSupervisionPeriod):
        sentence.supervision_periods.append(period)
    else:
        sentence.incarceration_periods.append(period)


def _only_keep_placeholder_periods_on_sentence(
    sentence: schema.SchemaSentenceType, period_type: Type[schema.SchemaPeriodType]
) -> None:
    """Removes all non placeholder periods of type |period_type| from the provided |sentence|."""
    sentence_periods = (
        sentence.supervision_periods
        if period_type == schema.StateSupervisionPeriod
        else sentence.incarceration_periods
    )

    placeholder_periods = [p for p in sentence_periods if is_placeholder(p)]

    if period_type == schema.StateSupervisionPeriod:
        sentence.supervision_periods = placeholder_periods
    else:
        sentence.incarceration_periods = placeholder_periods


def _is_sentence_ended_by_status(sentence: schema.SchemaSentenceType) -> bool:
    """Returns True if the provided |sentence| has a status that indicates the sentence has been ended."""
    if sentence.status is None or sentence.status in (
        StateSentenceStatus.EXTERNAL_UNKNOWN.value,
        StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
        StateSentenceStatus.SERVING.value,
        StateSentenceStatus.SUSPENDED.value,
    ):
        return False
    if sentence.status in (
        StateSentenceStatus.COMPLETED.value,
        StateSentenceStatus.COMMUTED.value,
        StateSentenceStatus.PARDONED.value,
        StateSentenceStatus.REVOKED.value,
        StateSentenceStatus.VACATED.value,
    ):
        return True
    raise ValueError(
        f"Unexpected sentence type [{sentence.status}] in _is_sentence_ended_by_status"
    )


def _get_date_matchable_sentences(
    sentences: List[schema.SchemaSentenceType],
) -> List[schema.SchemaSentenceType]:
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
    sentence_group: schema.StateSentenceGroup,
    period_type: Type[schema.SchemaPeriodType],
) -> None:
    """Looks at all SupervisionPeriods in the provided |sentence_group|, and attempts to match them to any
    corresponding sentences, based on date.
    """
    sentences = (
        sentence_group.supervision_sentences + sentence_group.incarceration_sentences
    )

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

            s_completion_date = (
                s.completion_date if s.completion_date else datetime.date.max
            )

            if date_spans_overlap_exclusive(
                start_1=p_start_date,
                end_1=p_end_date,
                start_2=s_start_date,
                end_2=s_completion_date,
            ):
                matched = True
                _add_period_to_sentence(p, s)

        # Unmatched periods will be re-added to a placeholder sentence at the end.
        if not matched:
            unmatched_periods.append(p)

    # Add unmatched periods to a placeholder sentence
    if unmatched_periods:
        placeholder_sentences = [s for s in sentences if is_placeholder(s)]
        if not placeholder_sentences:
            placeholder_sentence = get_or_create_placeholder_child(
                sentence_group,
                "supervision_sentences",
                schema.StateSupervisionSentence,
                state_code=sentence_group.state_code,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
                person=sentence_group.person,
            )
        else:
            placeholder_sentence = placeholder_sentences[0]
        for unmatched_period in unmatched_periods:
            _add_period_to_sentence(unmatched_period, placeholder_sentence)


def move_violations_onto_supervision_periods_for_sentence(
    matched_persons: List[schema.StatePerson],
) -> None:
    """Given a list of |matched_persons|, for each sentence (either Incarceration or Supervision) associates all
    violations in that sentence with the corresponding SupervisionPeriod(s) based on date.
    """
    for person in matched_persons:
        for sentence_group in person.sentence_groups:
            for sentence in (
                sentence_group.supervision_sentences
                + sentence_group.incarceration_sentences
            ):
                unmatched_svs = _move_events_onto_supervision_periods(
                    sentence,
                    schema.StateSupervisionViolation,
                    "supervision_violation_entries",
                )
                if not unmatched_svs:
                    continue
                placeholder_sp = get_or_create_placeholder_child(
                    sentence,
                    "supervision_periods",
                    schema.StateSupervisionPeriod,
                    person=person,
                    state_code=sentence.state_code,
                )
                placeholder_sp.supervision_violation_entries = unmatched_svs


def _move_events_onto_supervision_periods_for_person(
    matched_persons: List[schema.StatePerson],
    event_cls: Type[DatabaseEntity],
    event_field_name: str,
    state_code: str,
) -> None:
    """For each person in |matched_persons|, moves all events of type |event_cls| onto the |event_field_name| field of
    a matching supervision period, based on date. If there is no matching supervision period, ensures that the events
    hang off of a placeholder chain.
    """
    if not StateCode.is_valid(state_code):
        raise ValueError(f"Invalid state code: [{state_code}]")

    for person in matched_persons:
        unmatched_events = _move_events_onto_supervision_periods(
            person, event_cls, event_field_name
        )
        if not unmatched_events:
            continue

        # We may hit this case if an entity that has already been committed to the DB has a date updated in a
        # later run such that the dates of the existing supervision periods no longer line up with one of the
        # existing events. In this case, we want to store the event on a placeholder chain starting at sentence_group.
        # We do this to show that the supervision violation isn't associated with anything other than the person.
        placeholder_sg = get_or_create_placeholder_child(
            person,
            "sentence_groups",
            schema.StateSentenceGroup,
            state_code=state_code,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
        )
        placeholder_s = get_or_create_placeholder_child(
            placeholder_sg,
            "supervision_sentences",
            schema.StateSupervisionSentence,
            person=person,
            state_code=state_code,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
        )
        placeholder_sp = get_or_create_placeholder_child(
            placeholder_s,
            "supervision_periods",
            schema.StateSupervisionPeriod,
            person=person,
            state_code=state_code,
        )
        placeholder_sp.set_field_from_list(event_field_name, unmatched_events)


def move_violations_onto_supervision_periods_for_person(
    matched_persons: List[schema.StatePerson], state_code: str
) -> None:
    return _move_events_onto_supervision_periods_for_person(
        matched_persons,
        schema.StateSupervisionViolation,
        "supervision_violation_entries",
        state_code,
    )


def move_contacts_onto_supervision_periods_for_person(
    matched_persons: List[schema.StatePerson], state_code: str
) -> None:
    return _move_events_onto_supervision_periods_for_person(
        matched_persons,
        schema.StateSupervisionContact,
        "supervision_contacts",
        state_code,
    )


def _move_events_onto_supervision_periods(
    source: DatabaseEntity, event_cls: Type[DatabaseEntity], event_field_name: str
) -> List[DatabaseEntity]:
    """Looks at all events of type |event_cls| in the provided |source|, and attempts to place them onto a matching
    SupervisionPeriod's |event_field_name| field. Matching is based on dates, and all unmatched events are returned
    to the caller to store.
    """
    supervision_periods = get_all_entities_of_cls(
        [source], schema.StateSupervisionPeriod
    )
    events = get_all_entities_of_cls(supervision_periods, event_cls)

    # Clear the links from supervision period to supervision violations. We will
    # re-add/update these relationships below.
    for supervision_period in supervision_periods:
        supervision_period.set_field(event_field_name, [])

    unmatched_events = []
    non_placeholder_periods = [
        sp for sp in supervision_periods if not is_placeholder(sp)
    ]

    # Match events onto to non_placeholder_periods by date.
    for event in events:
        matched = False
        event_date = _get_event_date(event)
        if event_date:
            for sp in non_placeholder_periods:
                sp_end_date = (
                    sp.termination_date if sp.termination_date else datetime.date.max
                )
                sp_start_date = sp.start_date
                if sp_start_date <= event_date < sp_end_date:
                    matched = True
                    sp_events = sp.get_field(event_field_name)
                    sp_events.append(event)
                    sp.set_field(event_field_name, sp_events)

        # Unmatched SVs will be returned
        if not matched:
            unmatched_events.append(event)

    return unmatched_events


def _get_event_date(source: DatabaseEntity) -> Optional[datetime.date]:
    if isinstance(source, StateSupervisionViolation):
        return _get_approximate_violation_date(source)
    if isinstance(source, StateSupervisionContact):
        return source.contact_date
    raise ValueError(
        f"Unexpected event entity found of type [{source.get_entity_name()}]"
    )


def _get_approximate_violation_date(
    violation: StateSupervisionViolation,
) -> Optional[datetime.date]:
    """For the provided |violation|, returns the violation date (if present), otherwise relies on the earliest
    violation response date.
    """
    if violation.violation_date:
        return violation.violation_date

    response_dates = [
        svr.response_date
        for svr in violation.supervision_violation_responses
        if svr.response_date
    ]
    if not response_dates:
        return None
    return min(response_dates)
