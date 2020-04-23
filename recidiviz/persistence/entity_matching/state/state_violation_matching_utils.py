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
"""Specific entity matching utils for StateSupervisionViolation entities and its children."""
import datetime
from typing import List, Optional

from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodStatus
from recidiviz.common.constants.state.state_supervision_violation_response import \
    StateSupervisionViolationResponseRevocationType
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.schema.state.schema import StateSupervisionViolation
from recidiviz.persistence.entity.entity_utils import is_placeholder
from recidiviz.persistence.entity_matching.state.state_matching_utils import get_or_create_placeholder_child, \
    get_all_entities_of_cls
from recidiviz.persistence.errors import EntityMatchingError


def revoked_to_prison(svr: schema.StateSupervisionViolationResponse) -> bool:
    """Determines if the provided |svr| resulted in a revocation."""
    if not svr.revocation_type:
        return False
    reincarceration_types = [
        StateSupervisionViolationResponseRevocationType.REINCARCERATION.value,
        StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION.value,
        StateSupervisionViolationResponseRevocationType.TREATMENT_IN_PRISON.value]
    non_reincarceration_types = [
        StateSupervisionViolationResponseRevocationType.RETURN_TO_SUPERVISION.value]
    if svr.revocation_type in reincarceration_types:
        return True
    if svr.revocation_type in non_reincarceration_types:
        return False
    raise EntityMatchingError(f"Unexpected StateSupervisionViolationRevocationType {svr.revocation_type}.",
                              svr.get_entity_name())


def move_violations_onto_supervision_periods_for_sentence(matched_persons: List[schema.StatePerson]):
    """Given a list of |matched_persons|, for each sentence (either Incarceration or Supervision) associates all
    violations in that sentence with the corresponding SupervisionPeriod(s) based on date.
    """
    for person in matched_persons:
        for sentence_group in person.sentence_groups:
            for sentence in sentence_group.supervision_sentences + sentence_group.incarceration_sentences:
                unmatched_svs = _move_violations_onto_supervision_periods(sentence)
                if not unmatched_svs:
                    continue
                placeholder_sp = get_or_create_placeholder_child(
                    sentence, 'supervision_periods', schema.StateSupervisionPeriod,
                    person=person, state_code=sentence.state_code,
                    status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO.value)
                placeholder_sp.supervision_violation_entries = unmatched_svs


def move_violations_onto_supervision_periods_for_person(matched_persons: List[schema.StatePerson], state_code: str):
    for person in matched_persons:
        unmatched_svs = _move_violations_onto_supervision_periods(person)
        if not unmatched_svs:
            continue

        # We may hit this case if an entity that has already been committed to the DB has a date updated in a
        # later run such that the dates of the existing supervision periods no longer line up with one of the
        # existing supervision violations. In this case, we want to store the supervision violation on a placeholder
        # chain starting at sentence_group. We do this to show that the supervision violation isn't associated with
        # anything other than the person.
        placeholder_sg = get_or_create_placeholder_child(
            person, 'sentence_groups', schema.StateSentenceGroup,
            state_code=state_code, status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value)
        placeholder_s = get_or_create_placeholder_child(
            placeholder_sg, 'supervision_sentences', schema.StateSupervisionSentence,
            person=person, state_code=state_code, status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value)
        placeholder_sp = get_or_create_placeholder_child(
            placeholder_s, 'supervision_periods', schema.StateSupervisionPeriod,
            person=person, state_code=state_code, status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO.value)
        placeholder_sp.supervision_violation_entries = unmatched_svs


def _move_violations_onto_supervision_periods(source: DatabaseEntity) -> List[schema.StateSupervisionViolation]:
    """Looks at all SupervisionViolations in the provided |sentence|, and attempts to match them to the corresponding
    SupervisionPeriod, based on date. Returns all unmatched StateSupervisionViolations for the caller to store.
    """
    supervision_periods = get_all_entities_of_cls([source], schema.StateSupervisionPeriod)
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
                if sp_start_date <= violation_date < sp_end_date:
                    matched = True
                    sp.supervision_violation_entries.append(sv)

        # Unmatched SVs will be returned
        if not matched:
            unmatched_svs.append(sv)

    return unmatched_svs


def _get_approximate_violation_date(violation: StateSupervisionViolation) -> Optional[datetime.date]:
    """For the provided |violation|, returns the violation date (if present), otherwise relies on the earliest
    violation response date.
    """
    if violation.violation_date:
        return violation.violation_date

    response_dates = [svr.response_date for svr in violation.supervision_violation_responses if svr.response_date]
    if not response_dates:
        return None
    return min(response_dates)
