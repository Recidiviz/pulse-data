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
"""US_MO specific entity matching tests."""
import datetime

import attr

from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_period import \
    StateSupervisionPeriodStatus
from recidiviz.persistence.entity.state.entities import \
    StatePersonExternalId, StatePerson, \
    StateSupervisionSentence, StateSupervisionViolation, \
    StateSupervisionPeriod, StateSentenceGroup, \
    StateSupervisionViolationResponse, StateAgent
from recidiviz.persistence.entity_matching import entity_matching
from recidiviz.tests.persistence.database.schema.state.schema_test_utils \
    import generate_person, generate_external_id, \
    generate_incarceration_sentence, \
    generate_sentence_group, \
    generate_supervision_period, \
    generate_supervision_violation, generate_agent, \
    generate_supervision_sentence
from recidiviz.tests.persistence.entity_matching.state. \
    base_state_entity_matcher_test import BaseStateEntityMatcherTest

_EXTERNAL_ID = 'EXTERNAL_ID'
_EXTERNAL_ID_2 = 'EXTERNAL_ID_2'
_EXTERNAL_ID_3 = 'EXTERNAL_ID_3'
_EXTERNAL_ID_WITH_SUFFIX = 'EXTERNAL_ID-SEO-FSO'
_FULL_NAME = 'FULL_NAME'
_ID = 1
_ID_2 = 2
_ID_3 = 3
_ID_TYPE = 'ID_TYPE'
_US_MO = 'US_MO'
_DATE_1 = datetime.date(year=2019, month=1, day=1)
_DATE_2 = datetime.date(year=2019, month=2, day=1)
_DATE_3 = datetime.date(year=2019, month=3, day=1)
_DATE_4 = datetime.date(year=2019, month=4, day=1)


class TestMoEntityMatching(BaseStateEntityMatcherTest):
    """Test class for US_MO specific entity matching logic."""

    def test_supervisionViolationsWithDifferentParents_mergesViolations(self):
        db_person = generate_person(person_id=_ID, full_name=_FULL_NAME)
        db_supervision_violation = generate_supervision_violation(
            person=db_person,
            state_code=_US_MO,
            supervision_violation_id=_ID,
            external_id=_EXTERNAL_ID)
        db_placeholder_supervision_period = generate_supervision_period(
            person=db_person,
            state_code=_US_MO,
            supervision_period_id=_ID,
            supervision_violation_entries=[db_supervision_violation])
        db_incarceration_sentence = generate_incarceration_sentence(
            person=db_person,
            state_code=_US_MO,
            incarceration_sentence_id=_ID,
            external_id=_EXTERNAL_ID,
            supervision_periods=[db_placeholder_supervision_period])
        db_sentence_group = generate_sentence_group(
            person=db_person,
            state_code=_US_MO,
            sentence_group_id=_ID,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[db_incarceration_sentence])
        db_external_id = generate_external_id(
            person=db_person,
            person_external_id_id=_ID,
            state_code=_US_MO,
            external_id=_EXTERNAL_ID)
        db_person.sentence_groups = [db_sentence_group]
        db_person.external_ids = [db_external_id]

        self._commit_to_db(db_person)

        supervision_violation = attr.evolve(
            self.to_entity(db_supervision_violation),
            supervision_violation_id=None,
            external_id=_EXTERNAL_ID_WITH_SUFFIX)
        placeholder_supervision_period = attr.evolve(
            self.to_entity(db_placeholder_supervision_period),
            supervision_period_id=None,
            supervision_violation_entries=[supervision_violation])
        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            state_code=_US_MO,
            external_id=_EXTERNAL_ID,
            supervision_periods=[placeholder_supervision_period])
        sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            sentence_group_id=None,
            incarceration_sentences=[],
            supervision_sentences=[supervision_sentence])
        external_id = attr.evolve(
            self.to_entity(db_external_id),
            person_external_id_id=None)
        person = attr.evolve(
            self.to_entity(db_person),
            person_id=None,
            sentence_groups=[sentence_group],
            external_ids=[external_id])

        expected_supervision_violation = attr.evolve(
            self.to_entity(db_supervision_violation))
        expected_placeholder_supervision_period_is = attr.evolve(
            placeholder_supervision_period,
            supervision_violation_entries=[expected_supervision_violation])
        expected_placeholder_supervision_period_ss = attr.evolve(
            self.to_entity(db_placeholder_supervision_period),
            supervision_violation_entries=[expected_supervision_violation])
        expected_supervision_sentence = attr.evolve(
            supervision_sentence,
            supervision_periods=[expected_placeholder_supervision_period_ss])
        expected_incarceration_sentence = attr.evolve(
            self.to_entity(db_incarceration_sentence),
            supervision_periods=[expected_placeholder_supervision_period_is])
        expected_sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            incarceration_sentences=[expected_incarceration_sentence],
            supervision_sentences=[expected_supervision_sentence])
        expected_external_id = attr.evolve(self.to_entity(db_external_id))
        expected_person = attr.evolve(
            self.to_entity(db_person),
            external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group])
        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _US_MO, ingested_people=[person])

        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_removeSeosFromSupervisionViolation(self):
        supervision_violation_response = \
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_US_MO,
                external_id=_EXTERNAL_ID_WITH_SUFFIX)
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            state_code=_US_MO,
            external_id=_EXTERNAL_ID_WITH_SUFFIX,
            supervision_violation_responses=[supervision_violation_response])
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code=_US_MO,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            supervision_violation_entries=[supervision_violation])
        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            state_code=_US_MO,
            external_id=_EXTERNAL_ID,
            supervision_periods=[supervision_period])
        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code=_US_MO,
            external_id=_EXTERNAL_ID_WITH_SUFFIX,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[],
            supervision_sentences=[supervision_sentence])
        external_id = StatePersonExternalId.new_with_defaults(
            state_code=_US_MO,
            id_type=_ID_TYPE,
            external_id=_EXTERNAL_ID)
        person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group],
            external_ids=[external_id])

        updated_external_id = _EXTERNAL_ID
        expected_supervision_violation_response = attr.evolve(
            supervision_violation_response, external_id=updated_external_id)
        expected_supervision_violation = attr.evolve(
            supervision_violation,
            external_id=updated_external_id,
            supervision_violation_responses=[
                expected_supervision_violation_response])
        expected_supervision_period = attr.evolve(
            supervision_period,
            supervision_violation_entries=[expected_supervision_violation])
        expected_supervision_sentence = attr.evolve(
            supervision_sentence,
            supervision_periods=[expected_supervision_period])
        expected_sentence_group = attr.evolve(
            sentence_group,
            supervision_sentences=[expected_supervision_sentence])
        expected_external_id = attr.evolve(external_id)
        expected_person = attr.evolve(
            person,
            external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group])

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _US_MO, ingested_people=[person])

        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_runMatch_moveSupervisingOfficerOntoOpenSupervisionPeriods(self):
        db_supervising_officer = generate_agent(
            agent_id=_ID, external_id=_EXTERNAL_ID, state_code=_US_MO)
        db_person = generate_person(person_id=_ID)
        db_external_id = generate_external_id(
            person_external_id_id=_ID, external_id=_EXTERNAL_ID,
            state_code=_US_MO,
            id_type=_ID_TYPE)
        db_supervision_period = generate_supervision_period(
            person=db_person,
            supervision_period_id=_ID,
            external_id=_EXTERNAL_ID,
            start_date=_DATE_1,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_US_MO,
            supervising_officer=db_supervising_officer)
        db_supervision_period_another = generate_supervision_period(
            person=db_person,
            supervision_period_id=_ID_2,
            external_id=_EXTERNAL_ID_2,
            start_date=_DATE_2,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_US_MO,
            supervising_officer=db_supervising_officer)
        db_closed_supervision_period = generate_supervision_period(
            person=db_person,
            supervision_period_id=_ID_3,
            external_id=_EXTERNAL_ID_3,
            start_date=_DATE_3,
            termination_date=_DATE_4,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_US_MO,
            supervising_officer=db_supervising_officer)
        db_supervision_sentence = generate_supervision_sentence(
            person=db_person,
            external_id=_EXTERNAL_ID, supervision_sentence_id=_ID,
            supervision_periods=[db_supervision_period,
                                 db_supervision_period_another,
                                 db_closed_supervision_period])
        db_sentence_group = generate_sentence_group(
            external_id=_EXTERNAL_ID, sentence_group_id=_ID,
            supervision_sentences=[db_supervision_sentence])
        db_person.external_ids = [db_external_id]
        db_person.sentence_groups = [db_sentence_group]
        self._commit_to_db(db_person)

        external_id = attr.evolve(self.to_entity(db_external_id),
                                  person_external_id_id=None)
        new_supervising_officer = StateAgent.new_with_defaults(
            external_id=_EXTERNAL_ID_2,
            state_code=_US_MO,
            agent_type=StateAgentType.SUPERVISION_OFFICER)
        person = StatePerson.new_with_defaults(
            external_ids=[external_id],
            supervising_officer=new_supervising_officer)

        expected_supervising_officer = attr.evolve(
            self.to_entity(db_supervising_officer),
            agent_id=None)

        expected_new_supervising_officer = attr.evolve(new_supervising_officer)
        expected_supervision_period = attr.evolve(
            self.to_entity(db_supervision_period),
            supervising_officer=expected_new_supervising_officer)
        expected_supervision_period_another = attr.evolve(
            self.to_entity(db_supervision_period_another),
            supervising_officer=expected_new_supervising_officer)
        expected_closed_supervision_period = attr.evolve(
            self.to_entity(db_closed_supervision_period),
            supervising_officer=expected_supervising_officer)
        expected_supervision_sentence = attr.evolve(
            self.to_entity(db_supervision_sentence),
            supervision_periods=[expected_supervision_period,
                                 expected_supervision_period_another,
                                 expected_closed_supervision_period])
        expected_sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            supervision_sentences=[expected_supervision_sentence])
        expected_external_id = self.to_entity(db_external_id)
        expected_person = attr.evolve(
            self.to_entity(db_person),
            external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group],
            supervising_officer=expected_new_supervising_officer)

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _US_MO, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)
