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
"""Tests for state_entity_matcher.py."""
import datetime
from typing import List

import attr
from mock import patch

from recidiviz.common.constants.bond import BondStatus
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.county.sentence import SentenceStatus
from recidiviz.common.constants.person_characteristics import Gender, Race, \
    Ethnicity
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_case_type import \
    StateSupervisionCaseType
from recidiviz.common.constants.state.state_fine import StateFineStatus
from recidiviz.common.constants.state.state_incarceration import \
    StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodStatus, StateIncarcerationPeriodAdmissionReason
from recidiviz.common.constants.state.state_parole_decision import \
    StateParoleDecisionOutcome
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_period import \
    StateSupervisionPeriodStatus, StateSupervisionPeriodAdmissionReason
from recidiviz.common.constants.state.state_supervision_violation import \
    StateSupervisionViolationType
from recidiviz.common.constants.state.state_supervision_violation_response \
    import StateSupervisionViolationResponseDecision, \
    StateSupervisionViolationResponseRevocationType
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.entity.state.entities import StatePersonAlias, \
    StatePersonExternalId, StatePersonRace, StatePersonEthnicity, StatePerson, \
    StateCourtCase, StateCharge, StateFine, StateIncarcerationIncident, \
    StateIncarcerationPeriod, StateIncarcerationSentence, StateSentenceGroup, \
    StateAgent, StateSupervisionViolation, StateSupervisionViolationResponse, \
    StateSupervisionPeriod, StateSupervisionSentence, \
    StateSupervisionCaseTypeEntry
from recidiviz.persistence.entity_matching import entity_matching
from recidiviz.persistence.entity_matching.state import state_matching_utils
from recidiviz.persistence.entity_matching.state.\
    base_state_matching_delegate import BaseStateMatchingDelegate
from recidiviz.tests.persistence.database.schema.state.schema_test_utils \
    import generate_person, generate_external_id, generate_court_case, \
    generate_charge, generate_fine, generate_incarceration_sentence, \
    generate_sentence_group, generate_race, generate_alias, \
    generate_ethnicity, generate_bond, generate_assessment, generate_agent, \
    generate_incarceration_incident, generate_parole_decision, \
    generate_incarceration_period, generate_supervision_violation_response, \
    generate_supervision_violation, generate_supervision_period, \
    generate_supervision_sentence, \
    generate_supervision_violation_response_decision_entry, \
    generate_supervision_violation_type_entry, \
    generate_supervision_violated_condition_entry, \
    generate_supervision_case_type_entry
from recidiviz.tests.persistence.entity_matching.state.\
    base_state_entity_matcher_test_classes import BaseStateEntityMatcherTest

_EXTERNAL_ID = 'EXTERNAL_ID-1'
_EXTERNAL_ID_2 = 'EXTERNAL_ID-2'
_EXTERNAL_ID_3 = 'EXTERNAL_ID-3'
_EXTERNAL_ID_4 = 'EXTERNAL_ID-4'
_EXTERNAL_ID_5 = 'EXTERNAL_ID-5'
_EXTERNAL_ID_6 = 'EXTERNAL_ID-6'
_ID = 1
_ID_2 = 2
_ID_3 = 3
_ID_4 = 4
_ID_5 = 5
_ID_6 = 6
_ID_TYPE = 'ID_TYPE'
_ID_TYPE_ANOTHER = 'ID_TYPE_ANOTHER'
_FULL_NAME = 'FULL_NAME'
_FULL_NAME_ANOTHER = 'FULL_NAME_ANOTHER'
_COUNTY_CODE = 'Iredell'
_STATE_CODE = 'NC'
_DATE_1 = datetime.date(year=2019, month=1, day=1)
_DATE_2 = datetime.date(year=2019, month=2, day=1)


class FakeStateMatchingDelegate(BaseStateMatchingDelegate):
    def __init__(self, region_code: str):
        super().__init__(region_code)

    def read_potential_match_db_persons(
            self, session: Session, ingested_persons: List[schema.StatePerson]) -> List[schema.StatePerson]:
        return state_matching_utils.read_persons(session, self.region_code, ingested_persons)


class TestStateEntityMatching(BaseStateEntityMatcherTest):
    """Tests for default state entity matching logic."""

    def setUp(self) -> None:
        super().setUp()
        self.matching_delegate_patcher = patch(
            "recidiviz.persistence.entity_matching.state."
            "state_matching_delegate_factory.StateMatchingDelegateFactory."
            "build", new=self._get_base_delegate)
        self.matching_delegate_patcher.start()
        self.addCleanup(self.matching_delegate_patcher.stop)

    def _get_base_delegate(self, **_kwargs):
        return FakeStateMatchingDelegate(_STATE_CODE)

    def test_match_newPerson(self) -> None:
        # Arrange 1 - Match
        db_person = schema.StatePerson(person_id=_ID, full_name=_FULL_NAME, state_code=_STATE_CODE)
        db_external_id = schema.StatePersonExternalId(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID, id_type=_ID_TYPE)
        db_person.external_ids = [db_external_id]

        self._commit_to_db(db_person)

        external_id_2 = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2, id_type=_ID_TYPE)
        person_2 = StatePerson.new_with_defaults(
            full_name=_FULL_NAME, external_ids=[external_id_2], state_code=_STATE_CODE)

        expected_db_person = self.to_entity(db_person)
        expected_person_2 = attr.evolve(person_2)

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE,
            [person_2])

        self.assert_people_match_pre_and_post_commit(
            [expected_person_2],
            matched_entities.people,
            session,
            expected_unmatched_db_people=[expected_db_person])
        self.assertEqual(1, matched_entities.total_root_entities)
        self.assert_no_errors(matched_entities)

    def test_match_overwriteAgent(self) -> None:
        # Arrange 1 - Match
        db_agent = generate_agent(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            agent_type=StateAgentType.SUPERVISION_OFFICER.value)
        db_external_id = generate_external_id(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID, id_type=_ID_TYPE)
        db_person = generate_person(person_id=_ID, full_name=_FULL_NAME,
                                    external_ids=[db_external_id],
                                    supervising_officer=db_agent,
                                    state_code=_STATE_CODE)

        self._commit_to_db(db_person)

        external_id = attr.evolve(
            self.to_entity(db_external_id), person_external_id_id=None)
        agent = StateAgent.new_with_defaults(
            external_id=_EXTERNAL_ID_2,
            state_code=_STATE_CODE,
            agent_type=StateAgentType.SUPERVISION_OFFICER)
        person = attr.evolve(
            self.to_entity(db_person),
            person_id=None,
            external_ids=[external_id],
            supervising_officer=agent)

        expected_external_id = attr.evolve(
            external_id, person_external_id_id=_ID)
        expected_agent = attr.evolve(agent)
        expected_person = attr.evolve(
            person, person_id=_ID, external_ids=[expected_external_id],
            supervising_officer=expected_agent)

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE, [person])

        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)
        self.assert_people_match_pre_and_post_commit(
            [expected_person],
            matched_entities.people,
            session)

    def test_match_twoMatchingIngestedPersons(self) -> None:
        # Arrange
        external_id = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID, id_type=_ID_TYPE)
        external_id_2 = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID,
            id_type=_ID_TYPE_ANOTHER)
        external_id_dup = attr.evolve(external_id)
        external_id_3 = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID_2,
            id_type=_ID_TYPE_ANOTHER)
        person = StatePerson.new_with_defaults(
            full_name=_FULL_NAME, external_ids=[external_id, external_id_2], state_code=_STATE_CODE)
        person_2 = StatePerson.new_with_defaults(
            full_name=_FULL_NAME, external_ids=[external_id_dup, external_id_3], state_code=_STATE_CODE)

        expected_external_id_1 = attr.evolve(external_id)
        expected_external_id_2 = attr.evolve(external_id_2)
        expected_external_id_3 = attr.evolve(external_id_3)
        expected_person = attr.evolve(
            person, external_ids=[
                expected_external_id_1,
                expected_external_id_2,
                expected_external_id_3])

        # Act
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE,
            [person, person_2])

        # Assert
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_match_noPlaceholders_simple(self) -> None:
        # Arrange 1 - Match
        db_person = schema.StatePerson(person_id=_ID, full_name=_FULL_NAME, state_code=_STATE_CODE)
        db_external_id = schema.StatePersonExternalId(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID, id_type=_ID_TYPE)
        db_person.external_ids = [db_external_id]

        self._commit_to_db(db_person)

        external_id = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID, id_type=_ID_TYPE)
        external_id_2 = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2, id_type=_ID_TYPE)
        person = StatePerson.new_with_defaults(
            full_name=_FULL_NAME, external_ids=[external_id, external_id_2], state_code=_STATE_CODE)

        expected_person = attr.evolve(
            person,
            person_id=_ID,
            external_ids=[attr.evolve(external_id, person_external_id_id=_ID),
                          external_id_2])

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE,
            [person])

        # Assert 1 - Match
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)

    def test_match_noPlaceholders_success(self) -> None:
        # Arrange 1 - Match
        db_person = schema.StatePerson(person_id=_ID, full_name=_FULL_NAME, state_code=_STATE_CODE)
        db_fine = schema.StateFine(
            person=db_person,
            status=StateFineStatus.EXTERNAL_UNKNOWN.value,
            fine_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            county_code='county_code')
        db_sentence_group = schema.StateSentenceGroup(
            sentence_group_id=_ID,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE, county_code='county_code',
            fines=[db_fine])
        db_external_id = schema.StatePersonExternalId(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID, id_type=_ID_TYPE)

        db_person.sentence_groups = [db_sentence_group]
        db_person.external_ids = [db_external_id]

        db_external_id_another = schema.StatePersonExternalId(
            person_external_id_id=_ID_2, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2, id_type=_ID_TYPE)
        db_person_another = schema.StatePerson(
            person_id=_ID_2, full_name=_FULL_NAME,
            external_ids=[db_external_id_another], state_code=_STATE_CODE)
        self._commit_to_db(db_person, db_person_another)

        new_charge = StateCharge.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            status=ChargeStatus.PRESENT_WITHOUT_INFO)
        fine = StateFine.new_with_defaults(
            state_code=_STATE_CODE,
            status=StateFineStatus.EXTERNAL_UNKNOWN,
            external_id=_EXTERNAL_ID,
            county_code='county_code-updated',
            charges=[new_charge])
        sentence_group = StateSentenceGroup.new_with_defaults(
            status=StateSentenceStatus.EXTERNAL_UNKNOWN,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE, county_code='county_code-updated',
            fines=[fine])
        external_id = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID, id_type=_ID_TYPE)
        person = StatePerson.new_with_defaults(
            full_name=_FULL_NAME, external_ids=[external_id],
            sentence_groups=[sentence_group],
            state_code=_STATE_CODE)

        external_id_another = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2, id_type=_ID_TYPE)
        person_another = StatePerson.new_with_defaults(
            full_name=_FULL_NAME,
            external_ids=[external_id_another],
            state_code=_STATE_CODE)

        expected_person = attr.evolve(
            person, person_id=_ID, external_ids=[], sentence_groups=[])
        expected_charge = attr.evolve(new_charge)
        expected_fine = attr.evolve(
            fine, fine_id=_ID, charges=[expected_charge])
        expected_sentence_group = attr.evolve(
            sentence_group, sentence_group_id=_ID, fines=[expected_fine])
        expected_external_id = attr.evolve(
            external_id, person_external_id_id=_ID)
        expected_person.sentence_groups = [expected_sentence_group]
        expected_person.external_ids = [expected_external_id]

        expected_person_another = attr.evolve(person_another, person_id=_ID_2)
        expected_external_id = attr.evolve(
            external_id_another, person_external_id_id=_ID_2)
        expected_person_another.external_ids = [expected_external_id]

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE,
            [person, person_another])

        # Assert 1 - Match
        self.assert_no_errors(matched_entities)
        self.assertEqual(2, matched_entities.total_root_entities)
        self.assert_people_match_pre_and_post_commit(
            [expected_person, expected_person_another],
            matched_entities.people, session)

    def test_match_oneMatchOneError(self) -> None:
        # Arrange 1 - Match
        db_external_id = generate_external_id(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID, id_type=_ID_TYPE)
        db_person = generate_person(
            person_id=_ID, external_ids=[db_external_id], state_code=_STATE_CODE)

        db_external_id_2 = generate_external_id(
            person_external_id_id=_ID_2, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2, id_type=_ID_TYPE)
        db_sentence_group = generate_sentence_group(
            sentence_group_id=_ID, external_id=_EXTERNAL_ID)
        db_person_2 = generate_person(
            person_id=_ID_2,
            sentence_groups=[db_sentence_group],
            external_ids=[db_external_id_2],
            state_code=_STATE_CODE)

        self._commit_to_db(db_person, db_person_2)

        external_id = attr.evolve(
            self.to_entity(db_external_id), person_external_id_id=None)
        person = attr.evolve(
            self.to_entity(db_person),
            person_id=None, external_ids=[external_id])

        sentence_group = attr.evolve(
            self.to_entity(db_sentence_group), sentence_group_id=None)
        sentence_group_dup = attr.evolve(sentence_group)
        external_id_2 = attr.evolve(
            self.to_entity(db_external_id_2), person_external_id_id=None)
        person_2 = attr.evolve(
            self.to_entity(db_person_2),
            person_id=None, external_ids=[external_id_2],
            sentence_groups=[sentence_group, sentence_group_dup])

        expected_person = attr.evolve(person, person_id=_ID)
        expected_external_id = attr.evolve(
            external_id, person_external_id_id=_ID)
        expected_person.external_ids = [expected_external_id]
        expected_unmatched_db_person = self.to_entity(db_person_2)

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE, [person, person_2])

        # Assert 1 - Match
        self.assertEqual(matched_entities.error_count, 1)
        self.assertEqual(matched_entities.database_cleanup_error_count, 0)
        self.assertEqual(2, matched_entities.total_root_entities)
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session,
            expected_unmatched_db_people=[expected_unmatched_db_person])

    def test_matchPersons_multipleIngestedPeopleMatchOneDbPerson(self) -> None:
        db_external_id = generate_external_id(
            person_external_id_id=_ID, external_id=_EXTERNAL_ID,
            id_type=_ID_TYPE)
        db_external_id_2 = generate_external_id(
            person_external_id_id=_ID_2, external_id=_EXTERNAL_ID,
            id_type=_ID_TYPE_ANOTHER)
        db_person = generate_person(
            person_id=_ID, full_name=_FULL_NAME,
            external_ids=[db_external_id, db_external_id_2],
            state_code=_STATE_CODE)
        self._commit_to_db(db_person)
        external_id = attr.evolve(self.to_entity(db_external_id),
                                  person_external_id_id=None)
        race_1 = StatePersonRace.new_with_defaults(
            state_code=_STATE_CODE, race=Race.WHITE)
        person = attr.evolve(
            self.to_entity(db_person),
            person_id=None,
            races=[race_1],
            external_ids=[external_id])
        race_2 = StatePersonRace.new_with_defaults(
            state_code=_STATE_CODE, race=Race.OTHER)
        external_id_2 = attr.evolve(
            self.to_entity(db_external_id_2),
            person_external_id_id=None)
        person_dup = attr.evolve(person,
                                 races=[race_2],
                                 external_ids=[attr.evolve(external_id_2)])

        expected_external_id = attr.evolve(external_id,
                                           person_external_id_id=_ID)
        expected_external_id_2 = attr.evolve(external_id_2,
                                             person_external_id_id=_ID_2)
        expected_race_1 = attr.evolve(race_1)
        expected_race_2 = attr.evolve(race_2)
        expected_person = attr.evolve(
            person, person_id=_ID,
            races=[expected_race_1, expected_race_2],
            external_ids=[expected_external_id, expected_external_id_2])

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE, [person, person_dup])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)
        self.assertEqual(2, matched_entities.total_root_entities)
        self.assert_no_errors(matched_entities)

    def test_matchPersons_conflictingExternalIds_error(self) -> None:
        # Arrange 1 - Match
        db_person = generate_person(person_id=_ID, state_code=_STATE_CODE)
        db_court_case = generate_court_case(
            court_case_id=_ID,
            person=db_person,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE, county_code='county_code')
        db_charge = generate_charge(
            charge_id=_ID,
            person=db_person,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            description='charge_1',
            status=ChargeStatus.PRESENT_WITHOUT_INFO.value,
            court_case=db_court_case)
        db_fine = generate_fine(
            fine_id=_ID,
            person=db_person,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            county_code='county_code',
            charges=[db_charge])
        db_incarceration_sentence = generate_incarceration_sentence(
            incarceration_sentence_id=_ID,
            person=db_person,
            status=StateSentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE, county_code='county_code')
        db_sentence_group = generate_sentence_group(
            sentence_group_id=_ID,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE, county_code='county_code',
            incarceration_sentences=[db_incarceration_sentence],
            fines=[db_fine])
        db_external_id = generate_external_id(
            person_external_id_id=_ID,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID, id_type=_ID_TYPE)
        db_person.external_ids = [db_external_id]
        db_person.sentence_groups = [db_sentence_group]
        self._commit_to_db(db_person)

        conflicting_court_case = attr.evolve(
            self.to_entity(db_court_case),
            court_case_id=None, external_id=_EXTERNAL_ID_2)
        charge_1 = attr.evolve(
            self.to_entity(db_charge),
            charge_id=None, court_case=conflicting_court_case)
        fine = attr.evolve(
            self.to_entity(db_fine), fine_id=None, charges=[charge_1])
        sentence_group = attr.evolve(
            self.to_entity(db_sentence_group), sentence_group_id=None,
            fines=[fine])
        external_id = attr.evolve(
            self.to_entity(db_external_id), person_external_id_id=None)
        person = attr.evolve(
            self.to_entity(db_person), person_id=None,
            external_ids=[external_id],
            sentence_groups=[sentence_group])
        expected_unmatched_person = attr.evolve(self.to_entity(db_person))

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [], matched_entities.people, session,
            expected_unmatched_db_people=[expected_unmatched_person])
        self.assertEqual(1, matched_entities.total_root_entities)
        self.assertEqual(1, matched_entities.error_count)
        self.assertEqual(0, matched_entities.database_cleanup_error_count)

    # TODO(#3194): Uncomment asserts once the bug is fixed.
    def test_matchPersons_replaceSingularChildFromMatchedParent(self) -> None:
        """Tests that if we have a singular placeholder child (ex StateCharge.court_case) in the DB, we properly update
        that child entity to no longer be a placeholder in the case that we have match an ingested parent with a
        non-placeholder child.
        """
        # When our DB is in a strange state and has an entity has a singluar placeholder child, we throw an error
        # when that child entity is updated. This test has logic that needs to be uncommented once this bug is resolved.
        # Arrange 1 - Match
        db_person = generate_person(person_id=_ID, state_code=_STATE_CODE)
        db_placeholder_court_case = generate_court_case(court_case_id=_ID, person=db_person, state_code=_STATE_CODE)
        db_charge = generate_charge(
            charge_id=_ID,
            person=db_person,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            status=ChargeStatus.PRESENT_WITHOUT_INFO.value,
            court_case=db_placeholder_court_case)
        db_supervision_sentence = generate_supervision_sentence(
            supervision_sentence_id=_ID,
            person=db_person,
            status=StateSentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID,
            charges=[db_charge],
            state_code=_STATE_CODE)
        db_sentence_group = generate_sentence_group(
            sentence_group_id=_ID,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            supervision_sentences=[db_supervision_sentence])
        db_external_id = generate_external_id(
            person_external_id_id=_ID, state_code=_STATE_CODE, external_id=_EXTERNAL_ID, id_type=_ID_TYPE)
        db_person.sentence_groups = [db_sentence_group]
        db_person.external_ids = [db_external_id]
        self._commit_to_db(db_person)

        # New court case should overwrite the existing placeholder.
        court_case = attr.evolve(
            self.to_entity(db_placeholder_court_case), court_case_id=None, external_id=_EXTERNAL_ID)
        charge = attr.evolve(self.to_entity(db_charge), charge_id=None, external_id=_EXTERNAL_ID, court_case=court_case)
        sentence = attr.evolve(self.to_entity(db_supervision_sentence), supervision_sentence_id=None, charges=[charge])
        sentence_group = attr.evolve(
            self.to_entity(db_sentence_group), sentence_group_id=None, supervision_sentences=[sentence])
        external_id = attr.evolve(self.to_entity(db_external_id), person_external_id_id=None)
        person = attr.evolve(
            self.to_entity(db_person), person_id=None, external_ids=[external_id], sentence_groups=[sentence_group])

        expected_updated_court_case = attr.evolve(court_case, court_case_id=db_placeholder_court_case.court_case_id)
        expected_charge = attr.evolve(self.to_entity(db_charge), court_case=expected_updated_court_case)
        expected_supervision_sentence = attr.evolve(
            self.to_entity(db_supervision_sentence), charges=[expected_charge])
        expected_sentence_group = attr.evolve(
            self.to_entity(db_sentence_group), supervision_sentences=[expected_supervision_sentence])
        expected_external_id = attr.evolve(self.to_entity(db_external_id))
        _expected_person = attr.evolve(
            self.to_entity(db_person), external_ids=[expected_external_id], sentence_groups=[expected_sentence_group])

        # Act 1 - Match
        session = self._session()
        _matched_entities = entity_matching.match(session, _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        # TODO(#3194): Uncomment once resolved
        # self.assert_people_match_pre_and_post_commit([expected_person], matched_entities.people, session)
        # self.assertEqual(1, matched_entities.total_root_entities)
        # self.assertEqual(0, matched_entities.error_count)
        # self.assertEqual(0, matched_entities.database_cleanup_error_count)

    def test_matchPersons_partialUpdateBeforeError(self) -> None:
        """Tests that if a DB person tree is partially updated before an error occurs, that we properly set
        backedges on all entities of the person tree, even though an error occurred. This is important when the partial
        merge added a new entity to the DB tree.
        """
        # Arrange 1 - Match
        db_person = generate_person(person_id=_ID, state_code=_STATE_CODE)
        db_placeholder_charge = generate_charge(
            person=db_person,
            state_code=_STATE_CODE,
            status=ChargeStatus.PRESENT_WITHOUT_INFO.value)
        db_supervision_sentence = generate_supervision_sentence(
            person=db_person,
            status=StateSentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID,
            charges=[db_placeholder_charge],
            state_code=_STATE_CODE)

        db_court_case_2 = generate_court_case(
            external_id=_EXTERNAL_ID_2,
            person=db_person,
            state_code=_STATE_CODE)
        db_charge_2 = generate_charge(
            external_id=_EXTERNAL_ID_2,
            person=db_person,
            state_code=_STATE_CODE,
            status=ChargeStatus.PRESENT_WITHOUT_INFO.value,
            court_case=db_court_case_2)
        db_supervision_sentence_2 = generate_supervision_sentence(
            person=db_person,
            status=StateSentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID_2,
            state_code=_STATE_CODE,
            charges=[db_charge_2])
        db_sentence_group = generate_sentence_group(
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            supervision_sentences=[db_supervision_sentence, db_supervision_sentence_2])
        db_external_id = generate_external_id(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            id_type=_ID_TYPE)
        db_person.sentence_groups = [db_sentence_group]
        db_person.external_ids = [db_external_id]
        self._commit_to_db(db_person)

        charge_1 = attr.evolve(
            self.to_entity(db_placeholder_charge), charge_id=None, external_id=_EXTERNAL_ID)
        sentence_1 = attr.evolve(
            self.to_entity(db_supervision_sentence), supervision_sentence_id=None, charges=[charge_1])

        # court case conflicts with existing court case in DB, throws error.
        conflicting_court_case = attr.evolve(self.to_entity(db_court_case_2), external_id=_EXTERNAL_ID_3)
        charge_2 = StateCharge.new_with_defaults(
            external_id=_EXTERNAL_ID_2,
            state_code=_STATE_CODE,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            charge_id=None, court_case=conflicting_court_case)
        sentence_2 = StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE,
            status=SentenceStatus.PRESENT_WITHOUT_INFO,
            external_id=_EXTERNAL_ID_2,
            charges=[charge_2])
        sentence_group = attr.evolve(
            self.to_entity(db_sentence_group), sentence_group_id=None, supervision_sentences=[sentence_1, sentence_2])
        external_id = attr.evolve(self.to_entity(db_external_id), person_external_id_id=None)
        person = attr.evolve(
            self.to_entity(db_person), person_id=None, external_ids=[external_id], sentence_groups=[sentence_group])

        expected_placeholder_charge = attr.evolve(self.to_entity(db_placeholder_charge))
        expected_charge_1 = attr.evolve(charge_1)
        expected_supervision_sentence = attr.evolve(
            self.to_entity(db_supervision_sentence),
            charges=[expected_placeholder_charge, expected_charge_1])
        expected_unchanged_court_case = attr.evolve(self.to_entity(db_court_case_2))
        expected_charge_2 = attr.evolve(self.to_entity(db_charge_2), court_case=expected_unchanged_court_case)
        expected_supervision_sentence_2 = attr.evolve(
            self.to_entity(db_supervision_sentence_2), charges=[expected_charge_2])
        expected_sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            supervision_sentences=[expected_supervision_sentence, expected_supervision_sentence_2])
        expected_external_id = attr.evolve(self.to_entity(db_external_id))
        expected_person = attr.evolve(
            self.to_entity(db_person), external_ids=[expected_external_id], sentence_groups=[expected_sentence_group])

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(session, _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [], matched_entities.people, session, expected_unmatched_db_people=[expected_person])
        self.assertEqual(1, matched_entities.total_root_entities)
        self.assertEqual(1, matched_entities.error_count)
        self.assertEqual(0, matched_entities.database_cleanup_error_count)

    def test_matchPersons_sentenceGroupRootEntity_IngMatchesMultipleDb(self) -> None:
        # Arrange 1 - Match
        db_sentence_group = generate_sentence_group(
            sentence_group_id=_ID, external_id=_EXTERNAL_ID)
        db_sentence_group_2 = generate_sentence_group(
            sentence_group_id=_ID_2, external_id=_EXTERNAL_ID_2)
        db_sentence_group_3 = generate_sentence_group(
            sentence_group_id=_ID_3, external_id=_EXTERNAL_ID_3)
        db_sentence_group_3_dup = generate_sentence_group(
            sentence_group_id=_ID_4, external_id=_EXTERNAL_ID_3)
        db_external_id = generate_external_id(
            person_external_id_id=_ID, id_type=_ID_TYPE,
            external_id=_EXTERNAL_ID)
        db_person = generate_person(
            person_id=_ID, sentence_groups=[
                db_sentence_group, db_sentence_group_2, db_sentence_group_3,
                db_sentence_group_3_dup],
            external_ids=[db_external_id],
            state_code=_STATE_CODE)
        self._commit_to_db(db_person)

        sentence_group = attr.evolve(
            self.to_entity(db_sentence_group), sentence_group_id=None)
        sentence_group_2 = attr.evolve(
            self.to_entity(db_sentence_group_2), sentence_group_id=None)
        sentence_group_3 = attr.evolve(
            self.to_entity(db_sentence_group_3), sentence_group_id=None)
        person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group, sentence_group_2,
                             sentence_group_3],
            state_code=_STATE_CODE)

        expected_sentence_group = attr.evolve(
            sentence_group, sentence_group_id=_ID)
        expected_sentence_group_2 = attr.evolve(
            sentence_group_2, sentence_group_id=_ID_2)
        expected_sentence_group_3 = attr.evolve(
            sentence_group_3, sentence_group_id=_ID_3)
        expected_sentence_group_4 = attr.evolve(
            self.to_entity(db_sentence_group_3_dup))
        expected_external_id = attr.evolve(self.to_entity(db_external_id))
        expected_person = attr.evolve(
            self.to_entity(db_person), external_ids=[expected_external_id],
            sentence_groups=[
                expected_sentence_group, expected_sentence_group_2,
                expected_sentence_group_3, expected_sentence_group_4])

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)
        self.assertEqual(3, matched_entities.total_root_entities)
        self.assertEqual(1, matched_entities.error_count)
        self.assertEqual(1, matched_entities.database_cleanup_error_count)

    def test_ingestedTreeHasDuplicateEntitiesInParentsAndChildrenEntities(self) -> None:
        """This tests an edge case in the interaction of `merge_multiparent_entities` and
        `_merge_new_parent_child_links`. Here we ingest duplicate supervision sentences with duplicate charge children.
        In entity matching, the charge children get merged by `merge_multiparent_entities`, and the
        supervision sentences get merged as a part of `merge_new_parent_child_links`. This test ensures that in
        this second step, we do NOT accidentally make the charge children placeholder objects.
        """
        # Arrange 1 - Match
        external_id = StatePersonExternalId.new_with_defaults(
            external_id=_EXTERNAL_ID, id_type=_ID_TYPE, state_code=_STATE_CODE)
        charge = StateCharge.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            status=ChargeStatus.DROPPED)
        charge_dup = attr.evolve(charge)
        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[charge])
        supervision_sentence_dup = attr.evolve(supervision_sentence, charges=[charge_dup])
        sentence_group = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            state_code=_STATE_CODE,
            supervision_sentences=[supervision_sentence, supervision_sentence_dup])
        person = StatePerson.new_with_defaults(
            full_name=_FULL_NAME, external_ids=[external_id], sentence_groups=[sentence_group], state_code=_STATE_CODE)

        expected_charge = attr.evolve(charge)
        expected_supervision_sentence = attr.evolve(supervision_sentence, charges=[expected_charge])
        expected_placeholder_supervision_sentence = attr.evolve(supervision_sentence_dup, external_id=None, charges=[])
        expected_external_id = attr.evolve(external_id)
        expected_sentence_group = attr.evolve(
            sentence_group,
            supervision_sentences=[expected_supervision_sentence, expected_placeholder_supervision_sentence])
        expected_person = attr.evolve(
            person, sentence_groups=[expected_sentence_group], external_ids=[expected_external_id])

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(session, _STATE_CODE, [person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit([expected_person], matched_entities.people, session)

    def test_matchPersons_matchesTwoDbPeople_mergeDbPeopleMoveChildren(self) -> None:
        """Tests that our system correctly handles the situation where we have
        2 distinct people in our DB, but we learn the two DB people should be
        merged into 1 person based on a new ingested person. Here the two DB
        people to merge have distinct DB children.
        """
        # Arrange 1 - Match
        db_sentence_group = generate_sentence_group(
            sentence_group_id=_ID, external_id=_EXTERNAL_ID)
        db_external_id = generate_external_id(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID, id_type=_ID_TYPE)
        db_external_id_2 = generate_external_id(
            person_external_id_id=_ID_2, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2, id_type=_ID_TYPE_ANOTHER)
        db_person = generate_person(
            person_id=_ID, full_name=_FULL_NAME,
            external_ids=[db_external_id],
            state_code=_STATE_CODE)
        db_person_2 = generate_person(
            person_id=_ID_2, full_name=_FULL_NAME,
            external_ids=[db_external_id_2],
            sentence_groups=[db_sentence_group],
            state_code=_STATE_CODE)
        self._commit_to_db(db_person, db_person_2)

        race = StatePersonRace.new_with_defaults(
            state_code=_STATE_CODE, race=Race.BLACK)
        alias = StatePersonAlias.new_with_defaults(
            state_code=_STATE_CODE, full_name=_FULL_NAME_ANOTHER)
        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code=_STATE_CODE, ethnicity=Ethnicity.NOT_HISPANIC)
        external_id = attr.evolve(
            self.to_entity(db_external_id), person_external_id_id=None)
        external_id_2 = attr.evolve(
            self.to_entity(db_external_id_2), person_external_id_id=None)
        person = attr.evolve(
            self.to_entity(db_person), person_id=None,
            external_ids=[external_id, external_id_2],
            races=[race], aliases=[alias], ethnicities=[ethnicity])

        expected_race = attr.evolve(race)
        expected_ethnicity = attr.evolve(ethnicity)
        expected_alias = attr.evolve(alias)
        expected_person = attr.evolve(
            self.to_entity(db_person),
            external_ids=[self.to_entity(db_external_id),
                          self.to_entity(db_external_id_2)],
            races=[expected_race], ethnicities=[expected_ethnicity],
            aliases=[expected_alias],
            sentence_groups=[self.to_entity(db_sentence_group)])
        expected_placeholder_person = attr.evolve(
            self.to_entity(db_person_2), full_name=None, sentence_groups=[],
            external_ids=[])

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)
        self.assert_people_match_pre_and_post_commit(
            [expected_person, expected_placeholder_person],
            matched_entities.people,
            session)

    def test_matchPersons_matchesTwoDbPeople_mergeAndMoveChildren(self) -> None:
        """Tests that our system correctly handles the situation where we have
        2 distinct people in our DB, but we learn the two DB people should be
        merged into 1 person based on a new ingested person. Here the two DB
        people to merge have children which match with each other, and therefore
        need to be merged properly themselves.
        """
        # Arrange 1 - Match
        db_person = generate_person(person_id=_ID, full_name=_FULL_NAME, state_code=_STATE_CODE)
        db_supervision_sentence = generate_supervision_sentence(
            person=db_person, supervision_sentence_id=_ID,
            external_id=_EXTERNAL_ID)
        db_sentence_group = generate_sentence_group(
            sentence_group_id=_ID, external_id=_EXTERNAL_ID, is_life=True,
            supervision_sentences=[db_supervision_sentence])
        db_external_id = generate_external_id(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID, id_type=_ID_TYPE)
        db_agent = generate_agent(
            agent_id=_ID, external_id=_EXTERNAL_ID,
            agent_type=StateAgentType.SUPERVISION_OFFICER.value,
            state_code=_STATE_CODE)
        db_person.external_ids = [db_external_id]
        db_person.sentence_groups = [db_sentence_group]
        db_person.supervising_officer = db_agent

        db_person_dup = generate_person(person_id=_ID_2, full_name=_FULL_NAME, state_code=_STATE_CODE)
        db_agent_dup = generate_agent(
            agent_id=_ID_2, external_id=_EXTERNAL_ID,
            agent_type=StateAgentType.SUPERVISION_OFFICER.value,
            state_code=_STATE_CODE)
        db_supervision_period = generate_supervision_period(
            person=db_person_dup, supervision_period_id=_ID_2,
            external_id=_EXTERNAL_ID, start_date=_DATE_1,
            termination_date=_DATE_2)
        db_supervision_sentence_dup = generate_supervision_sentence(
            person=db_person_dup, supervision_sentence_id=_ID_2,
            external_id=_EXTERNAL_ID, max_length_days=10,
            supervision_periods=[db_supervision_period])
        db_sentence_group_dup = generate_sentence_group(
            sentence_group_id=_ID_2,
            external_id=_EXTERNAL_ID, status=StateSentenceStatus.SERVING.value,
            supervision_sentences=[db_supervision_sentence_dup])
        db_external_id_2 = generate_external_id(
            person_external_id_id=_ID_2, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2, id_type=_ID_TYPE_ANOTHER)
        db_person_dup.external_ids = [db_external_id_2]
        db_person_dup.sentence_groups = [db_sentence_group_dup]
        db_person_dup.supervising_officer = db_agent_dup
        self._commit_to_db(db_person, db_person_dup)

        external_id = attr.evolve(
            self.to_entity(db_external_id), person_external_id_id=None)
        external_id_2 = attr.evolve(
            self.to_entity(db_external_id_2), person_external_id_id=None)
        person = attr.evolve(
            self.to_entity(db_person), person_id=None,
            external_ids=[external_id, external_id_2])

        expected_supervision_period = attr.evolve(
            self.to_entity(db_supervision_period))
        expected_supervision_sentence = attr.evolve(
            self.to_entity(db_supervision_sentence),
            max_length_days=10,
            supervision_periods=[expected_supervision_period])
        expected_sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            status=StateSentenceStatus.SERVING, is_life=True,
            supervision_sentences=[expected_supervision_sentence])
        expected_agent = attr.evolve(self.to_entity(db_agent))
        expected_person = attr.evolve(
            self.to_entity(db_person),
            external_ids=[self.to_entity(db_external_id),
                          self.to_entity(db_external_id_2)],
            supervising_officer=expected_agent,
            sentence_groups=[expected_sentence_group])
        expected_placeholder_supervision_sentence = attr.evolve(
            self.to_entity(db_supervision_sentence_dup), external_id=None,
            max_length_days=None, supervision_periods=[])
        expected_placeholder_sentence_group = attr.evolve(
            self.to_entity(db_sentence_group_dup), external_id=None,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO, is_life=None,
            supervision_sentences=[expected_placeholder_supervision_sentence])
        expected_placeholder_agent = attr.evolve(
            self.to_entity(db_agent_dup), external_id=None,
            agent_type=StateAgentType.PRESENT_WITHOUT_INFO)
        expected_placeholder_person = attr.evolve(
            self.to_entity(db_person_dup), full_name=None,
            sentence_groups=[expected_placeholder_sentence_group],
            supervising_officer=expected_placeholder_agent,
            external_ids=[])

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)
        self.assert_people_match_pre_and_post_commit(
            [expected_person, expected_placeholder_person],
            matched_entities.people, session)

    def test_matchPersons_sentenceGroupRootEntity_DbMatchesMultipleIng(self) -> None:
        # Arrange 1 - Match
        db_sentence_group = generate_sentence_group(
            sentence_group_id=_ID, external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE)
        db_sentence_group_2 = generate_sentence_group(
            sentence_group_id=_ID_2, external_id=_EXTERNAL_ID_2,
            state_code=_STATE_CODE)
        db_sentence_group_3 = generate_sentence_group(
            sentence_group_id=_ID_3, external_id=_EXTERNAL_ID_3,
            state_code=_STATE_CODE)
        db_external_id = generate_external_id(
            person_external_id_id=_ID, id_type=_ID_TYPE,
            external_id=_EXTERNAL_ID)
        db_person = generate_person(
            person_id=_ID, sentence_groups=[
                db_sentence_group, db_sentence_group_2, db_sentence_group_3],
            external_ids=[db_external_id],
            state_code=_STATE_CODE)
        self._commit_to_db(db_person)

        sentence_group = attr.evolve(self.to_entity(db_sentence_group),
                                     sentence_group_id=None)
        sentence_group_2 = attr.evolve(self.to_entity(db_sentence_group_2),
                                       sentence_group_id=None)
        sentence_group_3 = attr.evolve(self.to_entity(db_sentence_group_3),
                                       sentence_group_id=None)
        sentence_group_3_dup = attr.evolve(self.to_entity(db_sentence_group_3),
                                           sentence_group_id=None,
                                           county_code=_COUNTY_CODE)
        person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group, sentence_group_2,
                             sentence_group_3, sentence_group_3_dup],
            state_code=_STATE_CODE)

        expected_sentence_group = attr.evolve(
            sentence_group, sentence_group_id=_ID)
        expected_sentence_group_2 = attr.evolve(
            sentence_group_2, sentence_group_id=_ID_2)
        expected_sentence_group_3 = attr.evolve(
            sentence_group_3, sentence_group_id=_ID_3)
        expected_external_id = attr.evolve(self.to_entity(db_external_id))
        expected_person = attr.evolve(
            self.to_entity(db_person), external_ids=[expected_external_id],
            sentence_groups=[
                expected_sentence_group, expected_sentence_group_2,
                expected_sentence_group_3])

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            self._session(), _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)
        self.assertEqual(4, matched_entities.total_root_entities)
        self.assertEqual(1, matched_entities.error_count)
        self.assertEqual(0, matched_entities.database_cleanup_error_count)

    def test_matchPersons_noPlaceholders_newPerson(self) -> None:
        # Arrange 1 - Match
        alias = StatePersonAlias.new_with_defaults(
            state_code=_STATE_CODE, full_name=_FULL_NAME)
        external_id = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE, id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        race = StatePersonRace.new_with_defaults(
            state_code=_STATE_CODE, race=Race.WHITE)
        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code=_STATE_CODE, ethnicity=Ethnicity.NOT_HISPANIC)
        person = StatePerson.new_with_defaults(
            gender=Gender.MALE, aliases=[alias], external_ids=[external_id],
            races=[race], ethnicities=[ethnicity], state_code=_STATE_CODE)

        expected_person = attr.evolve(person)

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_noPlaceholders_updatePersonAttributes(self) -> None:
        # Arrange 1 - Match
        db_race = generate_race(
            person_race_id=_ID, state_code=_STATE_CODE,
            race=Race.WHITE.value)
        db_alias = generate_alias(
            person_alias_id=_ID, state_code=_STATE_CODE,
            full_name=_FULL_NAME)
        db_ethnicity = generate_ethnicity(
            person_ethnicity_id=_ID, state_code=_STATE_CODE,
            ethnicity=Ethnicity.HISPANIC.value)
        db_external_id = generate_external_id(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID, id_type=_ID_TYPE)
        db_person = generate_person(
            person_id=_ID, full_name=_FULL_NAME,
            external_ids=[db_external_id],
            races=[db_race], aliases=[db_alias], ethnicities=[db_ethnicity],
            state_code=_STATE_CODE)
        self._commit_to_db(db_person)

        race = StatePersonRace.new_with_defaults(
            state_code=_STATE_CODE, race=Race.BLACK)
        alias = StatePersonAlias.new_with_defaults(
            state_code=_STATE_CODE, full_name=_FULL_NAME_ANOTHER)
        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code=_STATE_CODE, ethnicity=Ethnicity.NOT_HISPANIC)
        external_id = attr.evolve(
            self.to_entity(db_external_id), person_external_id_id=None)
        person = attr.evolve(
            self.to_entity(db_person), person_id=None,
            external_ids=[external_id],
            races=[race], aliases=[alias], ethnicities=[ethnicity])

        expected_person = attr.evolve(
            self.to_entity(db_person), races=[self.to_entity(db_race), race],
            ethnicities=[self.to_entity(db_ethnicity), ethnicity],
            aliases=[self.to_entity(db_alias), alias])

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_noPlaceholders_partialTreeIngested(self) -> None:
        # Arrange 1 - Match
        db_person = generate_person(person_id=_ID, full_name=_FULL_NAME, state_code=_STATE_CODE)
        db_court_case = generate_court_case(
            person=db_person,
            court_case_id=_ID,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE, county_code='county_code')
        db_bond = generate_bond(
            person=db_person,
            bond_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            status=BondStatus.PRESENT_WITHOUT_INFO.value,
            bond_agent='agent')
        db_charge_1 = generate_charge(
            person=db_person,
            charge_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            description='charge_1',
            status=ChargeStatus.PRESENT_WITHOUT_INFO.value,
            bond=db_bond)
        db_charge_2 = generate_charge(
            person=db_person,
            charge_id=_ID_2, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2,
            description='charge_2',
            status=ChargeStatus.PRESENT_WITHOUT_INFO.value,
            court_case=db_court_case)
        db_fine = generate_fine(
            person=db_person,
            fine_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            county_code='county_code',
            charges=[db_charge_1, db_charge_2])
        db_incarceration_sentence = \
            generate_incarceration_sentence(
                person=db_person,
                incarceration_sentence_id=_ID,
                status=StateSentenceStatus.SERVING.value,
                external_id=_EXTERNAL_ID,
                state_code=_STATE_CODE, county_code='county_code')
        db_sentence_group = generate_sentence_group(
            person=db_person,
            sentence_group_id=_ID,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE, county_code='county_code',
            incarceration_sentences=[db_incarceration_sentence],
            fines=[db_fine])
        db_external_id = generate_external_id(
            person=db_person,
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID, id_type=_ID_TYPE)
        db_person.external_ids = [db_external_id]
        db_person.sentence_groups = [db_sentence_group]

        self._commit_to_db(db_person)

        new_court_case = StateCourtCase.new_with_defaults(
            external_id=_EXTERNAL_ID_2, state_code=_STATE_CODE,
            county_code='county_code')
        bond = attr.evolve(
            self.to_entity(db_bond), bond_id=None, bond_agent='agent-updated')
        charge_1 = attr.evolve(
            self.to_entity(db_charge_1), charge_id=None,
            description='charge_1-updated', bond=bond,
            court_case=new_court_case)
        charge_2 = attr.evolve(
            self.to_entity(db_charge_2), charge_id=None,
            description='charge_2-updated', court_case=None)
        fine = attr.evolve(
            self.to_entity(db_fine), fine_id=None, county_code='county-updated',
            charges=[charge_1, charge_2])
        sentence_group = attr.evolve(
            self.to_entity(db_sentence_group), sentence_group_id=None,
            county_code='county_code-updated', fines=[fine])
        external_id = attr.evolve(
            self.to_entity(db_external_id), person_external_id_id=None,
            id_type=_ID_TYPE)
        person = attr.evolve(
            self.to_entity(db_person), person_id=None,
            external_ids=[external_id],
            sentence_groups=[sentence_group])

        expected_unchanged_court_case = attr.evolve(
            self.to_entity(db_court_case))
        expected_new_court_case = attr.evolve(new_court_case)
        expected_bond = attr.evolve(bond, bond_id=_ID)
        expected_charge1 = attr.evolve(
            charge_1, charge_id=_ID, court_case=expected_new_court_case,
            bond=expected_bond)
        expected_charge2 = attr.evolve(
            charge_2, charge_id=_ID_2, court_case=expected_unchanged_court_case)
        expected_fine = attr.evolve(
            fine, fine_id=_ID, charges=[expected_charge1, expected_charge2])
        expected_unchanged_incarceration_sentence = attr.evolve(
            self.to_entity(db_incarceration_sentence))
        expected_sentence_group = attr.evolve(
            sentence_group, sentence_group_id=_ID, fines=[expected_fine],
            incarceration_sentences=[expected_unchanged_incarceration_sentence])
        expected_external_id = attr.evolve(
            external_id, person_external_id_id=_ID)
        expected_person = attr.evolve(
            person, person_id=_ID, external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group])

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_noPlaceholders_completeTreeUpdate(self) -> None:
        # Arrange 1 - Match
        db_person = generate_person(full_name=_FULL_NAME, state_code=_STATE_CODE)
        db_bond = generate_bond(
            person=db_person,
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID,
            status=BondStatus.PRESENT_WITHOUT_INFO.value, bond_agent='agent')
        db_court_case = generate_court_case(
            person=db_person,
            external_id=_EXTERNAL_ID, state_code=_STATE_CODE,
            county_code='county_code')
        db_charge_1 = generate_charge(
            person=db_person,
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID,
            description='charge_1',
            status=ChargeStatus.PRESENT_WITHOUT_INFO.value,
            bond=db_bond, court_case=db_court_case)
        db_charge_2 = generate_charge(
            person=db_person,
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID_2,
            description='charge_2',
            status=ChargeStatus.PRESENT_WITHOUT_INFO.value,
            bond=db_bond, court_case=db_court_case)
        db_charge_3 = generate_charge(
            person=db_person,
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID_3,
            description='charge_3',
            status=ChargeStatus.PRESENT_WITHOUT_INFO.value)
        db_fine = generate_fine(
            person=db_person,
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID,
            county_code='county_code', charges=[db_charge_1, db_charge_2])
        db_assessment = generate_assessment(
            person=db_person,
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID,
            assessment_metadata='metadata')
        db_assessment_2 = generate_assessment(
            person=db_person,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2, assessment_metadata='metadata_2')
        db_agent = generate_agent(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID,
            full_name='full_name')
        db_agent_2 = generate_agent(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID_2,
            full_name='full_name_2')
        db_agent_po = generate_agent(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID_5,
            full_name='full_name_po')
        db_agent_term = generate_agent(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID_6,
            full_name='full_name_term')
        db_incarceration_incident = \
            generate_incarceration_incident(
                person=db_person,
                state_code=_STATE_CODE,
                external_id=_EXTERNAL_ID, incident_details='details',
                responding_officer=db_agent)
        db_parole_decision = generate_parole_decision(
            person=db_person,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            decision_outcome=StateParoleDecisionOutcome.EXTERNAL_UNKNOWN.value,
            decision_agents=[db_agent_2])
        db_parole_decision_2 = generate_parole_decision(
            person=db_person,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2,
            decision_outcome=StateParoleDecisionOutcome.EXTERNAL_UNKNOWN.value,
            decision_agents=[db_agent_2])
        db_incarceration_period = generate_incarceration_period(
            person=db_person,
            external_id=_EXTERNAL_ID,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY.value,
            state_code=_STATE_CODE, facility='facility',
            incarceration_incidents=[db_incarceration_incident],
            parole_decisions=[db_parole_decision, db_parole_decision_2],
            assessments=[db_assessment])
        db_incarceration_sentence = \
            generate_incarceration_sentence(
                person=db_person,
                status=StateSentenceStatus.SERVING.value,
                external_id=_EXTERNAL_ID, state_code=_STATE_CODE,
                county_code='county_code', charges=[db_charge_2, db_charge_3],
                incarceration_periods=[db_incarceration_period])
        db_supervision_violation_response_decision = \
            generate_supervision_violation_response_decision_entry(
                person=db_person,
                decision=StateSupervisionViolationResponseDecision.
                REVOCATION.value,
                revocation_type=
                StateSupervisionViolationResponseRevocationType.
                TREATMENT_IN_PRISON.value
            )
        db_supervision_violation_response = \
            generate_supervision_violation_response(
                person=db_person,
                state_code=_STATE_CODE,
                external_id=_EXTERNAL_ID,
                decision=
                StateSupervisionViolationResponseDecision.CONTINUANCE.value,
                supervision_violation_response_decisions=[
                    db_supervision_violation_response_decision],
                decision_agents=[db_agent_term])
        db_supervision_violation_type = \
            generate_supervision_violation_type_entry(
                person=db_person,
                violation_type=StateSupervisionViolationType.ABSCONDED.value,
            )
        db_supervision_violated_condition = \
            generate_supervision_violated_condition_entry(
                person=db_person,
                condition='COND'
            )
        db_supervision_violation = generate_supervision_violation(
            person=db_person,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID, is_violent=True,
            supervision_violation_types=[db_supervision_violation_type],
            supervision_violated_conditions=[
                db_supervision_violated_condition],
            supervision_violation_responses=[db_supervision_violation_response])
        db_case_type_dv = generate_supervision_case_type_entry(
            person=db_person,
            state_code=_STATE_CODE,
            case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE.value,
            case_type_raw_text='DV')
        db_supervision_period = generate_supervision_period(
            person=db_person,
            external_id=_EXTERNAL_ID,
            status=StateSupervisionPeriodStatus.EXTERNAL_UNKNOWN.value,
            state_code=_STATE_CODE, county_code='county_code',
            assessments=[db_assessment_2],
            supervision_violation_entries=[db_supervision_violation],
            case_type_entries=[db_case_type_dv],
            supervising_officer=db_agent_po)
        db_supervision_sentence = generate_supervision_sentence(
            person=db_person,
            status=SentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            min_length_days=0,
            supervision_periods=[db_supervision_period])
        db_sentence_group = generate_sentence_group(
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID, state_code=_STATE_CODE,
            county_code='county_code',
            supervision_sentences=[db_supervision_sentence],
            incarceration_sentences=[db_incarceration_sentence],
            fines=[db_fine])
        db_external_id = generate_external_id(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID)
        db_person.external_ids = [db_external_id]
        db_person.sentence_groups = [db_sentence_group]

        self._commit_to_db(db_person)

        bond = attr.evolve(
            self.to_entity(db_bond), bond_id=None, bond_agent='agent-updated')
        court_case = attr.evolve(
            self.to_entity(db_court_case), court_case_id=None,
            county_code='county_code-updated')
        charge_1 = attr.evolve(
            self.to_entity(db_charge_1), charge_id=None,
            description='charge_1-updated', bond=bond, court_case=court_case)
        charge_2 = attr.evolve(
            self.to_entity(db_charge_2), charge_id=None,
            description='charge_2-updated', bond=bond, court_case=court_case)
        charge_3 = attr.evolve(
            self.to_entity(db_charge_3), charge_id=None,
            description='charge_3-updated')
        fine = attr.evolve(
            self.to_entity(db_fine), fine_id=None,
            county_code='county_code-updated', charges=[charge_1, charge_2])
        assessment = attr.evolve(
            self.to_entity(db_assessment), assessment_id=None,
            assessment_metadata='metadata_updated')
        assessment_2 = attr.evolve(
            self.to_entity(db_assessment_2), assessment_id=None,
            assessment_metadata='metadata_2-updated')
        agent = attr.evolve(self.to_entity(db_agent), agent_id=None,
                            full_name='full_name-updated')
        agent_2 = attr.evolve(self.to_entity(db_agent_2), agent_id=None,
                              full_name='full_name_2-updated')
        agent_po = attr.evolve(self.to_entity(db_agent_po), agent_id=None,
                               full_name='full_name_po-updated')
        agent_term = attr.evolve(
            self.to_entity(db_agent_term), agent_id=None,
            full_name='full_name_term-updated')
        incarceration_incident = attr.evolve(
            self.to_entity(db_incarceration_incident),
            incarceration_incident_id=None,
            incident_details='details-updated', responding_officer=agent)
        parole_decision = attr.evolve(
            self.to_entity(db_parole_decision), parole_decision_id=None,
            decision_outcome=StateParoleDecisionOutcome.PAROLE_GRANTED,
            decision_agents=[agent_2])
        parole_decision_2 = attr.evolve(
            self.to_entity(db_parole_decision_2), parole_decision_id=None,
            decision_outcome=StateParoleDecisionOutcome.PAROLE_GRANTED,
            decision_agents=[agent_2])
        incarceration_period = attr.evolve(
            self.to_entity(db_incarceration_period),
            incarceration_period_id=None,
            facility='facility-updated',
            incarceration_incidents=[incarceration_incident],
            parole_decisions=[parole_decision, parole_decision_2],
            assessments=[assessment])
        incarceration_sentence = attr.evolve(
            self.to_entity(db_incarceration_sentence),
            incarceration_sentence_id=None,
            county_code='county_code-updated', charges=[charge_2, charge_3],
            incarceration_periods=[incarceration_period])
        supervision_violation_response_decision = attr.evolve(
            self.to_entity(db_supervision_violation_response_decision),
            supervision_violation_response_decision_entry_id=None)
        supervision_violation_response = attr.evolve(
            self.to_entity(db_supervision_violation_response),
            supervision_violation_response_id=None,
            decision=StateSupervisionViolationResponseDecision.EXTENSION,
            decision_agents=[agent_term],
            supervision_violation_response_decisions=[
                supervision_violation_response_decision])
        supervision_violation_type = attr.evolve(
            self.to_entity(db_supervision_violation_type),
            supervision_violation_type_entry_id=None)
        supervision_violated_condition = attr.evolve(
            self.to_entity(db_supervision_violated_condition),
            supervision_violated_condition_entry_id=None)
        supervision_violation = attr.evolve(
            self.to_entity(db_supervision_violation),
            supervision_violation_id=None, is_violent=False,
            supervision_violation_responses=[supervision_violation_response],
            supervision_violated_conditions=[supervision_violated_condition],
            supervision_violation_types=[supervision_violation_type])
        case_type_dv = attr.evolve(
            self.to_entity(db_case_type_dv),
            supervision_case_type_entry_id=None)
        case_type_so = StateSupervisionCaseTypeEntry.new_with_defaults(
            state_code=_STATE_CODE,
            case_type=StateSupervisionCaseType.SEX_OFFENSE,
            case_type_raw_text='SO')
        supervision_period = attr.evolve(
            self.to_entity(db_supervision_period),
            supervision_period_id=None,
            county_code='county_code-updated',
            assessments=[assessment_2],
            supervision_violation_entries=[supervision_violation],
            case_type_entries=[case_type_dv, case_type_so],
            supervising_officer=agent_po)
        supervision_sentence = attr.evolve(
            self.to_entity(db_supervision_sentence),
            supervision_sentence_id=None,
            min_length_days=1, supervision_periods=[supervision_period])
        sentence_group = attr.evolve(
            self.to_entity(db_sentence_group), sentence_group_id=None,
            county_code='county_code-updated',
            supervision_sentences=[supervision_sentence],
            incarceration_sentences=[incarceration_sentence], fines=[fine])
        external_id = attr.evolve(self.to_entity(db_external_id),
                                  person_external_id_id=None)
        person = attr.evolve(
            self.to_entity(db_person), person_id=None,
            external_ids=[external_id], sentence_groups=[sentence_group],
            assessments=[])

        expected_bond = attr.evolve(bond, bond_id=_ID)
        expected_court_case = attr.evolve(court_case, court_case_id=_ID)
        expected_charge_1 = attr.evolve(
            charge_1, charge_id=_ID, bond=expected_bond,
            court_case=expected_court_case)
        expected_charge_2 = attr.evolve(
            charge_2, charge_id=_ID_2, bond=expected_bond,
            court_case=expected_court_case)
        expected_charge_3 = attr.evolve(charge_3, charge_id=_ID_3)
        expected_fine = attr.evolve(
            fine, fine_id=_ID, charges=[expected_charge_1, expected_charge_2])
        expected_assessment = attr.evolve(assessment, assessment_id=_ID)
        expected_assessment_2 = attr.evolve(assessment_2, assessment_id=_ID_2)
        expected_agent = attr.evolve(agent, agent_id=_ID)
        expected_agent_2 = attr.evolve(agent_2, agent_id=_ID_2)
        expected_agent_po = attr.evolve(agent_po, agent_id=_ID_5)
        expected_agent_term = attr.evolve(agent_term, agent_id=_ID_6)
        expected_incarceration_incident = attr.evolve(
            incarceration_incident, incarceration_incident_id=_ID,
            responding_officer=expected_agent)
        expected_parole_decision = attr.evolve(
            parole_decision, parole_decision_id=_ID,
            decision_agents=[expected_agent_2])
        expected_parole_decision_2 = attr.evolve(
            parole_decision_2, parole_decision_id=_ID_2,
            decision_agents=[expected_agent_2])
        expected_incarceration_period = attr.evolve(
            incarceration_period, incarceration_period_id=_ID,
            incarceration_incidents=[expected_incarceration_incident],
            parole_decisions=[expected_parole_decision,
                              expected_parole_decision_2],
            assessments=[expected_assessment])
        expected_incarceration_sentence = attr.evolve(
            incarceration_sentence, incarceration_sentence_id=_ID,
            charges=[expected_charge_2, expected_charge_3],
            incarceration_periods=[expected_incarceration_period])
        expected_supervision_violation_response_decision = attr.evolve(
            supervision_violation_response_decision,
            supervision_violation_response_decision_entry_id=_ID)
        expected_supervision_violation_response = attr.evolve(
            supervision_violation_response,
            supervision_violation_response_id=_ID,
            decision_agents=[expected_agent_term],
            supervision_violation_response_decisions=[
                expected_supervision_violation_response_decision])
        expected_supervision_violation_type = attr.evolve(
            supervision_violation_type,
            supervision_violation_type_entry_id=_ID)
        expected_supervision_violated_condition = attr.evolve(
            supervision_violated_condition,
            supervision_violated_condition_entry_id=_ID)
        expected_supervision_violation = attr.evolve(
            supervision_violation, supervision_violation_id=_ID,
            supervision_violation_responses=[
                expected_supervision_violation_response],
            supervision_violated_conditions=[
                expected_supervision_violated_condition],
            supervision_violation_types=[
                expected_supervision_violation_type
            ])
        expected_case_type_dv = attr.evolve(
            case_type_dv,
            supervision_case_type_entry_id=_ID)
        expected_case_type_so = attr.evolve(case_type_so)
        expected_supervision_period = attr.evolve(
            supervision_period, supervision_period_id=_ID,
            assessments=[expected_assessment_2],
            supervision_violation_entries=[expected_supervision_violation],
            case_type_entries=[expected_case_type_dv, expected_case_type_so],
            supervising_officer=expected_agent_po)
        expected_supervision_sentence = attr.evolve(
            supervision_sentence, supervision_sentence_id=_ID,
            supervision_periods=[expected_supervision_period])
        expected_sentence_group = attr.evolve(
            sentence_group, sentence_group_id=_ID,
            supervision_sentences=[expected_supervision_sentence],
            incarceration_sentences=[expected_incarceration_sentence],
            fines=[expected_fine])
        expected_external_id = attr.evolve(
            external_id, person_external_id_id=_ID)
        expected_person = attr.evolve(
            person, person_id=_ID, external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group],
            assessments=[expected_assessment, expected_assessment_2])

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        self.assert_no_errors(matched_entities)
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_ingestedPersonWithNewExternalId(self) -> None:
        db_external_id = generate_external_id(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID, id_type=_ID_TYPE)
        db_person = generate_person(
            person_id=_ID, full_name=_FULL_NAME,
            external_ids=[db_external_id],
            state_code=_STATE_CODE)

        self._commit_to_db(db_person)

        external_id = attr.evolve(self.to_entity(db_external_id),
                                  person_external_id_id=None)
        external_id_another = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID_2,
            id_type=_ID_TYPE_ANOTHER)
        person = StatePerson.new_with_defaults(
            external_ids=[external_id, external_id_another], state_code=_STATE_CODE)

        expected_external_id = attr.evolve(
            external_id, person_external_id_id=_ID)
        expected_external_id_another = attr.evolve(external_id_another)
        expected_person = attr.evolve(self.to_entity(db_person), external_ids=[
            expected_external_id, expected_external_id_another])

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        self.assert_no_errors(matched_entities)
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_holeInDbGraph(self) -> None:
        db_person = generate_person(person_id=_ID, full_name=_FULL_NAME, state_code=_STATE_CODE)
        db_supervision_sentence = generate_supervision_sentence(
            person=db_person,
            supervision_sentence_id=_ID, status=SentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            min_length_days=0)
        db_supervision_sentence_unchanged = \
            generate_supervision_sentence(
                person=db_person,
                supervision_sentence_id=_ID_2,
                status=SentenceStatus.SERVING.value,
                external_id=_EXTERNAL_ID_2,
                state_code=_STATE_CODE,
                min_length_days=0)
        db_placeholder_sentence_group = generate_sentence_group(
            sentence_group_id=_ID,
            supervision_sentences=[db_supervision_sentence,
                                   db_supervision_sentence_unchanged])

        db_external_id = generate_external_id(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID)

        db_person.external_ids = [db_external_id]
        db_person.sentence_groups = [db_placeholder_sentence_group]

        self._commit_to_db(db_person)

        supervision_sentence_updated = attr.evolve(
            self.to_entity(db_supervision_sentence),
            supervision_sentence_id=None,
            min_length_days=1)
        new_sentence_group = attr.evolve(
            self.to_entity(db_placeholder_sentence_group),
            sentence_group_id=None,
            external_id=_EXTERNAL_ID,
            supervision_sentences=[
                supervision_sentence_updated])
        external_id = attr.evolve(self.to_entity(db_external_id),
                                  person_external_id_id=None)
        person = attr.evolve(
            self.to_entity(db_person), person_id=None,
            external_ids=[external_id], sentence_groups=[new_sentence_group])

        expected_supervision_sentence = attr.evolve(
            supervision_sentence_updated, supervision_sentence_id=_ID)
        expected_supervision_sentence_unchanged = attr.evolve(
            self.to_entity(db_supervision_sentence_unchanged))
        expected_placeholder_sentence_group = attr.evolve(
            self.to_entity(db_placeholder_sentence_group),
            supervision_sentences=[expected_supervision_sentence_unchanged])
        expected_new_sentence_group = attr.evolve(
            new_sentence_group,
            supervision_sentences=[expected_supervision_sentence])
        expected_external_id = attr.evolve(
            external_id, person_external_id_id=_ID)
        expected_person = attr.evolve(
            person, person_id=_ID, external_ids=[expected_external_id],
            sentence_groups=[expected_new_sentence_group,
                             expected_placeholder_sentence_group])

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_holeInIngestedGraph(self) -> None:
        db_person = generate_person(person_id=_ID, full_name=_FULL_NAME, state_code=_STATE_CODE)
        db_supervision_sentence = generate_supervision_sentence(
            person=db_person,
            supervision_sentence_id=_ID, status=SentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            min_length_days=0)
        db_supervision_sentence_another = \
            generate_supervision_sentence(
                person=db_person,
                supervision_sentence_id=_ID_2,
                status=SentenceStatus.SERVING.value,
                external_id=_EXTERNAL_ID_2,
                state_code=_STATE_CODE,
                min_length_days=0)
        db_sentence_group = generate_sentence_group(
            sentence_group_id=_ID,
            external_id=_EXTERNAL_ID,
            supervision_sentences=[
                db_supervision_sentence, db_supervision_sentence_another])
        db_external_id = generate_external_id(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID)
        db_person.external_ids = [db_external_id]
        db_person.sentence_groups = [db_sentence_group]

        self._commit_to_db(db_person)

        supervision_sentence_updated = attr.evolve(
            self.to_entity(db_supervision_sentence),
            supervision_sentence_id=None, min_length_days=1)
        sentence_group_placeholder = StateSentenceGroup.new_with_defaults(
            supervision_sentences=[supervision_sentence_updated])
        external_id = attr.evolve(
            self.to_entity(db_external_id), person_external_id_id=None)
        person = attr.evolve(
            self.to_entity(db_person), person_id=None,
            external_ids=[external_id],
            sentence_groups=[sentence_group_placeholder])

        expected_supervision_sentence = attr.evolve(
            supervision_sentence_updated, supervision_sentence_id=_ID)
        expected_supervision_sentence_unchanged = attr.evolve(
            self.to_entity(db_supervision_sentence_another))
        expected_sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            supervision_sentences=[expected_supervision_sentence,
                                   expected_supervision_sentence_unchanged])
        expected_external_id = attr.evolve(
            external_id, person_external_id_id=_ID)
        expected_person = attr.evolve(
            person, person_id=_ID, external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group])

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_dbPlaceholderSplits(self) -> None:
        db_person = generate_person(person_id=_ID, full_name=_FULL_NAME, state_code=_STATE_CODE)
        db_supervision_sentence = generate_supervision_sentence(
            person=db_person,
            supervision_sentence_id=_ID, status=SentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            min_length_days=0)
        db_supervision_sentence_another = \
            generate_supervision_sentence(
                person=db_person,
                supervision_sentence_id=_ID_2,
                status=SentenceStatus.SERVING.value,
                external_id=_EXTERNAL_ID_2,
                state_code=_STATE_CODE,
                min_length_days=10)
        db_placeholder_sentence_group = generate_sentence_group(
            sentence_group_id=_ID,
            supervision_sentences=[db_supervision_sentence,
                                   db_supervision_sentence_another])

        db_external_id = generate_external_id(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID)
        db_person.external_ids = [db_external_id]
        db_person.sentence_groups = [db_placeholder_sentence_group]

        self._commit_to_db(db_person)

        supervision_sentence_updated = attr.evolve(
            self.to_entity(db_supervision_sentence),
            supervision_sentence_id=None, min_length_days=1)
        supervision_sentence_another_updated = attr.evolve(
            self.to_entity(db_supervision_sentence_another),
            supervision_sentence_id=None, min_length_days=11)
        sentence_group_new = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            status=SentenceStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[supervision_sentence_updated])
        sentence_group_new_another = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID_2,
            state_code=_STATE_CODE,
            status=SentenceStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[supervision_sentence_another_updated])
        external_id = attr.evolve(self.to_entity(db_external_id),
                                  person_external_id_id=None)
        person = attr.evolve(
            self.to_entity(db_person), person_id=None,
            external_ids=[external_id],
            sentence_groups=[sentence_group_new, sentence_group_new_another])

        expected_supervision_sentence = attr.evolve(
            supervision_sentence_updated, supervision_sentence_id=_ID)
        expected_supervision_sentence_another = attr.evolve(
            supervision_sentence_another_updated, supervision_sentence_id=_ID_2)
        expected_sentence_group = attr.evolve(
            sentence_group_new,
            supervision_sentences=[expected_supervision_sentence])
        expected_sentence_group_another = attr.evolve(
            sentence_group_new_another,
            supervision_sentences=[expected_supervision_sentence_another])
        expected_placeholder_sentence_group = attr.evolve(
            self.to_entity(db_placeholder_sentence_group),
            supervision_sentences=[])
        expected_external_id = attr.evolve(
            external_id, person_external_id_id=_ID)
        expected_person = attr.evolve(
            person, person_id=_ID, external_ids=[expected_external_id],
            sentence_groups=[
                expected_sentence_group, expected_sentence_group_another,
                expected_placeholder_sentence_group])

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_dbMatchesMultipleIngestedPlaceholders_success(
            self):
        db_person = generate_person(person_id=_ID, full_name=_FULL_NAME, state_code=_STATE_CODE)
        db_supervision_sentence = generate_supervision_sentence(
            person=db_person,
            supervision_sentence_id=_ID, status=SentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            min_length_days=0)
        db_supervision_sentence_another = \
            generate_supervision_sentence(
                person=db_person,
                supervision_sentence_id=_ID_2,
                status=SentenceStatus.SERVING.value,
                external_id=_EXTERNAL_ID_2,
                state_code=_STATE_CODE,
                min_length_days=10)
        db_sentence_group = generate_sentence_group(
            external_id=_EXTERNAL_ID,
            sentence_group_id=_ID,
            supervision_sentences=[db_supervision_sentence,
                                   db_supervision_sentence_another])
        db_external_id = generate_external_id(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID)
        db_person.external_ids = [db_external_id]
        db_person.sentence_groups = [db_sentence_group]

        self._commit_to_db(db_person)

        supervision_sentence_updated = attr.evolve(
            self.to_entity(db_supervision_sentence),
            supervision_sentence_id=None,
            min_length_days=1)
        placeholder_sentence_group = StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE,
            status=SentenceStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[supervision_sentence_updated])

        supervision_sentence_another_updated = attr.evolve(
            self.to_entity(db_supervision_sentence_another),
            supervision_sentence_id=None, min_length_days=11)
        fine = StateFine.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            status=StateFineStatus.PRESENT_WITHOUT_INFO)
        placeholder_sentence_group_another = \
            StateSentenceGroup.new_with_defaults(
                state_code=_STATE_CODE,
                status=SentenceStatus.PRESENT_WITHOUT_INFO,
                supervision_sentences=[supervision_sentence_another_updated],
                fines=[fine])
        placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[placeholder_sentence_group,
                             placeholder_sentence_group_another],
            state_code=_STATE_CODE)

        expected_supervision_sentence = attr.evolve(
            supervision_sentence_updated, supervision_sentence_id=_ID)
        expected_supervision_sentence_another = attr.evolve(
            supervision_sentence_another_updated, supervision_sentence_id=_ID_2)
        expected_sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            supervision_sentences=[expected_supervision_sentence,
                                   expected_supervision_sentence_another])
        expected_external_id = attr.evolve(self.to_entity(db_external_id))
        expected_person = attr.evolve(
            self.to_entity(db_person), external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group])

        expected_fine = attr.evolve(fine)
        expected_placeholder_sentence_group = attr.evolve(
            placeholder_sentence_group_another,
            supervision_sentences=[], fines=[expected_fine])
        expected_placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[expected_placeholder_sentence_group], state_code=_STATE_CODE)

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE, ingested_people=[placeholder_person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person, expected_placeholder_person],
            matched_entities.people, session)
        self.assert_no_errors(matched_entities)
        self.assertEqual(2, matched_entities.total_root_entities)

    def test_matchPersons_multipleIngestedPersonsMatchToPlaceholderDb(self) -> None:
        db_person = generate_person(person_id=_ID, full_name=_FULL_NAME, state_code=_STATE_CODE)
        db_supervision_sentence = generate_supervision_sentence(
            person=db_person,
            supervision_sentence_id=_ID, status=SentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            min_length_days=0)
        db_supervision_sentence_another = \
            generate_supervision_sentence(
                person=db_person,
                supervision_sentence_id=_ID_2,
                status=SentenceStatus.SERVING.value,
                external_id=_EXTERNAL_ID_2,
                state_code=_STATE_CODE,
                min_length_days=10)
        db_sentence_group = generate_sentence_group(
            external_id=_EXTERNAL_ID,
            sentence_group_id=_ID,
            supervision_sentences=[db_supervision_sentence])
        db_sentence_group_another = generate_sentence_group(
            sentence_group_id=_ID_2,
            external_id=_EXTERNAL_ID_2,
            supervision_sentences=[db_supervision_sentence_another])
        db_external_id = generate_external_id(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID)
        db_person.external_ids = [db_external_id]
        db_person.sentence_groups = [db_sentence_group,
                                     db_sentence_group_another]

        self._commit_to_db(db_person)

        supervision_sentence_updated = attr.evolve(
            self.to_entity(db_supervision_sentence),
            supervision_sentence_id=None, min_length_days=1)
        placeholder_sentence_group = StateSentenceGroup.new_with_defaults(
            supervision_sentences=[supervision_sentence_updated])
        placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[placeholder_sentence_group], state_code=_STATE_CODE)

        supervision_sentence_another_updated = attr.evolve(
            self.to_entity(db_supervision_sentence_another),
            supervision_sentence_id=None, min_length_days=11)
        placeholder_sentence_group_another = \
            StateSentenceGroup.new_with_defaults(
                supervision_sentences=[supervision_sentence_another_updated])
        placeholder_person_another = StatePerson.new_with_defaults(
            sentence_groups=[placeholder_sentence_group_another], state_code=_STATE_CODE)

        expected_supervision_sentence = attr.evolve(
            supervision_sentence_updated, supervision_sentence_id=_ID)
        expected_supervision_sentence_another = attr.evolve(
            supervision_sentence_another_updated, supervision_sentence_id=_ID_2)
        expected_sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            supervision_sentences=[expected_supervision_sentence])
        expected_sentence_group_another = attr.evolve(
            self.to_entity(db_sentence_group_another),
            supervision_sentences=[expected_supervision_sentence_another])
        expected_external_id = attr.evolve(self.to_entity(db_external_id))
        expected_person = attr.evolve(
            self.to_entity(db_person), external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group,
                             expected_sentence_group_another])

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE,
            ingested_people=[placeholder_person, placeholder_person_another])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)
        self.assert_no_errors(matched_entities)
        self.assertEqual(2, matched_entities.total_root_entities)

    def test_matchPersons_ingestedPlaceholderSplits(self) -> None:
        db_person = generate_person(person_id=_ID, full_name=_FULL_NAME, state_code=_STATE_CODE)
        db_supervision_sentence = generate_supervision_sentence(
            person=db_person,
            supervision_sentence_id=_ID, status=SentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            min_length_days=0)
        db_supervision_sentence_another = \
            generate_supervision_sentence(
                person=db_person,
                supervision_sentence_id=_ID_2,
                status=SentenceStatus.SERVING.value,
                external_id=_EXTERNAL_ID_2,
                state_code=_STATE_CODE,
                min_length_days=10)
        db_sentence_group = generate_sentence_group(
            external_id=_EXTERNAL_ID,
            sentence_group_id=_ID,
            supervision_sentences=[db_supervision_sentence])
        db_sentence_group_another = generate_sentence_group(
            sentence_group_id=_ID_2,
            external_id=_EXTERNAL_ID_2,
            supervision_sentences=[db_supervision_sentence_another])
        db_external_id = generate_external_id(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID)
        db_person.external_ids = [db_external_id]
        db_person.sentence_groups = [db_sentence_group,
                                     db_sentence_group_another]

        self._commit_to_db(db_person)

        supervision_sentence_updated = attr.evolve(
            self.to_entity(db_supervision_sentence),
            supervision_sentence_id=None, min_length_days=1)
        supervision_sentence_another_updated = attr.evolve(
            self.to_entity(db_supervision_sentence_another),
            supervision_sentence_id=None, min_length_days=11)
        placeholder_sentence_group = StateSentenceGroup.new_with_defaults(
            supervision_sentences=[
                supervision_sentence_updated,
                supervision_sentence_another_updated])
        external_id = attr.evolve(self.to_entity(db_external_id),
                                  person_external_id_id=None)
        person = attr.evolve(
            self.to_entity(db_person), person_id=None,
            external_ids=[external_id],
            sentence_groups=[placeholder_sentence_group])

        expected_supervision_sentence = attr.evolve(
            supervision_sentence_updated, supervision_sentence_id=_ID)
        expected_supervision_sentence_another = attr.evolve(
            supervision_sentence_another_updated, supervision_sentence_id=_ID_2)
        expected_sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            supervision_sentences=[expected_supervision_sentence])
        expected_sentence_group_another = attr.evolve(
            self.to_entity(db_sentence_group_another),
            supervision_sentences=[expected_supervision_sentence_another])
        expected_external_id = attr.evolve(external_id,
                                           person_external_id_id=_ID)
        expected_person = attr.evolve(
            person, person_id=_ID, external_ids=[expected_external_id],
            sentence_groups=[
                expected_sentence_group, expected_sentence_group_another])

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_multipleHolesInIngestedGraph(self) -> None:

        # Arrange 1 - Match
        db_person = generate_person(person_id=_ID, full_name=_FULL_NAME, state_code=_STATE_CODE)
        db_supervision_sentence = generate_supervision_sentence(
            person=db_person,
            supervision_sentence_id=_ID, status=SentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            min_length_days=0)
        db_sentence_group = generate_sentence_group(
            external_id=_EXTERNAL_ID,
            sentence_group_id=_ID,
            supervision_sentences=[db_supervision_sentence])
        db_external_id = generate_external_id(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID)
        db_person.external_ids = [db_external_id]
        db_person.sentence_groups = [db_sentence_group]

        self._commit_to_db(db_person)

        supervision_sentence_updated = attr.evolve(
            self.to_entity(db_supervision_sentence),
            supervision_sentence_id=None, min_length_days=1)
        placeholder_sentence_group = StateSentenceGroup.new_with_defaults(
            supervision_sentences=[supervision_sentence_updated])
        placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[placeholder_sentence_group], state_code=_STATE_CODE)

        expected_supervision_sentence = attr.evolve(
            supervision_sentence_updated, supervision_sentence_id=_ID)
        expected_sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            supervision_sentences=[expected_supervision_sentence])
        expected_external_id = attr.evolve(self.to_entity(db_external_id))
        expected_person = attr.evolve(
            self.to_entity(db_person), external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group])

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE, ingested_people=[placeholder_person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_multipleHolesInDbGraph(self) -> None:
        # Arrange 1 - Match
        db_placeholder_person = generate_person(person_id=_ID, state_code=_STATE_CODE)
        db_supervision_sentence = generate_supervision_sentence(
            person=db_placeholder_person,
            supervision_sentence_id=_ID, status=SentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            min_length_days=0)
        db_placeholder_sentence_group = generate_sentence_group(
            sentence_group_id=_ID,
            supervision_sentences=[db_supervision_sentence])
        db_placeholder_person.sentence_groups = [db_placeholder_sentence_group]

        self._commit_to_db(db_placeholder_person)

        supervision_sentence_updated = attr.evolve(
            self.to_entity(db_supervision_sentence),
            supervision_sentence_id=None, min_length_days=1)
        sentence_group = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            status=SentenceStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[supervision_sentence_updated])
        external_id = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, id_type=_ID_TYPE)
        person = StatePerson.new_with_defaults(
            person_id=None,
            full_name=_FULL_NAME, external_ids=[external_id],
            sentence_groups=[sentence_group],
            state_code=_STATE_CODE)

        expected_supervision_sentence_updated = attr.evolve(
            supervision_sentence_updated, supervision_sentence_id=_ID)
        expected_sentence_group = attr.evolve(
            sentence_group,
            status=SentenceStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[expected_supervision_sentence_updated])
        expected_placeholder_sentence_group = attr.evolve(
            self.to_entity(db_placeholder_sentence_group),
            supervision_sentences=[])
        expected_external_id = attr.evolve(external_id)
        expected_person = attr.evolve(
            person, external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group])
        expected_placeholder_person = attr.evolve(
            self.to_entity(db_placeholder_person),
            sentence_groups=[expected_placeholder_sentence_group])

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person, expected_placeholder_person],
            matched_entities.people, session)
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_holesInBothGraphs_ingestedPersonPlaceholder(self) -> None:
        db_person = generate_person(person_id=_ID, full_name=_FULL_NAME, state_code=_STATE_CODE)
        db_supervision_sentence = generate_supervision_sentence(
            person=db_person,
            supervision_sentence_id=_ID, status=SentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            min_length_days=0)
        db_supervision_sentence_another = \
            generate_supervision_sentence(
                person=db_person,
                supervision_sentence_id=_ID_2,
                status=SentenceStatus.SERVING.value,
                external_id=_EXTERNAL_ID_2,
                state_code=_STATE_CODE,
                min_length_days=10)
        db_placeholder_sentence_group = generate_sentence_group(
            sentence_group_id=_ID,
            supervision_sentences=[
                db_supervision_sentence, db_supervision_sentence_another])

        db_external_id = generate_external_id(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID)
        db_person.external_ids = [db_external_id]
        db_person.sentence_groups = [db_placeholder_sentence_group]

        self._commit_to_db(db_person)

        supervision_sentence_updated = attr.evolve(
            self.to_entity(db_supervision_sentence),
            supervision_sentence_id=None, min_length_days=1)
        supervision_sentence_another_updated = attr.evolve(
            self.to_entity(db_supervision_sentence_another),
            supervision_sentence_id=None, min_length_days=11)
        sentence_group_new = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            status=SentenceStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[supervision_sentence_updated])
        sentence_group_new_another = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID_2,
            state_code=_STATE_CODE,
            status=SentenceStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[supervision_sentence_another_updated])

        placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group_new, sentence_group_new_another], state_code=_STATE_CODE)

        expected_supervision_sentence = attr.evolve(
            supervision_sentence_updated, supervision_sentence_id=_ID)
        expected_supervision_sentence_another = attr.evolve(
            supervision_sentence_another_updated, supervision_sentence_id=_ID_2)
        expected_sentence_group = attr.evolve(
            sentence_group_new,
            supervision_sentences=[expected_supervision_sentence])
        expected_sentence_group_another = attr.evolve(
            sentence_group_new_another,
            supervision_sentences=[expected_supervision_sentence_another])
        expected_sentence_group_placeholder = attr.evolve(
            self.to_entity(db_placeholder_sentence_group),
            supervision_sentences=[])
        expected_external_id = attr.evolve(self.to_entity(db_external_id))
        expected_person = attr.evolve(
            self.to_entity(db_person), external_ids=[expected_external_id],
            sentence_groups=[
                expected_sentence_group, expected_sentence_group_another,
                expected_sentence_group_placeholder])

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE, ingested_people=[placeholder_person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)
        self.assert_no_errors(matched_entities)
        self.assertEqual(2, matched_entities.total_root_entities)

    def test_matchPersons_holesInBothGraphs_dbPersonPlaceholder(self) -> None:
        db_placeholder_person = generate_person(person_id=_ID, state_code=_STATE_CODE)
        db_supervision_sentence = generate_supervision_sentence(
            person=db_placeholder_person,
            supervision_sentence_id=_ID, status=SentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            min_length_days=0)
        db_supervision_sentence_another = \
            generate_supervision_sentence(
                person=db_placeholder_person,
                supervision_sentence_id=_ID_2,
                status=SentenceStatus.SERVING.value,
                external_id=_EXTERNAL_ID_2, state_code=_STATE_CODE,
                min_length_days=10)
        db_sentence_group = generate_sentence_group(
            external_id=_EXTERNAL_ID,
            sentence_group_id=_ID,
            supervision_sentences=[db_supervision_sentence])
        db_sentence_group_another = generate_sentence_group(
            sentence_group_id=_ID_2,
            external_id=_EXTERNAL_ID_2,
            supervision_sentences=[db_supervision_sentence_another])

        db_placeholder_person.sentence_groups = [
            db_sentence_group, db_sentence_group_another]

        self._commit_to_db(db_placeholder_person)

        supervision_sentence_updated = attr.evolve(
            self.to_entity(db_supervision_sentence),
            supervision_sentence_id=None,
            min_length_days=1)
        supervision_sentence_another_updated = attr.evolve(
            self.to_entity(db_supervision_sentence_another),
            supervision_sentence_id=None, min_length_days=11)
        placeholder_sentence_group = StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[
                supervision_sentence_updated,
                supervision_sentence_another_updated])
        external_id = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, id_type=_ID_TYPE)
        person = StatePerson.new_with_defaults(
            full_name=_FULL_NAME,
            external_ids=[external_id],
            sentence_groups=[placeholder_sentence_group],
            state_code=_STATE_CODE)

        expected_supervision_sentence = attr.evolve(
            supervision_sentence_updated, supervision_sentence_id=_ID)
        expected_supervision_sentence_another = attr.evolve(
            supervision_sentence_another_updated, supervision_sentence_id=_ID_2)
        expected_sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            supervision_sentences=[expected_supervision_sentence])
        expected_sentence_group_another = attr.evolve(
            self.to_entity(db_sentence_group_another),
            supervision_sentences=[expected_supervision_sentence_another])
        expected_external_id = attr.evolve(external_id)
        expected_person = attr.evolve(
            person, external_ids=[expected_external_id],
            sentence_groups=[
                expected_sentence_group, expected_sentence_group_another])

        expected_placeholder_person = attr.evolve(
            self.to_entity(db_placeholder_person), sentence_groups=[])

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person, expected_placeholder_person],
            matched_entities.people, session)
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_matchAfterManyIngestedPlaceholders(self) -> None:
        # Arrange 1 - Match
        db_sentence_group = generate_sentence_group(
            external_id=_EXTERNAL_ID, sentence_group_id=_ID)
        db_external_id = generate_external_id(
            person_external_id_id=_ID,
            state_code=_STATE_CODE, id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        db_person = generate_person(
            person_id=_ID, sentence_groups=[db_sentence_group],
            external_ids=[db_external_id],
            state_code=_STATE_CODE)

        self._commit_to_db(db_person)

        incarceration_incident = StateIncarcerationIncident.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID)
        placeholder_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
                state_code=_STATE_CODE,
                incarceration_incidents=[incarceration_incident])
        placeholder_incarceration_sentence = \
            StateIncarcerationSentence.new_with_defaults(
                status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
                state_code=_STATE_CODE,
                incarceration_periods=[placeholder_incarceration_period])
        sentence_group = attr.evolve(
            self.to_entity(db_sentence_group), sentence_group_id=None,
            incarceration_sentences=[placeholder_incarceration_sentence])
        external_id = attr.evolve(self.to_entity(db_external_id),
                                  person_external_id_id=None)
        person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group], external_ids=[external_id], state_code=_STATE_CODE)

        expected_incarceration_incident = attr.evolve(incarceration_incident)

        expected_incarceration_period = attr.evolve(
            placeholder_incarceration_period,
            incarceration_incidents=[expected_incarceration_incident])
        expected_incarceration_sentence = attr.evolve(
            placeholder_incarceration_sentence,
            incarceration_periods=[expected_incarceration_period])
        expected_sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            incarceration_sentences=[expected_incarceration_sentence])
        expected_external_id = attr.evolve(self.to_entity(db_external_id))
        expected_person = attr.evolve(
            self.to_entity(db_person), external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group])

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_runMatch_multipleExternalIdsOnRootEntity(self) -> None:
        db_external_id = generate_external_id(
            person_external_id_id=_ID, external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            id_type=_ID_TYPE)
        db_external_id_2 = generate_external_id(
            person_external_id_id=_ID_2, external_id=_EXTERNAL_ID_2,
            state_code=_STATE_CODE,
            id_type=_ID_TYPE_ANOTHER)
        db_person = generate_person(
            person_id=_ID, external_ids=[db_external_id, db_external_id_2], state_code=_STATE_CODE)
        self._commit_to_db(db_person)

        external_id = attr.evolve(
            self.to_entity(db_external_id),
            person_external_id_id=None)
        external_id_2 = attr.evolve(
            self.to_entity(db_external_id_2),
            person_external_id_id=None)
        race = StatePersonRace.new_with_defaults(
            race=Race.WHITE, state_code=_STATE_CODE)
        person = StatePerson.new_with_defaults(
            external_ids=[external_id, external_id_2], races=[race], state_code=_STATE_CODE)

        expected_race = attr.evolve(race)
        expected_person = attr.evolve(self.to_entity(db_person),
                                      races=[expected_race])

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE, ingested_people=[person])

        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_mergeMultiParentEntities(self) -> None:
        # Arrange 1 - Match
        charge_merged = StateCharge.new_with_defaults(
            charge_id=_ID,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            status=ChargeStatus.PRESENT_WITHOUT_INFO)
        charge_unmerged = attr.evolve(charge_merged, charge_id=None)
        charge_duplicate_unmerged = attr.evolve(charge_unmerged)
        sentence = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[charge_unmerged])
        sentence_2 = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID_2,
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[charge_duplicate_unmerged])
        sentence_3 = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID_3,
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[charge_merged])
        sentence_group = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[sentence, sentence_2, sentence_3])
        person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group],
            state_code=_STATE_CODE)

        expected_charge = attr.evolve(charge_merged)
        expected_sentence = attr.evolve(sentence, charges=[expected_charge])
        expected_sentence_2 = attr.evolve(sentence_2, charges=[expected_charge])
        expected_sentence_3 = attr.evolve(sentence_3, charges=[expected_charge])
        expected_sentence_group = attr.evolve(
            sentence_group,
            incarceration_sentences=[
                expected_sentence, expected_sentence_2, expected_sentence_3])
        expected_person = attr.evolve(
            person, sentence_groups=[expected_sentence_group])

        # Arrange 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE, [person])

        # Assert 1 - Match
        sentence_group = matched_entities.people[0].sentence_groups[0]
        found_charge = sentence_group.incarceration_sentences[0].charges[0]
        found_charge_2 = sentence_group.incarceration_sentences[1].charges[0]
        found_charge_3 = sentence_group.incarceration_sentences[2].charges[0]
        self.assertEqual(
            id(found_charge), id(found_charge_2), id(found_charge_3))
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)

    def test_mergeMultiParentEntities_mergeChargesAndCourtCases(self) -> None:
        # Arrange 1 - Match
        court_case_merged = StateCourtCase.new_with_defaults(
            court_case_id=_ID,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID)
        court_case_unmerged = attr.evolve(court_case_merged, court_case_id=None)
        charge_merged = StateCharge.new_with_defaults(
            charge_id=_ID,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            court_case=court_case_merged)
        charge_unmerged = attr.evolve(
            charge_merged, charge_id=None, court_case=court_case_unmerged)
        charge_2 = StateCharge.new_with_defaults(
            external_id=_EXTERNAL_ID_2,
            state_code=_STATE_CODE,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            court_case=court_case_unmerged)
        sentence = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[charge_unmerged])
        sentence_2 = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID_2,
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[charge_merged])
        sentence_3 = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID_3,
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[charge_2])
        sentence_group = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[sentence, sentence_2, sentence_3])
        person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group],
            state_code=_STATE_CODE)

        expected_court_case = attr.evolve(court_case_merged)
        expected_charge = attr.evolve(
            charge_merged, court_case=expected_court_case)
        expected_charge_2 = attr.evolve(
            charge_2, court_case=expected_court_case)
        expected_sentence = attr.evolve(sentence, charges=[expected_charge])
        expected_sentence_2 = attr.evolve(sentence_2, charges=[expected_charge])
        expected_sentence_3 = attr.evolve(
            sentence_3, charges=[expected_charge_2])
        expected_sentence_group = attr.evolve(
            sentence_group, incarceration_sentences=[
                expected_sentence, expected_sentence_2, expected_sentence_3])
        expected_person = attr.evolve(
            person, sentence_groups=[expected_sentence_group])

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE, [person])

        # Assert 1 - Match
        sentence_group = matched_entities.people[0].sentence_groups[0]
        found_charge = sentence_group.incarceration_sentences[0].charges[0]
        found_charge_2 = sentence_group.incarceration_sentences[1].charges[0]
        self.assertEqual(id(found_charge), id(found_charge_2))

        found_court_case = sentence_group.incarceration_sentences[0] \
            .charges[0].court_case
        found_court_case_2 = sentence_group.incarceration_sentences[1] \
            .charges[0].court_case
        found_court_case_3 = sentence_group.incarceration_sentences[2] \
            .charges[0].court_case
        self.assertEqual(id(found_court_case), id(found_court_case_2),
                         id(found_court_case_3))
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)

    def test_mergeMultiParentEntities_mergeCharges_errorInMerge_keepsOne(
            self):
        # Arrange 1 - Match
        court_case = StateCourtCase.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID)
        different_court_case = StateCourtCase.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2)
        charge = StateCharge.new_with_defaults(
            charge_id=_ID,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            court_case=court_case)
        charge_matching = attr.evolve(
            charge, charge_id=None, court_case=different_court_case)
        sentence = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[charge_matching])
        sentence_2 = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID_2,
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[charge])
        sentence_group = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[sentence, sentence_2])
        person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group],
            state_code=_STATE_CODE)

        # `different_court_case` was not successfully merged. An error is logged
        # but otherwise, the failure is silent, as intended (so we do not halt
        # ingestion because of a failure here).
        expected_court_case = attr.evolve(court_case)
        expected_charge = attr.evolve(charge, court_case=expected_court_case)
        expected_sentence = attr.evolve(sentence, charges=[expected_charge])
        expected_sentence_2 = attr.evolve(sentence_2, charges=[expected_charge])
        expected_sentence_group = attr.evolve(
            sentence_group, incarceration_sentences=[
                expected_sentence, expected_sentence_2])
        expected_person = attr.evolve(
            person, sentence_groups=[expected_sentence_group])

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE, [person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)

    def test_mergeMultiParentEntities_mergeCourtCaseWithJudge(self) -> None:
        # Arrange 1 - Match
        db_person = generate_person(person_id=_ID, state_code=_STATE_CODE)
        db_court_case = generate_court_case(
            court_case_id=_ID,
            person=db_person,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            county_code='county_code')
        db_charge = generate_charge(
            charge_id=_ID,
            person=db_person,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            description='charge_1',
            status=ChargeStatus.PRESENT_WITHOUT_INFO.value,
            court_case=db_court_case)
        db_incarceration_sentence = generate_incarceration_sentence(
            incarceration_sentence_id=_ID,
            person=db_person,
            status=StateSentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            county_code='county_code',
            charges=[db_charge])
        db_sentence_group = generate_sentence_group(
            sentence_group_id=_ID,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE, county_code='county_code',
            incarceration_sentences=[db_incarceration_sentence])
        db_external_id = generate_external_id(
            person_external_id_id=_ID,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID, id_type=_ID_TYPE)
        db_person.external_ids = [db_external_id]
        db_person.sentence_groups = [db_sentence_group]

        self._commit_to_db(db_person)

        new_agent = StateAgent.new_with_defaults(
            agent_type=StateAgentType.JUDGE,
            state_code=_STATE_CODE)

        person = StatePerson.new_with_defaults(
            state_code=_STATE_CODE,
            sentence_groups=[
                StateSentenceGroup.new_with_defaults(
                    state_code=_STATE_CODE,
                    external_id=_EXTERNAL_ID,
                    incarceration_sentences=[
                        StateIncarcerationSentence.new_with_defaults(
                            state_code=_STATE_CODE,
                            external_id=_EXTERNAL_ID,
                            charges=[
                                StateCharge.new_with_defaults(
                                    state_code=_STATE_CODE,
                                    external_id=_EXTERNAL_ID,
                                    court_case=StateCourtCase.new_with_defaults(
                                        state_code=_STATE_CODE,
                                        external_id=_EXTERNAL_ID,
                                        judge=new_agent
                                    ),
                                ),
                            ]
                        ),
                    ]),
            ])

        expected_person = attr.evolve(self.to_entity(db_person))
        expected_incarceration_sentence = \
            expected_person.sentence_groups[0].incarceration_sentences[0]
        expected_incarceration_sentence.charges[0].court_case.judge = \
            attr.evolve(new_agent)

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE, [person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)

    def test_mergeMultiParentEntities_mergeCourtCaseWithJudge2(self) -> None:
        # Arrange 1 - Match
        db_person = generate_person(person_id=_ID, state_code=_STATE_CODE)
        db_court_case = generate_court_case(
            court_case_id=_ID,
            person=db_person,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            county_code='county_code')
        db_charge = generate_charge(
            charge_id=_ID,
            person=db_person,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            description='charge_1',
            status=ChargeStatus.PRESENT_WITHOUT_INFO.value,
            court_case=db_court_case)
        db_incarceration_sentence = generate_incarceration_sentence(
            incarceration_sentence_id=_ID,
            person=db_person,
            status=StateSentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            county_code='county_code',
            charges=[db_charge])
        db_sentence_group = generate_sentence_group(
            sentence_group_id=_ID,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE, county_code='county_code',
            incarceration_sentences=[db_incarceration_sentence])
        db_external_id = generate_external_id(
            person_external_id_id=_ID,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID, id_type=_ID_TYPE)
        db_person.external_ids = [db_external_id]
        db_person.sentence_groups = [db_sentence_group]

        self._commit_to_db(db_person)

        new_agent = StateAgent.new_with_defaults(
            agent_type=StateAgentType.JUDGE,
            state_code=_STATE_CODE)

        person = StatePerson.new_with_defaults(
            state_code=_STATE_CODE,
            sentence_groups=[
                StateSentenceGroup.new_with_defaults(
                    state_code=_STATE_CODE,
                    external_id=_EXTERNAL_ID,
                    incarceration_sentences=[
                        StateIncarcerationSentence.new_with_defaults(
                            state_code=_STATE_CODE,
                            charges=[
                                StateCharge.new_with_defaults(
                                    state_code=_STATE_CODE,
                                    court_case=StateCourtCase.new_with_defaults(
                                        state_code=_STATE_CODE,
                                        external_id=_EXTERNAL_ID,
                                        judge=new_agent
                                    ),
                                ),
                            ]
                        ),
                    ]),
            ])

        expected_person = attr.evolve(self.to_entity(db_person))
        expected_incarceration_sentence = \
            expected_person.sentence_groups[0].incarceration_sentences[0]
        expected_incarceration_sentence.charges[0].court_case.judge = \
            attr.evolve(new_agent)

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE, [person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)

    def test_parentChildLinkAtRootDiscoveredAfterBothWritten(self) -> None:
        # Arrange 1 - Match
        db_sentence_group = generate_sentence_group(
            sentence_group_id=_ID,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            min_length_days=5,
        )
        db_placeholder_person = generate_person(
            person_id=_ID, sentence_groups=[db_sentence_group], state_code=_STATE_CODE)

        db_external_id = generate_external_id(
            person_external_id_id=_ID,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID, id_type=_ID_TYPE)
        db_person = generate_person(person_id=_ID_2,
                                    external_ids=[db_external_id],
                                    state_code=_STATE_CODE)

        self._commit_to_db(db_placeholder_person, db_person)

        external_id = attr.evolve(
            self.to_entity(db_external_id), person_external_id_id=None)
        sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            sentence_group_id=None,
            is_life=True)
        person = attr.evolve(self.to_entity(db_person),
                             person_id=None,
                             full_name=_FULL_NAME,
                             external_ids=[external_id],
                             sentence_groups=[sentence_group],
                             state_code=_STATE_CODE)

        expected_person = attr.evolve(person, person_id=db_person.person_id, state_code=db_person.state_code)
        expected_placeholder_person = \
            self.to_entity(generate_person(
                person_id=db_placeholder_person.person_id,
                state_code=db_placeholder_person.state_code))
        expected_placeholder_sentence_group = \
            self.to_entity(generate_sentence_group(
                sentence_group_id=db_sentence_group.sentence_group_id,
                person_id=db_placeholder_person.person_id,
                state_code=_STATE_CODE))
        expected_placeholder_person.sentence_groups = \
            [expected_placeholder_sentence_group]

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE, [person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person, expected_placeholder_person],
            matched_entities.people, session)

    def test_parentChildLinkInSubtreeDiscoveredAfterBothWritten(self) -> None:
        # Arrange 1 - Match
        db_person = generate_person(state_code=_STATE_CODE)
        db_external_id = generate_external_id(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID, id_type=_ID_TYPE)
        db_sentence_group = generate_sentence_group(
            person=db_person,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
        )
        db_incarceration_sentence = \
            generate_incarceration_sentence(
                person=db_person,
                status=StateSentenceStatus.SERVING.value,
                external_id=_EXTERNAL_ID,
                state_code=_STATE_CODE)
        db_placeholder_sentence_group = generate_sentence_group(
            person=db_person,
            incarceration_sentences=[db_incarceration_sentence])
        db_person.external_ids = [db_external_id]
        db_person.sentence_groups = [
            db_sentence_group, db_placeholder_sentence_group]

        self._commit_to_db(db_person)

        incarceration_sentence = attr.evolve(
            self.to_entity(db_incarceration_sentence),
            incarceration_sentence_id=None,
            incarceration_type=StateIncarcerationType.STATE_PRISON)

        sentence_group = attr.evolve(self.to_entity(db_sentence_group),
                                     sentence_group_id=None,
                                     is_life=True,
                                     incarceration_sentences=[incarceration_sentence])
        placeholder_person = StatePerson.new_with_defaults(
            state_code=_STATE_CODE,
            sentence_groups=[sentence_group]
        )

        expected_incarceration_sentence = attr.evolve(incarceration_sentence)
        expected_sentence_group = attr.evolve(sentence_group, incarceration_sentences=[expected_incarceration_sentence])
        expected_placeholder_incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        expected_placeholder_sentence_group = StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[expected_placeholder_incarceration_sentence]
        )
        expected_person = \
            attr.evolve(self.to_entity(db_person),
                        person_id=db_person.person_id,
                        sentence_groups=[expected_sentence_group, expected_placeholder_sentence_group])

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE, [placeholder_person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person],
            matched_entities.people, session)

    def test_databaseCleanup_dontMergeMultiParentEntities(self) -> None:
        db_person = generate_person(person_id=_ID, full_name=_FULL_NAME, state_code=_STATE_CODE)
        db_court_case = generate_court_case(
            person=db_person, court_case_id=_ID, external_id=_EXTERNAL_ID)
        db_placeholder_charge = generate_charge(
            person=db_person, charge_id=_ID,
            court_case=db_court_case)
        db_placeholder_fine = generate_fine(
            person=db_person, fine_id=_ID, charges=[db_placeholder_charge])
        db_placeholder_sentence_group = generate_sentence_group(
            person=db_person,
            sentence_group_id=_ID,
            fines=[db_placeholder_fine])
        db_external_id = generate_external_id(
            person=db_person,
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID, id_type=_ID_TYPE)
        db_person.external_ids = [db_external_id]
        db_person.sentence_groups = [db_placeholder_sentence_group]

        db_person_2 = generate_person(person_id=_ID_2, full_name=_FULL_NAME, state_code=_STATE_CODE)
        db_court_case_2 = generate_court_case(
            person=db_person_2, court_case_id=_ID_2, external_id=_EXTERNAL_ID)
        db_placeholder_charge_2 = generate_charge(
            person=db_person_2, charge_id=_ID_2,
            court_case=db_court_case_2)
        db_placeholder_fine_2 = generate_fine(
            person=db_person_2, fine_id=_ID_2,
            charges=[db_placeholder_charge_2])
        db_placeholder_sentence_group_2 = generate_sentence_group(
            person=db_person_2,
            sentence_group_id=_ID_2,
            fines=[db_placeholder_fine_2])
        db_external_id_2 = generate_external_id(
            person=db_person_2,
            person_external_id_id=_ID_2, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2, id_type=_ID_TYPE)
        db_person_2.external_ids = [db_external_id_2]
        db_person_2.sentence_groups = [db_placeholder_sentence_group_2]

        self._commit_to_db(db_person, db_person_2)

        court_case = attr.evolve(
            self.to_entity(db_court_case), court_case_id=None)
        placeholder_charge = attr.evolve(
            self.to_entity(db_placeholder_charge),
            charge_id=None, court_case=court_case)
        placeholder_fine = attr.evolve(
            self.to_entity(db_placeholder_fine),
            fine_id=None, charges=[placeholder_charge])
        placeholder_sentence_group = attr.evolve(
            self.to_entity(db_placeholder_sentence_group),
            sentence_group_id=None, fines=[placeholder_fine])
        external_id = attr.evolve(
            self.to_entity(db_external_id), person_external_id_id=None)
        person = attr.evolve(
            self.to_entity(db_person), person_id=None,
            external_ids=[external_id],
            sentence_groups=[placeholder_sentence_group],
            current_address='address')

        expected_person = attr.evolve(self.to_entity(db_person),
                                      current_address='address')
        expected_person_2 = attr.evolve(self.to_entity(db_person_2))

        # Act 1 - Match
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session,
            expected_unmatched_db_people=[expected_person_2])
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_mergeIncarcerationPeriod_multipleSentenceParents(self) -> None:
        """This tests that ingesting an incarceration period that is the same
        as one we've seen before, but is now being referenced by a different
        sentence object, will be pointed to by both sentence objects. This
        should be supported as the relationship between sentence and period is
        many-to-many."""
        # Arrange
        db_person = generate_person(state_code=_STATE_CODE)
        db_incarceration_period = generate_incarceration_period(
            person=db_person,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2,
            admission_date=_DATE_1,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION.value)
        db_incarceration_sentence = generate_incarceration_sentence(
            person=db_person,
            status=StateSentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            county_code='county_code',
            incarceration_periods=[db_incarceration_period])
        db_supervision_sentence = generate_supervision_sentence(
            person=db_person,
            status=StateSentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            county_code='county_code')
        db_sentence_group = generate_sentence_group(
            status=StateSentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE, county_code='county_code',
            incarceration_sentences=[db_incarceration_sentence],
            supervision_sentences=[db_supervision_sentence])
        db_external_id = generate_external_id(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            id_type=_ID_TYPE)
        db_person.external_ids = [db_external_id]
        db_person.sentence_groups = [db_sentence_group]

        self._commit_to_db(db_person)

        # Now we ingest the exact same period but with a flat field update and
        # with a new reference to a parent supervision sentence, since it is
        # related to both sentences
        updated_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            external_id=db_incarceration_period.external_id,
            state_code=_STATE_CODE,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
            release_date=_DATE_2,
            incarceration_period_id=None
        )
        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            external_id=db_supervision_sentence.external_id,
            state_code=_STATE_CODE,
            incarceration_periods=[updated_incarceration_period]
        )
        sentence_group = StateSentenceGroup.new_with_defaults(
            external_id=db_sentence_group.external_id,
            state_code=_STATE_CODE,
            supervision_sentences=[supervision_sentence]
        )
        external_id = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            id_type=_ID_TYPE
        )
        person = StatePerson.new_with_defaults(
            state_code=_STATE_CODE,
            external_ids=[external_id],
            sentence_groups=[sentence_group]
        )

        expected_incarceration_period = attr.evolve(
            self.to_entity(db_incarceration_period),
            release_date=_DATE_2,
            incarceration_period_id=db_incarceration_period.incarceration_period_id
        )
        # db_incarceration_sentence still points to the updated incarceration
        # period
        expected_incarceration_sentence = attr.evolve(
            self.to_entity(db_incarceration_sentence),
            incarceration_periods=[expected_incarceration_period]
        )
        # db_supervision_sentence did NOT point to incarceration period before
        # but it does now
        expected_supervision_sentence = attr.evolve(
            self.to_entity(db_supervision_sentence),
            supervision_sentence_id=db_supervision_sentence.supervision_sentence_id,
            incarceration_periods=[expected_incarceration_period]
        )
        expected_sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            incarceration_sentences=[expected_incarceration_sentence],
            supervision_sentences=[expected_supervision_sentence]
        )
        expected_person = attr.evolve(
            self.to_entity(db_person),
            sentence_groups=[expected_sentence_group]
        )

        # Act
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE, ingested_people=[person])

        # Assert
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_mergeSupervisionPeriod_multipleSentenceParents(self) -> None:
        """Repeats the same test as directly above, but with a supervision
        period instead of an incarceration period."""

        # Arrange
        db_person = generate_person(person_id=_ID, state_code=_STATE_CODE)
        db_supervision_period = generate_supervision_period(
            supervision_period_id=_ID_3,
            person=db_person,
            external_id=_EXTERNAL_ID_3,
            start_date=_DATE_1,
            admission_reason=
            StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE.value)
        db_incarceration_sentence = generate_incarceration_sentence(
            incarceration_sentence_id=_ID,
            person=db_person,
            status=StateSentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            county_code='county_code')
        db_supervision_sentence = generate_supervision_sentence(
            supervision_sentence_id=_ID,
            person=db_person,
            status=StateSentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            county_code='county_code',
            supervision_periods=[db_supervision_period])
        db_sentence_group = generate_sentence_group(
            sentence_group_id=_ID,
            status=StateSentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE, county_code='county_code',
            incarceration_sentences=[db_incarceration_sentence],
            supervision_sentences=[db_supervision_sentence])
        db_external_id = generate_external_id(
            person_external_id_id=_ID,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID, id_type=_ID_TYPE)
        db_person.external_ids = [db_external_id]
        db_person.sentence_groups = [db_sentence_group]

        self._commit_to_db(db_person)

        # Now we ingest the exact same period but with a flat field update and
        # with a new reference to a parent incarceration sentence, since it is
        # related to both sentences
        updated_supervision_period = attr.evolve(
            self.to_entity(db_supervision_period),
            termination_date=_DATE_2,
            supervision_period_id=None
        )
        incarceration_sentence = attr.evolve(
            self.to_entity(db_incarceration_sentence),
            incarceration_sentence_id=None,
            supervision_periods=[updated_supervision_period]
        )
        sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            sentence_group_id=None,
            incarceration_sentences=[incarceration_sentence]
        )
        external_id = attr.evolve(
            self.to_entity(db_external_id),
            person_external_id_id=None
        )
        person = attr.evolve(
            self.to_entity(db_person),
            person_id=None,
            external_ids=[external_id],
            sentence_groups=[sentence_group]
        )

        expected_supervision_period = attr.evolve(
            updated_supervision_period,
            supervision_period_id=_ID_3
        )
        # db_incarceration_sentence did NOT point to supervision period before
        # but it does now
        expected_incarceration_sentence = attr.evolve(
            incarceration_sentence,
            incarceration_sentence_id=_ID,
            supervision_periods=[expected_supervision_period]
        )
        # db_supervision_sentence still points to the updated supervision period
        expected_supervision_sentence = attr.evolve(
            self.to_entity(db_supervision_sentence),
            supervision_periods=[expected_supervision_period]
        )
        expected_sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            incarceration_sentences=[expected_incarceration_sentence],
            supervision_sentences=[expected_supervision_sentence]
        )
        expected_person = attr.evolve(
            self.to_entity(db_person),
            sentence_groups=[expected_sentence_group]
        )

        # Act
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE, ingested_people=[person])

        # Assert
        self.assert_people_match_pre_and_post_commit(
            [expected_person], matched_entities.people, session)
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_mergeMultiParentEntityParentAndChild_multipleSentenceParents(self) -> None:
        """Tests merging multi-parent entities, but the two types of entities
        that must be merged are directly connected (themselves parent/child).
        In this tests case they are StateSupervisionViolation and
        StateSupervisionViolationResponse.
        """
        # Arrange
        db_person = generate_person(person_id=_ID, state_code=_STATE_CODE)
        db_incarceration_sentence = generate_incarceration_sentence(
            incarceration_sentence_id=_ID,
            person=db_person,
            status=StateSentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            county_code='county_code')
        db_sentence_group = generate_sentence_group(
            sentence_group_id=_ID,
            status=StateSentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE, county_code='county_code',
            incarceration_sentences=[db_incarceration_sentence])
        db_external_id = generate_external_id(
            person_external_id_id=_ID,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID, id_type=_ID_TYPE)
        db_person.external_ids = [db_external_id]
        db_person.sentence_groups = [db_sentence_group]

        self._commit_to_db(db_person)

        supervision_violation_response = \
            StateSupervisionViolationResponse.new_with_defaults(
                external_id=_EXTERNAL_ID,
                state_code=_STATE_CODE)
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            supervision_violation_responses=[supervision_violation_response])
        supervision_period_is = StateSupervisionPeriod.new_with_defaults(
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
            state_code=_STATE_CODE,
            supervision_violation_entries=[supervision_violation],
        )

        supervision_violation_response_dup = attr.evolve(
            supervision_violation_response)
        supervision_violation_dup = attr.evolve(
            supervision_violation,
            supervision_violation_responses=[
                supervision_violation_response_dup])
        supervision_period_ss = attr.evolve(
            supervision_period_is,
            supervision_violation_entries=[supervision_violation_dup])

        incarceration_sentence = attr.evolve(
            self.to_entity(db_incarceration_sentence),
            incarceration_sentence_id=None,
            supervision_periods=[supervision_period_is]
        )
        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            status=StateSentenceStatus.COMPLETED,
            state_code=_STATE_CODE,
            supervision_periods=[supervision_period_ss]
        )
        sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            sentence_group_id=None,
            incarceration_sentences=[incarceration_sentence],
            supervision_sentences=[supervision_sentence],
        )
        external_id = attr.evolve(
            self.to_entity(db_external_id),
            person_external_id_id=None
        )
        person = attr.evolve(
            self.to_entity(db_person),
            person_id=None,
            external_ids=[external_id],
            sentence_groups=[sentence_group]
        )

        # Only one expected violation response and violation, as they've been
        # merged
        expected_supervision_violation_response = attr.evolve(
            supervision_violation_response,
        )
        expected_supervision_violation = attr.evolve(
            supervision_violation,
            supervision_violation_responses=[
                expected_supervision_violation_response]
        )
        expected_supervision_period_is = attr.evolve(
            supervision_period_is,
            supervision_violation_entries=[expected_supervision_violation]
        )
        expected_supervision_period_ss = attr.evolve(
            supervision_period_ss,
            supervision_violation_entries=[expected_supervision_violation]
        )
        expected_incarceration_sentence = attr.evolve(
            incarceration_sentence,
            incarceration_sentence_id=_ID,
            supervision_periods=[expected_supervision_period_is]
        )
        expected_supervision_sentence = attr.evolve(
            supervision_sentence,
            supervision_periods=[expected_supervision_period_ss]
        )
        expected_sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            incarceration_sentences=[expected_incarceration_sentence],
            supervision_sentences=[expected_supervision_sentence]
        )
        expected_person = attr.evolve(
            self.to_entity(db_person),
            sentence_groups=[expected_sentence_group]
        )

        # Act
        session = self._session()
        matched_entities = entity_matching.match(
            session, _STATE_CODE, ingested_people=[person])

        # Assert
        self.assert_people_match_pre_and_post_commit([expected_person], matched_entities.people, session)
        self.assert_no_errors(matched_entities)
        self.assertEqual(1, matched_entities.total_root_entities)
