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
from unittest import TestCase

import attr
from mock import patch, create_autospec

from recidiviz.common.constants.bond import BondStatus
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.county.sentence import SentenceStatus
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.constants.person_characteristics import Gender, Race, \
    Ethnicity
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_fine import StateFineStatus
from recidiviz.common.constants.state.state_incarceration import \
    StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodStatus, StateIncarcerationPeriodAdmissionReason, \
    StateIncarcerationPeriodReleaseReason
from recidiviz.common.constants.state.state_parole_decision import \
    StateParoleDecisionOutcome
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_period import \
    StateSupervisionPeriodStatus
from recidiviz.common.constants.state.state_supervision_violation_response \
    import StateSupervisionViolationResponseDecision, \
    StateSupervisionViolationResponseRevocationType
from recidiviz.persistence.database.schema_entity_converter import (
    schema_entity_converter as converter
)
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.schema.state import schema, dao
from recidiviz.persistence.database.base_schema import StateBase
from recidiviz.persistence.entity.state.entities import StatePersonAlias, \
    StatePersonExternalId, StatePersonRace, StatePersonEthnicity, StatePerson, \
    StateCourtCase, StateCharge, StateFine, StateIncarcerationIncident, \
    StateIncarcerationPeriod, StateIncarcerationSentence, \
    StateSupervisionSentence, StateSupervisionViolationResponse, \
    StateSupervisionViolation, StateSupervisionPeriod, StateSentenceGroup, \
    StateAgent
from recidiviz.persistence.entity_matching import entity_matching
from recidiviz.tests.persistence.database.schema.state.schema_test_utils \
    import generate_person, generate_external_id, generate_court_case, \
    generate_charge, generate_fine, generate_incarceration_sentence, \
    generate_sentence_group, generate_race, generate_alias, \
    generate_ethnicity, generate_bond, generate_assessment, generate_agent, \
    generate_incarceration_incident, generate_parole_decision, \
    generate_incarceration_period, generate_supervision_violation_response, \
    generate_supervision_violation, generate_supervision_period, \
    generate_supervision_sentence
from recidiviz.tests.utils import fakes
from recidiviz.utils.regions import Region

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
_US_ND = 'US_ND'
_STATE_CODE = 'NC'
_STATE_CODE_2 = 'SC'
_FACILITY = 'FACILITY'
_FACILITY_2 = 'FACILITY_2'
_DATE_1 = datetime.date(year=2019, month=1, day=1)
_DATE_2 = datetime.date(year=2019, month=2, day=1)
_DATE_3 = datetime.date(year=2019, month=3, day=1)
_DATE_4 = datetime.date(year=2019, month=4, day=1)
_DATE_5 = datetime.date(year=2019, month=5, day=1)
_DATE_6 = datetime.date(year=2019, month=6, day=1)


@patch("recidiviz.persistence.entity_matching.state"
       ".state_matching_utils.get_region")
class TestStateEntityMatching(TestCase):
    """Tests for state specific entity matching logic."""

    def setUp(self) -> None:
        fakes.use_in_memory_sqlite_database(StateBase)

    def to_entity(self, schema_obj):
        return converter.convert_schema_object_to_entity(
            schema_obj, populate_back_edges=False)

    def assert_people_match(self, expected_people, matched_people):
        converted_matched = \
            converter.convert_schema_objects_to_entity(matched_people)
        db_expected_with_backedges = \
            converter.convert_entity_people_to_schema_people(expected_people)
        expected_with_backedges = \
            converter.convert_schema_objects_to_entity(
                db_expected_with_backedges)
        self.assertEqual(expected_with_backedges, converted_matched)

    def _session(self):
        return SessionFactory.for_schema_base(StateBase)

    def _commit_to_db(self, *persons):
        session = self._session()
        for person in persons:
            session.add(person)
        session.commit()

    def test_match_newPerson(self, _):
        # Arrange 1 - Match
        db_person = schema.StatePerson(person_id=_ID, full_name=_FULL_NAME)
        db_external_id = schema.StatePersonExternalId(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID, id_type=_ID_TYPE)
        db_person.external_ids = [db_external_id]

        self._commit_to_db(db_person)

        external_id_2 = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2, id_type=_ID_TYPE)
        person_2 = StatePerson.new_with_defaults(
            full_name=_FULL_NAME, external_ids=[external_id_2])

        # Act 1 - Match
        act_session = self._session()
        matched_entities = entity_matching.match(
            act_session, _STATE_CODE,
            [person_2])

        # Assert 1 - Match
        expected_person_2 = attr.evolve(person_2)

        self.assertEqual(matched_entities.error_count, 0)
        self.assertEqual(1, matched_entities.total_root_entities)
        self.assert_people_match([expected_person_2], matched_entities.people)

        # Arrange 2 - Commit
        expected_db_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=1,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID, id_type=_ID_TYPE)
        expected_db_person = StatePerson.new_with_defaults(
            person_id=1,
            full_name=_FULL_NAME,
            external_ids=[expected_db_external_id])

        expected_db_person_2 = attr.evolve(expected_person_2)
        expected_db_person_2.person_id = 2
        expected_db_person_2.external_ids[0].person_external_id_id = 2

        # Act 2 - Commit
        act_session.commit()
        act_session.close()

        # Assert 2 - Commit
        assert_session = self._session()
        result_db_people = dao.read_people(assert_session)

        self.assert_people_match([expected_db_person, expected_db_person_2],
                                 result_db_people)
        assert_session.close()

    def test_match_noPlaceholders_simple(self, _):
        # Arrange 1 - Match
        db_person = schema.StatePerson(person_id=_ID, full_name=_FULL_NAME)
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
            full_name=_FULL_NAME, external_ids=[external_id, external_id_2])

        expected_person = attr.evolve(
            person,
            person_id=_ID,
            external_ids=[attr.evolve(external_id, person_external_id_id=_ID),
                          external_id_2])

        # Act 1 - Match
        act_session = self._session()
        matched_entities = entity_matching.match(
            act_session, _STATE_CODE,
            [person])

        # Assert 1 - Match
        self.assertEqual(matched_entities.error_count, 0)
        self.assertEqual(1, matched_entities.total_root_entities)
        self.assert_people_match([expected_person], matched_entities.people)

        # Arrange 2 - Commit
        expected_db_person = attr.evolve(expected_person)
        expected_db_person.external_ids[1].person_external_id_id = 2

        # Act 2 - Commit
        act_session.commit()
        act_session.close()

        # Assert 2 - Commit
        assert_session = self._session()
        result_db_people = dao.read_people(assert_session)

        self.assert_people_match([expected_db_person], result_db_people)
        assert_session.close()

    def test_match_noPlaceholders_success(self, _):
        # Arrange 1 - Match
        db_person = schema.StatePerson(person_id=_ID, full_name=_FULL_NAME)
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
            external_ids=[db_external_id_another])
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
            sentence_groups=[sentence_group])

        external_id_another = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2, id_type=_ID_TYPE)
        person_another = StatePerson.new_with_defaults(
            full_name=_FULL_NAME,
            external_ids=[external_id_another])

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
        act_session = self._session()
        matched_entities = entity_matching.match(
            act_session, _STATE_CODE,
            [person, person_another])

        # Assert 1 - Match
        self.assertEqual(matched_entities.error_count, 0)
        self.assertEqual(2, matched_entities.total_root_entities)
        self.assert_people_match(
            [expected_person, expected_person_another], matched_entities.people)

        # Arrange 2 - Commit
        expected_db_person = attr.evolve(expected_person)
        expected_person.sentence_groups[0].fines[0].charges[0].charge_id = 1
        expected_db_person_another = attr.evolve(expected_person_another)

        # Act 2 - Commit
        act_session.commit()
        act_session.close()

        # Assert 2 - Commit
        assert_session = self._session()
        result_db_people = dao.read_people(assert_session)

        self.assert_people_match(
            [expected_db_person, expected_db_person_another], result_db_people)
        assert_session.close()

    def test_match_noPlaceholder_oneMatchOneError(self, _):
        # Arrange 1 - Match
        db_external_id = schema.StatePersonExternalId(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID, id_type=_ID_TYPE)
        db_person = schema.StatePerson(
            person_id=_ID, full_name=_FULL_NAME, external_ids=[db_external_id])
        self._commit_to_db(db_person)

        external_id = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, id_type=_ID_TYPE)
        person = StatePerson.new_with_defaults(
            full_name=_FULL_NAME, external_ids=[external_id])

        external_id_dup = attr.evolve(external_id)
        person_dup = attr.evolve(person, external_ids=[external_id_dup],
                                 full_name=_FULL_NAME_ANOTHER)

        expected_person = attr.evolve(person, person_id=_ID)
        expected_external_id = attr.evolve(
            external_id, person_external_id_id=_ID)
        expected_person.external_ids = [expected_external_id]

        # Act 1 - Match
        act_session = self._session()
        matched_entities = entity_matching.match(
            act_session, _STATE_CODE, [person, person_dup])

        # Assert 1 - Match
        self.assertEqual(matched_entities.error_count, 1)
        self.assertEqual(2, matched_entities.total_root_entities)
        self.assert_people_match([expected_person], matched_entities.people)

        # Arrange 2 - Commit
        expected_db_person = attr.evolve(expected_person)

        # Act 2 - Commit
        act_session.commit()
        act_session.close()

        # Assert 2 - Commit
        assert_session = self._session()
        result_db_people = dao.read_people(assert_session)

        self.assert_people_match([expected_db_person], result_db_people)
        assert_session.close()

    def test_matchPersons_multipleMatchesToDb_oneSuccessOneError(self, _):
        db_external_id = generate_external_id(
            person_external_id_id=_ID, external_id=_EXTERNAL_ID)
        db_person = generate_person(
            person_id=_ID, full_name=_FULL_NAME, external_ids=[db_external_id])
        self._commit_to_db(db_person)
        external_id = attr.evolve(self.to_entity(db_external_id),
                                  person_external_id_id=None)
        person = attr.evolve(
            self.to_entity(db_person),
            person_id=None,
            external_ids=[external_id])
        person_dup = attr.evolve(person,
                                 full_name='another',
                                 external_ids=[attr.evolve(external_id)])

        expected_external_id = attr.evolve(external_id,
                                           person_external_id_id=_ID)
        expected_person = attr.evolve(person, person_id=_ID,
                                      external_ids=[expected_external_id])

        # Act 1 - Match
        act_session = self._session()
        matched_entities = entity_matching.match(
            act_session, _STATE_CODE, [person, person_dup])

        # Assert 1 - Match
        self.assert_people_match([expected_person], matched_entities.people)
        self.assertEqual(2, matched_entities.total_root_entities)
        self.assertEqual(1, matched_entities.error_count)

        # Arrange 2 - Commit
        expected_db_person = attr.evolve(expected_person)

        # Act 2 - Commit
        act_session.commit()
        act_session.close()

        # Assert 2 - Commit
        assert_session = self._session()
        result_db_people = dao.read_people(assert_session)

        self.assert_people_match([expected_db_person], result_db_people)
        assert_session.close()

    def test_matchPersons_conflictingExternalIds_error(self, _):
        # Arrange 1 - Match
        db_person = generate_person(person_id=_ID)
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

        # Act 1 - Match
        matched_entities = entity_matching.match(
            self._session(), _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        self.assertEqual([], matched_entities.people)
        self.assertEqual(1, matched_entities.total_root_entities)
        self.assertEqual(1, matched_entities.error_count)

    def test_matchPersons_sentenceGroupRootEntity_IngMatchesMultipleDb(self, _):
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
            external_ids=[db_external_id])
        self._commit_to_db(db_person)

        sentence_group = attr.evolve(
            self.to_entity(db_sentence_group), sentence_group_id=None)
        sentence_group_2 = attr.evolve(
            self.to_entity(db_sentence_group_2), sentence_group_id=None)
        sentence_group_3 = attr.evolve(
            self.to_entity(db_sentence_group_3), sentence_group_id=None)
        person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group, sentence_group_2,
                             sentence_group_3])

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
        matched_entities = entity_matching.match(
            self._session(), _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match([expected_person], matched_entities.people)
        self.assertEqual(3, matched_entities.total_root_entities)
        self.assertEqual(1, matched_entities.error_count)

    def test_matchPersons_matchesTwoDbPeople_mergeDbPeopleMoveChildren(self, _):
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
            external_ids=[db_external_id])
        db_person_2 = generate_person(
            person_id=_ID_2, full_name=_FULL_NAME,
            external_ids=[db_external_id_2],
            sentence_groups=[db_sentence_group])
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
        act_session = self._session()
        matched_entities = entity_matching.match(
            act_session, _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)
        self.assert_people_match([expected_person, expected_placeholder_person],
                                 matched_entities.people)

        # Arrange 2 - Commit
        expected_race.person_race_id = 1
        expected_alias.person_alias_id = 1
        expected_ethnicity.person_ethnicity_id = 1

        # Act 2 - Commit
        act_session.commit()
        act_session.close()

        # Assert 2 - Commit
        assert_session = self._session()
        result_db_people = dao.read_people(assert_session)

        self.assert_people_match([expected_person, expected_placeholder_person],
                                 result_db_people)
        assert_session.close()

    def test_matchPersons_matchesTwoDbPeople_mergeAndMoveChildren(self, _):
        """Tests that our system correctly handles the situation where we have
        2 distinct people in our DB, but we learn the two DB people should be
        merged into 1 person based on a new ingested person. Here the two DB
        people to merge have children which match with each other, and therefore
        need to be merged properly themselves.
        """
        # Arrange 1 - Match
        db_person = generate_person(person_id=_ID, full_name=_FULL_NAME)
        db_supervision_sentence = generate_supervision_sentence(
            person=db_person, supervision_sentence_id=_ID,
            external_id=_EXTERNAL_ID)
        db_sentence_group = generate_sentence_group(
            sentence_group_id=_ID, external_id=_EXTERNAL_ID, is_life=True,
            supervision_sentences=[db_supervision_sentence])
        db_external_id = generate_external_id(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID, id_type=_ID_TYPE)
        db_person.external_ids = [db_external_id]
        db_person.sentence_groups = [db_sentence_group]

        db_person_dup = generate_person(person_id=_ID_2, full_name=_FULL_NAME)
        db_supervision_period = generate_supervision_period(
            person=db_person_dup, supervision_period_id=_ID_2,
            external_id=_EXTERNAL_ID, start_date=_DATE_1)
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
        expected_person = attr.evolve(
            self.to_entity(db_person),
            external_ids=[self.to_entity(db_external_id),
                          self.to_entity(db_external_id_2)],
            sentence_groups=[expected_sentence_group])
        expected_placeholder_supervision_sentence = attr.evolve(
            self.to_entity(db_supervision_sentence_dup), external_id=None,
            max_length_days=None, supervision_periods=[])
        expected_placeholder_sentence_group = attr.evolve(
            self.to_entity(db_sentence_group_dup), external_id=None,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO, is_life=None,
            supervision_sentences=[expected_placeholder_supervision_sentence])
        expected_placeholder_person = attr.evolve(
            self.to_entity(db_person_dup), full_name=None,
            sentence_groups=[expected_placeholder_sentence_group],
            external_ids=[])

        # Act 1 - Match
        act_session = self._session()
        matched_entities = entity_matching.match(
            act_session, _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)
        self.assert_people_match([expected_person, expected_placeholder_person],
                                 matched_entities.people)

        # Act 2 - Arrange, nothing needs to be done, DB ids shouldn't change.

        # Act 2 - Commit
        act_session.commit()
        act_session.close()

        # Assert 2 - Commit
        assert_session = self._session()
        result_db_people = dao.read_people(assert_session)

        self.assert_people_match([expected_person, expected_placeholder_person],
                                 result_db_people)
        assert_session.close()

    def test_matchPersons_sentenceGroupRootEntity_DbMatchesMultipleIng(self, _):
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
            external_ids=[db_external_id])
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
                             sentence_group_3, sentence_group_3_dup])

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
        matched_entities = entity_matching.match(
            self._session(), _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match([expected_person], matched_entities.people)
        self.assertEqual(4, matched_entities.total_root_entities)
        self.assertEqual(1, matched_entities.error_count)

    def test_matchPersons_noPlaceholders_newPerson(self, _):
        # Arrange 1 - Match
        alias = StatePersonAlias.new_with_defaults(
            state_code=_STATE_CODE, full_name=_FULL_NAME)
        external_id = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID)
        race = StatePersonRace.new_with_defaults(
            state_code=_STATE_CODE, race=Race.WHITE)
        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code=_STATE_CODE, ethnicity=Ethnicity.NOT_HISPANIC)
        person = StatePerson.new_with_defaults(
            gender=Gender.MALE, aliases=[alias], external_ids=[external_id],
            races=[race], ethnicities=[ethnicity])

        expected_person = attr.evolve(person)

        # Act 1 - Match
        matched_entities = entity_matching.match(
            self._session(), _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match([expected_person], matched_entities.people)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_noPlaceholders_updatePersonAttributes(self, _):
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
            races=[db_race], aliases=[db_alias], ethnicities=[db_ethnicity])
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
        matched_entities = entity_matching.match(
            self._session(), _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match([expected_person], matched_entities.people)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_noPlaceholders_partialTreeIngested(self, _):
        # Arrange 1 - Match
        db_person = generate_person(person_id=_ID, full_name=_FULL_NAME)
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
        matched_entities = entity_matching.match(
            self._session(), _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        self.assertEqual(0, matched_entities.error_count)
        self.assert_people_match([expected_person], matched_entities.people)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_noPlaceholders_completeTreeUpdate(self, _):

        # Arrange 1 - Match
        db_person = generate_person(person_id=_ID, full_name=_FULL_NAME)
        db_bond = generate_bond(
            person=db_person,
            bond_id=_ID, state_code=_STATE_CODE, external_id=_EXTERNAL_ID,
            status=BondStatus.PRESENT_WITHOUT_INFO.value, bond_agent='agent')
        db_court_case = generate_court_case(
            person=db_person,
            court_case_id=_ID, external_id=_EXTERNAL_ID, state_code=_STATE_CODE,
            county_code='county_code')
        db_charge_1 = generate_charge(
            person=db_person,
            charge_id=_ID, state_code=_STATE_CODE, external_id=_EXTERNAL_ID,
            description='charge_1',
            status=ChargeStatus.PRESENT_WITHOUT_INFO.value,
            bond=db_bond, court_case=db_court_case)
        db_charge_2 = generate_charge(
            person=db_person,
            charge_id=_ID_2, state_code=_STATE_CODE, external_id=_EXTERNAL_ID_2,
            description='charge_2',
            status=ChargeStatus.PRESENT_WITHOUT_INFO.value,
            bond=db_bond, court_case=db_court_case)
        db_charge_3 = generate_charge(
            person=db_person,
            charge_id=_ID_3, state_code=_STATE_CODE, external_id=_EXTERNAL_ID_3,
            description='charge_3',
            status=ChargeStatus.PRESENT_WITHOUT_INFO.value)
        db_fine = generate_fine(
            person=db_person,
            fine_id=_ID, state_code=_STATE_CODE, external_id=_EXTERNAL_ID,
            county_code='county_code', charges=[db_charge_1, db_charge_2])
        db_assessment = generate_assessment(
            person=db_person,
            assessment_id=_ID, state_code=_STATE_CODE, external_id=_EXTERNAL_ID,
            assessment_metadata='metadata')
        db_assessment_2 = generate_assessment(
            person=db_person,
            assessment_id=_ID_2, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2, assessment_metadata='metadata_2')
        db_agent = generate_agent(
            agent_id=_ID, state_code=_STATE_CODE, external_id=_EXTERNAL_ID,
            full_name='full_name')
        db_agent_2 = generate_agent(
            agent_id=_ID_2, state_code=_STATE_CODE, external_id=_EXTERNAL_ID_2,
            full_name='full_name_2')
        db_agent_po = generate_agent(
            agent_id=_ID_5, state_code=_STATE_CODE, external_id=_EXTERNAL_ID_5,
            full_name='full_name_po')
        db_agent_term = generate_agent(
            agent_id=_ID_6, state_code=_STATE_CODE, external_id=_EXTERNAL_ID_6,
            full_name='full_name_term')
        db_incarceration_incident = \
            generate_incarceration_incident(
                person=db_person,
                incarceration_incident_id=_ID, state_code=_STATE_CODE,
                external_id=_EXTERNAL_ID, incident_details='details',
                responding_officer=db_agent)
        db_parole_decision = generate_parole_decision(
            person=db_person,
            parole_decision_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            decision_outcome=StateParoleDecisionOutcome.EXTERNAL_UNKNOWN.value,
            decision_agents=[db_agent_2])
        db_parole_decision_2 = generate_parole_decision(
            person=db_person,
            parole_decision_id=_ID_2, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2,
            decision_outcome=StateParoleDecisionOutcome.EXTERNAL_UNKNOWN.value,
            decision_agents=[db_agent_2])
        db_incarceration_period = generate_incarceration_period(
            person=db_person,
            incarceration_period_id=_ID, external_id=_EXTERNAL_ID,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY.value,
            state_code=_STATE_CODE, facility='facility',
            incarceration_incidents=[db_incarceration_incident],
            parole_decisions=[db_parole_decision, db_parole_decision_2],
            assessments=[db_assessment])
        db_incarceration_sentence = \
            generate_incarceration_sentence(
                person=db_person,
                incarceration_sentence_id=_ID,
                status=StateSentenceStatus.SERVING.value,
                external_id=_EXTERNAL_ID, state_code=_STATE_CODE,
                county_code='county_code', charges=[db_charge_2, db_charge_3],
                incarceration_periods=[db_incarceration_period])
        db_supervision_violation_response = \
            generate_supervision_violation_response(
                person=db_person,
                supervision_violation_response_id=_ID, state_code=_STATE_CODE,
                external_id=_EXTERNAL_ID,
                decision=
                StateSupervisionViolationResponseDecision.CONTINUANCE.value,
                decision_agents=[db_agent_term])
        db_supervision_violation = generate_supervision_violation(
            person=db_person,
            supervision_violation_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID, is_violent=True,
            supervision_violation_responses=[db_supervision_violation_response])
        db_supervision_period = generate_supervision_period(
            person=db_person,
            supervision_period_id=_ID, external_id=_EXTERNAL_ID,
            status=StateSupervisionPeriodStatus.EXTERNAL_UNKNOWN.value,
            state_code=_STATE_CODE, county_code='county_code',
            assessments=[db_assessment_2],
            supervision_violations=[db_supervision_violation],
            supervising_officer=db_agent_po)
        db_supervision_sentence = generate_supervision_sentence(
            person=db_person,
            supervision_sentence_id=_ID, status=SentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            min_length_days=0,
            supervision_periods=[db_supervision_period])
        db_sentence_group = generate_sentence_group(
            sentence_group_id=_ID,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID, state_code=_STATE_CODE,
            county_code='county_code',
            supervision_sentences=[db_supervision_sentence],
            incarceration_sentences=[db_incarceration_sentence],
            fines=[db_fine])
        db_external_id = generate_external_id(
            person_external_id_id=_ID, state_code=_STATE_CODE,
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
        supervision_violation_response = attr.evolve(
            self.to_entity(db_supervision_violation_response),
            supervision_violation_response_id=None,
            decision=StateSupervisionViolationResponseDecision.EXTENSION,
            decision_agents=[agent_term])
        supervision_violation = attr.evolve(
            self.to_entity(db_supervision_violation),
            supervision_violation_id=None, is_violent=False,
            supervision_violation_responses=[supervision_violation_response])
        supervision_period = attr.evolve(
            self.to_entity(db_supervision_period),
            supervision_period_id=None,
            county_code='county_code-updated',
            assessments=[assessment_2],
            supervision_violations=[supervision_violation],
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
        expected_supervision_violation_response = attr.evolve(
            supervision_violation_response,
            supervision_violation_response_id=_ID,
            decision_agents=[expected_agent_term])
        expected_supervision_violation = attr.evolve(
            supervision_violation, supervision_violation_id=_ID,
            supervision_violation_responses=[
                expected_supervision_violation_response])
        expected_supervision_period = attr.evolve(
            supervision_period, supervision_period_id=_ID,
            assessments=[expected_assessment_2],
            supervision_violations=[expected_supervision_violation],
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
        matched_entities = entity_matching.match(
            self._session(), _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match([expected_person], matched_entities.people)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_ingestedPersonWithNewExternalId(self, _):
        db_external_id = generate_external_id(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID, id_type=_ID_TYPE)
        db_person = generate_person(
            person_id=_ID, full_name=_FULL_NAME,
            external_ids=[db_external_id])

        self._commit_to_db(db_person)

        external_id = attr.evolve(self.to_entity(db_external_id),
                                  person_external_id_id=None)
        external_id_another = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID_2,
            id_type=_ID_TYPE_ANOTHER)
        person = StatePerson.new_with_defaults(
            external_ids=[external_id, external_id_another])

        expected_external_id = attr.evolve(
            external_id, person_external_id_id=_ID)
        expected_external_id_another = attr.evolve(external_id_another)
        expected_person = attr.evolve(self.to_entity(db_person), external_ids=[
            expected_external_id, expected_external_id_another])

        # Act 1 - Match
        matched_entities = entity_matching.match(
            self._session(), _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        self.assertEqual(0, matched_entities.error_count)
        self.assert_people_match([expected_person], matched_entities.people)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_holeInDbGraph(self, _):
        db_person = generate_person(person_id=_ID, full_name=_FULL_NAME)
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
        matched_entities = entity_matching.match(
            self._session(), _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match([expected_person], matched_entities.people)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_holeInIngestedGraph(self, _):
        db_person = generate_person(person_id=_ID, full_name=_FULL_NAME)
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
        matched_entities = entity_matching.match(
            self._session(), _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match([expected_person], matched_entities.people)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_dbPlaceholderSplits(self, _):
        db_person = generate_person(person_id=_ID, full_name=_FULL_NAME)
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
            supervision_sentences=[supervision_sentence_updated])
        sentence_group_new_another = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID_2,
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
        matched_entities = entity_matching.match(
            self._session(), _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match([expected_person], matched_entities.people)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_dbMatchesMultipleIngestedPlaceholders_success(
            self, _):
        db_person = generate_person(person_id=_ID, full_name=_FULL_NAME)
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
            supervision_sentences=[supervision_sentence_updated])

        supervision_sentence_another_updated = attr.evolve(
            self.to_entity(db_supervision_sentence_another),
            supervision_sentence_id=None, min_length_days=11)
        fine = StateFine.new_with_defaults(external_id=_EXTERNAL_ID)
        placeholder_sentence_group_another = \
            StateSentenceGroup.new_with_defaults(
                supervision_sentences=[supervision_sentence_another_updated],
                fines=[fine])
        placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[placeholder_sentence_group,
                             placeholder_sentence_group_another])

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
        expected_placeholder_sentence_group = \
            StateSentenceGroup.new_with_defaults(fines=[expected_fine])
        expected_placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[expected_placeholder_sentence_group])

        # Act 1 - Match
        matched_entities = entity_matching.match(
            self._session(), _STATE_CODE, ingested_people=[placeholder_person])

        # Assert 1 - Match
        self.assert_people_match([expected_person, expected_placeholder_person],
                                 matched_entities.people)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(2, matched_entities.total_root_entities)

    def test_matchPersons_multipleIngestedPersonsMatchToPlaceholderDb(self, _):
        db_person = generate_person(person_id=_ID, full_name=_FULL_NAME)
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
            sentence_groups=[placeholder_sentence_group])

        supervision_sentence_another_updated = attr.evolve(
            self.to_entity(db_supervision_sentence_another),
            supervision_sentence_id=None, min_length_days=11)
        placeholder_sentence_group_another = \
            StateSentenceGroup.new_with_defaults(
                supervision_sentences=[supervision_sentence_another_updated])
        placeholder_person_another = StatePerson.new_with_defaults(
            sentence_groups=[placeholder_sentence_group_another])

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
        matched_entities = entity_matching.match(
            self._session(), _STATE_CODE,
            ingested_people=[placeholder_person, placeholder_person_another])

        # Assert 1 - Match
        self.assert_people_match([expected_person], matched_entities.people)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(2, matched_entities.total_root_entities)

    def test_matchPersons_ingestedPlaceholderSplits(self, _):
        db_person = generate_person(person_id=_ID, full_name=_FULL_NAME)
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
        matched_entities = entity_matching.match(
            self._session(), _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match([expected_person], matched_entities.people)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_multipleHolesInIngestedGraph(self, _):

        # Arrange 1 - Match
        db_person = generate_person(person_id=_ID, full_name=_FULL_NAME)
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
            sentence_groups=[placeholder_sentence_group])

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
        matched_entities = entity_matching.match(
            self._session(), _STATE_CODE, ingested_people=[placeholder_person])

        # Assert 1 - Match
        self.assert_people_match([expected_person], matched_entities.people)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_multipleHolesInDbGraph(self, _):
        # Arrange 1 - Match
        db_placeholder_person = generate_person(person_id=_ID)
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
            supervision_sentences=[supervision_sentence_updated])
        external_id = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID)
        person = StatePerson.new_with_defaults(
            person_id=_ID, full_name=_FULL_NAME, external_ids=[external_id],
            sentence_groups=[sentence_group])

        expected_supervision_sentence_updated = attr.evolve(
            supervision_sentence_updated, supervision_sentence_id=_ID)
        expected_sentence_group = attr.evolve(
            sentence_group,
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
        matched_entities = entity_matching.match(
            self._session(), _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match([expected_person, expected_placeholder_person],
                                 matched_entities.people)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_holesInBothGraphs_ingestedPersonPlaceholder(self, _):
        db_person = generate_person(person_id=_ID, full_name=_FULL_NAME)
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
            supervision_sentences=[supervision_sentence_updated])
        sentence_group_new_another = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID_2,
            supervision_sentences=[supervision_sentence_another_updated])

        placeholder_person = StatePerson.new_with_defaults(
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
        matched_entities = entity_matching.match(
            self._session(), _STATE_CODE, ingested_people=[placeholder_person])

        # Assert 1 - Match
        self.assert_people_match([expected_person], matched_entities.people)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(2, matched_entities.total_root_entities)

    def test_matchPersons_holesInBothGraphs_dbPersonPlaceholder(self, _):
        db_placeholder_person = generate_person(person_id=_ID)
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
            supervision_sentences=[
                supervision_sentence_updated,
                supervision_sentence_another_updated])
        external_id = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID)
        person = StatePerson.new_with_defaults(
            full_name=_FULL_NAME,
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
        expected_external_id = attr.evolve(external_id)
        expected_person = attr.evolve(
            person, external_ids=[expected_external_id],
            sentence_groups=[
                expected_sentence_group, expected_sentence_group_another])

        expected_placeholder_person = attr.evolve(
            self.to_entity(db_placeholder_person), sentence_groups=[])

        # Act 1 - Match
        matched_entities = entity_matching.match(
            self._session(), _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match([expected_person, expected_placeholder_person],
                                 matched_entities.people)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_matchAfterManyIngestedPlaceholders(self, _):
        # Arrange 1 - Match
        db_sentence_group = generate_sentence_group(
            external_id=_EXTERNAL_ID, sentence_group_id=_ID)
        db_external_id = generate_external_id(
            person_external_id_id=_ID,
            state_code=_STATE_CODE, id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        db_person = generate_person(
            person_id=_ID, sentence_groups=[db_sentence_group],
            external_ids=[db_external_id])

        self._commit_to_db(db_person)

        incarceration_incident = StateIncarcerationIncident.new_with_defaults(
            external_id=_EXTERNAL_ID)
        placeholder_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_incidents=[incarceration_incident])
        placeholder_incarceration_sentence = \
            StateIncarcerationSentence.new_with_defaults(
                incarceration_periods=[placeholder_incarceration_period])
        sentence_group = attr.evolve(
            self.to_entity(db_sentence_group), sentence_group_id=None,
            incarceration_sentences=[placeholder_incarceration_sentence])
        external_id = attr.evolve(self.to_entity(db_external_id),
                                  person_external_id_id=None)
        person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group], external_ids=[external_id])

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
        matched_entities = entity_matching.match(
            self._session(), _STATE_CODE, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match([expected_person], matched_entities.people)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    # TODO(2037): Move test to state specific file.
    def test_runMatch_multipleExternalIdsOnRootEntity(self, _):
        db_external_id = generate_external_id(
            person_external_id_id=_ID, external_id=_EXTERNAL_ID,
            state_code=_US_ND,
            id_type=_ID_TYPE)
        db_external_id_2 = generate_external_id(
            person_external_id_id=_ID_2, external_id=_EXTERNAL_ID_2,
            state_code=_US_ND,
            id_type=_ID_TYPE_ANOTHER)
        db_person = generate_person(
            person_id=_ID, external_ids=[db_external_id, db_external_id_2])
        self._commit_to_db(db_person)

        external_id = attr.evolve(
            self.to_entity(db_external_id),
            person_external_id_id=None)
        external_id_2 = attr.evolve(
            self.to_entity(db_external_id_2),
            person_external_id_id=None)
        race = StatePersonRace.new_with_defaults(race=Race.WHITE)
        person = StatePerson.new_with_defaults(
            external_ids=[external_id, external_id_2], races=[race])

        expected_race = attr.evolve(race)
        expected_person = attr.evolve(self.to_entity(db_person),
                                      races=[expected_race])

        # Act 1 - Match
        matched_entities = entity_matching.match(
            self._session(), _US_ND, ingested_people=[person])

        self.assert_people_match([expected_person], matched_entities.people)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_runMatch_checkPlaceholdersWhenNoRootEntityMatch(self, _):
        """Tests that ingested people are matched with DB placeholder people
        when a root entity match doesn't exist. Specific to US_ND.
        """
        db_placeholder_person = generate_person(person_id=_ID)
        db_sg = generate_sentence_group(
            sentence_group_id=_ID, state_code=_US_ND, external_id=_EXTERNAL_ID)
        db_placeholder_person.sentence_groups = [db_sg]
        db_placeholder_person_other_state = generate_person(person_id=_ID_2)
        db_sg_other_state = generate_sentence_group(
            sentence_group_id=_ID_2, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID)
        db_placeholder_person_other_state.sentence_groups = [db_sg_other_state]

        self._commit_to_db(
            db_placeholder_person, db_placeholder_person_other_state)

        sg = attr.evolve(
            self.to_entity(db_sg), sentence_group_id=None)
        external_id = StatePersonExternalId.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_US_ND,
            id_type=_ID_TYPE)
        person = attr.evolve(
            self.to_entity(db_placeholder_person), person_id=None,
            full_name=_FULL_NAME,
            external_ids=[external_id],
            sentence_groups=[sg])

        expected_sg = attr.evolve(self.to_entity(db_sg))
        expected_external_id = attr.evolve(external_id)
        expected_person = attr.evolve(
            person,
            external_ids=[expected_external_id],
            sentence_groups=[expected_sg])
        expected_placeholder_person = attr.evolve(
            self.to_entity(db_placeholder_person), sentence_groups=[])
        expected_placeholder_person_other_state = self.to_entity(
            db_placeholder_person_other_state)

        # Act 1 - Match
        matched_entities = entity_matching.match(
            self._session(), _US_ND, ingested_people=[person])

        # Assert 1 - Match
        self.assert_people_match(
            [expected_person, expected_placeholder_person,
             expected_placeholder_person_other_state],
            matched_entities.people)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_runMatch_sameEntities_noDuplicates(self, _):
        db_placeholder_person = generate_person(person_id=_ID)
        db_is = generate_incarceration_sentence(
            person=db_placeholder_person,
            incarceration_sentence_id=_ID, date_imposed=_DATE_1)
        db_sg = generate_sentence_group(
            sentence_group_id=_ID, external_id=_EXTERNAL_ID,
            incarceration_sentences=[db_is])
        db_placeholder_person.sentence_groups = [db_sg]

        self._commit_to_db(db_placeholder_person)

        inc_s = attr.evolve(self.to_entity(db_is),
                            incarceration_sentence_id=None)
        sg = attr.evolve(
            self.to_entity(db_sg), sentence_group_id=None,
            incarceration_sentences=[inc_s])
        placeholder_person = attr.evolve(
            self.to_entity(db_placeholder_person), person_id=None,
            sentence_groups=[sg])

        expected_is = attr.evolve(self.to_entity(db_is))
        expected_sg = attr.evolve(self.to_entity(db_sg),
                                  incarceration_sentences=[expected_is])
        expected_placeholder_person = attr.evolve(
            self.to_entity(db_placeholder_person),
            sentence_groups=[expected_sg])

        # Act 1 - Match
        matched_entities = entity_matching.match(
            self._session(), _STATE_CODE, ingested_people=[placeholder_person])

        # Assert 1 - Match
        self.assert_people_match([expected_placeholder_person],
                                 matched_entities.people)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_runMatch_associateSvrsToIps(self, _):
        # Arrange 1 - Match
        db_placeholder_person = generate_person(person_id=_ID)
        db_ip_1 = generate_incarceration_period(
            person=db_placeholder_person,
            state_code=_STATE_CODE,
            incarceration_period_id=_ID,
            admission_date=_DATE_2,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION.value)
        db_ip_2 = generate_incarceration_period(
            person=db_placeholder_person,
            state_code=_STATE_CODE,
            incarceration_period_id=_ID_2,
            admission_date=_DATE_3,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION.value)
        db_placeholder_is = generate_incarceration_sentence(
            person=db_placeholder_person,
            state_code=_STATE_CODE,
            incarceration_sentence_id=_ID,
            incarceration_periods=[db_ip_1, db_ip_2])

        db_sg = generate_sentence_group(
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            sentence_group_id=_ID,
            incarceration_sentences=[db_placeholder_is],
        )

        db_placeholder_person.sentence_groups = [db_sg]

        self._commit_to_db(db_placeholder_person)

        svr_1 = StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE,
            response_date=_DATE_2 + datetime.timedelta(days=1),
            revocation_type=
            StateSupervisionViolationResponseRevocationType.REINCARCERATION)
        svr_2 = StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE,
            response_date=_DATE_3 - datetime.timedelta(days=1),
            revocation_type=
            StateSupervisionViolationResponseRevocationType.REINCARCERATION)
        placeholder_sv = StateSupervisionViolation.new_with_defaults(
            state_code=_STATE_CODE,
            supervision_violation_responses=[svr_1, svr_2])
        placeholder_sp = StateSupervisionPeriod.new_with_defaults(
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
            state_code=_STATE_CODE,
            supervision_violations=[placeholder_sv])
        placeholder_ss = StateSupervisionSentence.new_with_defaults(
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            state_code=_STATE_CODE,
            supervision_periods=[placeholder_sp])
        sg = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            supervision_sentences=[placeholder_ss])
        placeholder_person = StatePerson.new_with_defaults(sentence_groups=[sg])

        expected_svr_1 = attr.evolve(svr_1)
        expected_svr_2 = attr.evolve(svr_2)
        expected_placeholder_sv = attr.evolve(
            placeholder_sv,
            supervision_violation_responses=[expected_svr_1, expected_svr_2])
        expected_placeholder_sp = attr.evolve(
            placeholder_sp, supervision_violations=[expected_placeholder_sv])
        expected_placeholder_ss = attr.evolve(
            placeholder_ss, supervision_periods=[expected_placeholder_sp])

        expected_ip_1 = attr.evolve(
            self.to_entity(db_ip_1),
            source_supervision_violation_response=expected_svr_1)
        expected_ip_2 = attr.evolve(
            self.to_entity(db_ip_2),
            source_supervision_violation_response=expected_svr_2)

        expected_placeholder_is = attr.evolve(
            self.to_entity(db_placeholder_is),
            incarceration_periods=[expected_ip_1, expected_ip_2])
        expected_placeholder_sg = attr.evolve(
            self.to_entity(db_sg),
            supervision_sentences=[expected_placeholder_ss],
            incarceration_sentences=[expected_placeholder_is])
        expected_placeholder_person = attr.evolve(
            placeholder_person, person_id=_ID,
            sentence_groups=[expected_placeholder_sg])

        # Act 1 - Match
        matched_entities = entity_matching.match(
            self._session(), _STATE_CODE, ingested_people=[placeholder_person])

        # Assert 1 - Match
        self.assert_people_match([expected_placeholder_person],
                                 matched_entities.people)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_match_mergeIncomingIncarcerationSentences(self, _):
        # Arrange 1 - Match
        db_person = schema.StatePerson(person_id=_ID, full_name=_FULL_NAME)
        db_incarceration_sentence = \
            schema.StateIncarcerationSentence(
                state_code=_STATE_CODE,
                incarceration_sentence_id=_ID,
                status=StateIncarcerationPeriodStatus.EXTERNAL_UNKNOWN.value,
                external_id=_EXTERNAL_ID,
                person=db_person)
        db_sentence_group = schema.StateSentenceGroup(
            state_code=_STATE_CODE,
            sentence_group_id=_ID,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[db_incarceration_sentence],
            person=db_person)
        db_external_id = schema.StatePersonExternalId(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID,
            person=db_person)
        db_person.sentence_groups = [db_sentence_group]
        db_person.external_ids = [db_external_id]

        self._commit_to_db(db_person)

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            facility=_FACILITY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            admission_date=datetime.date(year=2019, month=1, day=1),
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION)
        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2,
            facility=_FACILITY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            release_date=datetime.date(year=2019, month=1, day=2),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)
        incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_3,
            facility=_FACILITY_2,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            admission_date=datetime.date(year=2019, month=1, day=2),
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.TRANSFER)
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID,
            incarceration_periods=[incarceration_period,
                                   incarceration_period_2,
                                   incarceration_period_3])
        placeholder_sentence_group = StateSentenceGroup.new_with_defaults(
            incarceration_sentences=[incarceration_sentence])
        placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[placeholder_sentence_group])

        expected_person = StatePerson.new_with_defaults(
            person_id=_ID, full_name=_FULL_NAME)
        expected_complete_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                external_id=_EXTERNAL_ID + '|' + _EXTERNAL_ID_2,
                state_code=_STATE_CODE,
                facility=_FACILITY,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                admission_date=datetime.date(year=2019, month=1, day=1),
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                release_date=datetime.date(year=2019, month=1, day=2),
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)
        expected_incomplete_incarceration_period = attr.evolve(
            incarceration_period_3)
        expected_incarceration_sentence = \
            StateIncarcerationSentence.new_with_defaults(
                incarceration_sentence_id=_ID,
                status=StateSentenceStatus.EXTERNAL_UNKNOWN,
                state_code=_STATE_CODE,
                external_id=_EXTERNAL_ID,
                incarceration_periods=[
                    expected_complete_incarceration_period,
                    expected_incomplete_incarceration_period])
        expected_sentence_group = StateSentenceGroup.new_with_defaults(
            sentence_group_id=_ID,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[expected_incarceration_sentence])
        expected_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        expected_person.external_ids = [expected_external_id]
        expected_person.sentence_groups = [expected_sentence_group]

        # Act 1 - Match
        act_session = self._session()
        matched_entities = entity_matching.match(
            act_session, _STATE_CODE, ingested_people=[placeholder_person])

        # Assert 1 - Match
        self.assert_people_match([expected_person], matched_entities.people)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

        # Arrange 2 - Commit
        expected_complete_incarceration_period.incarceration_period_id = 1
        expected_incomplete_incarceration_period.incarceration_period_id = 2

        # Act 2 - Commit
        act_session.commit()
        act_session.close()

        # Assert 2 - Commit
        assert_session = self._session()
        result_db_people = dao.read_people(assert_session)

        self.assert_people_match([expected_person], result_db_people)
        assert_session.close()

    def test_matchPersons_mergeIncompleteIncarcerationPeriodOntoComplete(
            self, _):
        """Tests correct matching behavior when an incomplete period is ingested
        and a matching complete period is in the db.
        """
        # Arrange 1 - Match
        db_person = generate_person(person_id=_ID, full_name=_FULL_NAME)
        db_complete_incarceration_period = \
            generate_incarceration_period(
                person=db_person,
                incarceration_period_id=_ID_2,
                state_code=_US_ND,
                external_id=_EXTERNAL_ID + '|' + _EXTERNAL_ID_2,
                facility=_FACILITY,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY.value,
                admission_date=datetime.date(year=2018, month=1, day=1),
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION.value,
                release_date=datetime.date(year=2018, month=1, day=2),
                release_reason=
                StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED.value)

        db_incarceration_sentence = \
            generate_incarceration_sentence(
                person=db_person,
                state_code=_US_ND,
                incarceration_sentence_id=_ID,
                status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
                external_id=_EXTERNAL_ID,
                incarceration_periods=[db_complete_incarceration_period])
        db_sentence_group = generate_sentence_group(
            state_code=_US_ND,
            sentence_group_id=_ID,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[db_incarceration_sentence])
        db_external_id = generate_external_id(
            person_external_id_id=_ID,
            state_code=_US_ND,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        db_person.sentence_groups = [db_sentence_group]
        db_person.external_ids = [db_external_id]

        self._commit_to_db(db_person)

        incomplete_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                state_code=_US_ND,
                external_id=_EXTERNAL_ID_2,
                release_date=datetime.date(year=2018, month=1, day=2),
                release_reason=
                StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                county_code=_COUNTY_CODE)

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            incarceration_periods=[incomplete_incarceration_period])
        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[incarceration_sentence])
        placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group])

        expected_complete_incarceration_period = attr.evolve(
            self.to_entity(db_complete_incarceration_period),
            county_code=_COUNTY_CODE)

        expected_incarceration_sentence = \
            StateIncarcerationSentence.new_with_defaults(
                incarceration_sentence_id=_ID,
                status=StateSentenceStatus.EXTERNAL_UNKNOWN,
                state_code=_US_ND,
                external_id=_EXTERNAL_ID,
                incarceration_periods=[
                    expected_complete_incarceration_period])
        expected_sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            incarceration_sentences=[expected_incarceration_sentence])
        expected_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=_ID,
            state_code=_US_ND,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        expected_person = StatePerson.new_with_defaults(
            person_id=_ID, full_name=_FULL_NAME,
            external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group])

        # Act 1 - Match
        act_session = self._session()
        matched_entities = entity_matching.match(
            act_session, _US_ND, ingested_people=[placeholder_person])

        # Assert 1 - Match
        self.assert_people_match([expected_person], matched_entities.people)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

        # Arrange 2 - Commit
        # Do nothing - no DB ids should change with commit

        # Act 2 - Commit
        act_session.commit()
        act_session.close()

        # Assert 2 - Commit
        assert_session = self._session()
        result_db_people = dao.read_people(assert_session)

        self.assert_people_match([expected_person], result_db_people)
        assert_session.close()

    def test_matchPersons_mergeCompleteIncarcerationPeriodOntoIncomplete(
            self, _):
        """Tests correct matching behavior when a complete period is ingested
        and a matching incomplete period is in the db.
        """
        # Arrange 1 - Match
        db_person = generate_person(person_id=_ID, full_name=_FULL_NAME)
        db_incomplete_incarceration_period = \
            generate_incarceration_period(
                person=db_person,
                state_code=_US_ND,
                incarceration_period_id=_ID,
                external_id=_EXTERNAL_ID,
                facility=_FACILITY,
                incarceration_type=StateIncarcerationType.STATE_PRISON.value,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY.value,
                admission_date=datetime.date(year=2019, month=1, day=1),
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION.value)

        db_incarceration_sentence = \
            generate_incarceration_sentence(
                person=db_person,
                state_code=_US_ND,
                incarceration_sentence_id=_ID,
                status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
                external_id=_EXTERNAL_ID,
                incarceration_periods=[db_incomplete_incarceration_period])
        db_sentence_group = generate_sentence_group(
            state_code=_US_ND,
            sentence_group_id=_ID,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[db_incarceration_sentence])
        db_external_id = generate_external_id(
            person_external_id_id=_ID,
            state_code=_US_ND,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        db_person.sentence_groups = [db_sentence_group]
        db_person.external_ids = [db_external_id]

        self._commit_to_db(db_person)

        complete_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                state_code=_US_ND,
                external_id=_EXTERNAL_ID + '|' + _EXTERNAL_ID_2,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                facility=_FACILITY,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                admission_date=datetime.date(year=2019, month=1, day=1),
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=datetime.date(year=2019, month=1, day=2),
                release_reason=
                StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            incarceration_periods=[complete_incarceration_period])
        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN,
            incarceration_sentences=[incarceration_sentence])
        placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group])

        expected_complete_incarceration_period = attr.evolve(
            complete_incarceration_period, incarceration_period_id=_ID)

        expected_incarceration_sentence = \
            StateIncarcerationSentence.new_with_defaults(
                incarceration_sentence_id=_ID,
                status=StateSentenceStatus.EXTERNAL_UNKNOWN,
                state_code=_US_ND,
                external_id=_EXTERNAL_ID,
                incarceration_periods=[expected_complete_incarceration_period])
        expected_sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            incarceration_sentences=[expected_incarceration_sentence])
        expected_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=_ID,
            state_code=_US_ND,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        expected_person = StatePerson.new_with_defaults(
            person_id=_ID, full_name=_FULL_NAME,
            external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group])

        # Act 1 - Match
        act_session = self._session()
        matched_entities = entity_matching.match(
            act_session, _US_ND, ingested_people=[placeholder_person])

        # Assert 1 - Match
        self.assert_people_match([expected_person], matched_entities.people)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

        # Arrange 2 - Commit
        # Do nothing - no DB ids should change with commit

        # Act 2 - Commit
        act_session.commit()
        act_session.close()

        # Assert 2 - Commit
        assert_session = self._session()
        result_db_people = dao.read_people(assert_session)

        self.assert_people_match([expected_person], result_db_people)
        assert_session.close()

    def test_matchPersons_mergeCompleteIncarcerationPeriods(self, _):
        """Tests correct matching behavior when a complete period is ingested
        and a matching complete period is in the db.
        """
        # Arrange 1 - Match
        db_person = generate_person(person_id=_ID, full_name=_FULL_NAME)
        db_complete_incarceration_period = \
            generate_incarceration_period(
                person=db_person,
                incarceration_period_id=_ID,
                state_code=_US_ND,
                external_id=_EXTERNAL_ID + '|' + _EXTERNAL_ID_2,
                facility=_FACILITY,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY.value,
                admission_date=datetime.date(year=2018, month=1, day=1),
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION.value,
                release_date=datetime.date(year=2018, month=1, day=2),
                release_reason=
                StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED.value)

        db_incarceration_sentence = \
            generate_incarceration_sentence(
                person=db_person,
                state_code=_US_ND,
                incarceration_sentence_id=_ID,
                status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
                external_id=_EXTERNAL_ID,
                incarceration_periods=[db_complete_incarceration_period])
        db_sentence_group = generate_sentence_group(
            state_code=_US_ND,
            sentence_group_id=_ID,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[db_incarceration_sentence])
        db_external_id = generate_external_id(
            person_external_id_id=_ID,
            state_code=_US_ND,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        db_person.sentence_groups = [db_sentence_group]
        db_person.external_ids = [db_external_id]

        self._commit_to_db(db_person)

        updated_incarceration_period = attr.evolve(
            self.to_entity(db_complete_incarceration_period),
            incarceration_period_id=None,
            county_code=_COUNTY_CODE)
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_US_ND,
            incarceration_periods=[updated_incarceration_period])
        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[incarceration_sentence])
        placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group])

        expected_complete_incarceration_period = attr.evolve(
            updated_incarceration_period, incarceration_period_id=_ID)

        expected_incarceration_sentence = \
            StateIncarcerationSentence.new_with_defaults(
                incarceration_sentence_id=_ID,
                status=StateSentenceStatus.EXTERNAL_UNKNOWN,
                state_code=_US_ND,
                external_id=_EXTERNAL_ID,
                incarceration_periods=[expected_complete_incarceration_period])
        expected_sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            incarceration_sentences=[expected_incarceration_sentence])
        expected_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=_ID,
            state_code=_US_ND,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        expected_person = StatePerson.new_with_defaults(
            person_id=_ID, full_name=_FULL_NAME,
            external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group])

        # Act 1 - Match
        act_session = self._session()
        matched_entities = entity_matching.match(
            act_session, _US_ND, ingested_people=[placeholder_person])

        # Assert 1 - Match
        self.assert_people_match([expected_person], matched_entities.people)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

        # Arrange 2 - Commit
        # Do nothing - no DB ids should change with commit

        # Act 2 - Commit
        act_session.commit()
        act_session.close()

        # Assert 2 - Commit
        assert_session = self._session()
        result_db_people = dao.read_people(assert_session)

        self.assert_people_match([expected_person], result_db_people)
        assert_session.close()

    def test_matchPersons_mergeIncompleteIncarcerationPeriods(self, _):
        """Tests correct matching behavior when an incomplete period is ingested
        and a matching incomplete period is in the db.
        """
        # Arrange 1 - Match
        db_person = generate_person(person_id=_ID, full_name=_FULL_NAME)
        db_incomplete_incarceration_period = \
            generate_incarceration_period(
                person=db_person,
                incarceration_period_id=_ID,
                state_code=_US_ND,
                external_id=_EXTERNAL_ID_3,
                facility=_FACILITY,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY.value,
                admission_date=datetime.date(year=2019, month=1, day=1),
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION.value)
        db_complete_incarceration_period = \
            generate_incarceration_period(
                person=db_person,
                incarceration_period_id=_ID_2,
                state_code=_US_ND,
                external_id=_EXTERNAL_ID + '|' + _EXTERNAL_ID_2,
                facility=_FACILITY,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY.value,
                admission_date=datetime.date(year=2018, month=1, day=1),
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION.value,
                release_date=datetime.date(year=2018, month=1, day=2),
                release_reason=
                StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED.value)

        db_incarceration_sentence = \
            generate_incarceration_sentence(
                person=db_person,
                state_code=_US_ND,
                incarceration_sentence_id=_ID,
                status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
                external_id=_EXTERNAL_ID,
                incarceration_periods=[
                    db_complete_incarceration_period,
                    db_incomplete_incarceration_period])
        db_sentence_group = generate_sentence_group(
            state_code=_US_ND,
            sentence_group_id=_ID,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[db_incarceration_sentence])
        db_external_id = generate_external_id(
            person_external_id_id=_ID,
            state_code=_US_ND,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        db_person.sentence_groups = [db_sentence_group]
        db_person.external_ids = [db_external_id]

        self._commit_to_db(db_person)

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID_4,
            facility=_FACILITY,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            release_date=datetime.date(year=2019, month=1, day=2),
            release_reason=
            StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_US_ND,
            incarceration_periods=[incarceration_period])
        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[incarceration_sentence])
        placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group])

        expected_complete_incarceration_period = attr.evolve(
            self.to_entity(db_complete_incarceration_period))
        expected_new_complete_incarceration_period = attr.evolve(
            self.to_entity(db_incomplete_incarceration_period),
            external_id=_EXTERNAL_ID_3 + '|' + _EXTERNAL_ID_4,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            release_date=datetime.date(year=2019, month=1, day=2),
            release_reason=
            StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

        expected_incarceration_sentence = \
            StateIncarcerationSentence.new_with_defaults(
                incarceration_sentence_id=_ID,
                status=StateSentenceStatus.EXTERNAL_UNKNOWN,
                state_code=_US_ND,
                external_id=_EXTERNAL_ID,
                incarceration_periods=[
                    expected_new_complete_incarceration_period,
                    expected_complete_incarceration_period])
        expected_sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            incarceration_sentences=[expected_incarceration_sentence])
        expected_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=_ID,
            state_code=_US_ND,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        expected_person = StatePerson.new_with_defaults(
            person_id=_ID, full_name=_FULL_NAME,
            external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group])

        # Act 1 - Match
        act_session = self._session()
        matched_entities = entity_matching.match(
            act_session, _US_ND, ingested_people=[placeholder_person])

        # Assert 1 - Match
        self.assert_people_match([expected_person], matched_entities.people)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

        # Arrange 2 - Commit
        # Do nothing - no DB ids should change with commit

        # Act 2 - Commit
        act_session.commit()
        act_session.close()

        # Assert 2 - Commit
        assert_session = self._session()
        result_db_people = dao.read_people(assert_session)

        self.assert_people_match([expected_person], result_db_people)
        assert_session.close()

    def test_matchPersons_dontMergePeriodsFromDifferentStates(self, _):
        """Tests that incarceration periods don't match when either doesn't have
        US_ND as its state code.
        """
        # Arrange 1 - Match
        db_person = generate_person(person_id=_ID, full_name=_FULL_NAME)
        db_incomplete_incarceration_period = \
            generate_incarceration_period(
                person=db_person,
                incarceration_period_id=_ID,
                state_code=_US_ND,
                external_id=_EXTERNAL_ID_3,
                facility=_FACILITY,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY.value,
                admission_date=datetime.date(year=2019, month=1, day=1),
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION.value)

        db_incarceration_sentence = \
            generate_incarceration_sentence(
                person=db_person,
                state_code=_US_ND,
                incarceration_sentence_id=_ID,
                status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
                external_id=_EXTERNAL_ID,
                incarceration_periods=[db_incomplete_incarceration_period])
        db_sentence_group = generate_sentence_group(
            state_code=_US_ND,
            sentence_group_id=_ID,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[db_incarceration_sentence])
        db_external_id = generate_external_id(
            person_external_id_id=_ID,
            state_code=_US_ND,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        db_person.sentence_groups = [db_sentence_group]
        db_person.external_ids = [db_external_id]

        self._commit_to_db(db_person)

        incarceration_period_different_state = \
            StateIncarcerationPeriod.new_with_defaults(
                state_code=_STATE_CODE,
                external_id=_EXTERNAL_ID_4,
                facility=_FACILITY,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                release_date=datetime.date(year=2019, month=1, day=2),
                release_reason=
                StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_US_ND,
            incarceration_periods=[incarceration_period_different_state])
        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[incarceration_sentence])
        placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group])

        expected_incarceration_period = attr.evolve(
            self.to_entity(db_incomplete_incarceration_period))
        expected_incarceration_period_different_state = attr.evolve(
            incarceration_period_different_state)

        expected_incarceration_sentence = \
            StateIncarcerationSentence.new_with_defaults(
                incarceration_sentence_id=_ID,
                status=StateSentenceStatus.EXTERNAL_UNKNOWN,
                state_code=_US_ND,
                external_id=_EXTERNAL_ID,
                incarceration_periods=[
                    expected_incarceration_period,
                    expected_incarceration_period_different_state])
        expected_sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            incarceration_sentences=[expected_incarceration_sentence])
        expected_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=_ID,
            state_code=_US_ND,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        expected_person = StatePerson.new_with_defaults(
            person_id=_ID, full_name=_FULL_NAME,
            external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group])

        # Act 1 - Match
        act_session = self._session()
        matched_entities = entity_matching.match(
            act_session, _US_ND, ingested_people=[placeholder_person])

        # Assert 1 - Match
        self.assert_people_match([expected_person], matched_entities.people)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

    def test_matchPersons_temporaryCustodyPeriods(self, mock_get_region):
        # Arrange 1 - Match
        fake_region = create_autospec(Region)
        overrides_builder = EnumOverrides.Builder()
        overrides_builder.add(
            'ADM', StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION)
        fake_region.get_enum_overrides.return_value = overrides_builder.build()
        mock_get_region.return_value = fake_region

        db_person = generate_person(person_id=_ID, full_name=_FULL_NAME)
        db_incomplete_temporary_custody = \
            generate_incarceration_period(
                person=db_person,
                incarceration_period_id=_ID,
                state_code=_US_ND,
                external_id=_EXTERNAL_ID,
                facility=_FACILITY,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY.value,
                incarceration_type=StateIncarcerationType.COUNTY_JAIL.value,
                admission_date=datetime.date(year=2018, month=1, day=1),
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION.value,
                admission_reason_raw_text='ADM')

        db_incarceration_sentence = \
            generate_incarceration_sentence(
                person=db_person,
                state_code=_US_ND,
                incarceration_sentence_id=_ID,
                status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
                external_id=_EXTERNAL_ID,
                incarceration_periods=[db_incomplete_temporary_custody])
        db_sentence_group = generate_sentence_group(
            state_code=_US_ND,
            sentence_group_id=_ID,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[db_incarceration_sentence])
        db_external_id = generate_external_id(
            person_external_id_id=_ID,
            state_code=_US_ND,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        db_person.sentence_groups = [db_sentence_group]
        db_person.external_ids = [db_external_id]

        self._commit_to_db(db_person)

        new_temporary_custody_period = \
            StateIncarcerationPeriod.new_with_defaults(
                state_code=_US_ND,
                external_id=_EXTERNAL_ID_2,
                facility=_FACILITY,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                incarceration_type=StateIncarcerationType.COUNTY_JAIL,
                release_date=datetime.date(year=2018, month=1, day=2),
                release_reason=
                StateIncarcerationPeriodAdmissionReason.TRANSFER)

        new_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID_3,
            facility=_FACILITY_2,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=datetime.date(year=2018, month=1, day=3),
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.TRANSFER)
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID,
            state_code=_US_ND,
            incarceration_periods=[new_temporary_custody_period,
                                   new_incarceration_period])
        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[incarceration_sentence])
        placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group])

        expected_complete_temporary_custody = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=_ID,
                state_code=_US_ND,
                external_id=_EXTERNAL_ID + '|' + _EXTERNAL_ID_2,
                facility=_FACILITY,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                incarceration_type=StateIncarcerationType.COUNTY_JAIL,
                admission_date=datetime.date(year=2018, month=1, day=1),
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
                admission_reason_raw_text='ADM',
                release_date=datetime.date(year=2018, month=1, day=2),
                release_reason=
                StateIncarcerationPeriodReleaseReason
                .RELEASED_FROM_TEMPORARY_CUSTODY)
        expected_new_period = attr.evolve(
            new_incarceration_period,
            incarceration_period_id=_ID_2,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION)

        expected_incarceration_sentence = \
            StateIncarcerationSentence.new_with_defaults(
                incarceration_sentence_id=_ID,
                status=StateSentenceStatus.EXTERNAL_UNKNOWN,
                state_code=_US_ND,
                external_id=_EXTERNAL_ID,
                incarceration_periods=[
                    expected_complete_temporary_custody, expected_new_period])
        expected_sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            incarceration_sentences=[expected_incarceration_sentence])
        expected_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=_ID,
            state_code=_US_ND,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        expected_person = StatePerson.new_with_defaults(
            person_id=_ID, full_name=_FULL_NAME,
            external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group])

        # Act 1 - Match
        act_session = self._session()
        matched_entities = entity_matching.match(
            act_session, _US_ND, ingested_people=[placeholder_person])

        # Assert 1 - Match
        self.assert_people_match([expected_person], matched_entities.people)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

        # Arrange 2 - Commit
        # Do nothing - no DB ids should change with commit

        # Act 2 - Commit
        act_session.commit()
        act_session.close()

        # Assert 2 - Commit
        assert_session = self._session()
        result_db_people = dao.read_people(assert_session)

        self.assert_people_match([expected_person], result_db_people)
        assert_session.close()

    def test_matchPersons_mergeIngestedAndDbIncarcerationPeriods_reverse(
            self, _):
        """Tests that periods are correctly merged when the release period is
        ingested before the admission period."""
        # Arrange 1 - Match
        db_person = generate_person(person_id=_ID, full_name=_FULL_NAME)
        db_incomplete_incarceration_period = generate_incarceration_period(
            person=db_person,
            incarceration_period_id=_ID,
            state_code=_US_ND,
            external_id=_EXTERNAL_ID_4,
            facility=_FACILITY,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY.value,
            release_date=datetime.date(year=2019, month=1, day=2),
            release_reason=
            StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED.value)
        db_complete_incarceration_period = \
            generate_incarceration_period(
                person=db_person,
                incarceration_period_id=_ID_2,
                state_code=_US_ND,
                external_id=_EXTERNAL_ID + '|' + _EXTERNAL_ID_2,
                facility=_FACILITY,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY.value,
                admission_date=datetime.date(year=2018, month=1, day=1),
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION.value,
                release_date=datetime.date(year=2018, month=1, day=2),
                release_reason=
                StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED.value)

        db_incarceration_sentence = \
            generate_incarceration_sentence(
                person=db_person,
                state_code=_US_ND,
                incarceration_sentence_id=_ID,
                status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
                external_id=_EXTERNAL_ID,
                incarceration_periods=[
                    db_complete_incarceration_period,
                    db_incomplete_incarceration_period])
        db_sentence_group = generate_sentence_group(
            state_code=_US_ND,
            sentence_group_id=_ID,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[db_incarceration_sentence])
        db_external_id = generate_external_id(
            person_external_id_id=_ID,
            state_code=_US_ND,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        db_person.sentence_groups = [db_sentence_group]
        db_person.external_ids = [db_external_id]

        self._commit_to_db(db_person)

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=_ID,
            state_code=_US_ND,
            external_id=_EXTERNAL_ID_3,
            facility=_FACILITY,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            admission_date=datetime.date(year=2019, month=1, day=1),
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION)
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_periods=[incarceration_period])
        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code=_US_ND,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[incarceration_sentence])
        placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group])

        expected_complete_incarceration_period = attr.evolve(
            self.to_entity(db_complete_incarceration_period))
        expected_new_complete_incarceration_period = attr.evolve(
            self.to_entity(db_incomplete_incarceration_period),
            external_id=_EXTERNAL_ID_3 + '|' + _EXTERNAL_ID_4,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=2019, month=1, day=1),
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION)

        expected_incarceration_sentence = \
            StateIncarcerationSentence.new_with_defaults(
                incarceration_sentence_id=_ID,
                status=StateSentenceStatus.EXTERNAL_UNKNOWN,
                state_code=_US_ND,
                external_id=_EXTERNAL_ID,
                incarceration_periods=[
                    expected_new_complete_incarceration_period,
                    expected_complete_incarceration_period])
        expected_sentence_group = attr.evolve(
            self.to_entity(db_sentence_group),
            incarceration_sentences=[expected_incarceration_sentence])
        expected_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=_ID,
            state_code=_US_ND,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        expected_person = StatePerson.new_with_defaults(
            person_id=_ID, full_name=_FULL_NAME,
            external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group])

        # Act 1 - Match
        act_session = self._session()
        matched_entities = entity_matching.match(
            act_session, _US_ND, ingested_people=[placeholder_person])

        # Assert 1 - Match
        self.assert_people_match([expected_person], matched_entities.people)
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(1, matched_entities.total_root_entities)

        # Arrange 2 - Commit
        # Do nothing - no DB ids should change with commit

        # Act 2 - Commit
        act_session.commit()
        act_session.close()

        # Assert 2 - Commit
        assert_session = self._session()
        result_db_people = dao.read_people(assert_session)

        self.assert_people_match([expected_person], result_db_people)
        assert_session.close()

    def test_mergeMultiParentEntities(self, _):
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
            sentence_groups=[sentence_group])

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
        act_session = self._session()
        matched_entities = entity_matching.match(
            act_session, _STATE_CODE, [person])

        # Assert 1 - Match
        self.assert_people_match([expected_person], matched_entities.people)
        sentence_group = matched_entities.people[0].sentence_groups[0]
        found_charge = sentence_group.incarceration_sentences[0].charges[0]
        found_charge_2 = sentence_group.incarceration_sentences[1].charges[0]
        found_charge_3 = sentence_group.incarceration_sentences[2].charges[0]
        self.assertEqual(
            id(found_charge), id(found_charge_2), id(found_charge_3))

        # Arrange 2 - Commit
        expected_db_person = attr.evolve(expected_person)
        expected_db_person.person_id = 1
        sg = expected_db_person.sentence_groups[0]
        sg.sentence_group_id = 1
        sg.incarceration_sentences[0].incarceration_sentence_id = 1
        sg.incarceration_sentences[1].incarceration_sentence_id = 2
        sg.incarceration_sentences[2].incarceration_sentence_id = 3

        # Act 2 - Commit
        act_session.commit()
        act_session.close()

        # Assert 2 - Commit
        assert_session = self._session()
        result_db_people = dao.read_people(assert_session)

        self.assert_people_match([expected_db_person], result_db_people)
        assert_session.close()

    def test_mergeMultiParentEntities_mergeChargesAndCourtCases(self, _):
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
            sentence_groups=[sentence_group])

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
        act_session = self._session()
        matched_entities = entity_matching.match(
            act_session, _STATE_CODE, [person])

        # Assert 1 - Match
        self.assert_people_match([expected_person], matched_entities.people)
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

        # Arrange 2 - Commit
        expected_db_person = attr.evolve(expected_person)
        expected_db_person.person_id = 1
        sg = expected_db_person.sentence_groups[0]
        sg.sentence_group_id = 1
        sg.incarceration_sentences[0].incarceration_sentence_id = 1
        sg.incarceration_sentences[1].incarceration_sentence_id = 2
        sg.incarceration_sentences[2].incarceration_sentence_id = 3
        sg.incarceration_sentences[2].charges[0].charge_id = 2

        # Act 2 - Commit
        act_session.commit()
        act_session.close()

        # Assert 2 - Commit
        assert_session = self._session()
        result_db_people = dao.read_people(assert_session)

        self.assert_people_match([expected_db_person], result_db_people)
        assert_session.close()

    def test_mergeMultiParentEntities_mergeCourtCaseWithJudge(self, _):
        # Arrange 1 - Match
        db_person = generate_person(person_id=_ID)
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
        act_session = self._session()
        matched_entities = entity_matching.match(
            act_session, _STATE_CODE, [person])

        # Assert 1 - Match
        with act_session.no_autoflush:
            self.assert_people_match([expected_person], matched_entities.people)

        # Arrange 2 - Commit
        expected_db_person = attr.evolve(expected_person)
        expected_db_incarceration_sentence = \
            expected_db_person.sentence_groups[0].incarceration_sentences[0]
        expected_db_judge = \
            expected_db_incarceration_sentence.charges[0].court_case.judge
        expected_db_judge.agent_id = 1

        # Act 2 - Commit
        act_session.commit()
        act_session.close()

        # Assert 2 - Commit
        assert_session = self._session()
        result_db_people = dao.read_people(assert_session)

        self.assert_people_match([expected_db_person], result_db_people)
        assert_session.close()

    def test_mergeMultiParentEntities_mergeCourtCaseWithJudge2(self, _):
        # Arrange 1 - Match
        db_person = generate_person(person_id=_ID)
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
        act_session = self._session()
        matched_entities = entity_matching.match(
            act_session, _STATE_CODE, [person])

        # Assert 1 - Match
        with act_session.no_autoflush:
            self.assert_people_match([expected_person], matched_entities.people)

        # Arrange 2 - Commit
        expected_db_person = attr.evolve(expected_person)
        expected_db_incarceration_sentence = \
            expected_db_person.sentence_groups[0].incarceration_sentences[0]
        expected_db_judge = \
            expected_db_incarceration_sentence.charges[0].court_case.judge
        expected_db_judge.agent_id = 1

        # Act 2 - Commit
        act_session.commit()
        act_session.close()

        # Assert 2 - Commit
        assert_session = self._session()
        result_db_people = dao.read_people(assert_session)

        self.assert_people_match([expected_db_person], result_db_people)
        assert_session.close()
