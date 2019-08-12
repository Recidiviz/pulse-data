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

from recidiviz.common.constants.bond import BondStatus
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.county.sentence import SentenceStatus
from recidiviz.common.constants.person_characteristics import Gender, Race, \
    Ethnicity
from recidiviz.common.constants.state.state_fine import StateFineStatus
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodStatus, StateIncarcerationPeriodAdmissionReason, \
    StateIncarcerationPeriodReleaseReason
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_period import \
    StateSupervisionPeriodStatus
from recidiviz.common.constants.state.state_supervision_violation_response \
    import StateSupervisionViolationResponseDecision, \
    StateSupervisionViolationResponseRevocationType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.base_schema import StateBase
from recidiviz.persistence.entity.state.entities import StatePersonAlias, \
    StatePersonExternalId, StatePersonRace, StatePersonEthnicity, StatePerson, \
    StateBond, StateCourtCase, StateCharge, StateFine, StateAgent, \
    StateIncarcerationIncident, StateParoleDecision, StateIncarcerationPeriod, \
    StateAssessment, StateIncarcerationSentence, StateSupervisionSentence, \
    StateSupervisionViolationResponse, StateSupervisionViolation, \
    StateSupervisionPeriod, StateSentenceGroup
from recidiviz.persistence.entity_matching import entity_matching
from recidiviz.persistence.entity_matching.state import state_entity_matcher
from recidiviz.persistence.entity_matching.state.state_entity_matcher import \
    merge_multiparent_entities
from recidiviz.tests.utils import fakes

_EXTERNAL_ID = 'EXTERNAL_ID-1'
_EXTERNAL_ID_2 = 'EXTERNAL_ID-2'
_EXTERNAL_ID_3 = 'EXTERNAL_ID-3'
_EXTERNAL_ID_4 = 'EXTERNAL_ID-4'
_ID = 1
_ID_2 = 2
_ID_3 = 3
_ID_4 = 4
_ID_TYPE = 'ID_TYPE'
_ID_TYPE_ANOTHER = 'ID_TYPE_ANOTHER'
_FULL_NAME = 'FULL_NAME'
_FULL_NAME_ANOTHER = 'FULL_NAME_ANOTHER'
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


# pylint: disable=protected-access
class TestStateEntityMatching(TestCase):
    """Tests for state specific entity matching logic."""

    def setup_method(self, _test_method):
        fakes.use_in_memory_sqlite_database(StateBase)

    def test_match_noPlaceholders_success(self):
        # Arrange
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

        session = SessionFactory.for_schema_base(StateBase)
        session.add(db_person)
        session.add(db_person_another)
        session.commit()

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
        expected_charge = attr.evolve(
            new_charge, person=expected_person)
        expected_fine = attr.evolve(
            fine, fine_id=_ID, person=expected_person,
            charges=[expected_charge])
        expected_sentence_group = attr.evolve(
            sentence_group, sentence_group_id=_ID, person=expected_person,
            fines=[expected_fine])
        expected_external_id = attr.evolve(
            external_id, person_external_id_id=_ID, person=expected_person)
        expected_person.sentence_groups = [expected_sentence_group]
        expected_person.external_ids = [expected_external_id]

        expected_person_another = attr.evolve(person_another, person_id=_ID_2)
        expected_external_id = attr.evolve(
            external_id_another, person_external_id_id=_ID_2,
            person=expected_person_another)
        expected_person_another.external_ids = [expected_external_id]

        # Act
        matched_entities = entity_matching.match(
            SessionFactory.for_schema_base(StateBase), _STATE_CODE,
            [person, person_another])

        # Assert
        self.assertEqual(matched_entities.error_count, 0)
        self.assertEqual(2, matched_entities.total_root_entities)
        self.assertEqual([expected_person, expected_person_another],
                         matched_entities.people)

    def test_match_noPlaceholder_oneMatchOneError(self):
        # Arrange
        db_external_id = schema.StatePersonExternalId(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID, id_type=_ID_TYPE)
        db_person = schema.StatePerson(
            person_id=_ID, full_name=_FULL_NAME, external_ids=[db_external_id])

        session = SessionFactory.for_schema_base(StateBase)
        session.add(db_person)
        session.commit()

        external_id = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID, id_type=_ID_TYPE)
        person = StatePerson.new_with_defaults(
            full_name=_FULL_NAME, external_ids=[external_id])

        external_id_dup = attr.evolve(external_id)
        person_dup = attr.evolve(person, external_ids=[external_id_dup],
                                 full_name=_FULL_NAME_ANOTHER)

        expected_person = attr.evolve(person, person_id=_ID)
        expected_external_id = attr.evolve(
            external_id, person_external_id_id=_ID, person=expected_person)
        expected_person.external_ids = [expected_external_id]

        # Act
        matched_entities = entity_matching.match(
            SessionFactory.for_schema_base(StateBase), _STATE_CODE,
            [person, person_dup])

        # Assert
        self.assertEqual(matched_entities.error_count, 1)
        self.assertEqual(2, matched_entities.total_root_entities)
        self.assertEqual([expected_person], matched_entities.people)

    def test_matchPersons_multipleMatchesToDb_oneSuccessOneError(self):
        db_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID)
        db_person = StatePerson.new_with_defaults(
            person_id=_ID, full_name=_FULL_NAME, external_ids=[db_external_id])

        external_id = attr.evolve(db_external_id, person_external_id_id=None)
        person = attr.evolve(
            db_person, person_id=None, external_ids=[external_id])
        person_dup = attr.evolve(person, full_name='another')

        expected_external_id = attr.evolve(external_id,
                                           person_external_id_id=_ID)
        expected_person = attr.evolve(person, person_id=_ID,
                                      external_ids=[expected_external_id])

        # Act
        merged_entities = state_entity_matcher._match_persons(
            ingested_persons=[person, person_dup], db_persons=[db_person])

        # Assert
        self.assertEqual([expected_person], merged_entities.people)
        self.assertEqual(1, merged_entities.error_count)

    def test_matchPersons_conflictingExternalIds_error(self):
        # Arrange
        db_court_case = StateCourtCase.new_with_defaults(
            court_case_id=_ID,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE, county_code='county_code')
        db_charge = StateCharge.new_with_defaults(
            charge_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            description='charge_1',
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            court_case=db_court_case)
        db_fine = StateFine.new_with_defaults(
            fine_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            county_code='county_code',
            charges=[db_charge])
        db_incarceration_sentence = \
            StateIncarcerationSentence.new_with_defaults(
                incarceration_sentence_id=_ID,
                status=StateSentenceStatus.SERVING,
                external_id=_EXTERNAL_ID,
                state_code=_STATE_CODE, county_code='county_code')
        db_sentence_group = StateSentenceGroup.new_with_defaults(
            sentence_group_id=_ID, status=StateSentenceStatus.EXTERNAL_UNKNOWN,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE, county_code='county_code',
            incarceration_sentences=[db_incarceration_sentence],
            fines=[db_fine])
        db_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID, id_type=_ID_TYPE)
        db_person = StatePerson.new_with_defaults(
            person_id=_ID, full_name=_FULL_NAME,
            external_ids=[db_external_id], sentence_groups=[db_sentence_group])

        conflicting_court_case = attr.evolve(
            db_court_case, court_case_id=None, external_id=_EXTERNAL_ID_2)
        charge_1 = attr.evolve(
            db_charge, charge_id=None, court_case=conflicting_court_case)
        fine = attr.evolve(db_fine, fine_id=None, charges=[charge_1])
        sentence_group = attr.evolve(
            db_sentence_group, sentence_group_id=None, fines=[fine])
        external_id = attr.evolve(
            db_external_id, person_external_id_id=None)
        person = attr.evolve(
            db_person, person_id=None, external_ids=[external_id],
            sentence_groups=[sentence_group])

        # Act
        merged_entities = state_entity_matcher._match_persons(
            ingested_persons=[person], db_persons=[db_person])

        # Assert
        self.assertEqual([], merged_entities.people)
        self.assertEqual(1, merged_entities.total_root_entities)
        self.assertEqual(1, merged_entities.error_count)

    def test_matchPersons_sentenceGroupRootEntity_IngMatchesMultipleDb(self):
        # Arrange
        db_sentence_group = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID, sentence_group_id=_ID)
        db_sentence_group_2 = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID_2, sentence_group_id=_ID_2)
        db_sentence_group_3 = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID_3, sentence_group_id=_ID_3)
        db_sentence_group_3_dup = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID_3, sentence_group_id=_ID_4)
        db_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=_ID, id_type=_ID_TYPE,
            external_id=_EXTERNAL_ID)
        db_person = StatePerson(
            person_id=_ID,
            sentence_groups=[
                db_sentence_group, db_sentence_group_2, db_sentence_group_3,
                db_sentence_group_3_dup],
            external_ids=[db_external_id])

        sentence_group = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID, state_code=_STATE_CODE)
        sentence_group_2 = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID_2, state_code=_STATE_CODE)
        sentence_group_3 = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID_3, state_code=_STATE_CODE)
        person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group, sentence_group_2,
                             sentence_group_3])

        expected_sentence_group = attr.evolve(
            sentence_group, sentence_group_id=_ID)
        expected_sentence_group_2 = attr.evolve(
            sentence_group_2, sentence_group_id=_ID_2)
        expected_sentence_group_3 = attr.evolve(db_sentence_group_3)
        expected_sentence_group_4 = attr.evolve(db_sentence_group_3_dup)
        expected_external_id = attr.evolve(db_external_id)
        expected_person = attr.evolve(
            db_person, external_ids=[expected_external_id],
            sentence_groups=[
                expected_sentence_group, expected_sentence_group_2,
                expected_sentence_group_3, expected_sentence_group_4])

        # Act
        merged_entities = state_entity_matcher._match_persons(
            ingested_persons=[person], db_persons=[db_person])

        # Assert
        self.assertEqual([expected_person], merged_entities.people)
        self.assertEqual(3, merged_entities.total_root_entities)
        self.assertEqual(1, merged_entities.error_count)

    def test_matchPersons_sentenceGroupRootEntity_DbMatchesMultipleIng(self):
        # Arrange
        db_sentence_group = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID, sentence_group_id=_ID)
        db_sentence_group_2 = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID_2, sentence_group_id=_ID_2)
        db_sentence_group_3 = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID_3, sentence_group_id=_ID_3)
        db_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=_ID, id_type=_ID_TYPE,
            external_id=_EXTERNAL_ID)
        db_person = StatePerson(
            person_id=_ID,
            sentence_groups=[
                db_sentence_group, db_sentence_group_2, db_sentence_group_3],
            external_ids=[db_external_id])

        sentence_group = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID, state_code=_STATE_CODE)
        sentence_group_2 = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID_2, state_code=_STATE_CODE)
        sentence_group_3 = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID_3, state_code=_STATE_CODE)
        sentence_group_3_dup = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID_3, state_code=_STATE_CODE_2)
        person = StatePerson.new_with_defaults(
            sentence_groups=[sentence_group, sentence_group_2,
                             sentence_group_3, sentence_group_3_dup])

        expected_sentence_group = attr.evolve(
            sentence_group, sentence_group_id=_ID)
        expected_sentence_group_2 = attr.evolve(
            sentence_group_2, sentence_group_id=_ID_2)
        expected_sentence_group_3 = attr.evolve(
            sentence_group_3, sentence_group_id=_ID_3)
        expected_external_id = attr.evolve(db_external_id)
        expected_person = attr.evolve(
            db_person, external_ids=[expected_external_id],
            sentence_groups=[
                expected_sentence_group, expected_sentence_group_2,
                expected_sentence_group_3])

        # Act
        merged_entities = state_entity_matcher._match_persons(
            ingested_persons=[person], db_persons=[db_person])

        # Assert
        self.assertEqual([expected_person], merged_entities.people)
        self.assertEqual(4, merged_entities.total_root_entities)
        self.assertEqual(1, merged_entities.error_count)

    def test_matchPersons_noPlaceholders_newPerson(self):
        # Arrange
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

        # Act
        merged_entities = state_entity_matcher._match_persons(
            ingested_persons=[person], db_persons=[])

        # Assert
        self.assertEqual([expected_person], merged_entities.people)
        self.assertEqual(0, merged_entities.error_count)

    def test_matchPersons_noPlaceholders_updatePersonAttributes(self):
        # Arrange
        db_race = StatePersonRace.new_with_defaults(
            person_race_id=_ID, state_code=_STATE_CODE,
            race=Race.WHITE)
        db_alias = StatePersonAlias.new_with_defaults(
            person_alias_id=_ID, state_code=_STATE_CODE,
            full_name=_FULL_NAME)
        db_ethnicity = StatePersonEthnicity.new_with_defaults(
            person_ethnicity_id=_ID, state_code=_STATE_CODE,
            ethnicity=Ethnicity.HISPANIC)
        db_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID, id_type=_ID_TYPE)
        db_person = StatePerson.new_with_defaults(
            person_id=_ID, full_name=_FULL_NAME,
            external_ids=[db_external_id],
            races=[db_race], aliases=[db_alias], ethnicities=[db_ethnicity])

        race = StatePersonRace.new_with_defaults(
            state_code=_STATE_CODE, race=Race.BLACK)
        alias = StatePersonAlias.new_with_defaults(
            state_code=_STATE_CODE, full_name=_FULL_NAME_ANOTHER)
        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code=_STATE_CODE, ethnicity=Ethnicity.NOT_HISPANIC)
        external_id = attr.evolve(
            db_external_id, person_external_id_id=None)
        person = attr.evolve(
            db_person, person_id=None,
            external_ids=[external_id],
            races=[race], aliases=[alias], ethnicities=[ethnicity])

        expected_person = attr.evolve(
            db_person, races=[db_race, race],
            ethnicities=[db_ethnicity, ethnicity],
            aliases=[db_alias, alias])

        # Act
        merged_entities = state_entity_matcher._match_persons(
            ingested_persons=[person], db_persons=[db_person])

        # Assert
        self.assertEqual([expected_person], merged_entities.people)
        self.assertEqual(0, merged_entities.error_count)

    def test_matchPersons_noPlaceholders_partialTreeIngested(self):
        # Arrange
        db_court_case = StateCourtCase.new_with_defaults(
            court_case_id=_ID,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE, county_code='county_code')
        db_bond = StateBond.new_with_defaults(
            bond_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            status=BondStatus.PRESENT_WITHOUT_INFO,
            bond_agent='agent')
        db_charge_1 = StateCharge.new_with_defaults(
            charge_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            description='charge_1',
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            bond=db_bond)
        db_charge_2 = StateCharge.new_with_defaults(
            charge_id=_ID_2, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2,
            description='charge_2',
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            court_case=db_court_case)
        db_fine = StateFine.new_with_defaults(
            fine_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            county_code='county_code',
            charges=[db_charge_1, db_charge_2])
        db_incarceration_sentence = \
            StateIncarcerationSentence.new_with_defaults(
                incarceration_sentence_id=_ID,
                status=StateSentenceStatus.SERVING,
                external_id=_EXTERNAL_ID,
                state_code=_STATE_CODE, county_code='county_code')
        db_sentence_group = StateSentenceGroup.new_with_defaults(
            sentence_group_id=_ID, status=StateSentenceStatus.EXTERNAL_UNKNOWN,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE, county_code='county_code',
            incarceration_sentences=[db_incarceration_sentence],
            fines=[db_fine])
        db_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID, id_type=_ID_TYPE)
        db_person = StatePerson.new_with_defaults(
            person_id=_ID, full_name=_FULL_NAME,
            external_ids=[db_external_id], sentence_groups=[db_sentence_group])

        new_court_case = StateCourtCase.new_with_defaults(
            external_id=_EXTERNAL_ID_2, state_code=_STATE_CODE,
            county_code='county_code')
        bond = attr.evolve(db_bond, bond_id=None, bond_agent='agent-updated')
        charge_1 = attr.evolve(
            db_charge_1, charge_id=None, description='charge_1-updated',
            bond=bond, court_case=new_court_case)
        charge_2 = attr.evolve(
            db_charge_2, charge_id=None, description='charge_2-updated',
            court_case=None)
        fine = attr.evolve(
            db_fine, fine_id=None, county_code='county-updated',
            charges=[charge_1, charge_2])
        sentence_group = attr.evolve(
            db_sentence_group, sentence_group_id=None,
            county_code='county_code-updated', fines=[fine])
        external_id = attr.evolve(
            db_external_id, person_external_id_id=None, id_type=_ID_TYPE)
        person = attr.evolve(
            db_person, person_id=None, external_ids=[external_id],
            sentence_groups=[sentence_group])

        expected_unchanged_court_case = attr.evolve(db_court_case)
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
            db_incarceration_sentence)
        expected_sentence_group = attr.evolve(
            sentence_group, sentence_group_id=_ID, fines=[expected_fine],
            incarceration_sentences=[expected_unchanged_incarceration_sentence])
        expected_external_id = attr.evolve(
            external_id, person_external_id_id=_ID)
        expected_person = attr.evolve(
            person, person_id=_ID, external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group])

        # Act
        merged_entities = state_entity_matcher._match_persons(
            ingested_persons=[person], db_persons=[db_person])

        # Assert
        self.assertEqual([expected_person], merged_entities.people)
        self.assertEqual(0, merged_entities.error_count)

    def test_matchPersons_noPlaceholders_completeTreeUpdate(self):
        # Arrange
        db_bond = StateBond.new_with_defaults(
            bond_id=_ID, state_code=_STATE_CODE, external_id=_EXTERNAL_ID,
            status=BondStatus.PRESENT_WITHOUT_INFO, bond_agent='agent')
        db_court_case = StateCourtCase.new_with_defaults(
            court_case_id=_ID, external_id=_EXTERNAL_ID, state_code=_STATE_CODE,
            county_code='county_code')
        db_charge_1 = StateCharge.new_with_defaults(
            charge_id=_ID, state_code=_STATE_CODE, external_id=_EXTERNAL_ID,
            description='charge_1', status=ChargeStatus.PRESENT_WITHOUT_INFO,
            bond=db_bond, court_case=db_court_case)
        db_charge_2 = StateCharge.new_with_defaults(
            charge_id=_ID_2, state_code=_STATE_CODE, external_id=_EXTERNAL_ID_2,
            description='charge_2', status=ChargeStatus.PRESENT_WITHOUT_INFO,
            bond=db_bond, court_case=db_court_case)
        db_charge_3 = StateCharge.new_with_defaults(
            charge_id=_ID_3, state_code=_STATE_CODE, external_id=_EXTERNAL_ID_3,
            description='charge_3', status=ChargeStatus.PRESENT_WITHOUT_INFO)
        db_fine = StateFine.new_with_defaults(
            fine_id=_ID, state_code=_STATE_CODE, external_id=_EXTERNAL_ID,
            county_code='county_code', charges=[db_charge_1, db_charge_2])
        db_assessment = StateAssessment.new_with_defaults(
            assessment_id=_ID, state_code=_STATE_CODE, external_id=_EXTERNAL_ID,
            assessment_metadata='metadata')
        db_assessment_2 = StateAssessment.new_with_defaults(
            assessment_id=_ID_2, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2, assessment_metadata='metadata_2')
        db_agent = StateAgent.new_with_defaults(
            agent_id=_ID, state_code=_STATE_CODE, external_id=_EXTERNAL_ID,
            full_name='full_name')
        db_agent_2 = StateAgent.new_with_defaults(
            agent_id=_ID_2, state_code=_STATE_CODE, external_id=_EXTERNAL_ID_2,
            full_name='full_name_2')
        db_incarceration_incident = \
            StateIncarcerationIncident.new_with_defaults(
                incarceration_incident_id=_ID, state_code=_STATE_CODE,
                external_id=_EXTERNAL_ID, incident_details='details',
                responding_officer=db_agent)
        db_parole_decision = StateParoleDecision.new_with_defaults(
            parole_decision_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID, decision_outcome='outcome',
            decision_agents=[db_agent_2])
        db_parole_decision_2 = StateParoleDecision.new_with_defaults(
            parole_decision_id=_ID_2, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2,
            decision_outcome='outcome_2', decision_agents=[db_agent_2])
        db_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=_ID, external_id=_EXTERNAL_ID,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code=_STATE_CODE, facility='facility',
            incarceration_incidents=[db_incarceration_incident],
            parole_decisions=[db_parole_decision, db_parole_decision_2],
            assessments=[db_assessment])
        db_incarceration_sentence = \
            StateIncarcerationSentence.new_with_defaults(
                incarceration_sentence_id=_ID,
                status=StateSentenceStatus.SERVING,
                external_id=_EXTERNAL_ID, state_code=_STATE_CODE,
                county_code='county_code', charges=[db_charge_2, db_charge_3],
                incarceration_periods=[db_incarceration_period])
        db_supervision_violation_response = \
            StateSupervisionViolationResponse.new_with_defaults(
                supervision_violation_response_id=_ID, state_code=_STATE_CODE,
                external_id=_EXTERNAL_ID,
                decision=StateSupervisionViolationResponseDecision.CONTINUANCE)
        db_supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID, is_violent=True,
            supervision_violation_responses=[db_supervision_violation_response])
        db_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_ID, external_id=_EXTERNAL_ID,
            status=StateSupervisionPeriodStatus.EXTERNAL_UNKNOWN,
            state_code=_STATE_CODE, county_code='county_code',
            assessments=[db_assessment_2],
            supervision_violations=[db_supervision_violation])
        db_supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=_ID, status=SentenceStatus.SERVING,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            min_length_days=0,
            supervision_periods=[db_supervision_period])
        db_sentence_group = StateSentenceGroup.new_with_defaults(
            sentence_group_id=_ID, status=StateSentenceStatus.EXTERNAL_UNKNOWN,
            external_id=_EXTERNAL_ID, state_code=_STATE_CODE,
            county_code='county_code',
            supervision_sentences=[db_supervision_sentence],
            incarceration_sentences=[db_incarceration_sentence],
            fines=[db_fine])
        db_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID)
        db_person = StatePerson.new_with_defaults(
            person_id=_ID, full_name=_FULL_NAME, external_ids=[db_external_id],
            sentence_groups=[db_sentence_group])

        bond = attr.evolve(db_bond, bond_id=None, bond_agent='agent-updated')
        court_case = attr.evolve(
            db_court_case, court_case_id=None,
            county_code='county_code-updated')
        charge_1 = attr.evolve(
            db_charge_1, charge_id=None, description='charge_1-updated',
            bond=bond, court_case=court_case)
        charge_2 = attr.evolve(
            db_charge_2, charge_id=None, description='charge_2-updated',
            bond=bond, court_case=court_case)
        charge_3 = attr.evolve(
            db_charge_3, charge_id=None, description='charge_3-updated')
        fine = attr.evolve(
            db_fine, fine_id=None, county_code='county_code-updated',
            charges=[charge_1, charge_2])
        assessment = attr.evolve(
            db_assessment, assessment_id=None,
            assessment_metadata='metadata_updated')
        assessment_2 = attr.evolve(
            db_assessment_2, assessment_id=None,
            assessment_metadata='metadata_2-updated')
        agent = attr.evolve(
            db_agent, agent_id=None, full_name='full_name-updated')
        agent_2 = attr.evolve(
            db_agent_2, agent_id=None, full_name='full_name_2-updated')
        incarceration_incident = attr.evolve(
            db_incarceration_incident, incarceration_incident_id=None,
            incident_details='details-updated', responding_officer=agent)
        parole_decision = attr.evolve(
            db_parole_decision, parole_decision_id=None,
            decision_outcome='outcome-updated', decision_agents=[agent_2])
        parole_decision_2 = attr.evolve(
            db_parole_decision_2, parole_decision_id=None,
            decision_outcome='outcome_2-updated', decision_agents=[agent_2])
        incarceration_period = attr.evolve(
            db_incarceration_period, incarceration_period_id=None,
            facility='facility-updated',
            incarceration_incidents=[incarceration_incident],
            parole_decisions=[parole_decision, parole_decision_2],
            assessments=[assessment])
        incarceration_sentence = attr.evolve(
            db_incarceration_sentence, incarceration_sentence_id=None,
            county_code='county_code-updated', charges=[charge_2, charge_3],
            incarceration_periods=[incarceration_period])
        supervision_violation_response = attr.evolve(
            db_supervision_violation_response,
            supervision_violation_response_id=None,
            decision=StateSupervisionViolationResponseDecision.EXTENSION)
        supervision_violation = attr.evolve(
            db_supervision_violation, supervision_violation_id=None,
            is_violent=False,
            supervision_violation_responses=[supervision_violation_response])
        supervision_period = attr.evolve(
            db_supervision_period, supervision_period_id=None,
            county_code='county_code-updated',
            assessments=[assessment_2],
            supervision_violations=[supervision_violation])
        supervision_sentence = attr.evolve(
            db_supervision_sentence, supervision_sentence_id=None,
            min_length_days=1, supervision_periods=[supervision_period])
        sentence_group = attr.evolve(
            db_sentence_group, sentence_group_id=None,
            county_code='county_code-updated',
            supervision_sentences=[supervision_sentence],
            incarceration_sentences=[incarceration_sentence], fines=[fine])
        external_id = attr.evolve(db_external_id, person_external_id_id=None)
        person = attr.evolve(
            db_person, person_id=None, external_ids=[external_id],
            sentence_groups=[sentence_group])

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
            supervision_violation_response_id=_ID)
        expected_supervision_violation = attr.evolve(
            supervision_violation, supervision_violation_id=_ID,
            supervision_violation_responses=[
                expected_supervision_violation_response])
        expected_supervision_period = attr.evolve(
            supervision_period, supervision_period_id=_ID,
            assessments=[expected_assessment_2],
            supervision_violations=[expected_supervision_violation])
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
            sentence_groups=[expected_sentence_group])

        # Act
        merged_entities = state_entity_matcher._match_persons(
            ingested_persons=[person], db_persons=[db_person])

        # Assert
        self.assertEqual(0, merged_entities.error_count)
        self.assertEqual([expected_person], merged_entities.people)

    def test_matchPersons_ingestedPersonWithNewExternalId(self):
        db_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID, id_type=_ID_TYPE)
        db_person = StatePerson.new_with_defaults(
            person_id=_ID, full_name=_FULL_NAME,
            external_ids=[db_external_id])

        external_id = attr.evolve(db_external_id, person_external_id_id=None)
        external_id_another = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID_2,
            id_type=_ID_TYPE_ANOTHER)
        person = StatePerson.new_with_defaults(
            external_ids=[external_id, external_id_another])

        expected_external_id = attr.evolve(
            external_id, person_external_id_id=_ID)
        expected_external_id_another = attr.evolve(external_id_another)
        expected_person = attr.evolve(db_person, external_ids=[
            expected_external_id, expected_external_id_another])

        # Act
        merged_entities = state_entity_matcher._match_persons(
            ingested_persons=[person], db_persons=[db_person])

        # Assert
        self.assertEqual(0, merged_entities.error_count)
        self.assertEqual([expected_person], merged_entities.people)

    def test_matchPersons_holeInDbGraph(self):
        db_supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=_ID, status=SentenceStatus.SERVING,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            min_length_days=0)
        db_supervision_sentence_unchanged = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=_ID_2, status=SentenceStatus.SERVING,
                external_id=_EXTERNAL_ID_2,
                state_code=_STATE_CODE,
                min_length_days=0)
        db_placeholder_sentence_group = StateSentenceGroup.new_with_defaults(
            sentence_group_id=_ID,
            supervision_sentences=[db_supervision_sentence,
                                   db_supervision_sentence_unchanged])

        db_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID)
        db_person = StatePerson.new_with_defaults(
            person_id=_ID, full_name=_FULL_NAME,
            external_ids=[db_external_id],
            sentence_groups=[db_placeholder_sentence_group])

        supervision_sentence_updated = attr.evolve(
            db_supervision_sentence, supervision_sentence_id=None,
            min_length_days=1)
        new_sentence_group = attr.evolve(
            db_placeholder_sentence_group, external_id=_EXTERNAL_ID,
            supervision_sentences=[
                supervision_sentence_updated])
        external_id = attr.evolve(db_external_id, person_external_id_id=None)
        person = attr.evolve(
            db_person, person_id=None, external_ids=[external_id],
            sentence_groups=[new_sentence_group])

        expected_supervision_sentence = attr.evolve(
            supervision_sentence_updated, supervision_sentence_id=_ID)
        expected_supervision_sentence_unchanged = attr.evolve(
            db_supervision_sentence_unchanged)
        expected_placeholder_sentence_group = attr.evolve(
            db_placeholder_sentence_group,
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

        # Act
        merged_entities = state_entity_matcher._match_persons(
            ingested_persons=[person], db_persons=[db_person])

        # Assert
        self.assertEqual([expected_person], merged_entities.people)
        self.assertEqual(0, merged_entities.error_count)

    def test_matchPersons_holeInIngestedGraph(self):
        db_supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=_ID, status=SentenceStatus.SERVING,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            min_length_days=0)
        db_supervision_sentence_another = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=_ID_2, status=SentenceStatus.SERVING,
                external_id=_EXTERNAL_ID_2,
                state_code=_STATE_CODE,
                min_length_days=0)
        db_sentence_group = StateSentenceGroup.new_with_defaults(
            sentence_group_id=_ID,
            external_id=_EXTERNAL_ID,
            supervision_sentences=[
                db_supervision_sentence, db_supervision_sentence_another])
        db_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID)
        db_person = StatePerson.new_with_defaults(
            person_id=_ID, full_name=_FULL_NAME,
            external_ids=[db_external_id],
            sentence_groups=[db_sentence_group])

        supervision_sentence_updated = attr.evolve(
            db_supervision_sentence, supervision_sentence_id=None,
            min_length_days=1)
        sentence_group_placeholder = StateSentenceGroup.new_with_defaults(
            supervision_sentences=[supervision_sentence_updated])
        external_id = attr.evolve(
            db_external_id, person_external_id_id=None)
        person = attr.evolve(
            db_person, person_id=None, external_ids=[external_id],
            sentence_groups=[sentence_group_placeholder])

        expected_supervision_sentence = attr.evolve(
            supervision_sentence_updated, supervision_sentence_id=_ID)
        expected_supervision_sentence_unchanged = attr.evolve(
            db_supervision_sentence_another)
        expected_sentence_group = attr.evolve(
            db_sentence_group,
            supervision_sentences=[expected_supervision_sentence,
                                   expected_supervision_sentence_unchanged])
        expected_external_id = attr.evolve(
            external_id, person_external_id_id=_ID)
        expected_person = attr.evolve(
            person, person_id=_ID, external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group])

        # Act
        merged_entities = state_entity_matcher._match_persons(
            ingested_persons=[person], db_persons=[db_person])

        # Assert
        self.assertEqual([expected_person], merged_entities.people)
        self.assertEqual(0, merged_entities.error_count)

    def test_matchPersons_dbPlaceholderSplits(self):
        db_supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=_ID, status=SentenceStatus.SERVING,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            min_length_days=0)
        db_supervision_sentence_another = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=_ID_2, status=SentenceStatus.SERVING,
                external_id=_EXTERNAL_ID_2,
                state_code=_STATE_CODE,
                min_length_days=10)
        db_placeholder_sentence_group = StateSentenceGroup.new_with_defaults(
            sentence_group_id=_ID,
            supervision_sentences=[db_supervision_sentence,
                                   db_supervision_sentence_another])

        db_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID)
        db_person = StatePerson.new_with_defaults(
            person_id=_ID, full_name=_FULL_NAME,
            external_ids=[db_external_id],
            sentence_groups=[db_placeholder_sentence_group])

        supervision_sentence_updated = attr.evolve(
            db_supervision_sentence, supervision_sentence_id=None,
            min_length_days=1)
        supervision_sentence_another_updated = attr.evolve(
            db_supervision_sentence_another, supervision_sentence_id=None,
            min_length_days=11)
        sentence_group_new = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID,
            supervision_sentences=[supervision_sentence_updated])
        sentence_group_new_another = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID_2,
            supervision_sentences=[supervision_sentence_another_updated])
        external_id = attr.evolve(db_external_id, person_external_id_id=None)
        person = attr.evolve(
            db_person, person_id=None, external_ids=[external_id],
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
            db_placeholder_sentence_group, supervision_sentences=[])
        expected_external_id = attr.evolve(external_id,
                                           person_external_id_id=_ID)
        expected_person = attr.evolve(
            person, person_id=_ID, external_ids=[expected_external_id],
            sentence_groups=[
                expected_sentence_group, expected_sentence_group_another,
                expected_placeholder_sentence_group])

        # Act
        merged_entities = state_entity_matcher._match_persons(
            ingested_persons=[person], db_persons=[db_person])

        # Assert
        self.assertEqual([expected_person], merged_entities.people)
        self.assertEqual(0, merged_entities.error_count)

    def test_matchPersons_dbMatchesMultipleIngestedPlaceholders_success(self):
        db_supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=_ID, status=SentenceStatus.SERVING,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            min_length_days=0)
        db_supervision_sentence_another = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=_ID_2, status=SentenceStatus.SERVING,
                external_id=_EXTERNAL_ID_2,
                state_code=_STATE_CODE,
                min_length_days=10)
        db_sentence_group = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID,
            sentence_group_id=_ID,
            supervision_sentences=[db_supervision_sentence,
                                   db_supervision_sentence_another])
        db_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID)
        db_person = StatePerson.new_with_defaults(
            person_id=_ID, full_name=_FULL_NAME, external_ids=[db_external_id],
            sentence_groups=[db_sentence_group])

        supervision_sentence_updated = attr.evolve(
            db_supervision_sentence, supervision_sentence_id=None,
            min_length_days=1)
        placeholder_sentence_group = StateSentenceGroup.new_with_defaults(
            supervision_sentences=[supervision_sentence_updated])

        supervision_sentence_another_updated = attr.evolve(
            db_supervision_sentence_another, supervision_sentence_id=None,
            min_length_days=11)
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
            db_sentence_group,
            supervision_sentences=[expected_supervision_sentence,
                                   expected_supervision_sentence_another])
        expected_external_id = attr.evolve(db_external_id)
        expected_person = attr.evolve(
            db_person, external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group])

        expected_fine = attr.evolve(fine)
        expected_placeholder_sentence_group = \
            StateSentenceGroup.new_with_defaults(fines=[expected_fine])
        expected_placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[expected_placeholder_sentence_group])

        # Act
        merged_entities = state_entity_matcher._match_persons(
            ingested_persons=[placeholder_person],
            db_persons=[db_person])

        # Assert
        self.assertEqual([expected_person, expected_placeholder_person],
                         merged_entities.people)
        self.assertEqual(0, merged_entities.error_count)

    def test_matchPersons_multipleIngestedPersonsMatchToPlaceholderDb(self):
        db_supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=_ID, status=SentenceStatus.SERVING,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            min_length_days=0)
        db_supervision_sentence_another = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=_ID_2, status=SentenceStatus.SERVING,
                external_id=_EXTERNAL_ID_2,
                state_code=_STATE_CODE,
                min_length_days=10)
        db_sentence_group = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID,
            sentence_group_id=_ID,
            supervision_sentences=[db_supervision_sentence])
        db_sentence_group_another = StateSentenceGroup.new_with_defaults(
            sentence_group_id=_ID_2,
            external_id=_EXTERNAL_ID_2,
            supervision_sentences=[db_supervision_sentence_another])
        db_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID)
        db_person = StatePerson.new_with_defaults(
            person_id=_ID, full_name=_FULL_NAME, external_ids=[db_external_id],
            sentence_groups=[db_sentence_group, db_sentence_group_another])

        supervision_sentence_updated = attr.evolve(
            db_supervision_sentence, supervision_sentence_id=None,
            min_length_days=1)
        placeholder_sentence_group = StateSentenceGroup.new_with_defaults(
            supervision_sentences=[supervision_sentence_updated])
        placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[placeholder_sentence_group])

        supervision_sentence_another_updated = attr.evolve(
            db_supervision_sentence_another, supervision_sentence_id=None,
            min_length_days=11)
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
            db_sentence_group,
            supervision_sentences=[expected_supervision_sentence])
        expected_sentence_group_another = attr.evolve(
            db_sentence_group_another,
            supervision_sentences=[expected_supervision_sentence_another])
        expected_external_id = attr.evolve(db_external_id)
        expected_person = attr.evolve(
            db_person, external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group,
                             expected_sentence_group_another])

        # Act
        merged_entities = state_entity_matcher._match_persons(
            ingested_persons=[placeholder_person, placeholder_person_another],
            db_persons=[db_person])

        # Assert
        self.assertEqual([expected_person], merged_entities.people)
        self.assertEqual(0, merged_entities.error_count)

    def test_matchPersons_ingestedPlaceholderSplits(self):
        db_supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=_ID, status=SentenceStatus.SERVING,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            min_length_days=0)
        db_supervision_sentence_another = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=_ID_2, status=SentenceStatus.SERVING,
                external_id=_EXTERNAL_ID_2,
                state_code=_STATE_CODE,
                min_length_days=10)
        db_sentence_group = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID,
            sentence_group_id=_ID,
            supervision_sentences=[db_supervision_sentence])
        db_sentence_group_another = StateSentenceGroup.new_with_defaults(
            sentence_group_id=_ID_2,
            external_id=_EXTERNAL_ID_2,
            supervision_sentences=[db_supervision_sentence_another])
        db_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID)
        db_person = StatePerson.new_with_defaults(
            person_id=_ID, full_name=_FULL_NAME, external_ids=[db_external_id],
            sentence_groups=[db_sentence_group, db_sentence_group_another])

        supervision_sentence_updated = attr.evolve(
            db_supervision_sentence, supervision_sentence_id=None,
            min_length_days=1)
        supervision_sentence_another_updated = attr.evolve(
            db_supervision_sentence_another, supervision_sentence_id=None,
            min_length_days=11)
        placeholder_sentence_group = StateSentenceGroup.new_with_defaults(
            supervision_sentences=[
                supervision_sentence_updated,
                supervision_sentence_another_updated])
        external_id = attr.evolve(db_external_id, person_external_id_id=None)
        person = attr.evolve(
            db_person, person_id=None,
            external_ids=[external_id],
            sentence_groups=[placeholder_sentence_group])

        expected_supervision_sentence = attr.evolve(
            supervision_sentence_updated, supervision_sentence_id=_ID)
        expected_supervision_sentence_another = attr.evolve(
            supervision_sentence_another_updated, supervision_sentence_id=_ID_2)
        expected_sentence_group = attr.evolve(
            db_sentence_group,
            supervision_sentences=[expected_supervision_sentence])
        expected_sentence_group_another = attr.evolve(
            db_sentence_group_another,
            supervision_sentences=[expected_supervision_sentence_another])
        expected_external_id = attr.evolve(external_id,
                                           person_external_id_id=_ID)
        expected_person = attr.evolve(
            person, person_id=_ID, external_ids=[expected_external_id],
            sentence_groups=[
                expected_sentence_group, expected_sentence_group_another])

        # Act
        merged_entities = state_entity_matcher._match_persons(
            ingested_persons=[person], db_persons=[db_person])

        # Assert
        self.assertEqual([expected_person], merged_entities.people)
        self.assertEqual(0, merged_entities.error_count)

    def test_matchPersons_multipleHolesInIngestedGraph(self):
        # Arrange
        db_supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=_ID, status=SentenceStatus.SERVING,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            min_length_days=0)
        db_sentence_group = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID,
            sentence_group_id=_ID,
            supervision_sentences=[db_supervision_sentence])
        db_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID)
        db_person = StatePerson.new_with_defaults(
            person_id=_ID, full_name=_FULL_NAME, external_ids=[db_external_id],
            sentence_groups=[db_sentence_group])

        supervision_sentence_updated = attr.evolve(
            db_supervision_sentence,
            supervision_sentence_id=None, min_length_days=1)
        placeholder_sentence_group = StateSentenceGroup.new_with_defaults(
            supervision_sentences=[supervision_sentence_updated])
        placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[placeholder_sentence_group])

        expected_supervision_sentence = attr.evolve(
            supervision_sentence_updated, supervision_sentence_id=_ID)
        expected_sentence_group = attr.evolve(
            db_sentence_group,
            supervision_sentences=[expected_supervision_sentence])
        expected_external_id = attr.evolve(db_external_id)
        expected_person = attr.evolve(
            db_person, external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group])

        # Act
        merged_entities = state_entity_matcher._match_persons(
            ingested_persons=[placeholder_person], db_persons=[db_person])

        # Assert
        self.assertEqual([expected_person], merged_entities.people)
        self.assertEqual(0, merged_entities.error_count)

    def test_matchPersons_multipleHolesInDbGraph(self):
        # Arrange
        db_supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=_ID, status=SentenceStatus.SERVING,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            min_length_days=0)
        db_placeholder_sentence_group = StateSentenceGroup.new_with_defaults(
            sentence_group_id=_ID,
            supervision_sentences=[db_supervision_sentence])
        db_placeholder_person = StatePerson.new_with_defaults(
            person_id=_ID,
            sentence_groups=[db_placeholder_sentence_group])

        supervision_sentence_updated = attr.evolve(
            db_supervision_sentence, supervision_sentence_id=None,
            min_length_days=1)
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
            db_placeholder_sentence_group, supervision_sentences=[])
        expected_external_id = attr.evolve(external_id)
        expected_person = attr.evolve(
            person, external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group])
        expected_placeholder_person = attr.evolve(
            db_placeholder_person,
            sentence_groups=[expected_placeholder_sentence_group])

        # Act
        merged_entities = state_entity_matcher._match_persons(
            ingested_persons=[person], db_persons=[db_placeholder_person])

        # Assert
        self.assertEqual([expected_person, expected_placeholder_person],
                         merged_entities.people)
        self.assertEqual(0, merged_entities.error_count)

    def test_matchPersons_holesInBothGraphs_ingestedPersonPlaceholder(self):
        db_supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=_ID, status=SentenceStatus.SERVING,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            min_length_days=0)
        db_supervision_sentence_another = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=_ID_2, status=SentenceStatus.SERVING,
                external_id=_EXTERNAL_ID_2,
                state_code=_STATE_CODE,
                min_length_days=10)
        db_placeholder_sentence_group = StateSentenceGroup.new_with_defaults(
            sentence_group_id=_ID,
            supervision_sentences=[
                db_supervision_sentence, db_supervision_sentence_another])

        db_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID)
        db_person = StatePerson.new_with_defaults(
            person_id=_ID, full_name=_FULL_NAME,
            external_ids=[db_external_id],
            sentence_groups=[db_placeholder_sentence_group])

        supervision_sentence_updated = attr.evolve(
            db_supervision_sentence, supervision_sentence_id=None,
            min_length_days=1)
        supervision_sentence_another_updated = attr.evolve(
            db_supervision_sentence_another, supervision_sentence_id=None,
            min_length_days=11)
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
        expected_sentence_group_placeholder = \
            StateSentenceGroup.new_with_defaults(sentence_group_id=_ID)
        expected_external_id = attr.evolve(db_external_id)
        expected_person = attr.evolve(
            db_person, external_ids=[expected_external_id],
            sentence_groups=[
                expected_sentence_group, expected_sentence_group_another,
                expected_sentence_group_placeholder])

        # Act
        merged_entities = state_entity_matcher._match_persons(
            ingested_persons=[placeholder_person], db_persons=[db_person])

        # Assert
        self.assertEqual([expected_person], merged_entities.people)
        self.assertEqual(0, merged_entities.error_count)

    def test_matchPersons_holesInBothGraphs_dbPersonPlaceholder(self):
        db_supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=_ID, status=SentenceStatus.SERVING,
            external_id=_EXTERNAL_ID,
            state_code=_STATE_CODE,
            min_length_days=0)
        db_supervision_sentence_another = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=_ID_2, status=SentenceStatus.SERVING,
                external_id=_EXTERNAL_ID_2, state_code=_STATE_CODE,
                min_length_days=10)
        db_sentence_group = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID,
            sentence_group_id=_ID,
            supervision_sentences=[db_supervision_sentence])
        db_sentence_group_another = StateSentenceGroup.new_with_defaults(
            sentence_group_id=_ID_2,
            external_id=_EXTERNAL_ID_2,
            supervision_sentences=[db_supervision_sentence_another])

        db_placeholder_person = StatePerson.new_with_defaults(
            person_id=_ID,
            sentence_groups=[db_sentence_group, db_sentence_group_another])

        supervision_sentence_updated = attr.evolve(
            db_supervision_sentence, supervision_sentence_id=None,
            min_length_days=1)
        supervision_sentence_another_updated = attr.evolve(
            db_supervision_sentence_another, supervision_sentence_id=None,
            min_length_days=11)
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
            db_sentence_group,
            supervision_sentences=[expected_supervision_sentence])
        expected_sentence_group_another = attr.evolve(
            db_sentence_group_another,
            supervision_sentences=[expected_supervision_sentence_another])
        expected_external_id = attr.evolve(external_id)
        expected_person = attr.evolve(
            person, external_ids=[expected_external_id],
            sentence_groups=[
                expected_sentence_group, expected_sentence_group_another])

        expected_placeholder_person = attr.evolve(db_placeholder_person,
                                                  sentence_groups=[])

        # Act
        merged_entities = state_entity_matcher._match_persons(
            ingested_persons=[person], db_persons=[db_placeholder_person])

        # Assert
        self.assertEqual([expected_person, expected_placeholder_person],
                         merged_entities.people)
        self.assertEqual(0, merged_entities.error_count)

    def test_matchPersons_matchAfterManyIngestedPlaceholders(self):
        # Arrange
        db_sentence_group = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID, sentence_group_id=_ID)
        db_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=_ID,
            state_code=_STATE_CODE, id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        db_person = StatePerson(sentence_groups=[db_sentence_group],
                                external_ids=[db_external_id])

        incarceration_incident = StateIncarcerationIncident.new_with_defaults(
            external_id=_EXTERNAL_ID)
        placeholder_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_incidents=[incarceration_incident])
        placeholder_incarceration_sentence = \
            StateIncarcerationSentence.new_with_defaults(
                incarceration_periods=[placeholder_incarceration_period])
        sentence_group = attr.evolve(db_sentence_group, sentence_group_id=None,
                                     incarceration_sentences=[
                                         placeholder_incarceration_sentence])
        external_id = attr.evolve(db_external_id, person_external_id_id=None)
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
            db_sentence_group,
            incarceration_sentences=[expected_incarceration_sentence])
        expected_external_id = attr.evolve(db_external_id)
        expected_person = attr.evolve(
            db_person, external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group])

        # Act
        merged_entities = state_entity_matcher._match_persons(
            ingested_persons=[person], db_persons=[db_person])

        # Assert
        self.assertEqual([expected_person], merged_entities.people)
        self.assertEqual(0, merged_entities.error_count)

    # TODO(2037): Move test to state specific file.
    def test_runMatch_sameEntities_noDuplicates(self):
        db_is = StateIncarcerationSentence.new_with_defaults(
            incarceration_sentence_id=_ID, date_imposed=_DATE_1)
        db_sg = StateSentenceGroup.new_with_defaults(
            sentence_group_id=_ID, external_id=_EXTERNAL_ID,
            incarceration_sentences=[db_is])
        db_placeholder_person = StatePerson.new_with_defaults(
            person_id=_ID, sentence_groups=[db_sg])

        inc_s = attr.evolve(db_is, incarceration_sentence_id=None)
        sg = attr.evolve(
            db_sg, sentence_group_id=None, incarceration_sentences=[inc_s])
        placeholder_person = attr.evolve(
            db_placeholder_person, person_id=None, sentence_groups=[sg])

        expected_is = attr.evolve(db_is)
        expected_sg = attr.evolve(db_sg, incarceration_sentences=[expected_is])
        expected_placeholder_person = attr.evolve(
            db_placeholder_person, sentence_groups=[expected_sg])

        # Act
        merged_entities = state_entity_matcher._match_persons(
            ingested_persons=[placeholder_person],
            db_persons=[db_placeholder_person])

        # Assert
        self.assertEqual([expected_placeholder_person], merged_entities.people)
        self.assertEqual(0, merged_entities.error_count)

    def test_runMatch_associateSvrsToIps(self):
        # Arrange
        db_ip_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=_ID,
            admission_date=_DATE_2,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION)
        db_ip_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=_ID_2,
            admission_date=_DATE_4,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION)
        db_placeholder_is = StateIncarcerationSentence.new_with_defaults(
            incarceration_sentence_id=_ID,
            incarceration_periods=[db_ip_1, db_ip_2])

        db_sg = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID,
            sentence_group_id=_ID,
            incarceration_sentences=[db_placeholder_is],
        )

        db_placeholder_person = StatePerson.new_with_defaults(
            person_id=_ID, sentence_groups=[db_sg])

        svr_1 = StateSupervisionViolationResponse.new_with_defaults(
            response_date=_DATE_1,
            revocation_type=StateSupervisionViolationResponseRevocationType.
            REINCARCERATION)
        svr_2 = StateSupervisionViolationResponse.new_with_defaults(
            response_date=_DATE_3,
            revocation_type=StateSupervisionViolationResponseRevocationType.
            REINCARCERATION)
        placeholder_sv = StateSupervisionViolation.new_with_defaults(
            supervision_violation_responses=[svr_1, svr_2])
        placeholder_sp = StateSupervisionPeriod.new_with_defaults(
            supervision_violations=[placeholder_sv])
        placeholder_ss = StateSupervisionSentence.new_with_defaults(
            supervision_periods=[placeholder_sp])
        sg = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID, supervision_sentences=[placeholder_ss])
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

        expected_ip_1 = attr.evolve(db_ip_1,
                                    source_supervision_violation_response=svr_1)
        expected_ip_2 = attr.evolve(db_ip_2,
                                    source_supervision_violation_response=svr_2)

        expected_placeholder_is = attr.evolve(
            db_placeholder_is,
            incarceration_periods=[expected_ip_1, expected_ip_2])
        expected_placeholder_sg = attr.evolve(
            db_sg,
            supervision_sentences=[expected_placeholder_ss],
            incarceration_sentences=[expected_placeholder_is])
        expected_placeholder_person = attr.evolve(
            placeholder_person, person_id=_ID,
            sentence_groups=[expected_placeholder_sg])

        # Act
        merged_entities = state_entity_matcher._run_match(
            ingested_persons=[placeholder_person],
            db_persons=[db_placeholder_person])

        # Assert
        self.assertEqual([expected_placeholder_person], merged_entities.people)
        self.assertEqual(0, merged_entities.error_count)

    def test_matchPersons_mergeIncomingIncarcerationSentences(self):
        # Arrange
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

        session = SessionFactory.for_schema_base(StateBase)
        session.add(db_person)
        session.commit()

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            facility=_FACILITY,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            admission_date=datetime.date(year=2019, month=1, day=1),
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION)
        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2,
            facility=_FACILITY,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            release_date=datetime.date(year=2019, month=1, day=2),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)
        incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_3,
            facility=_FACILITY_2,
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
        expected_merged_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                external_id=_EXTERNAL_ID + '|' + _EXTERNAL_ID_2,
                state_code=_STATE_CODE,
                facility=_FACILITY,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                admission_date=datetime.date(year=2019, month=1, day=1),
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=datetime.date(year=2019, month=1, day=2),
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
                person=expected_person)
        expected_unmerged_incarceration_period = attr.evolve(
            incarceration_period_3, person=expected_person)
        expected_incarceration_sentence = \
            StateIncarcerationSentence.new_with_defaults(
                incarceration_sentence_id=_ID,
                status=StateSentenceStatus.EXTERNAL_UNKNOWN,
                state_code=_STATE_CODE,
                external_id=_EXTERNAL_ID,
                incarceration_periods=[
                    expected_merged_incarceration_period,
                    expected_unmerged_incarceration_period],
                person=expected_person)
        expected_sentence_group = StateSentenceGroup.new_with_defaults(
            sentence_group_id=_ID,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[expected_incarceration_sentence],
            person=expected_person)
        expected_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID,
            person=expected_person)
        expected_person.external_ids = [expected_external_id]
        expected_person.sentence_groups = [expected_sentence_group]

        # Act
        matched_entities = entity_matching.match(
            SessionFactory.for_schema_base(StateBase),
            _STATE_CODE, [placeholder_person])

        # Assert
        self.assertEqual([expected_person], matched_entities.people)
        self.assertEqual(0, matched_entities.error_count)

    def test_matchPersons_mergeIngestedAndDbIncarcerationSentences(self):
        # Arrange
        db_unmerged_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=_ID,
                state_code=_STATE_CODE,
                external_id=_EXTERNAL_ID_3,
                facility=_FACILITY,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                admission_date=datetime.date(year=2019, month=1, day=1),
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION)
        db_merged_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=_ID_2,
                state_code=_STATE_CODE,
                external_id=_EXTERNAL_ID + '|' + _EXTERNAL_ID_2,
                facility=_FACILITY,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                admission_date=datetime.date(year=2018, month=1, day=1),
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=datetime.date(year=2018, month=1, day=2),
                release_reason=
                StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

        db_incarceration_sentence = \
            StateIncarcerationSentence.new_with_defaults(
                state_code=_STATE_CODE,
                incarceration_sentence_id=_ID,
                status=StateSentenceStatus.EXTERNAL_UNKNOWN,
                external_id=_EXTERNAL_ID,
                incarceration_periods=[
                    db_merged_incarceration_period,
                    db_unmerged_incarceration_period])
        db_sentence_group = StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE,
            sentence_group_id=_ID,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[db_incarceration_sentence])
        db_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        db_person = StatePerson.new_with_defaults(
            person_id=_ID, full_name=_FULL_NAME,
            sentence_groups=[db_sentence_group], external_ids=[db_external_id])

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_4,
            facility=_FACILITY,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            release_date=datetime.date(year=2019, month=1, day=2),
            release_reason=
            StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID,
            incarceration_periods=[incarceration_period])
        placeholder_sentence_group = StateSentenceGroup.new_with_defaults(
            incarceration_sentences=[incarceration_sentence])
        placeholder_person = StatePerson.new_with_defaults(
            sentence_groups=[placeholder_sentence_group])

        expected_merged_incarceration_period = attr.evolve(
            db_merged_incarceration_period)
        expected_new_merged_incarceration_period = attr.evolve(
            db_unmerged_incarceration_period,
            external_id=_EXTERNAL_ID_3 + '|' + _EXTERNAL_ID_4,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            release_date=datetime.date(year=2019, month=1, day=2),
            release_reason=
            StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

        expected_incarceration_sentence = \
            StateIncarcerationSentence.new_with_defaults(
                incarceration_sentence_id=_ID,
                status=StateSentenceStatus.EXTERNAL_UNKNOWN,
                state_code=_STATE_CODE,
                external_id=_EXTERNAL_ID,
                incarceration_periods=[
                    expected_new_merged_incarceration_period,
                    expected_merged_incarceration_period])
        expected_sentence_group = StateSentenceGroup.new_with_defaults(
            sentence_group_id=_ID,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN,
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID,
            incarceration_sentences=[expected_incarceration_sentence])
        expected_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        expected_person = StatePerson.new_with_defaults(
            person_id=_ID, full_name=_FULL_NAME,
            external_ids=[expected_external_id],
            sentence_groups=[expected_sentence_group])

        # Act
        merged_entities = state_entity_matcher._match_persons(
            ingested_persons=[placeholder_person], db_persons=[db_person])

        # Assert
        self.assertEqual([expected_person], merged_entities.people)
        self.assertEqual(0, merged_entities.error_count)

    def test_mergeMultiParentEntities(self):
        charge_merged = StateCharge.new_with_defaults(
            charge_id=_ID,
            external_id=_EXTERNAL_ID)
        charge_unmerged = attr.evolve(charge_merged, charge_id=None)
        charge_duplicate_unmerged = attr.evolve(charge_unmerged)
        sentence = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID,
            charges=[charge_unmerged])
        sentence_2 = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID_2,
            charges=[charge_duplicate_unmerged])
        sentence_3 = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID_3,
            charges=[charge_merged])
        sentence_group = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID,
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

        merge_multiparent_entities([person])
        self.assertEqual(expected_person, person)
        sentence_group = person.sentence_groups[0]
        found_charge = sentence_group.incarceration_sentences[0].charges[0]
        found_charge_2 = sentence_group.incarceration_sentences[1].charges[0]
        found_charge_3 = sentence_group.incarceration_sentences[2].charges[0]
        self.assertEqual(
            id(found_charge), id(found_charge_2), id(found_charge_3))

    def test_mergeMultiParentEntities_mergeChargesAndCourtCases(self):
        court_case_merged = StateCourtCase.new_with_defaults(
            court_case_id=_ID,
            external_id=_EXTERNAL_ID)
        court_case_unmerged = attr.evolve(court_case_merged, court_case_id=None)
        charge_merged = StateCharge.new_with_defaults(
            charge_id=_ID,
            external_id=_EXTERNAL_ID,
            court_case=court_case_merged)
        charge_unmerged = attr.evolve(
            charge_merged, charge_id=None, court_case=court_case_unmerged)
        charge_2 = StateCharge.new_with_defaults(
            external_id=_EXTERNAL_ID_2,
            court_case=court_case_unmerged)
        sentence = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID,
            charges=[charge_unmerged])
        sentence_2 = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID_2,
            charges=[charge_merged])
        sentence_3 = StateIncarcerationSentence.new_with_defaults(
            external_id=_EXTERNAL_ID_3,
            charges=[charge_2])
        sentence_group = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID,
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

        merge_multiparent_entities([person])
        self.assertEqual(expected_person, person)
        sentence_group = person.sentence_groups[0]
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
