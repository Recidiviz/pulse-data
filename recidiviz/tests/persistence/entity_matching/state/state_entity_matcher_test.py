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
from unittest import TestCase

import attr

from recidiviz import Session
from recidiviz.common.constants.bond import BondStatus
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.county.sentence import SentenceStatus
from recidiviz.common.constants.person_characteristics import Gender, Race, \
    Ethnicity
from recidiviz.common.constants.state.state_fine import StateFineStatus
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodStatus
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_period import \
    StateSupervisionPeriodStatus
from recidiviz.common.constants.state.state_supervision_violation_response \
    import StateSupervisionViolationResponseDecision
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.state.entities import StatePersonAlias, \
    StatePersonExternalId, StatePersonRace, StatePersonEthnicity, StatePerson, \
    StateBond, StateCourtCase, StateCharge, StateFine, StateAgent, \
    StateIncarcerationIncident, StateParoleDecision, StateIncarcerationPeriod, \
    StateAssessment, StateIncarcerationSentence, StateSupervisionSentence, \
    StateSupervisionViolationResponse, StateSupervisionViolation, \
    StateSupervisionPeriod, StateSentenceGroup
from recidiviz.persistence.entity_matching import entity_matching
from recidiviz.persistence.entity_matching.state import state_entity_matcher
from recidiviz.persistence.errors import EntityMatchingError
from recidiviz.tests.utils import fakes

_EXTERNAL_ID = 'EXTERNAL_ID'
_EXTERNAL_ID_2 = 'EXTERNAL_ID_2'
_EXTERNAL_ID_3 = 'EXTERNAL_ID_3'
_ID = 1
_ID_2 = 2
_ID_3 = 3
_FULL_NAME = 'FULL_NAME'
_STATE_CODE = 'NC'


# pylint: disable=protected-access
class TestStateEntityMatching(TestCase):
    """Tests for state specific entity matching logic."""

    def setup_method(self, _test_method):
        fakes.use_in_memory_sqlite_database()

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
            external_id=_EXTERNAL_ID, id_type='type')

        db_person.sentence_groups = [db_sentence_group]
        db_person.external_ids = [db_external_id]

        db_external_id_another = schema.StatePersonExternalId(
            person_external_id_id=_ID_2, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2, id_type='type')
        db_person_another = schema.StatePerson(
            person_id=_ID_2, full_name=_FULL_NAME,
            external_ids=[db_external_id_another])

        session = Session()
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
            external_id=_EXTERNAL_ID, id_type='type-updated')
        person = StatePerson.new_with_defaults(
            full_name=_FULL_NAME, external_ids=[external_id],
            sentence_groups=[sentence_group])

        external_id_another = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2, id_type='type-updated')
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
            Session(), _STATE_CODE, [person, person_another])

        # Assert
        self.assertEqual(matched_entities.error_count, 0)
        self.assertEqual([expected_person, expected_person_another],
                         matched_entities.people)

    def test_match_noPlaceholder_oneMatchOneError(self):
        # Arrange
        db_external_id = schema.StatePersonExternalId(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID, id_type='type')
        db_person = schema.StatePerson(
            person_id=_ID, full_name=_FULL_NAME, external_ids=[db_external_id])

        session = Session()
        session.add(db_person)
        session.commit()

        external_id = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID,
            id_type='type_updated')
        person = StatePerson.new_with_defaults(
            full_name=_FULL_NAME, external_ids=[external_id])

        external_id_dup = attr.evolve(external_id, id_type='type-duplicate')
        person_dup = attr.evolve(person, external_ids=[external_id_dup])

        expected_person = attr.evolve(person, person_id=_ID)
        expected_external_id = attr.evolve(
            external_id, person_external_id_id=_ID, person=expected_person)
        expected_person.external_ids = [expected_external_id]

        # Act
        matched_entities = entity_matching.match(
            Session(), _STATE_CODE, [person, person_dup])

        # Assert
        self.assertEqual(matched_entities.error_count, 1)
        self.assertEqual([expected_person], matched_entities.people)

    def test_matchEntities_multipleMatchesToDb_throws(self):
        db_external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID)
        db_person = StatePerson.new_with_defaults(
            person_id=_ID, full_name=_FULL_NAME, external_ids=[db_external_id])

        external_id = attr.evolve(db_external_id, person_external_id_id=None)
        person = attr.evolve(
            db_person, person_id=None, external_ids=[external_id])
        person_dup = attr.evolve(person)

        # Act / Assert
        with self.assertRaises(EntityMatchingError):
            state_entity_matcher._match_entities(
                ingested_entities=[person, person_dup], db_entities=[db_person])

    def test_matchEntities_conflictingExternalIds_throws(self):
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
            external_id=_EXTERNAL_ID, id_type='type')
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

        # Act / Assert
        with self.assertRaises(EntityMatchingError):
            state_entity_matcher._match_entities(
                ingested_entities=[person], db_entities=[db_person])

    def test_matchEntities_noPlaceholders_newPerson(self):
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
        merged_entities = state_entity_matcher._match_entities(
            ingested_entities=[person], db_entities=[])

        # Assert
        self.assertEqual([expected_person], merged_entities)

    def test_matchEntities_noPlaceholders_partialTreeIngested(self):
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
            external_id=_EXTERNAL_ID, id_type='type')
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
            db_external_id, person_external_id_id=None, id_type='type-updated')
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
        merged_entities = state_entity_matcher._match_entities(
            ingested_entities=[person], db_entities=[db_person])

        # Assert
        self.assertEqual([expected_person], merged_entities)

    def test_matchEntities_noPlaceholders_completeTreeUpdate(self):
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
        merged_entities = state_entity_matcher._match_entities(
            ingested_entities=[person], db_entities=[db_person])

        # Assert
        self.assertEqual([expected_person], merged_entities)
