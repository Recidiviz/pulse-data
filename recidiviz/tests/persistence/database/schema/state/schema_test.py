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
"""Tests for state-specific SQLAlchemy enums."""
import unittest
from typing import Optional

import pytest
import sqlalchemy

import recidiviz.common.constants.state.shared_enums
from recidiviz.common.constants.state import state_assessment, state_charge, \
    state_sentence, state_supervision, state_fine, state_incarceration, \
    state_court_case, state_agent, state_incarceration_period, \
    state_supervision_period, state_incarceration_incident, \
    state_supervision_violation, state_supervision_violation_response, \
    state_parole_decision, state_person_alias, state_program_assignment, state_supervision_contact
from recidiviz.common.constants.state.state_early_discharge import StateEarlyDischargeDecision, \
    StateEarlyDischargeDecisionStatus
from recidiviz.persistence.database.base_schema import StateBase
from recidiviz.persistence.database.schema import shared_enums
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.schema_utils import \
    get_all_table_classes_in_module, _get_all_database_entities_in_module
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.ingest_info_converter.state.entity_helpers import \
    state_supervision_case_type_entry
from recidiviz.tests.persistence.database.schema.schema_test import (
    TestSchemaEnums,
    TestSchemaTableConsistency)
from recidiviz.tests.persistence.database.schema.state.schema_test_utils import generate_person, generate_external_id
from recidiviz.tools.postgres import local_postgres_helpers


class TestStateSchemaEnums(TestSchemaEnums):
    """Tests for validating state schema enums are defined correctly"""

    # Test case ensuring enum values match between persistence layer enums and
    # schema enums
    def testPersistenceAndSchemaEnumsMatch(self):
        # Mapping between name of schema enum and persistence layer enum. This
        # map controls which pairs of enums are tested.
        #
        # If a schema enum does not correspond to a persistence layer enum,
        # it should be mapped to None.
        state_enums_mapping = {
            'state_assessment_class': state_assessment.StateAssessmentClass,
            'state_assessment_level': state_assessment.StateAssessmentLevel,
            'state_assessment_type': state_assessment.StateAssessmentType,
            'state_charge_classification_type': state_charge.StateChargeClassificationType,
            'state_sentence_status': state_sentence.StateSentenceStatus,
            'state_supervision_type': state_supervision.StateSupervisionType,
            'state_acting_body_type': recidiviz.common.constants.state.shared_enums.StateActingBodyType,
            'state_custodial_authority': recidiviz.common.constants.state.shared_enums.StateCustodialAuthority,
            'state_early_discharge_decision': StateEarlyDischargeDecision,
            'state_early_discharge_decision_status': StateEarlyDischargeDecisionStatus,
            'state_fine_status': state_fine.StateFineStatus,
            'state_incarceration_type': state_incarceration.StateIncarcerationType,
            'state_court_case_status': state_court_case.StateCourtCaseStatus,
            'state_court_type': state_court_case.StateCourtType,
            'state_agent_type': state_agent.StateAgentType,
            'state_incarceration_period_status': state_incarceration_period.StateIncarcerationPeriodStatus,
            'state_incarceration_facility_security_level':
                state_incarceration_period.StateIncarcerationFacilitySecurityLevel,
            'state_incarceration_period_admission_reason':
                state_incarceration_period.StateIncarcerationPeriodAdmissionReason,
            'state_incarceration_period_release_reason':
                state_incarceration_period.StateIncarcerationPeriodReleaseReason,
            'state_parole_decision_outcome': state_parole_decision.StateParoleDecisionOutcome,
            'state_person_alias_type': state_person_alias.StatePersonAliasType,
            'state_supervision_period_status': state_supervision_period.StateSupervisionPeriodStatus,
            'state_supervision_period_admission_reason': state_supervision_period.StateSupervisionPeriodAdmissionReason,
            'state_supervision_period_supervision_type': state_supervision_period.StateSupervisionPeriodSupervisionType,
            'state_supervision_period_termination_reason':
                state_supervision_period.StateSupervisionPeriodTerminationReason,
            'state_supervision_level': state_supervision_period.StateSupervisionLevel,
            'state_incarceration_incident_type': state_incarceration_incident.StateIncarcerationIncidentType,
            'state_incarceration_incident_outcome_type':
                state_incarceration_incident.StateIncarcerationIncidentOutcomeType,
            'state_specialized_purpose_for_incarceration':
                state_incarceration_period.StateSpecializedPurposeForIncarceration,
            'state_program_assignment_participation_status':
                state_program_assignment.StateProgramAssignmentParticipationStatus,
            'state_program_assignment_discharge_reason': state_program_assignment.StateProgramAssignmentDischargeReason,
            'state_supervision_case_type': state_supervision_case_type_entry.StateSupervisionCaseType,
            'state_supervision_contact_location': state_supervision_contact.StateSupervisionContactLocation,
            'state_supervision_contact_reason': state_supervision_contact.StateSupervisionContactReason,
            'state_supervision_contact_status': state_supervision_contact.StateSupervisionContactStatus,
            'state_supervision_contact_type': state_supervision_contact.StateSupervisionContactType,
            'state_supervision_violation_type': state_supervision_violation.StateSupervisionViolationType,
            'state_supervision_violation_response_type':
                state_supervision_violation_response.StateSupervisionViolationResponseType,
            'state_supervision_violation_response_decision':
                state_supervision_violation_response.StateSupervisionViolationResponseDecision,
            'state_supervision_violation_response_revocation_type':
                state_supervision_violation_response.StateSupervisionViolationResponseRevocationType,
            'state_supervision_violation_response_deciding_body_type':
                state_supervision_violation_response.StateSupervisionViolationResponseDecidingBodyType,
        }

        merged_mapping = {**self.SHARED_ENUMS_TEST_MAPPING,
                          **state_enums_mapping}

        self.check_persistence_and_schema_enums_match(merged_mapping, schema)

    def testAllEnumNamesPrefixedWithState(self):
        shared_enum_ids = \
            [id(enum)
             for enum in self._get_all_sqlalchemy_enums_in_module(shared_enums)]

        for enum in \
                self._get_all_sqlalchemy_enums_in_module(schema):
            if id(enum) in shared_enum_ids:
                continue
            self.assertTrue(enum.name.startswith('state_'))


class TestStateSchemaTableConsistency(TestSchemaTableConsistency):
    """Test class for validating state schema tables are defined correctly"""

    def testAllTableNamesPrefixedWithState(self):
        for cls in get_all_table_classes_in_module(schema):
            self.assertTrue(cls.name.startswith('state_'))

    def testAllDatabaseEntityNamesPrefixedWithState(self):
        for cls in _get_all_database_entities_in_module(schema):
            self.assertTrue(cls.__name__.startswith('State'))


@pytest.mark.uses_db
class TestUniqueExternalIdConstraint(unittest.TestCase):
    """Tests for UniqueConstraint on StatePersonExternalId."""

    EXTERNAL_ID_1 = 'EXTERNAL_ID_1'
    ID_TYPE_1 = 'ID_TYPE_1'
    ID_TYPE_2 = 'ID_TYPE_2'

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        local_postgres_helpers.use_on_disk_postgresql_database(StateBase)

        self.state_code = 'US_XX'

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(StateBase)

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(cls.temp_db_dir)

    def test_add_person_conflicting_external_id(self) -> None:
        # Arrange
        arrange_session = SessionFactory.for_schema_base(StateBase)

        db_external_id = generate_external_id(
            state_code=self.state_code,
            external_id=self.EXTERNAL_ID_1, id_type=self.ID_TYPE_1)

        db_person = generate_person(
            state_code=self.state_code,
            external_ids=[db_external_id])

        arrange_session.add(db_person)
        arrange_session.commit()

        db_external_id_duplicated = generate_external_id(
            state_code=self.state_code,
            external_id=self.EXTERNAL_ID_1, id_type=self.ID_TYPE_1)

        db_person_new = generate_person(
            state_code=self.state_code,
            external_ids=[db_external_id_duplicated])

        # Act
        session = SessionFactory.for_schema_base(StateBase)

        session.add(db_person_new)
        session.flush()

        with self.assertRaises(sqlalchemy.exc.IntegrityError):
            session.commit()

    def test_add_person_conflicting_external_id_no_flush(self) -> None:
        # Arrange
        arrange_session = SessionFactory.for_schema_base(StateBase)

        db_external_id = generate_external_id(
            state_code=self.state_code,
            external_id=self.EXTERNAL_ID_1, id_type=self.ID_TYPE_1)

        db_person = generate_person(
            state_code=self.state_code,
            external_ids=[db_external_id])

        arrange_session.add(db_person)
        arrange_session.commit()

        db_external_id_duplicated = generate_external_id(
            state_code=self.state_code,
            external_id=self.EXTERNAL_ID_1, id_type=self.ID_TYPE_1)

        db_person_new = generate_person(
            state_code=self.state_code,
            external_ids=[db_external_id_duplicated])

        # Act
        session = SessionFactory.for_schema_base(StateBase)
        session.add(db_person_new)
        with self.assertRaises(sqlalchemy.exc.IntegrityError):
            session.commit()

    def test_add_person_conflicting_external_id_different_type(self) -> None:
        # Arrange
        arrange_session = SessionFactory.for_schema_base(StateBase)

        db_external_id = generate_external_id(
            state_code=self.state_code,
            external_id=self.EXTERNAL_ID_1, id_type=self.ID_TYPE_1)

        db_person = generate_person(
            state_code=self.state_code,
            external_ids=[db_external_id])

        arrange_session.add(db_person)
        arrange_session.commit()

        db_external_id_duplicated = generate_external_id(
            state_code=self.state_code,
            external_id=self.EXTERNAL_ID_1, id_type=self.ID_TYPE_2)

        db_person_new = generate_person(
            state_code=self.state_code,
            external_ids=[db_external_id_duplicated])

        # Act
        session = SessionFactory.for_schema_base(StateBase)

        session.add(db_person_new)
        session.flush()
        session.commit()

    def test_add_person_conflicting_external_id_different_state(self) -> None:
        # Arrange
        arrange_session = SessionFactory.for_schema_base(StateBase)

        db_external_id = generate_external_id(
            state_code='OTHER_STATE_CODE',
            external_id=self.EXTERNAL_ID_1, id_type=self.ID_TYPE_1)

        db_person = generate_person(
            state_code='OTHER_STATE_CODE',
            external_ids=[db_external_id])

        arrange_session.add(db_person)
        arrange_session.commit()

        db_external_id_duplicated = generate_external_id(
            state_code=self.state_code,
            external_id=self.EXTERNAL_ID_1, id_type=self.ID_TYPE_1)

        db_person_new = generate_person(
            state_code=self.state_code,
            external_ids=[db_external_id_duplicated])

        # Act
        session = SessionFactory.for_schema_base(StateBase)

        session.add(db_person_new)
        session.flush()
        session.commit()

    def test_add_person_conflicting_external_id_same_session(self) -> None:
        # Arrange
        db_external_id = generate_external_id(
            state_code=self.state_code,
            external_id=self.EXTERNAL_ID_1, id_type=self.ID_TYPE_1)

        db_person = generate_person(
            state_code=self.state_code,
            external_ids=[db_external_id])

        db_external_id_duplicated = generate_external_id(
            state_code=self.state_code,
            external_id=self.EXTERNAL_ID_1, id_type=self.ID_TYPE_1)

        db_person_2 = generate_person(
            state_code=self.state_code,
            external_ids=[db_external_id_duplicated])

        # Act
        session = SessionFactory.for_schema_base(StateBase)

        session.add(db_person)
        session.add(db_person_2)
        session.flush()

        with self.assertRaises(sqlalchemy.exc.IntegrityError):
            session.commit()
