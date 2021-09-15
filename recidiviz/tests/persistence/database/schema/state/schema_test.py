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
from parameterized import parameterized

import recidiviz.common.constants.state.shared_enums
from recidiviz.common.constants.state import (
    state_agent,
    state_assessment,
    state_charge,
    state_court_case,
    state_fine,
    state_incarceration,
    state_incarceration_incident,
    state_incarceration_period,
    state_parole_decision,
    state_person_alias,
    state_program_assignment,
    state_sentence,
    state_supervision,
    state_supervision_contact,
    state_supervision_period,
    state_supervision_violation,
    state_supervision_violation_response,
)
from recidiviz.common.constants.state.state_early_discharge import (
    StateEarlyDischargeDecision,
    StateEarlyDischargeDecisionStatus,
)
from recidiviz.common.constants.state.state_fine import StateFineStatus
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.persistence.database.schema import shared_enums
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.schema_utils import (
    SchemaType,
    _get_all_database_entities_in_module,
    get_all_table_classes_in_module,
)
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.ingest_info_converter.state.entity_helpers import (
    state_supervision_case_type_entry,
)
from recidiviz.tests.persistence.database.schema.schema_test import (
    TestSchemaEnums,
    TestSchemaTableConsistency,
)
from recidiviz.tests.persistence.database.schema.state.schema_test_utils import (
    generate_agent,
    generate_charge,
    generate_court_case,
    generate_early_discharge,
    generate_external_id,
    generate_incarceration_period,
    generate_parole_decision,
    generate_person,
    generate_program_assignment,
    generate_supervision_case_type_entry,
    generate_supervision_contact,
    generate_supervision_violation,
    generate_supervision_violation_response,
)
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
            "state_assessment_class": state_assessment.StateAssessmentClass,
            "state_assessment_level": state_assessment.StateAssessmentLevel,
            "state_assessment_type": state_assessment.StateAssessmentType,
            "state_charge_classification_type": state_charge.StateChargeClassificationType,
            "state_sentence_status": state_sentence.StateSentenceStatus,
            "state_supervision_type": state_supervision.StateSupervisionType,
            "state_acting_body_type": recidiviz.common.constants.state.shared_enums.StateActingBodyType,
            "state_custodial_authority": recidiviz.common.constants.state.shared_enums.StateCustodialAuthority,
            "state_early_discharge_decision": StateEarlyDischargeDecision,
            "state_early_discharge_decision_status": StateEarlyDischargeDecisionStatus,
            "state_fine_status": state_fine.StateFineStatus,
            "state_incarceration_type": state_incarceration.StateIncarcerationType,
            "state_court_case_status": state_court_case.StateCourtCaseStatus,
            "state_court_type": state_court_case.StateCourtType,
            "state_agent_type": state_agent.StateAgentType,
            "state_incarceration_period_status": state_incarceration_period.StateIncarcerationPeriodStatus,
            "state_incarceration_facility_security_level": state_incarceration_period.StateIncarcerationFacilitySecurityLevel,
            "state_incarceration_period_admission_reason": state_incarceration_period.StateIncarcerationPeriodAdmissionReason,
            "state_incarceration_period_release_reason": state_incarceration_period.StateIncarcerationPeriodReleaseReason,
            "state_parole_decision_outcome": state_parole_decision.StateParoleDecisionOutcome,
            "state_person_alias_type": state_person_alias.StatePersonAliasType,
            "state_supervision_period_admission_reason": state_supervision_period.StateSupervisionPeriodAdmissionReason,
            "state_supervision_period_supervision_type": state_supervision_period.StateSupervisionPeriodSupervisionType,
            "state_supervision_period_termination_reason": state_supervision_period.StateSupervisionPeriodTerminationReason,
            "state_supervision_level": state_supervision_period.StateSupervisionLevel,
            "state_incarceration_incident_type": state_incarceration_incident.StateIncarcerationIncidentType,
            "state_incarceration_incident_outcome_type": state_incarceration_incident.StateIncarcerationIncidentOutcomeType,
            "state_specialized_purpose_for_incarceration": state_incarceration_period.StateSpecializedPurposeForIncarceration,
            "state_program_assignment_participation_status": state_program_assignment.StateProgramAssignmentParticipationStatus,
            "state_program_assignment_discharge_reason": state_program_assignment.StateProgramAssignmentDischargeReason,
            "state_supervision_case_type": state_supervision_case_type_entry.StateSupervisionCaseType,
            "state_supervision_contact_location": state_supervision_contact.StateSupervisionContactLocation,
            "state_supervision_contact_reason": state_supervision_contact.StateSupervisionContactReason,
            "state_supervision_contact_status": state_supervision_contact.StateSupervisionContactStatus,
            "state_supervision_contact_type": state_supervision_contact.StateSupervisionContactType,
            "state_supervision_contact_method": state_supervision_contact.StateSupervisionContactMethod,
            "state_supervision_violation_type": state_supervision_violation.StateSupervisionViolationType,
            "state_supervision_violation_response_type": state_supervision_violation_response.StateSupervisionViolationResponseType,
            "state_supervision_violation_response_decision": state_supervision_violation_response.StateSupervisionViolationResponseDecision,
            "state_supervision_violation_response_deciding_body_type": state_supervision_violation_response.StateSupervisionViolationResponseDecidingBodyType,
        }

        merged_mapping = {**self.SHARED_ENUMS_TEST_MAPPING, **state_enums_mapping}

        self.check_persistence_and_schema_enums_match(merged_mapping, schema)

    def testAllEnumNamesPrefixedWithState(self):
        shared_enum_ids = [
            id(enum) for enum in self._get_all_sqlalchemy_enums_in_module(shared_enums)
        ]

        for enum in self._get_all_sqlalchemy_enums_in_module(schema):
            if id(enum) in shared_enum_ids:
                continue
            self.assertTrue(enum.name.startswith("state_"))


class TestStateSchemaTableConsistency(TestSchemaTableConsistency):
    """Test class for validating state schema tables are defined correctly"""

    def testAllTableNamesPrefixedWithState(self):
        for cls in get_all_table_classes_in_module(schema):
            self.assertTrue(cls.name.startswith("state_"))

    def testAllDatabaseEntityNamesPrefixedWithState(self):
        for cls in _get_all_database_entities_in_module(schema):
            self.assertTrue(cls.__name__.startswith("State"))


@pytest.mark.uses_db
class TestUniqueExternalIdConstraint(unittest.TestCase):
    """Tests for UniqueConstraint on StatePersonExternalId."""

    EXTERNAL_ID_1 = "EXTERNAL_ID_1"
    ID_TYPE_1 = "ID_TYPE_1"
    ID_TYPE_2 = "ID_TYPE_2"

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.canonical_for_schema(SchemaType.STATE)
        local_postgres_helpers.use_on_disk_postgresql_database(self.database_key)

        self.state_code = "US_XX"

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(self.database_key)

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def test_add_person_conflicting_external_id(self) -> None:
        # Arrange
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as arrange_session:
            db_external_id = generate_external_id(
                state_code=self.state_code,
                external_id=self.EXTERNAL_ID_1,
                id_type=self.ID_TYPE_1,
            )

            db_person = generate_person(
                state_code=self.state_code, external_ids=[db_external_id]
            )

            arrange_session.add(db_person)
            arrange_session.commit()

        db_external_id_duplicated = generate_external_id(
            state_code=self.state_code,
            external_id=self.EXTERNAL_ID_1,
            id_type=self.ID_TYPE_1,
        )

        db_person_new = generate_person(
            state_code=self.state_code, external_ids=[db_external_id_duplicated]
        )

        # Act
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(db_person_new)
            session.flush()

            with self.assertRaises(sqlalchemy.exc.IntegrityError):
                session.commit()

    def test_add_person_conflicting_external_id_no_flush(self) -> None:
        # Arrange
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as arrange_session:
            db_external_id = generate_external_id(
                state_code=self.state_code,
                external_id=self.EXTERNAL_ID_1,
                id_type=self.ID_TYPE_1,
            )

            db_person = generate_person(
                state_code=self.state_code, external_ids=[db_external_id]
            )

            arrange_session.add(db_person)
            arrange_session.commit()

        db_external_id_duplicated = generate_external_id(
            state_code=self.state_code,
            external_id=self.EXTERNAL_ID_1,
            id_type=self.ID_TYPE_1,
        )

        db_person_new = generate_person(
            state_code=self.state_code, external_ids=[db_external_id_duplicated]
        )

        # Act
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(db_person_new)
            with self.assertRaises(sqlalchemy.exc.IntegrityError):
                session.commit()

    def test_add_person_conflicting_external_id_different_type(self) -> None:
        # Arrange
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as arrange_session:
            db_external_id = generate_external_id(
                state_code=self.state_code,
                external_id=self.EXTERNAL_ID_1,
                id_type=self.ID_TYPE_1,
            )

            db_person = generate_person(
                state_code=self.state_code, external_ids=[db_external_id]
            )

            arrange_session.add(db_person)
            arrange_session.commit()

        db_external_id_duplicated = generate_external_id(
            state_code=self.state_code,
            external_id=self.EXTERNAL_ID_1,
            id_type=self.ID_TYPE_2,
        )

        db_person_new = generate_person(
            state_code=self.state_code, external_ids=[db_external_id_duplicated]
        )

        # Act
        with SessionFactory.using_database(self.database_key) as session:
            session.add(db_person_new)
            session.flush()

    def test_add_person_conflicting_external_id_different_state(self) -> None:
        # Arrange
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as arrange_session:
            db_external_id = generate_external_id(
                state_code="OTHER_STATE_CODE",
                external_id=self.EXTERNAL_ID_1,
                id_type=self.ID_TYPE_1,
            )

            db_person = generate_person(
                state_code="OTHER_STATE_CODE", external_ids=[db_external_id]
            )

            arrange_session.add(db_person)
            arrange_session.commit()

        db_external_id_duplicated = generate_external_id(
            state_code=self.state_code,
            external_id=self.EXTERNAL_ID_1,
            id_type=self.ID_TYPE_1,
        )

        db_person_new = generate_person(
            state_code=self.state_code, external_ids=[db_external_id_duplicated]
        )

        # Act
        with SessionFactory.using_database(self.database_key) as session:
            session.add(db_person_new)
            session.flush()

    def test_add_person_conflicting_external_id_same_session(self) -> None:
        # Arrange
        db_external_id = generate_external_id(
            state_code=self.state_code,
            external_id=self.EXTERNAL_ID_1,
            id_type=self.ID_TYPE_1,
        )

        db_person = generate_person(
            state_code=self.state_code, external_ids=[db_external_id]
        )

        db_external_id_duplicated = generate_external_id(
            state_code=self.state_code,
            external_id=self.EXTERNAL_ID_1,
            id_type=self.ID_TYPE_1,
        )

        db_person_2 = generate_person(
            state_code=self.state_code, external_ids=[db_external_id_duplicated]
        )

        # Act
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(db_person)
            session.add(db_person_2)
            session.flush()

            with self.assertRaises(sqlalchemy.exc.IntegrityError):
                session.commit()


@pytest.mark.uses_db
class TestStateSchemaUniqueConstraints(unittest.TestCase):
    """Generalized Test Class for SupervisionViolation, SupervisionCaseTypeEntry, IncarcerationPeriod,
    SupervisionViolationResponse, Bond, ParoleDecision, EarlyDischarge, ProgramAssignment, SupervisionContact, Charge"""

    EXTERNAL_ID_1 = "EXTERNAL_ID_1"
    EXTERNAL_ID_2 = "EXTERNAL_ID_2"
    ID_TYPE_1 = "ID_TYPE_1"
    ID_TYPE_2 = "ID_TYPE_2"

    generate_functions = [
        (generate_supervision_violation,),
        (generate_supervision_case_type_entry,),
        (generate_incarceration_period,),
        (generate_supervision_violation_response,),
        (generate_parole_decision,),
        (generate_early_discharge,),
        (generate_program_assignment,),
        (generate_supervision_contact,),
        (generate_charge,),
    ]
    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.canonical_for_schema(SchemaType.STATE)
        local_postgres_helpers.use_on_disk_postgresql_database(
            SQLAlchemyDatabaseKey.canonical_for_schema(SchemaType.STATE)
        )

        self.state_code = "US_XX"

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(
            SQLAlchemyDatabaseKey.canonical_for_schema(SchemaType.STATE)
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    @parameterized.expand(generate_functions)
    def test_add_object_conflicting_external_id(self, generate_func) -> None:
        # Arrange
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as arrange_session:
            db_external_id = generate_external_id(
                state_code=self.state_code,
                external_id=self.EXTERNAL_ID_1,
                id_type=self.ID_TYPE_1,
            )
            db_person = generate_person(
                state_code=self.state_code, external_ids=[db_external_id]
            )
            db_object_new = generate_func(
                db_person,
                external_id=self.EXTERNAL_ID_1,
                state_code=self.state_code,
            )

            arrange_session.add(db_object_new)
            arrange_session.commit()

        db_external_id_duplicated = generate_external_id(
            state_code=self.state_code,
            external_id=self.EXTERNAL_ID_2,
            id_type=self.ID_TYPE_1,
        )
        db_person = generate_person(
            state_code=self.state_code, external_ids=[db_external_id_duplicated]
        )
        db_object_new = generate_func(
            db_person,
            external_id=self.EXTERNAL_ID_1,
            state_code=self.state_code,
        )

        # Act
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(db_object_new)
            session.flush()

            with self.assertRaises(sqlalchemy.exc.IntegrityError):
                session.commit()

    @parameterized.expand(generate_functions)
    def test_add_object_conflicting_external_id_no_flush(self, generate_func) -> None:
        # Arrange
        with SessionFactory.using_database(self.database_key) as arrange_session:
            db_external_id = generate_external_id(
                state_code=self.state_code,
                external_id=self.EXTERNAL_ID_1,
                id_type=self.ID_TYPE_1,
            )
            db_person = generate_person(
                state_code=self.state_code, external_ids=[db_external_id]
            )
            db_object_new = generate_func(
                db_person, external_id=self.EXTERNAL_ID_1, state_code=self.state_code
            )

            arrange_session.add(db_object_new)

        db_external_id = generate_external_id(
            state_code=self.state_code,
            external_id=self.EXTERNAL_ID_2,
            id_type=self.ID_TYPE_1,
        )
        db_person = generate_person(
            state_code=self.state_code, external_ids=[db_external_id]
        )
        db_object_new = generate_func(
            db_person, external_id=self.EXTERNAL_ID_1, state_code=self.state_code
        )

        # Act
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(db_object_new)
            with self.assertRaises(sqlalchemy.exc.IntegrityError):
                session.commit()

    @parameterized.expand(generate_functions)
    def test_add_object_conflicting_external_id_different_state(
        self, generate_func
    ) -> None:
        # Arrange
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as arrange_session:
            db_external_id = generate_external_id(
                state_code="OTHER_STATE_CODE",
                external_id=self.EXTERNAL_ID_1,
                id_type=self.ID_TYPE_1,
            )

            db_person = generate_person(
                state_code="OTHER_STATE_CODE", external_ids=[db_external_id]
            )

            db_object = generate_func(
                db_person,
                external_id=self.EXTERNAL_ID_1,
                state_code="OTHER_STATE_CODE",
            )
            arrange_session.add(db_object)

        db_external_id_duplicated = generate_external_id(
            state_code=self.state_code,
            external_id=self.EXTERNAL_ID_2,
            id_type=self.ID_TYPE_1,
        )

        db_person_new = generate_person(
            state_code=self.state_code, external_ids=[db_external_id_duplicated]
        )
        db_object_new = generate_func(
            db_person_new,
            external_id=self.EXTERNAL_ID_1,
            state_code=self.state_code,
        )

        # Act
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(db_object_new)
            session.flush()

    @parameterized.expand(generate_functions)
    def test_add_object_conflicting_external_id_same_session(
        self, generate_func
    ) -> None:
        # Arrange
        db_external_id = generate_external_id(
            state_code=self.state_code,
            external_id=self.EXTERNAL_ID_1,
            id_type=self.ID_TYPE_1,
        )
        db_person = generate_person(
            state_code=self.state_code, external_ids=[db_external_id]
        )
        db_object_new = generate_func(
            db_person,
            external_id=self.EXTERNAL_ID_1,
            state_code=self.state_code,
        )

        db_external_id = generate_external_id(
            state_code=self.state_code,
            external_id=self.EXTERNAL_ID_2,
            id_type=self.ID_TYPE_1,
        )
        db_person_2 = generate_person(
            state_code=self.state_code, external_ids=[db_external_id]
        )
        db_object_new_2 = generate_func(
            db_person_2,
            external_id=self.EXTERNAL_ID_1,
            state_code=self.state_code,
        )

        # Act
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(db_object_new)
            session.add(db_object_new_2)
            session.flush()

            with self.assertRaises(sqlalchemy.exc.IntegrityError):
                session.commit()

    @parameterized.expand(generate_functions)
    def test_add_object_diff_external_id_same_state(self, generate_func) -> None:
        # Arrange
        with SessionFactory.using_database(self.database_key) as arrange_session:
            db_external_id = generate_external_id(
                state_code=self.state_code,
                external_id=self.EXTERNAL_ID_2,
                id_type=self.ID_TYPE_1,
            )

            db_person = generate_person(
                state_code=self.state_code, external_ids=[db_external_id]
            )

            db_object = generate_func(
                db_person,
                external_id=self.EXTERNAL_ID_2,
                state_code=self.state_code,
            )
            arrange_session.add(db_object)

        db_external_id_duplicated = generate_external_id(
            state_code=self.state_code,
            external_id=self.EXTERNAL_ID_1,
            id_type=self.ID_TYPE_1,
        )

        db_person_new = generate_person(
            state_code=self.state_code, external_ids=[db_external_id_duplicated]
        )
        db_object_new = generate_func(
            db_person_new,
            external_id=self.EXTERNAL_ID_1,
            state_code=self.state_code,
        )

        # Act
        with SessionFactory.using_database(self.database_key) as session:
            session.add(db_object_new)
            session.flush()


@pytest.mark.uses_db
class TestUniqueExternalIdConstraintOnFine(unittest.TestCase):
    """Tests for UniqueConstraint on StateFine"""

    EXTERNAL_ID_1 = "EXTERNAL_ID_1"
    EXTERNAL_ID_2 = "EXTERNAL_ID_2"
    ID_TYPE_1 = "ID_TYPE_1"
    ID_TYPE_2 = "ID_TYPE_2"
    _ID_1 = 1
    _ID_2 = 2
    OTHER_STATE_CODE = "OTHER_STATE_CODE"
    FULL_NAME = "_FULL_NAME"
    COUNTY_CODE = "_COUNTY_CODE"

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.canonical_for_schema(SchemaType.STATE)
        local_postgres_helpers.use_on_disk_postgresql_database(
            SQLAlchemyDatabaseKey.canonical_for_schema(SchemaType.STATE)
        )

        self.state_code = "US_XX"

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(
            SQLAlchemyDatabaseKey.canonical_for_schema(SchemaType.STATE)
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def test_add_fine_conflicting_external_id(self) -> None:
        # Arrange
        with SessionFactory.using_database(self.database_key) as arrange_session:
            db_person = schema.StatePerson(
                full_name=self.FULL_NAME, state_code=self.state_code
            )
            db_fine = schema.StateFine(
                person=db_person,
                status=StateFineStatus.EXTERNAL_UNKNOWN.value,
                state_code=self.state_code,
                external_id=self.EXTERNAL_ID_1,
                county_code=self.COUNTY_CODE,
            )
            db_sentence_group = schema.StateSentenceGroup(
                status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
                external_id=self.EXTERNAL_ID_1,
                state_code=self.state_code,
                county_code=self.COUNTY_CODE,
                fines=[db_fine],
            )
            db_external_id = schema.StatePersonExternalId(
                state_code=self.state_code,
                external_id=self.EXTERNAL_ID_1,
                id_type=self.ID_TYPE_1,
            )
            db_person.sentence_groups = [db_sentence_group]
            db_person.external_ids = [db_external_id]

            arrange_session.add(db_fine)

        db_person_dupe = schema.StatePerson(
            full_name=self.FULL_NAME, state_code=self.state_code
        )
        db_fine_dupe = schema.StateFine(
            person=db_person_dupe,
            status=StateFineStatus.EXTERNAL_UNKNOWN.value,
            state_code=self.state_code,
            external_id=self.EXTERNAL_ID_1,
            county_code=self.COUNTY_CODE,
        )
        db_sentence_group_dupe = schema.StateSentenceGroup(
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=self.EXTERNAL_ID_2,
            state_code=self.state_code,
            county_code=self.COUNTY_CODE,
            fines=[db_fine_dupe],
        )
        db_external_id_dupe = schema.StatePersonExternalId(
            state_code=self.state_code,
            external_id=self.EXTERNAL_ID_2,
            id_type=self.ID_TYPE_1,
        )
        db_person_dupe.sentence_groups = [db_sentence_group_dupe]
        db_person_dupe.external_ids = [db_external_id_dupe]

        # Act
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(db_fine_dupe)
            session.flush()

            with self.assertRaises(sqlalchemy.exc.IntegrityError):
                session.commit()

    def test_add_fine_conflicting_external_id_no_flush(self) -> None:
        # Arrange
        with SessionFactory.using_database(self.database_key) as arrange_session:
            db_person = schema.StatePerson(
                person_id=self._ID_1,
                full_name=self.FULL_NAME,
                state_code=self.state_code,
            )
            db_fine = schema.StateFine(
                person=db_person,
                status=StateFineStatus.EXTERNAL_UNKNOWN.value,
                fine_id=self._ID_1,
                state_code=self.state_code,
                external_id=self.EXTERNAL_ID_1,
                county_code=self.COUNTY_CODE,
            )
            db_sentence_group = schema.StateSentenceGroup(
                sentence_group_id=self._ID_1,
                status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
                external_id=self.EXTERNAL_ID_1,
                state_code=self.state_code,
                county_code=self.COUNTY_CODE,
                fines=[db_fine],
            )
            db_external_id = schema.StatePersonExternalId(
                person_external_id_id=self._ID_1,
                state_code=self.state_code,
                external_id=self.EXTERNAL_ID_1,
                id_type=self.ID_TYPE_1,
            )
            db_person.sentence_groups = [db_sentence_group]
            db_person.external_ids = [db_external_id]

            arrange_session.add(db_fine)

        db_person_dupe = schema.StatePerson(
            person_id=self._ID_2,
            full_name=self.FULL_NAME,
            state_code=self.OTHER_STATE_CODE,
        )
        db_fine_dupe = schema.StateFine(
            person=db_person_dupe,
            status=StateFineStatus.EXTERNAL_UNKNOWN.value,
            fine_id=self._ID_2,
            state_code=self.state_code,
            external_id=self.EXTERNAL_ID_1,
            county_code=self.COUNTY_CODE,
        )
        db_sentence_group_dupe = schema.StateSentenceGroup(
            sentence_group_id=self._ID_2,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=self.EXTERNAL_ID_2,
            state_code=self.state_code,
            county_code=self.COUNTY_CODE,
            fines=[db_fine_dupe],
        )
        db_external_id_dupe = schema.StatePersonExternalId(
            person_external_id_id=self._ID_2,
            state_code=self.state_code,
            external_id=self.EXTERNAL_ID_2,
            id_type=self.ID_TYPE_1,
        )
        db_person_dupe.sentence_groups = [db_sentence_group_dupe]
        db_person_dupe.external_ids = [db_external_id_dupe]

        # Act
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(db_fine_dupe)
            with self.assertRaises(sqlalchemy.exc.IntegrityError):
                session.commit()

    def test_add_fine_conflicting_external_id_different_state(self) -> None:
        # Arrange
        with SessionFactory.using_database(self.database_key) as arrange_session:
            db_person = schema.StatePerson(
                person_id=self._ID_1,
                full_name=self.FULL_NAME,
                state_code=self.state_code,
            )
            db_fine = schema.StateFine(
                person=db_person,
                status=StateFineStatus.EXTERNAL_UNKNOWN.value,
                fine_id=self._ID_1,
                state_code=self.state_code,
                external_id=self.EXTERNAL_ID_1,
                county_code=self.COUNTY_CODE,
            )
            db_sentence_group = schema.StateSentenceGroup(
                sentence_group_id=self._ID_1,
                status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
                external_id=self.EXTERNAL_ID_1,
                state_code=self.state_code,
                county_code=self.COUNTY_CODE,
                fines=[db_fine],
            )
            db_external_id = schema.StatePersonExternalId(
                person_external_id_id=self._ID_1,
                state_code=self.state_code,
                external_id=self.EXTERNAL_ID_1,
                id_type=self.ID_TYPE_1,
            )
            db_person.sentence_groups = [db_sentence_group]
            db_person.external_ids = [db_external_id]

            arrange_session.add(db_fine)

        db_person_dupe = schema.StatePerson(
            person_id=self._ID_2,
            full_name=self.FULL_NAME,
            state_code=self.OTHER_STATE_CODE,
        )
        db_fine_dupe = schema.StateFine(
            person=db_person_dupe,
            status=StateFineStatus.EXTERNAL_UNKNOWN.value,
            fine_id=self._ID_2,
            state_code=self.OTHER_STATE_CODE,
            external_id=self.EXTERNAL_ID_1,
            county_code=self.COUNTY_CODE,
        )
        db_sentence_group_dupe = schema.StateSentenceGroup(
            sentence_group_id=self._ID_2,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=self.EXTERNAL_ID_2,
            state_code=self.OTHER_STATE_CODE,
            county_code=self.COUNTY_CODE,
            fines=[db_fine_dupe],
        )
        db_external_id_dupe = schema.StatePersonExternalId(
            person_external_id_id=self._ID_2,
            state_code=self.OTHER_STATE_CODE,
            external_id=self.EXTERNAL_ID_2,
            id_type=self.ID_TYPE_1,
        )
        db_person_dupe.sentence_groups = [db_sentence_group_dupe]
        db_person_dupe.external_ids = [db_external_id_dupe]

        # Act
        with SessionFactory.using_database(self.database_key) as session:
            session.add(db_fine_dupe)
            session.flush()

    def test_add_fine_conflicting_external_id_same_session(self) -> None:
        # Arrange
        db_person = schema.StatePerson(
            full_name=self.FULL_NAME, state_code=self.state_code
        )
        db_fine = schema.StateFine(
            person=db_person,
            status=StateFineStatus.EXTERNAL_UNKNOWN.value,
            state_code=self.state_code,
            external_id=self.EXTERNAL_ID_1,
            county_code=self.COUNTY_CODE,
        )
        db_sentence_group = schema.StateSentenceGroup(
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=self.EXTERNAL_ID_1,
            state_code=self.state_code,
            county_code=self.COUNTY_CODE,
            fines=[db_fine],
        )
        db_external_id = schema.StatePersonExternalId(
            state_code=self.state_code,
            external_id=self.EXTERNAL_ID_1,
            id_type=self.ID_TYPE_1,
        )
        db_person.sentence_groups = [db_sentence_group]
        db_person.external_ids = [db_external_id]

        db_person_dupe = schema.StatePerson(
            full_name=self.FULL_NAME, state_code=self.state_code
        )
        db_fine_dupe = schema.StateFine(
            person=db_person_dupe,
            status=StateFineStatus.EXTERNAL_UNKNOWN.value,
            state_code=self.state_code,
            external_id=self.EXTERNAL_ID_1,
            county_code=self.COUNTY_CODE,
        )
        db_sentence_group_dupe = schema.StateSentenceGroup(
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=self.EXTERNAL_ID_2,
            state_code=self.state_code,
            county_code=self.COUNTY_CODE,
            fines=[db_fine_dupe],
        )
        db_external_id_dupe = schema.StatePersonExternalId(
            state_code=self.state_code,
            external_id=self.EXTERNAL_ID_2,
            id_type=self.ID_TYPE_1,
        )
        db_person_dupe.sentence_groups = [db_sentence_group_dupe]
        db_person_dupe.external_ids = [db_external_id_dupe]

        # Act
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(db_fine)
            session.add(db_fine_dupe)
            session.flush()

            with self.assertRaises(sqlalchemy.exc.IntegrityError):
                session.commit()

    def test_add_fine_different_external_id_same_state(self) -> None:
        # Arrange
        with SessionFactory.using_database(self.database_key) as arrange_session:
            db_person = schema.StatePerson(
                person_id=self._ID_1,
                full_name=self.FULL_NAME,
                state_code=self.state_code,
            )
            db_fine = schema.StateFine(
                person=db_person,
                status=StateFineStatus.EXTERNAL_UNKNOWN.value,
                fine_id=self._ID_1,
                state_code=self.state_code,
                external_id=self.EXTERNAL_ID_1,
                county_code=self.COUNTY_CODE,
            )
            db_sentence_group = schema.StateSentenceGroup(
                sentence_group_id=self._ID_1,
                status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
                external_id=self.EXTERNAL_ID_1,
                state_code=self.state_code,
                county_code=self.COUNTY_CODE,
                fines=[db_fine],
            )
            db_external_id = schema.StatePersonExternalId(
                person_external_id_id=self._ID_1,
                state_code=self.state_code,
                external_id=self.EXTERNAL_ID_1,
                id_type=self.ID_TYPE_1,
            )
            db_person.sentence_groups = [db_sentence_group]
            db_person.external_ids = [db_external_id]

            arrange_session.add(db_fine)

        db_person_dupe = schema.StatePerson(
            person_id=self._ID_2, full_name=self.FULL_NAME, state_code=self.state_code
        )
        db_fine_dupe = schema.StateFine(
            person=db_person_dupe,
            status=StateFineStatus.EXTERNAL_UNKNOWN.value,
            fine_id=self._ID_2,
            state_code=self.state_code,
            external_id=self.EXTERNAL_ID_2,
            county_code=self.COUNTY_CODE,
        )
        db_sentence_group_dupe = schema.StateSentenceGroup(
            sentence_group_id=self._ID_2,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=self.EXTERNAL_ID_2,
            state_code=self.state_code,
            county_code=self.COUNTY_CODE,
            fines=[db_fine_dupe],
        )
        db_external_id_dupe = schema.StatePersonExternalId(
            person_external_id_id=self._ID_2,
            state_code=self.state_code,
            external_id=self.EXTERNAL_ID_2,
            id_type=self.ID_TYPE_1,
        )
        db_person_dupe.sentence_groups = [db_sentence_group_dupe]
        db_person_dupe.external_ids = [db_external_id_dupe]

        # Act
        with SessionFactory.using_database(self.database_key) as session:
            session.add(db_fine_dupe)
            session.flush()


@pytest.mark.uses_db
class TestUniqueExternalIdConstraintOnCourtCase(unittest.TestCase):
    """Tests for UniqueConstraint on StateCourtCase"""

    EXTERNAL_ID_1 = "EXTERNAL_ID_1"
    EXTERNAL_ID_2 = "EXTERNAL_ID_2"
    PERSON_ID_1 = 1
    OTHER_STATE_CODE = "US_YY"

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.canonical_for_schema(SchemaType.STATE)
        local_postgres_helpers.use_on_disk_postgresql_database(
            SQLAlchemyDatabaseKey.canonical_for_schema(SchemaType.STATE)
        )

        self.state_code = "US_XX"

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(
            SQLAlchemyDatabaseKey.canonical_for_schema(SchemaType.STATE)
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def test_add_court_case_conflicting_external_id_person_id(self) -> None:
        # Arrange
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            db_person = generate_person(
                state_code=self.state_code, person_id=self.PERSON_ID_1
            )
            db_judge = generate_agent(
                state_code=self.state_code, external_id=self.EXTERNAL_ID_1
            )
            db_court_case = generate_court_case(
                person=db_person,
                external_id=self.EXTERNAL_ID_1,
                state_code=self.state_code,
                judge=db_judge,
            )

            session.add(db_court_case)
            session.commit()

            db_judge = generate_agent(
                state_code=self.state_code, external_id=self.EXTERNAL_ID_2
            )
            db_court_case_dupe = generate_court_case(
                person=db_person,
                external_id=self.EXTERNAL_ID_1,
                state_code=self.state_code,
                judge=db_judge,
            )

            # Act
            session.add(db_court_case_dupe)
            session.flush()

            with self.assertRaises(sqlalchemy.exc.IntegrityError):
                session.commit()

    def test_add_court_case_conflicting_external_id_person_id_no_flush(self) -> None:
        # Arrange
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            db_person = generate_person(
                state_code=self.state_code, person_id=self.PERSON_ID_1
            )
            db_judge = generate_agent(
                state_code=self.state_code, external_id=self.EXTERNAL_ID_1
            )
            db_court_case = generate_court_case(
                person=db_person,
                external_id=self.EXTERNAL_ID_1,
                state_code=self.state_code,
                judge=db_judge,
            )

            session.add(db_court_case)
            session.commit()

            db_judge = generate_agent(
                state_code=self.state_code, external_id=self.EXTERNAL_ID_2
            )
            db_court_case_dupe = generate_court_case(
                person=db_person,
                external_id=self.EXTERNAL_ID_1,
                state_code=self.state_code,
                judge=db_judge,
            )

            # Act
            session.add(db_court_case_dupe)
            with self.assertRaises(sqlalchemy.exc.IntegrityError):
                session.commit()

    def test_add_court_case_conflicting_external_id_person_id_different_state(
        self,
    ) -> None:
        # Arrange
        with SessionFactory.using_database(self.database_key) as arrange_session:
            db_person = generate_person(state_code=self.state_code)
            db_court_case = generate_court_case(
                person=db_person,
                external_id=self.EXTERNAL_ID_1,
                state_code=self.state_code,
            )

            arrange_session.add(db_court_case)

        db_person_other_state = generate_person(state_code=self.OTHER_STATE_CODE)
        db_court_case_dupe = generate_court_case(
            person=db_person_other_state,
            external_id=self.EXTERNAL_ID_1,
            state_code=self.OTHER_STATE_CODE,
        )

        # Act
        with SessionFactory.using_database(self.database_key) as session:
            session.add(db_court_case_dupe)
            session.flush()

    def test_add_court_case_different_external_id_same_state(self) -> None:
        # Arrange
        with SessionFactory.using_database(self.database_key) as arrange_session:
            db_person = generate_person(state_code=self.state_code)
            db_court_case = generate_court_case(
                person=db_person,
                external_id=self.EXTERNAL_ID_1,
                state_code=self.state_code,
            )

            arrange_session.add(db_court_case)

        db_person = generate_person(state_code=self.state_code)
        db_court_case_dupe = generate_court_case(
            person=db_person,
            external_id=self.EXTERNAL_ID_2,
            state_code=self.state_code,
        )

        # Act
        with SessionFactory.using_database(self.database_key) as session:
            session.add(db_court_case_dupe)
            session.flush()
