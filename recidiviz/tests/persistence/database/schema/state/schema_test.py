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
import datetime
import unittest
from typing import Optional

import pytest
import sqlalchemy
from parameterized import parameterized

from recidiviz.common.constants.state import (
    enum_canonical_strings,
    state_agent,
    state_assessment,
    state_charge,
    state_court_case,
    state_incarceration,
    state_incarceration_incident,
    state_incarceration_period,
    state_person,
    state_person_alias,
    state_program_assignment,
    state_sentence,
    state_shared_enums,
    state_supervision_contact,
    state_supervision_period,
    state_supervision_sentence,
    state_supervision_violation,
    state_supervision_violation_response,
    state_task_deadline,
)
from recidiviz.common.constants.state.state_drug_screen import (
    StateDrugScreenResult,
    StateDrugScreenSampleType,
)
from recidiviz.common.constants.state.state_early_discharge import (
    StateEarlyDischargeDecision,
    StateEarlyDischargeDecisionStatus,
)
from recidiviz.common.constants.state.state_employment_period import (
    StateEmploymentPeriodEmploymentStatus,
    StateEmploymentPeriodEndReason,
)
from recidiviz.common.constants.state.state_task_deadline import StateTaskType
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

    def testPersistenceAndSchemaEnumsMatch(self) -> None:
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
            "state_charge_status": state_charge.StateChargeStatus,
            "state_drug_screen_result": StateDrugScreenResult,
            "state_drug_screen_sample_type": StateDrugScreenSampleType,
            "state_sentence_status": state_sentence.StateSentenceStatus,
            "state_supervision_sentence_supervision_type": state_supervision_sentence.StateSupervisionSentenceSupervisionType,
            "state_acting_body_type": state_shared_enums.StateActingBodyType,
            "state_custodial_authority": state_shared_enums.StateCustodialAuthority,
            "state_early_discharge_decision": StateEarlyDischargeDecision,
            "state_early_discharge_decision_status": StateEarlyDischargeDecisionStatus,
            "state_employment_period_employment_status": StateEmploymentPeriodEmploymentStatus,
            "state_employment_period_end_reason": StateEmploymentPeriodEndReason,
            "state_incarceration_type": state_incarceration.StateIncarcerationType,
            "state_court_case_status": state_court_case.StateCourtCaseStatus,
            "state_court_type": state_court_case.StateCourtType,
            "state_agent_type": state_agent.StateAgentType,
            "state_agent_subtype": state_agent.StateAgentSubtype,
            "state_incarceration_period_admission_reason": state_incarceration_period.StateIncarcerationPeriodAdmissionReason,
            "state_incarceration_period_release_reason": state_incarceration_period.StateIncarcerationPeriodReleaseReason,
            "state_gender": state_person.StateGender,
            "state_race": state_person.StateRace,
            "state_ethnicity": state_person.StateEthnicity,
            "state_residency_status": state_person.StateResidencyStatus,
            "state_person_alias_type": state_person_alias.StatePersonAliasType,
            "state_supervision_period_admission_reason": state_supervision_period.StateSupervisionPeriodAdmissionReason,
            "state_supervision_period_supervision_type": state_supervision_period.StateSupervisionPeriodSupervisionType,
            "state_supervision_period_termination_reason": state_supervision_period.StateSupervisionPeriodTerminationReason,
            "state_supervision_level": state_supervision_period.StateSupervisionLevel,
            "state_incarceration_incident_type": state_incarceration_incident.StateIncarcerationIncidentType,
            "state_incarceration_incident_outcome_type": state_incarceration_incident.StateIncarcerationIncidentOutcomeType,
            "state_specialized_purpose_for_incarceration": state_incarceration_period.StateSpecializedPurposeForIncarceration,
            "state_program_assignment_participation_status": state_program_assignment.StateProgramAssignmentParticipationStatus,
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
            "state_task_type": state_task_deadline.StateTaskType,
        }

        self.check_persistence_and_schema_enums_match(state_enums_mapping, schema)

    def testAllEnumNamesPrefixedWithState(self) -> None:
        for enum in self._get_all_sqlalchemy_enums_in_module(schema):
            self.assertTrue(enum.name.startswith("state_"))

    def testAllEnumsHaveUnknownValues(self) -> None:
        for enum in self._get_all_sqlalchemy_enums_in_module(schema):
            self.assertIn(enum_canonical_strings.internal_unknown, enum.enums)
            self.assertIn(enum_canonical_strings.external_unknown, enum.enums)


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
    SupervisionViolationResponse, Bond, EarlyDischarge, ProgramAssignment, SupervisionContact, Charge"""

    EXTERNAL_ID_1 = "EXTERNAL_ID_1"
    EXTERNAL_ID_2 = "EXTERNAL_ID_2"
    ID_TYPE_1 = "ID_TYPE_1"
    ID_TYPE_2 = "ID_TYPE_2"

    generate_functions = [
        (generate_supervision_violation,),
        (generate_supervision_case_type_entry,),
        (generate_incarceration_period,),
        (generate_supervision_violation_response,),
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


@pytest.mark.uses_db
class TestStateTaskDeadline(unittest.TestCase):
    """Tests for StateTaskDeadline schema entity."""

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

    def test_add_valid_task_deadlines(self) -> None:
        # Arrange
        db_person = generate_person(state_code=self.state_code)
        db_task_deadline_1 = schema.StateTaskDeadline(
            person=db_person,
            state_code=self.state_code,
            eligible_date=datetime.date(2022, 5, 1),
            due_date=datetime.date(2022, 5, 8),
            update_datetime=datetime.datetime(2022, 7, 1, 1, 2, 3),
            task_type=StateTaskType.DRUG_SCREEN.value,
            task_type_raw_text="DRU",
            task_subtype="MY_SUBTYPE",
        )
        db_task_deadline_2 = schema.StateTaskDeadline(
            person=db_person,
            state_code=self.state_code,
            eligible_date=None,
            due_date=datetime.date(2022, 5, 8),
            update_datetime=datetime.datetime(2022, 7, 1, 1, 2, 3),
            task_type=StateTaskType.HOME_VISIT.value,
            task_type_raw_text=None,
            task_subtype=None,
        )

        db_task_deadline_3 = schema.StateTaskDeadline(
            person=db_person,
            state_code=self.state_code,
            eligible_date=datetime.date(2022, 5, 1),
            due_date=None,
            update_datetime=datetime.datetime(2022, 7, 2, 3, 4, 5),
            task_type=StateTaskType.DISCHARGE_FROM_SUPERVISION.value,
            task_type_raw_text=None,
            task_subtype=None,
        )

        # Act
        with SessionFactory.using_database(self.database_key) as session:
            session.add(db_task_deadline_1)
            session.add(db_task_deadline_2)
            session.add(db_task_deadline_3)
            session.commit()

    def test_add_task_deadline_both_dates_null(self) -> None:
        # Arrange
        db_person = generate_person(state_code=self.state_code)
        db_task_deadline = schema.StateTaskDeadline(
            person=db_person,
            state_code=self.state_code,
            eligible_date=None,
            due_date=None,
            update_datetime=datetime.datetime(2022, 7, 1, 1, 2, 3),
            task_type=StateTaskType.DRUG_SCREEN.value,
            task_type_raw_text="DRU",
            task_subtype="MY_SUBTYPE",
        )

        # Act
        with SessionFactory.using_database(self.database_key) as session:
            session.add(db_task_deadline)
            session.commit()

    def test_add_task_deadline_eligible_after_due_date_violates_constraint(
        self,
    ) -> None:
        # Arrange
        db_person = generate_person(state_code=self.state_code)
        db_task_deadline = schema.StateTaskDeadline(
            person=db_person,
            state_code=self.state_code,
            eligible_date=datetime.date(2022, 5, 8),
            due_date=datetime.date(2022, 5, 1),
            update_datetime=datetime.datetime(2022, 7, 1, 1, 2, 3),
            task_type=StateTaskType.DRUG_SCREEN.value,
            task_type_raw_text="DRU",
            task_subtype="MY_SUBTYPE",
        )

        # Act
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(db_task_deadline)

            with self.assertRaisesRegex(
                sqlalchemy.exc.IntegrityError,
                'new row for relation "state_task_deadline" violates check constraint '
                '"eligible_date_before_due_date"',
            ):
                session.flush()

    def test_multiple_deadlines_same_update_datetime_violates_constraint(self) -> None:
        # Arrange
        db_person = generate_person(state_code=self.state_code)

        update_datetime = datetime.datetime(2022, 7, 1, 1, 2, 3)
        db_task_deadline_1 = schema.StateTaskDeadline(
            person=db_person,
            state_code=self.state_code,
            eligible_date=None,
            due_date=datetime.date(2022, 5, 8),
            update_datetime=update_datetime,
            task_type=StateTaskType.DRUG_SCREEN.value,
            task_type_raw_text=None,
            task_subtype="MY_SUBTYPE",
        )
        db_task_deadline_2 = schema.StateTaskDeadline(
            person=db_person,
            state_code=self.state_code,
            eligible_date=None,
            due_date=datetime.date(2022, 5, 1),
            update_datetime=update_datetime,
            task_type=StateTaskType.DRUG_SCREEN.value,
            task_type_raw_text=None,
            task_subtype="MY_SUBTYPE",
        )

        # Act
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(db_task_deadline_1)
            session.add(db_task_deadline_2)

            session.flush()

            with self.assertRaisesRegex(
                sqlalchemy.exc.IntegrityError,
                "duplicate key value violates unique constraint "
                '"state_task_deadline_unique_per_person_update_date_type"',
            ):
                session.commit()
