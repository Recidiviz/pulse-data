# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Unit tests to test validations for ingested entities."""
import unittest
from datetime import date, datetime
from typing import Dict, List, Type

import sqlalchemy
from more_itertools import one

from recidiviz.common.constants.state.state_charge import (
    StateChargeStatus,
    StateChargeV2Status,
)
from recidiviz.common.constants.state.state_sentence import (
    StateSentenceStatus,
    StateSentenceType,
    StateSentencingAuthority,
)
from recidiviz.common.constants.state.state_task_deadline import StateTaskType
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.schema_utils import (
    get_database_entity_by_table_name,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import get_all_entity_classes_in_module
from recidiviz.persistence.entity.state import entities as entities_schema
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.persistence.entity.state.entities import (
    StatePersonExternalId,
    StateStaffExternalId,
    StateTaskDeadline,
)
from recidiviz.pipelines.ingest.state.validator import validate_root_entity


class TestEntityValidations(unittest.TestCase):
    """Tests validations functions"""

    def test_valid_external_id_state_staff_entities(self) -> None:
        entity = state_entities.StateStaff(
            state_code="US_XX",
            staff_id=1111,
            external_ids=[
                StateStaffExternalId(
                    external_id="100",
                    state_code="US_XX",
                    id_type="US_XX_EMPLOYEE",
                ),
                StateStaffExternalId(
                    external_id="200",
                    state_code="US_XX",
                    id_type="US_EMP",
                ),
            ],
        )

        error_messages = validate_root_entity(entity)
        self.assertTrue(len(list(error_messages)) == 0)

    def test_missing_external_id_state_staff_entities(self) -> None:
        entity = state_entities.StateStaff(
            state_code="US_XX", staff_id=1111, external_ids=[]
        )

        error_messages = validate_root_entity(entity)
        self.assertRegex(
            one(error_messages),
            r"^Found \[StateStaff\] with id \[1111\] missing an external_id:",
        )

    def test_two_external_ids_same_type_state_staff_entities(self) -> None:
        entity = state_entities.StateStaff(
            state_code="US_XX",
            staff_id=1111,
            external_ids=[
                StateStaffExternalId(
                    external_id="100",
                    state_code="US_XX",
                    id_type="US_XX_EMPLOYEE",
                ),
                StateStaffExternalId(
                    external_id="200",
                    state_code="US_XX",
                    id_type="US_XX_EMPLOYEE",
                ),
            ],
        )

        error_messages = validate_root_entity(entity)
        self.assertRegex(
            one(error_messages),
            r"Duplicate external id types for \[StateStaff\] with id "
            r"\[1111\]: US_XX_EMPLOYEE",
        )

    def test_two_external_ids_exact_same_state_staff_entities(self) -> None:
        entity = state_entities.StateStaff(
            state_code="US_XX",
            staff_id=1111,
            external_ids=[
                StateStaffExternalId(
                    external_id="100",
                    state_code="US_XX",
                    id_type="US_XX_EMPLOYEE",
                ),
                StateStaffExternalId(
                    external_id="100",
                    state_code="US_XX",
                    id_type="US_XX_EMPLOYEE",
                ),
            ],
        )

        error_messages = validate_root_entity(entity)
        self.assertRegex(
            one(error_messages),
            r"Duplicate external id types for \[StateStaff\] with id "
            r"\[1111\]: US_XX_EMPLOYEE",
        )

    def test_valid_external_id_state_person_entities(self) -> None:
        entity = state_entities.StatePerson(
            state_code="US_XX",
            person_id=1111,
            external_ids=[
                StatePersonExternalId(
                    external_id="100",
                    state_code="US_XX",
                    id_type="US_XX_EMPLOYEE",
                ),
            ],
        )

        error_messages = validate_root_entity(entity)
        self.assertTrue(len(list(error_messages)) == 0)

    def test_missing_external_id_state_person_entities(self) -> None:
        entity = state_entities.StatePerson(
            state_code="US_XX", person_id=1111, external_ids=[]
        )

        error_messages = validate_root_entity(entity)
        self.assertRegex(
            one(error_messages),
            r"^Found \[StatePerson\] with id \[1111\] missing an external_id:",
        )

    def test_two_external_ids_same_type_state_person_entities(self) -> None:
        entity = state_entities.StatePerson(
            state_code="US_XX",
            person_id=1111,
            external_ids=[
                StatePersonExternalId(
                    external_id="100",
                    state_code="US_XX",
                    id_type="US_XX_EMPLOYEE",
                ),
                StatePersonExternalId(
                    external_id="200",
                    state_code="US_XX",
                    id_type="US_XX_EMPLOYEE",
                ),
            ],
        )

        error_messages = validate_root_entity(entity)
        self.assertRegex(
            one(error_messages),
            r"Duplicate external id types for \[StatePerson\] with id "
            r"\[1111\]: US_XX_EMPLOYEE",
        )

    def test_two_external_ids_exact_same_state_person_entities(self) -> None:
        entity = state_entities.StatePerson(
            state_code="US_XX",
            person_id=1111,
            external_ids=[
                StatePersonExternalId(
                    external_id="100",
                    state_code="US_XX",
                    id_type="US_XX_EMPLOYEE",
                ),
                StatePersonExternalId(
                    external_id="100",
                    state_code="US_XX",
                    id_type="US_XX_EMPLOYEE",
                ),
            ],
        )

        error_messages = validate_root_entity(entity)
        self.assertRegex(
            one(error_messages),
            r"Duplicate external id types for \[StatePerson\] with id "
            r"\[1111\]: US_XX_EMPLOYEE",
        )

    def test_entity_tree_unique_constraints_simple_valid(self) -> None:
        person = state_entities.StatePerson(
            state_code="US_XX",
            person_id=3111,
            external_ids=[
                StatePersonExternalId(
                    person_external_id_id=11114,
                    state_code="US_XX",
                    external_id="4001",
                    id_type="PERSON",
                ),
            ],
        )

        person.task_deadlines.append(
            StateTaskDeadline(
                task_deadline_id=1,
                state_code="US_XX",
                task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION,
                eligible_date=date(2020, 9, 11),
                update_datetime=datetime(2023, 2, 1, 11, 19),
                task_metadata='{"external_id": "00000001-111123-371006", "sentence_type": "INCARCERATION"}',
                person=person,
            )
        )

        person.task_deadlines.append(
            StateTaskDeadline(
                task_deadline_id=3,
                state_code="US_XX",
                task_type=StateTaskType.INTERNAL_UNKNOWN,
                eligible_date=date(2020, 9, 11),
                update_datetime=datetime(2023, 2, 1, 11, 19),
                task_metadata='{"external_id": "00000001-111123-371006", "sentence_type": "INCARCERATION"}',
                person=person,
            )
        )

        error_messages = validate_root_entity(person)
        self.assertTrue(len(list(error_messages)) == 0)

    def test_entity_tree_unique_constraints_simple_invalid(self) -> None:
        person = state_entities.StatePerson(
            state_code="US_XX",
            person_id=3111,
            external_ids=[
                StatePersonExternalId(
                    person_external_id_id=11114,
                    state_code="US_XX",
                    external_id="4001",
                    id_type="PERSON",
                ),
            ],
        )

        person.task_deadlines.append(
            StateTaskDeadline(
                task_deadline_id=1,
                state_code="US_XX",
                task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION,
                task_subtype=None,
                eligible_date=date(2020, 9, 11),
                update_datetime=datetime(2023, 2, 1, 11, 19),
                task_metadata='{"external_id": "00000001-111123-371006", "sentence_type": "INCARCERATION"}',
                person=person,
            )
        )

        # Add exact duplicate (only primary key is changed)
        person.task_deadlines.append(
            StateTaskDeadline(
                task_deadline_id=2,
                state_code="US_XX",
                task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION,
                task_subtype=None,
                eligible_date=date(2020, 9, 11),
                update_datetime=datetime(2023, 2, 1, 11, 19),
                task_metadata='{"external_id": "00000001-111123-371006", "sentence_type": "INCARCERATION"}',
                person=person,
            )
        )
        # Add similar with different task_type
        person.task_deadlines.append(
            StateTaskDeadline(
                task_deadline_id=3,
                state_code="US_XX",
                task_type=StateTaskType.INTERNAL_UNKNOWN,
                task_subtype=None,
                eligible_date=date(2020, 9, 11),
                update_datetime=datetime(2023, 2, 1, 11, 19),
                task_metadata='{"external_id": "00000001-111123-371006", "sentence_type": "INCARCERATION"}',
                person=person,
            )
        )

        error_messages = validate_root_entity(person)
        self.assertEqual(
            error_messages[0],
            (
                "Found [2] StateTaskDeadline entities with (state_code=US_XX, "
                "task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION, "
                "task_subtype=None, "
                'task_metadata={"external_id": "00000001-111123-371006", "sentence_type": "INCARCERATION"}, '
                "update_datetime=2023-02-01 11:19:00). First 2 entities found:\n"
                "  * StateTaskDeadline(task_deadline_id=1)\n"
                "  * StateTaskDeadline(task_deadline_id=2)"
            ),
        )
        self.assertIn(
            "If sequence_num is None, then the ledger's partition_key must be unique across hydrated entities.",
            error_messages[1],
        )

    def test_entity_tree_unique_constraints_invalid_all_nonnull(self) -> None:
        person = state_entities.StatePerson(
            state_code="US_XX",
            person_id=3111,
            external_ids=[
                StatePersonExternalId(
                    person_external_id_id=11114,
                    state_code="US_XX",
                    external_id="4001",
                    id_type="PERSON",
                ),
            ],
        )

        person.task_deadlines.append(
            StateTaskDeadline(
                task_deadline_id=1,
                state_code="US_XX",
                task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION,
                task_subtype="my_subtype",
                eligible_date=date(2020, 9, 11),
                update_datetime=datetime(2023, 2, 1, 11, 19),
                task_metadata='{"external_id": "00000001-111123-371006", "sentence_type": "INCARCERATION"}',
                person=person,
            )
        )

        # Add exact duplicate (only primary key is changed)
        person.task_deadlines.append(
            StateTaskDeadline(
                task_deadline_id=2,
                state_code="US_XX",
                task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION,
                task_subtype="my_subtype",
                eligible_date=date(2020, 9, 11),
                update_datetime=datetime(2023, 2, 1, 11, 19),
                task_metadata='{"external_id": "00000001-111123-371006", "sentence_type": "INCARCERATION"}',
                person=person,
            )
        )

        error_messages = validate_root_entity(person)
        expected_duplicate_error = (
            "Found [2] StateTaskDeadline entities with (state_code=US_XX, "
            "task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION, "
            "task_subtype=my_subtype, "
            'task_metadata={"external_id": "00000001-111123-371006", "sentence_type": "INCARCERATION"}, '
            "update_datetime=2023-02-01 11:19:00). First 2 entities found:\n"
            "  * StateTaskDeadline(task_deadline_id=1)\n"
            "  * StateTaskDeadline(task_deadline_id=2)"
        )
        expected_sequence_num_error = "If sequence_num is None, then the ledger's partition_key must be unique across hydrated entities."
        self.assertEqual(expected_duplicate_error, error_messages[0])
        self.assertIn(expected_sequence_num_error, error_messages[1])

    def test_entity_tree_unique_constraints_task_deadline_valid_tree(self) -> None:
        person = state_entities.StatePerson(
            state_code="US_XX",
            person_id=3111,
            external_ids=[
                StatePersonExternalId(
                    person_external_id_id=11114,
                    state_code="US_XX",
                    external_id="4001",
                    id_type="PERSON",
                ),
            ],
        )

        person.task_deadlines.append(
            StateTaskDeadline(
                task_deadline_id=1,
                state_code="US_XX",
                task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION,
                eligible_date=date(2020, 9, 11),
                update_datetime=datetime(2023, 2, 1, 11, 19),
                task_metadata='{"external_id": "00000001-111123-371006", "sentence_type": "INCARCERATION"}',
                person=person,
            )
        )

        person.task_deadlines.append(
            # Task is exactly identical except for task_deadline_id and task_metadata
            StateTaskDeadline(
                task_deadline_id=2,
                state_code="US_XX",
                task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION,
                eligible_date=date(2020, 9, 11),
                update_datetime=datetime(2023, 2, 1, 11, 19),
                task_metadata='{"external_id": "00000001-111123-371006", "sentence_type": "SUPERVISION"}',
                person=person,
            )
        )

        error_messages = validate_root_entity(person)
        self.assertTrue(len(list(error_messages)) == 0)

    def test_multiple_errors_returned_for_root_enities(self) -> None:
        person = state_entities.StatePerson(
            state_code="US_XX",
            person_id=3111,
            external_ids=[],
        )

        person.task_deadlines.append(
            StateTaskDeadline(
                task_deadline_id=1,
                state_code="US_XX",
                task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION,
                eligible_date=date(2020, 9, 11),
                update_datetime=datetime(2023, 2, 1, 11, 19),
                task_metadata='{"external_id": "00000001-111123-371006", "sentence_type": "INCARCERATION"}',
                person=person,
            )
        )

        # Add exact duplicate (only primary key is changed)
        person.task_deadlines.append(
            StateTaskDeadline(
                task_deadline_id=2,
                state_code="US_XX",
                task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION,
                eligible_date=date(2020, 9, 11),
                update_datetime=datetime(2023, 2, 1, 11, 19),
                task_metadata='{"external_id": "00000001-111123-371006", "sentence_type": "INCARCERATION"}',
                person=person,
            )
        )
        # Add similar with different task_type
        person.task_deadlines.append(
            StateTaskDeadline(
                task_deadline_id=3,
                state_code="US_XX",
                task_type=StateTaskType.INTERNAL_UNKNOWN,
                eligible_date=date(2020, 9, 11),
                update_datetime=datetime(2023, 2, 1, 11, 19),
                task_metadata='{"external_id": "00000001-111123-371006", "sentence_type": "INCARCERATION"}',
                person=person,
            )
        )

        error_messages = validate_root_entity(person)
        self.assertEqual(3, len(error_messages))
        self.assertRegex(
            error_messages[0],
            r"^Found \[StatePerson\] with id \[3111\] missing an external_id:",
        )
        expected_duplicate_error = (
            "Found [2] StateTaskDeadline entities with (state_code=US_XX, "
            "task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION, "
            "task_subtype=None, "
            'task_metadata={"external_id": "00000001-111123-371006", "sentence_type": "INCARCERATION"}, '
            "update_datetime=2023-02-01 11:19:00). First 2 entities found:\n"
            "  * StateTaskDeadline(task_deadline_id=1)\n"
            "  * StateTaskDeadline(task_deadline_id=2)"
        )
        self.assertEqual(expected_duplicate_error, error_messages[1])
        expected_ledger_error = "If sequence_num is None, then the ledger's partition_key must be unique across hydrated entities."
        self.assertIn(expected_ledger_error, error_messages[2])

    def test_normalized_entity_tree_no_backedges(self) -> None:
        normalized_person = normalized_entities.NormalizedStatePerson(
            state_code="US_XX",
            person_id=1,
            external_ids=[
                normalized_entities.NormalizedStatePersonExternalId(
                    state_code="US_XX",
                    person_external_id_id=1,
                    external_id="EXTERNAL_ID_1",
                    id_type="US_XX_ID_TYPE",
                )
            ],
        )

        error_messages = validate_root_entity(normalized_person)
        self.assertEqual(1, len(error_messages))
        expected_error = (
            "Found entity [NormalizedStatePersonExternalId("
            "external_id='EXTERNAL_ID_1', id_type='US_XX_ID_TYPE', "
            "person_external_id_id=1)] with null [person]. The [person] field must be "
            "set by the time we reach the validations step."
        )
        self.assertIn(expected_error, error_messages[0])

    def test_normalized_entity_tree_backedges_empty_list(self) -> None:
        charge = normalized_entities.NormalizedStateCharge(
            state_code="US_XX",
            charge_id=1,
            external_id="C1",
            status=StateChargeStatus.CONVICTED,
        )

        incarceration_sentence = (
            normalized_entities.NormalizedStateIncarcerationSentence(
                state_code="US_XX",
                incarceration_sentence_id=1,
                external_id="S1",
                status=StateSentenceStatus.SERVING,
                charges=[charge],
            )
        )
        charge.incarceration_sentences = [incarceration_sentence]
        # It should be valid to have a back-edge that is an empty list
        charge.supervision_sentences = []

        normalized_person = normalized_entities.NormalizedStatePerson(
            state_code="US_XX",
            person_id=1,
            external_ids=[
                normalized_entities.NormalizedStatePersonExternalId(
                    state_code="US_XX",
                    person_external_id_id=1,
                    external_id="EXTERNAL_ID_1",
                    id_type="US_XX_ID_TYPE",
                )
            ],
            incarceration_sentences=[incarceration_sentence],
        )
        incarceration_sentence.person = normalized_person
        charge.person = normalized_person

        for external_id in normalized_person.external_ids:
            external_id.person = normalized_person

        error_messages = validate_root_entity(normalized_person)
        self.assertEqual(0, len(error_messages))


class TestUniqueConstraintValid(unittest.TestCase):
    """Test that unique constraints specified on entities are valid"""

    def test_valid_field_columns_in_entities(self) -> None:
        all_entities = get_all_entity_classes_in_module(entities_schema)
        for entity in all_entities:
            constraints = entity.global_unique_constraints()
            entity_attrs = [a.name for a in entity.__dict__["__attrs_attrs__"]]
            for constraint in constraints:
                for column_name in constraint.fields:
                    self.assertTrue(column_name in entity_attrs)

    def test_valid_field_columns_in_schema(self) -> None:
        all_entities = get_all_entity_classes_in_module(entities_schema)
        for entity in all_entities:
            constraints = entity.global_unique_constraints()
            schema_entity = get_database_entity_by_table_name(
                schema, entity.get_entity_name()
            )
            for constraint in constraints:
                for column_name in constraint.fields:
                    self.assertTrue(hasattr(schema_entity, column_name))

    def test_equal_schema_uniqueness_constraint(self) -> None:
        expected_missing_schema_constraints: Dict[Type[Entity], List[str]] = {
            state_entities.StateTaskDeadline: [
                "state_task_deadline_unique_per_person_update_date_type"
            ]
        }
        all_entities = get_all_entity_classes_in_module(entities_schema)
        for entity in all_entities:
            constraints = (
                entity.global_unique_constraints()
                + entity.entity_tree_unique_constraints()
            )
            schema_entity = get_database_entity_by_table_name(
                schema, entity.get_entity_name()
            )
            constraint_names = [constraint.name for constraint in constraints]

            expected_missing = expected_missing_schema_constraints.get(entity) or []
            schema_constraint_names = [
                arg.name
                for arg in schema_entity.__table_args__
                if isinstance(arg, sqlalchemy.UniqueConstraint)
            ] + expected_missing
            self.assertListEqual(constraint_names, schema_constraint_names)


class TestSentencingRootEntityChecks(unittest.TestCase):
    """Test that root entity checks specific to the sentencing schema are valid."""

    def setUp(self) -> None:
        self.state_code = "US_XX"
        self.state_person = state_entities.StatePerson(
            state_code=self.state_code,
            person_id=1,
            external_ids=[
                StatePersonExternalId(
                    external_id="1",
                    state_code="US_XX",
                    id_type="US_XX_TEST_PERSON",
                ),
            ],
        )

    def test_snapshots_and_periods_are_not_both_hydrated(
        self,
    ) -> None:
        """
        If a sentence has both StateSentenceStatusSnapshot and
        StateSentenceServingPeriod entities we raise an error.
        """
        sentence = state_entities.StateSentence(
            state_code=self.state_code,
            external_id="SENT-EXTERNAL-1",
            person=self.state_person,
            sentence_type=StateSentenceType.STATE_PRISON,
            sentencing_authority=StateSentencingAuthority.PRESENT_WITHOUT_INFO,
            imposed_date=date(2022, 1, 1),
            parole_possible=None,
            charges=[
                state_entities.StateChargeV2(
                    external_id="CHARGE",
                    state_code=self.state_code,
                    status=StateChargeV2Status.PRESENT_WITHOUT_INFO,
                )
            ],
            parent_sentence_external_id_array=None,
            sentence_status_snapshots=[
                state_entities.StateSentenceStatusSnapshot(
                    state_code=self.state_code,
                    status_update_datetime=datetime(2022, 1, 1),
                    status=StateSentenceStatus.SERVING,
                )
            ],
            sentence_serving_periods=[
                state_entities.StateSentenceServingPeriod(
                    external_id="SERVING-PERIOD-1",
                    sentence_serving_period_id=1,
                    state_code=self.state_code,
                    serving_start_date=date(2022, 1, 1),
                    serving_end_date=None,
                )
            ],
        )
        self.state_person.sentences = [sentence]
        errors = validate_root_entity(self.state_person)
        self.assertEqual(
            errors,
            [
                f"Found {sentence.limited_pii_repr()} with BOTH StateSentenceStatusSnapshot "
                "and StateSentenceServingPeriod entities. We currently do not support ingesting both in the same state."
            ],
        )

    def test_parent_sentences_validation(
        self,
    ) -> None:
        """
        If a sentence has parent_sentence_external_id_array,
        then those sentences should exist for the StatePerson.
        """
        sentence = state_entities.StateSentence(
            state_code=self.state_code,
            external_id="SENT-EXTERNAL-1",
            person=self.state_person,
            sentence_type=StateSentenceType.STATE_PRISON,
            sentencing_authority=StateSentencingAuthority.PRESENT_WITHOUT_INFO,
            imposed_date=date(2022, 1, 1),
            parole_possible=None,
            charges=[
                state_entities.StateChargeV2(
                    external_id="CHARGE",
                    state_code=self.state_code,
                    status=StateChargeV2Status.PRESENT_WITHOUT_INFO,
                )
            ],
            parent_sentence_external_id_array=None,
        )

        # No parents, valid
        self.state_person.sentences = [sentence]
        errors = validate_root_entity(self.state_person)
        self.assertEqual(errors, [])

        # One parent, invalid
        sentence.parent_sentence_external_id_array = "NOT-REAL"
        self.state_person.sentences = [sentence]
        errors = validate_root_entity(self.state_person)
        self.assertEqual(
            errors,
            [
                "StateSentence(external_id='SENT-EXTERNAL-1', sentence_id=None) denotes "
                "parent sentence NOT-REAL, but StatePerson(person_id=1, "
                "external_ids=[StatePersonExternalId(external_id='1', "
                "id_type='US_XX_TEST_PERSON', person_external_id_id=None)]) "
                "does not have a sentence with that external ID.",
            ],
        )

        # Multiple parents, invalid
        sentence.parent_sentence_external_id_array = "NOT-REAL,NOT-HERE"
        self.state_person.sentences = [sentence]
        errors = validate_root_entity(self.state_person)
        self.assertEqual(
            errors,
            [
                # ---- First error message ----
                "StateSentence(external_id='SENT-EXTERNAL-1', sentence_id=None) denotes "
                "parent sentence NOT-REAL, but StatePerson(person_id=1, "
                "external_ids=[StatePersonExternalId(external_id='1', "
                "id_type='US_XX_TEST_PERSON', person_external_id_id=None)]) "
                "does not have a sentence with that external ID.",
                # ---- Second error message ----
                "StateSentence(external_id='SENT-EXTERNAL-1', sentence_id=None) denotes "
                "parent sentence NOT-HERE, but StatePerson(person_id=1, "
                "external_ids=[StatePersonExternalId(external_id='1', "
                "id_type='US_XX_TEST_PERSON', person_external_id_id=None)]) "
                "does not have a sentence with that external ID.",
            ],
        )

    def test_no_parole_possible_means_no_parole_projected_dates(
        self,
    ) -> None:
        """
        If a sentence has parole_possible=False, then there should be no parole related
        projected dates on all sentence_length entities.
        """
        sentence = state_entities.StateSentence(
            state_code=self.state_code,
            external_id="SENT-EXTERNAL-1",
            person=self.state_person,
            sentence_type=StateSentenceType.STATE_PRISON,
            sentencing_authority=StateSentencingAuthority.PRESENT_WITHOUT_INFO,
            imposed_date=date(2022, 1, 1),
            parole_possible=None,
            charges=[
                state_entities.StateChargeV2(
                    external_id="CHARGE",
                    state_code=self.state_code,
                    status=StateChargeV2Status.PRESENT_WITHOUT_INFO,
                )
            ],
            sentence_lengths=[
                state_entities.StateSentenceLength(
                    state_code=self.state_code,
                    length_update_datetime=datetime(2022, 1, 1),
                    parole_eligibility_date_external=date(2025, 1, 1),
                ),
            ],
        )
        self.state_person.sentences = [sentence]
        errors = validate_root_entity(self.state_person)
        self.assertEqual(errors, [])

        sentence.parole_possible = True
        self.state_person.sentences = [sentence]
        errors = validate_root_entity(self.state_person)
        self.assertEqual(errors, [])

        sentence.parole_possible = False
        self.state_person.sentences = [sentence]
        errors = validate_root_entity(self.state_person)
        self.assertEqual(
            errors[0],
            (
                "Sentence StateSentence(external_id='SENT-EXTERNAL-1', sentence_id=None) "
                "has parole projected dates, despite denoting that parole is not possible."
            ),
        )

    def test_sentences_have_charge_invalid(self) -> None:
        """Tests that sentences post root entity merge all have a sentence_type and imposed_date."""
        sentence = state_entities.StateSentence(
            state_code=self.state_code,
            external_id="SENT-EXTERNAL-1",
            person=self.state_person,
            sentence_type=StateSentenceType.STATE_PRISON,
            sentencing_authority=StateSentencingAuthority.PRESENT_WITHOUT_INFO,
            imposed_date=date(2022, 1, 1),
        )
        self.state_person.sentences.append(sentence)
        errors = validate_root_entity(self.state_person)
        self.assertEqual(len(errors), 1)
        self.assertEqual(
            "Found sentence StateSentence(external_id='SENT-EXTERNAL-1', sentence_id=None) with no charges.",
            errors[0],
        )

    def test_sentences_have_sentencing_authority_invalid(self) -> None:
        """Tests that sentences post root entity merge all have a sentencing_authority."""
        sentence = state_entities.StateSentence(
            state_code=self.state_code,
            external_id="SENT-EXTERNAL-1",
            person=self.state_person,
            sentence_type=StateSentenceType.PROBATION,
            imposed_date=date(2022, 1, 1),
            charges=[
                state_entities.StateChargeV2(
                    external_id="CHARGE",
                    state_code=self.state_code,
                    status=StateChargeV2Status.PRESENT_WITHOUT_INFO,
                )
            ],
        )
        self.state_person.sentences.append(sentence)
        errors = validate_root_entity(self.state_person)
        self.assertEqual(len(errors), 1)
        self.assertEqual(
            "Found entity [StateSentence(external_id='SENT-EXTERNAL-1', "
            "sentence_id=None)] with null [sentencing_authority]. The "
            "[sentencing_authority] field must be set by the time we reach the "
            "validations step.",
            errors[0],
        )

    def test_sentences_have_type_and_imposed_date_invalid(self) -> None:
        """Tests that sentences post root entity merge all have a sentence_type and imposed_date."""
        sentence = state_entities.StateSentence(
            state_code=self.state_code,
            external_id="SENT-EXTERNAL-1",
            person=self.state_person,
            sentencing_authority=StateSentencingAuthority.PRESENT_WITHOUT_INFO,
            charges=[
                state_entities.StateChargeV2(
                    external_id="CHARGE",
                    state_code=self.state_code,
                    status=StateChargeV2Status.PRESENT_WITHOUT_INFO,
                )
            ],
        )
        self.state_person.sentences.append(sentence)
        errors = validate_root_entity(self.state_person)
        self.assertEqual(len(errors), 2)
        self.assertEqual(
            errors[0],
            "Found entity [StateSentence(external_id='SENT-EXTERNAL-1', "
            "sentence_id=None)] with null [sentence_type]. The [sentence_type] field "
            "must be set by the time we reach the validations step.",
        )
        self.assertEqual(
            errors[1],
            "Found sentence StateSentence(external_id='SENT-EXTERNAL-1', sentence_id=None) with no imposed_date.",
        )

    def test_revoked_sentence_status_check_valid(self) -> None:
        probation_sentence = state_entities.StateSentence(
            state_code=self.state_code,
            external_id="SENT-EXTERNAL-1",
            sentence_type=StateSentenceType.PROBATION,
            imposed_date=date(2022, 1, 1),
            sentencing_authority=StateSentencingAuthority.PRESENT_WITHOUT_INFO,
            charges=[
                state_entities.StateChargeV2(
                    external_id="CHARGE",
                    state_code=self.state_code,
                    status=StateChargeV2Status.PRESENT_WITHOUT_INFO,
                )
            ],
            sentence_status_snapshots=[
                state_entities.StateSentenceStatusSnapshot(
                    state_code=self.state_code,
                    status=StateSentenceStatus.SERVING,
                    status_update_datetime=datetime(2022, 1, 1),
                ),
                state_entities.StateSentenceStatusSnapshot(
                    state_code=self.state_code,
                    status=StateSentenceStatus.REVOKED,
                    status_update_datetime=datetime(2022, 4, 1),
                ),
            ],
            person=self.state_person,
        )
        self.state_person.sentences.append(probation_sentence)
        parole_sentence = state_entities.StateSentence(
            state_code=self.state_code,
            external_id="SENT-EXTERNAL-4",
            sentence_type=StateSentenceType.PAROLE,
            sentencing_authority=StateSentencingAuthority.PRESENT_WITHOUT_INFO,
            imposed_date=date(2023, 1, 1),
            charges=[
                state_entities.StateChargeV2(
                    external_id="CHARGE",
                    state_code=self.state_code,
                    status=StateChargeV2Status.PRESENT_WITHOUT_INFO,
                )
            ],
            sentence_status_snapshots=[
                state_entities.StateSentenceStatusSnapshot(
                    state_code=self.state_code,
                    status=StateSentenceStatus.SERVING,
                    status_update_datetime=datetime(2023, 1, 1),
                ),
                state_entities.StateSentenceStatusSnapshot(
                    state_code=self.state_code,
                    status=StateSentenceStatus.REVOKED,
                    status_update_datetime=datetime(2023, 4, 1),
                ),
            ],
            person=self.state_person,
        )
        self.state_person.sentences.append(parole_sentence)
        errors = validate_root_entity(self.state_person)
        self.assertEqual(len(errors), 0)

    def test_revoked_sentence_status_check_invalid(self) -> None:
        state_prison_sentence = state_entities.StateSentence(
            state_code=self.state_code,
            external_id="SENT-EXTERNAL-2",
            sentence_type=StateSentenceType.STATE_PRISON,
            sentencing_authority=StateSentencingAuthority.PRESENT_WITHOUT_INFO,
            imposed_date=date(2022, 1, 1),
            charges=[
                state_entities.StateChargeV2(
                    external_id="CHARGE",
                    state_code=self.state_code,
                    status=StateChargeV2Status.PRESENT_WITHOUT_INFO,
                )
            ],
            sentence_status_snapshots=[
                state_entities.StateSentenceStatusSnapshot(
                    state_code=self.state_code,
                    status=StateSentenceStatus.SERVING,
                    status_update_datetime=datetime(2022, 1, 1),
                ),
                state_entities.StateSentenceStatusSnapshot(
                    state_code=self.state_code,
                    status=StateSentenceStatus.REVOKED,
                    status_update_datetime=datetime(2022, 4, 1),
                ),
                state_entities.StateSentenceStatusSnapshot(
                    state_code=self.state_code,
                    status=StateSentenceStatus.COMPLETED,
                    status_update_datetime=datetime(2022, 5, 1),
                ),
            ],
            person=self.state_person,
        )
        self.state_person.sentences.append(state_prison_sentence)
        errors = validate_root_entity(self.state_person)
        self.assertEqual(len(errors), 1)
        self.assertEqual(
            errors[0],
            (
                "Found person StatePerson(person_id=1, "
                "external_ids=[StatePersonExternalId(external_id='1', "
                "id_type='US_XX_TEST_PERSON', person_external_id_id=None)]) with REVOKED "
                "status on StateSentenceType.STATE_PRISON sentence. REVOKED statuses are only "
                "allowed on PROBATION and PAROLE type sentences."
            ),
        )

    def test_sequence_num_are_unique_for_each_sentence(self) -> None:
        probation_sentence = state_entities.StateSentence(
            state_code=self.state_code,
            external_id="SENT-EXTERNAL-1",
            sentence_type=StateSentenceType.PROBATION,
            sentencing_authority=StateSentencingAuthority.PRESENT_WITHOUT_INFO,
            imposed_date=date(2022, 1, 1),
            charges=[
                state_entities.StateChargeV2(
                    external_id="CHARGE",
                    state_code=self.state_code,
                    status=StateChargeV2Status.PRESENT_WITHOUT_INFO,
                )
            ],
            sentence_status_snapshots=[
                state_entities.StateSentenceStatusSnapshot(
                    sequence_num=1,
                    state_code=self.state_code,
                    status=StateSentenceStatus.SERVING,
                    status_update_datetime=datetime(2022, 1, 1),
                ),
                state_entities.StateSentenceStatusSnapshot(
                    sequence_num=2,
                    state_code=self.state_code,
                    status=StateSentenceStatus.REVOKED,
                    status_update_datetime=datetime(2022, 4, 1),
                ),
            ],
            person=self.state_person,
        )
        self.state_person.sentences.append(probation_sentence)
        incarceration_sentence = state_entities.StateSentence(
            state_code=self.state_code,
            external_id="SENT-EXTERNAL-2",
            sentence_type=StateSentenceType.PROBATION,
            sentencing_authority=StateSentencingAuthority.PRESENT_WITHOUT_INFO,
            imposed_date=date(2022, 1, 1),
            charges=[
                state_entities.StateChargeV2(
                    external_id="CHARGE",
                    state_code=self.state_code,
                    status=StateChargeV2Status.PRESENT_WITHOUT_INFO,
                )
            ],
            sentence_status_snapshots=[
                state_entities.StateSentenceStatusSnapshot(
                    sequence_num=1,
                    state_code=self.state_code,
                    status=StateSentenceStatus.SERVING,
                    status_update_datetime=datetime(2022, 1, 1),
                ),
                state_entities.StateSentenceStatusSnapshot(
                    sequence_num=2,
                    state_code=self.state_code,
                    status=StateSentenceStatus.REVOKED,
                    status_update_datetime=datetime(2022, 4, 1),
                ),
            ],
            person=self.state_person,
        )
        self.state_person.sentences.append(incarceration_sentence)
        errors = validate_root_entity(self.state_person)
        self.assertEqual(len(errors), 0)

    def test_sentence_to_sentence_group_reference(self) -> None:
        """Tests that StateSentenceGroup entities have a reference from a sentence."""
        self.state_person.sentence_groups.append(
            state_entities.StateSentenceGroup(
                state_code=self.state_code,
                external_id="TEST-SG",
            )
        )
        sentence = state_entities.StateSentence(
            state_code=self.state_code,
            external_id="SENT-EXTERNAL-1",
            sentence_group_external_id="TEST-SG",
            person=self.state_person,
            sentence_type=StateSentenceType.STATE_PRISON,
            sentencing_authority=StateSentencingAuthority.PRESENT_WITHOUT_INFO,
            imposed_date=date(2022, 1, 1),
            charges=[
                state_entities.StateChargeV2(
                    state_code=self.state_code,
                    external_id="CHARGE-EXTERNAL-1",
                    status=StateChargeV2Status.PRESENT_WITHOUT_INFO,
                )
            ],
        )
        self.state_person.sentences.append(sentence)
        errors = validate_root_entity(self.state_person)
        self.assertEqual(len(errors), 0)

        # Error when there is a SG but no sentence
        self.state_person.sentences = []
        errors = validate_root_entity(self.state_person)
        self.assertEqual(len(errors), 1)
        self.assertEqual(
            errors[0],
            "Found StateSentenceGroup TEST-SG without an associated sentence.",
        )

        # Error when there is a sentence but no SG (but at least one SG)
        sentence.sentence_group_external_id = "TEST-SG-2"
        self.state_person.sentences = [sentence]
        errors = validate_root_entity(self.state_person)
        self.assertEqual(len(errors), 2)
        self.assertEqual(
            errors[0],
            "Found sentence_ext_ids=['SENT-EXTERNAL-1'] referencing non-existent StateSentenceGroup TEST-SG-2.",
        )
        self.assertEqual(
            errors[1],
            "Found StateSentenceGroup TEST-SG without an associated sentence.",
        )

    def test_no_parole_possible_means_no_parole_projected_dates_group_level(
        self,
    ) -> None:
        """
        If all sentences in a sentence group have parole_possible=False,
        then there should be no parole related projected dates on all
        sentence_group_length entities.
        """
        # One sentence, parole_possible is None - no Error
        sentence = state_entities.StateSentence(
            state_code=self.state_code,
            external_id="SENT-EXTERNAL-1",
            sentence_group_external_id="SG-EXTERNAL-1",
            person=self.state_person,
            sentence_type=StateSentenceType.STATE_PRISON,
            sentencing_authority=StateSentencingAuthority.PRESENT_WITHOUT_INFO,
            imposed_date=date(2022, 1, 1),
            parole_possible=None,
            charges=[
                state_entities.StateChargeV2(
                    external_id="CHARGE",
                    state_code=self.state_code,
                    status=StateChargeV2Status.PRESENT_WITHOUT_INFO,
                )
            ],
        )
        group = state_entities.StateSentenceGroup(
            state_code=self.state_code,
            external_id="SG-EXTERNAL-1",
            sentence_group_lengths=[
                state_entities.StateSentenceGroupLength(
                    state_code=self.state_code,
                    group_update_datetime=datetime(2022, 1, 1),
                    parole_eligibility_date_external=date(2025, 1, 1),
                ),
            ],
        )
        self.state_person.sentences = [sentence]
        self.state_person.sentence_groups = [group]
        errors = validate_root_entity(self.state_person)
        self.assertEqual(errors, [])

        # One sentence, parole_possible is False - Expected Error
        sentence = state_entities.StateSentence(
            state_code=self.state_code,
            external_id="SENT-EXTERNAL-1",
            sentence_group_external_id="SG-EXTERNAL-1",
            person=self.state_person,
            sentence_type=StateSentenceType.STATE_PRISON,
            sentencing_authority=StateSentencingAuthority.PRESENT_WITHOUT_INFO,
            imposed_date=date(2022, 1, 1),
            parole_possible=False,
            charges=[
                state_entities.StateChargeV2(
                    external_id="CHARGE",
                    state_code=self.state_code,
                    status=StateChargeV2Status.PRESENT_WITHOUT_INFO,
                )
            ],
        )
        self.state_person.sentences = [sentence]
        self.state_person.sentence_groups = [group]
        errors = validate_root_entity(self.state_person)
        self.assertEqual(
            errors,
            [
                "StateSentenceGroup(external_id='SG-EXTERNAL-1', sentence_group_id=None) "
                "has parole eligibility date, but none of its sentences allow parole."
            ],
        )

        # Mutliple sentences, parole_possible is False - Expected Error
        sentence_1 = state_entities.StateSentence(
            state_code=self.state_code,
            external_id="SENT-EXTERNAL-1",
            sentence_group_external_id="SG-EXTERNAL-1",
            person=self.state_person,
            sentence_type=StateSentenceType.STATE_PRISON,
            sentencing_authority=StateSentencingAuthority.PRESENT_WITHOUT_INFO,
            imposed_date=date(2022, 1, 1),
            parole_possible=False,
            charges=[
                state_entities.StateChargeV2(
                    external_id="CHARGE",
                    state_code=self.state_code,
                    status=StateChargeV2Status.PRESENT_WITHOUT_INFO,
                )
            ],
        )
        sentence_2 = state_entities.StateSentence(
            state_code=self.state_code,
            external_id="SENT-EXTERNAL-2",
            sentence_group_external_id="SG-EXTERNAL-1",
            person=self.state_person,
            sentence_type=StateSentenceType.STATE_PRISON,
            sentencing_authority=StateSentencingAuthority.PRESENT_WITHOUT_INFO,
            imposed_date=date(2022, 1, 1),
            parole_possible=False,
            charges=[
                state_entities.StateChargeV2(
                    external_id="CHARGE",
                    state_code=self.state_code,
                    status=StateChargeV2Status.PRESENT_WITHOUT_INFO,
                )
            ],
        )
        self.state_person.sentences = [sentence_1, sentence_2]
        self.state_person.sentence_groups = [group]
        errors = validate_root_entity(self.state_person)
        self.assertEqual(
            errors,
            [
                "StateSentenceGroup(external_id='SG-EXTERNAL-1', sentence_group_id=None) "
                "has parole eligibility date, but none of its sentences allow parole."
            ],
        )

        # Mutliple sentences, parole_possible is True or None - No Error
        sentence_1 = state_entities.StateSentence(
            state_code=self.state_code,
            external_id="SENT-EXTERNAL-1",
            sentence_group_external_id="SG-EXTERNAL-1",
            person=self.state_person,
            sentence_type=StateSentenceType.STATE_PRISON,
            sentencing_authority=StateSentencingAuthority.PRESENT_WITHOUT_INFO,
            imposed_date=date(2022, 1, 1),
            parole_possible=None,
            charges=[
                state_entities.StateChargeV2(
                    external_id="CHARGE",
                    state_code=self.state_code,
                    status=StateChargeV2Status.PRESENT_WITHOUT_INFO,
                )
            ],
        )
        sentence_2 = state_entities.StateSentence(
            state_code=self.state_code,
            external_id="SENT-EXTERNAL-2",
            sentence_group_external_id="SG-EXTERNAL-1",
            person=self.state_person,
            sentence_type=StateSentenceType.STATE_PRISON,
            sentencing_authority=StateSentencingAuthority.PRESENT_WITHOUT_INFO,
            imposed_date=date(2022, 1, 1),
            parole_possible=True,
            charges=[
                state_entities.StateChargeV2(
                    external_id="CHARGE",
                    state_code=self.state_code,
                    status=StateChargeV2Status.PRESENT_WITHOUT_INFO,
                )
            ],
        )
        self.state_person.sentences = [sentence_1, sentence_2]
        self.state_person.sentence_groups = [group]
        errors = validate_root_entity(self.state_person)
        self.assertEqual(errors, [])


class TestIncarcerationPeriodRootEntityChecks(unittest.TestCase):
    """Test that root entity checks specific to incarceration periods are valid."""

    def setUp(self) -> None:
        self.state_code = "US_XX"
        self.state_person = state_entities.StatePerson(
            state_code=self.state_code,
            person_id=1,
            external_ids=[
                StatePersonExternalId(
                    external_id="1",
                    state_code="US_XX",
                    id_type="US_XX_TEST_PERSON",
                ),
            ],
        )

    def test_valid_incarceration_period(self) -> None:
        valid_period = state_entities.StateIncarcerationPeriod(
            state_code=self.state_code,
            external_id="IP-EXTERNAL-1",
            person=self.state_person,
            admission_date=date(2020, 1, 1),
        )
        self.state_person.incarceration_periods = [valid_period]
        errors = validate_root_entity(self.state_person)
        self.assertEqual(errors, [])

    def test_missing_date_incarceration_period(self) -> None:
        valid_period = state_entities.StateIncarcerationPeriod(
            state_code=self.state_code,
            external_id="IP-EXTERNAL-1",
            person=self.state_person,
            admission_date=date(2020, 1, 1),
        )
        missing_date_period = state_entities.StateIncarcerationPeriod(
            state_code=self.state_code,
            external_id="IP-EXTERNAL-2",
            person=self.state_person,
        )
        self.state_person.incarceration_periods = [valid_period, missing_date_period]
        errors = validate_root_entity(self.state_person)
        self.assertEqual(
            errors,
            [
                "Found StatePerson(person_id=1, "
                "external_ids=[StatePersonExternalId(external_id='1', "
                "id_type='US_XX_TEST_PERSON', person_external_id_id=None)]) having a "
                "StateIncarcerationPeriod with a null start date."
            ],
        )


class TestSupervisionPeriodRootEntityChecks(unittest.TestCase):
    """Test that root entity checks specific to supervision periods are valid."""

    def setUp(self) -> None:
        self.state_code = "US_XX"
        self.state_person = state_entities.StatePerson(
            state_code=self.state_code,
            person_id=1,
            external_ids=[
                StatePersonExternalId(
                    external_id="1",
                    state_code="US_XX",
                    id_type="US_XX_TEST_PERSON",
                ),
            ],
        )

    def test_valid_supervision_period(self) -> None:
        valid_period = state_entities.StateSupervisionPeriod(
            state_code=self.state_code,
            external_id="IP-EXTERNAL-1",
            person=self.state_person,
            start_date=date(2020, 1, 1),
        )
        self.state_person.supervision_periods = [valid_period]
        errors = validate_root_entity(self.state_person)
        self.assertEqual(errors, [])
