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
from unittest.mock import patch

import sqlalchemy
from more_itertools import one

from recidiviz.common.constants.state.state_charge import (
    StateChargeStatus,
    StateChargeV2Status,
)
from recidiviz.common.constants.state.state_person_address_period import (
    StatePersonAddressType,
)
from recidiviz.common.constants.state.state_person_staff_relationship_period import (
    StatePersonStaffRelationshipType,
)
from recidiviz.common.constants.state.state_sentence import (
    StateSentenceStatus,
    StateSentenceType,
    StateSentencingAuthority,
)
from recidiviz.common.constants.state.state_system_type import StateSystemType
from recidiviz.common.constants.state.state_task_deadline import StateTaskType
from recidiviz.common.constants.states import StateCode
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
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStatePerson,
    NormalizedStatePersonExternalId,
    NormalizedStatePersonStaffRelationshipPeriod,
)
from recidiviz.pipelines.ingest.state.validator import validate_root_entity
from recidiviz.utils.types import assert_type


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
                    is_current_display_id_for_type=True,
                    id_active_from_datetime=datetime(2020, 1, 1),
                    id_active_to_datetime=None,
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
                    is_current_display_id_for_type=True,
                    id_active_from_datetime=datetime(2020, 1, 1),
                    id_active_to_datetime=None,
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
        for entity in sorted(all_entities, key=lambda e: e.__name__):
            constraints = (
                entity.global_unique_constraints()
                + entity.entity_tree_unique_constraints()
            )
            schema_entity = get_database_entity_by_table_name(
                schema, entity.get_entity_name()
            )
            constraint_names = [constraint.name for constraint in constraints]

            expected_missing = expected_missing_schema_constraints.get(entity) or []

            schema_constraint_names = (
                [
                    arg.name
                    for arg in assert_type(schema_entity.__table_args__, tuple)
                    if isinstance(arg, sqlalchemy.UniqueConstraint)
                ]
                if hasattr(schema_entity, "__table_args__")
                else []
            ) + expected_missing
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

    def test_consecutive_sentences_check(
        self,
    ) -> None:
        """
        If a sentence has parent_sentence_external_id_array,
        then those sentences should exist for the StatePerson.
        """
        child_sentence = state_entities.StateSentence(
            state_code=self.state_code,
            external_id="CHILD",
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
        self.state_person.sentences = [child_sentence]
        errors = validate_root_entity(self.state_person)
        self.assertEqual(errors, [])

        # One parent, invalid
        child_sentence.parent_sentence_external_id_array = "NOT-REAL"
        self.state_person.sentences = [child_sentence]
        errors = validate_root_entity(self.state_person)
        self.assertEqual(
            errors,
            [
                "Found sentence StateSentence(external_id='CHILD', sentence_id=None) with parent sentence external ID NOT-REAL, but no sentence with that external ID exists."
            ],
        )

        # Multiple parents, invalid
        child_sentence.parent_sentence_external_id_array = "NOT-REAL,NOT-HERE"
        self.state_person.sentences = [child_sentence]
        errors = validate_root_entity(self.state_person)
        self.assertEqual(
            errors,
            [
                # 1st error
                "Found sentence StateSentence(external_id='CHILD', sentence_id=None) with parent sentence external ID NOT-REAL, "
                "but no sentence with that external ID exists.",
                # 2nd error
                "Found sentence StateSentence(external_id='CHILD', sentence_id=None) with parent sentence external ID NOT-HERE, "
                "but no sentence with that external ID exists.",
            ],
        )

        parent_sentence = state_entities.StateSentence(
            state_code=self.state_code,
            external_id="PARENT",
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
        child_sentence.parent_sentence_external_id_array = parent_sentence.external_id
        parent_sentence.parent_sentence_external_id_array = child_sentence.external_id
        self.state_person.sentences = [child_sentence, parent_sentence]
        errors = validate_root_entity(self.state_person)
        self.assertEqual(
            errors,
            [
                "StatePerson(person_id=1, external_ids=[StatePersonExternalId(external_id='1', id_type='US_XX_TEST_PERSON', person_external_id_id=None)]) has an invalid set of consecutive sentences that form a cycle: PARENT -> as child of -> CHILD; CHILD -> as child of -> PARENT. Did you intend to hydrate these a concurrent sentences?"
            ],
        )

        grand_parent_sentence = state_entities.StateSentence(
            state_code=self.state_code,
            external_id="GRAND_PARENT",
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

        # A sentence can still have two children
        child_sentence.parent_sentence_external_id_array = (
            grand_parent_sentence.external_id
        )
        parent_sentence.parent_sentence_external_id_array = (
            grand_parent_sentence.external_id
        )
        self.state_person.sentences = [
            child_sentence,
            parent_sentence,
            grand_parent_sentence,
        ]
        errors = validate_root_entity(self.state_person)
        self.assertEqual(errors, [])

        # Multi-level tree also works (child -> parent -> grandparent)
        child_sentence.parent_sentence_external_id_array = parent_sentence.external_id
        parent_sentence.parent_sentence_external_id_array = (
            grand_parent_sentence.external_id
        )
        self.state_person.sentences = [
            child_sentence,
            parent_sentence,
            grand_parent_sentence,
        ]
        errors = validate_root_entity(self.state_person)
        self.assertEqual(errors, [])

        # A cycle from the top breaks though
        grand_parent_sentence.parent_sentence_external_id_array = (
            child_sentence.external_id
        )
        self.state_person.sentences = [
            child_sentence,
            parent_sentence,
            grand_parent_sentence,
        ]
        assert child_sentence.parent_sentence_external_id_array == "PARENT"
        assert parent_sentence.parent_sentence_external_id_array == "GRAND_PARENT"
        errors = validate_root_entity(self.state_person)
        self.assertEqual(
            errors,
            [
                "StatePerson(person_id=1, external_ids=[StatePersonExternalId(external_id='1', id_type='US_XX_TEST_PERSON', person_external_id_id=None)]) has an invalid set of consecutive sentences that form a cycle: GRAND_PARENT -> as child of -> CHILD; PARENT -> as child of -> GRAND_PARENT; CHILD -> as child of -> PARENT. Did you intend to hydrate these a concurrent sentences?",
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
            "Found sentence StateSentence(external_id='SENT-EXTERNAL-1', sentence_id=None) with no CONVICTED charges.",
            errors[0],
        )
        sentence.charges = [
            state_entities.StateChargeV2(
                external_id="CHARGE",
                state_code=self.state_code,
                status=StateChargeV2Status.DROPPED,
            ),
            state_entities.StateChargeV2(
                external_id="CHARGE",
                state_code=self.state_code,
                status=StateChargeV2Status.ACQUITTED,
            ),
            state_entities.StateChargeV2(
                external_id="CHARGE",
                state_code=self.state_code,
                status=StateChargeV2Status.PENDING,
            ),
        ]
        errors = validate_root_entity(self.state_person)
        self.assertEqual(len(errors), 1)
        self.assertEqual(
            "Found sentence StateSentence(external_id='SENT-EXTERNAL-1', sentence_id=None) with no CONVICTED charges.",
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

        # Error when there is a sentence_group_external_id but no sentence group
        self.state_person.sentence_groups = []
        sentence.sentence_group_external_id = "SG-NOT-HYDRATED"
        self.state_person.sentences = [sentence]
        errors = validate_root_entity(self.state_person)
        self.assertEqual(len(errors), 1)
        self.assertEqual(
            errors[0],
            "Found sentence_ext_ids=['SENT-EXTERNAL-1'] referencing non-existent StateSentenceGroup SG-NOT-HYDRATED.",
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
            sentences=[
                state_entities.StateSentence(
                    state_code=self.state_code,
                    external_id="SENT-EXTERNAL-1",
                    sentence_type=StateSentenceType.STATE_PRISON,
                    sentencing_authority=StateSentencingAuthority.PRESENT_WITHOUT_INFO,
                    imposed_date=date(2022, 1, 1),
                    parole_possible=None,
                    charges=[
                        state_entities.StateChargeV2(
                            external_id="CHARGE",
                            state_code=self.state_code,
                            status=StateChargeV2Status.CONVICTED,
                        )
                    ],
                    parent_sentence_external_id_array=None,
                )
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
            sentences=[
                state_entities.StateSentence(
                    state_code=self.state_code,
                    external_id="SENT-EXTERNAL-1",
                    sentence_type=StateSentenceType.STATE_PRISON,
                    sentencing_authority=StateSentencingAuthority.PRESENT_WITHOUT_INFO,
                    imposed_date=date(2022, 1, 1),
                    parole_possible=None,
                    charges=[
                        state_entities.StateChargeV2(
                            external_id="CHARGE",
                            state_code=self.state_code,
                            status=StateChargeV2Status.CONVICTED,
                        )
                    ],
                    parent_sentence_external_id_array=None,
                )
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


class TestNormalizedEarlyDischargeChecks(unittest.TestCase):
    """Test that root entity checks specific to normalized early discharge are valid."""

    def setUp(self) -> None:
        self.state_code = "US_XX"
        self.state_person = normalized_entities.NormalizedStatePerson(
            state_code=self.state_code,
            person_id=1,
            external_ids=[
                normalized_entities.NormalizedStatePersonExternalId(
                    person_external_id_id=1,
                    external_id="1",
                    state_code="US_XX",
                    id_type="US_XX_TEST_PERSON",
                    is_current_display_id_for_type=True,
                    id_active_from_datetime=datetime(2020, 1, 1),
                    id_active_to_datetime=None,
                ),
            ],
        )
        for eid in self.state_person.external_ids:
            eid.person = self.state_person

    def test_valid_early_discharge(self) -> None:
        incarceration_sentence = (
            normalized_entities.NormalizedStateIncarcerationSentence(
                state_code=self.state_code,
                incarceration_sentence_id=123,
                external_id="is1",
                effective_date=date(2017, 1, 1),
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            )
        )
        incarceration_sentence.person = self.state_person

        early_discharge = normalized_entities.NormalizedStateEarlyDischarge(
            early_discharge_id=1,
            state_code=self.state_code,
            external_id="ED-EXTERNAL-1",
            person=self.state_person,
            request_date=date(2020, 1, 1),
        )
        early_discharge.person = self.state_person
        incarceration_sentence.early_discharges.append(early_discharge)
        early_discharge.incarceration_sentence = incarceration_sentence
        self.state_person.incarceration_sentences = [incarceration_sentence]
        errors = validate_root_entity(self.state_person)
        self.assertEqual(errors, [])

    def test_early_discharge_missing_sentence_backedge(self) -> None:
        incarceration_sentence = (
            normalized_entities.NormalizedStateIncarcerationSentence(
                state_code=self.state_code,
                incarceration_sentence_id=123,
                external_id="is1",
                effective_date=date(2017, 1, 1),
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            )
        )
        incarceration_sentence.person = self.state_person

        early_discharge = normalized_entities.NormalizedStateEarlyDischarge(
            early_discharge_id=1,
            state_code=self.state_code,
            external_id="ED-EXTERNAL-1",
            person=self.state_person,
            request_date=date(2020, 1, 1),
        )
        early_discharge.person = self.state_person
        incarceration_sentence.early_discharges.append(early_discharge)
        # Do not set a backedge between early_discharge and incarceration_sentence
        # early_discharge.incarceration_sentence = incarceration_sentence
        self.state_person.incarceration_sentences = [incarceration_sentence]
        errors = validate_root_entity(self.state_person)
        self.assertEqual(
            errors,
            [
                "Found entity NormalizedStateEarlyDischarge(early_discharge_id=1, "
                "external_id='ED-EXTERNAL-1') with neither one of "
                "incarceration_sentence or supervision_sentence backedges set."
            ],
        )


class TestStatePersonAddressPeriodChecks(unittest.TestCase):
    """Tests our entity tree checks on StatePersonAddressPeriod."""

    STATE_CODE_VALUE = "US_OZ"

    def _person(self) -> state_entities.StatePerson:
        return state_entities.StatePerson(
            state_code=self.STATE_CODE_VALUE,
            person_id=1,
            external_ids=[
                StatePersonExternalId(
                    external_id="Mr. Lion",
                    state_code=self.STATE_CODE_VALUE,
                    id_type="US_OZ_CHARACTER",
                ),
            ],
        )

    def test_unique_address_type_per_address(self) -> None:
        """Ensures an address has a unique type."""

        mr_lion = self._person()

        residence = state_entities.StatePersonAddressPeriod(
            state_code=self.STATE_CODE_VALUE,
            address_line_1="14 Yellow Brick Road",
            address_line_2="Unit 4401",
            address_city="Emerald City",
            address_start_date=date(2001, 1, 1),
            address_end_date=date(2021, 1, 1),
            address_type=StatePersonAddressType.PHYSICAL_RESIDENCE,
        )
        mailing = state_entities.StatePersonAddressPeriod(
            state_code=self.STATE_CODE_VALUE,
            address_line_1="14 Yellow Brick Road",
            address_line_2="Unit 4401",
            address_city="Emerald City",
            address_start_date=date(2001, 1, 1),
            address_end_date=date(2021, 1, 1),
            address_type=StatePersonAddressType.MAILING_ONLY,
        )
        mr_lion.address_periods = [residence, mailing]
        errors = validate_root_entity(mr_lion)
        # Mr. Lion's apartment can't be his residential and mailing address.
        assert errors == [
            "Found StatePerson(person_id=1, external_ids=[StatePersonExternalId(external_id='Mr. Lion', id_type='US_OZ_CHARACTER', person_external_id_id=None)]) with StateAddressPeriod address used with multiple StatePersonAddressType enums. If this assumption is too strict for your state (e.g. we need an address to be both PHYSICAL_RESIDENCE and MAILING), ping #platform-team to discuss!"
        ]

        # Mr. Lion's building has a mail room. The now unique address can
        # be his mailing address.
        updated_mailing = state_entities.StatePersonAddressPeriod(
            state_code=self.STATE_CODE_VALUE,
            address_line_1="14 Yellow Brick Road",
            address_line_2="Attn: Mr. Lion, Box 4401",
            address_city="Emerald City",
            address_start_date=date(2001, 1, 1),
            address_end_date=date(2021, 1, 1),
            address_type=StatePersonAddressType.MAILING_ONLY,
        )

        mr_lion.address_periods = [residence, updated_mailing]
        errors = validate_root_entity(mr_lion)
        assert not any(errors)

        # Mr. Lion decided he'd rather receive mail at his local post office.
        post_office = state_entities.StatePersonAddressPeriod(
            state_code=self.STATE_CODE_VALUE,
            address_line_1="PO Box 42",
            address_city="Emerald City",
            address_start_date=date(2021, 1, 1),
            address_type=StatePersonAddressType.MAILING_ONLY,
        )
        mr_lion.address_periods = [residence, updated_mailing, post_office]
        errors = validate_root_entity(mr_lion)
        assert not any(errors)

    def test_unique_date_ranges_per_address_type(self) -> None:
        """Ensures each address type has no overlapping periods."""

        mr_lion = self._person()

        residence = state_entities.StatePersonAddressPeriod(
            state_code=self.STATE_CODE_VALUE,
            address_line_1="14 Yellow Brick Road",
            address_line_2="Unit 4401",
            address_city="Emerald City",
            address_start_date=date(2001, 1, 1),
            address_end_date=date(2021, 1, 1),
            address_type=StatePersonAddressType.PHYSICAL_RESIDENCE,
        )
        the_wilderness = state_entities.StatePersonAddressPeriod(
            state_code=self.STATE_CODE_VALUE,
            address_line_1="The Wilderness",
            address_start_date=date(2000, 1, 1),
            address_end_date=date(2002, 1, 1),
            address_type=StatePersonAddressType.PHYSICAL_RESIDENCE,
        )
        mr_lion.address_periods = [residence, the_wilderness]
        errors = validate_root_entity(mr_lion)

        assert errors == [
            "Found StatePerson(person_id=1, external_ids=[StatePersonExternalId(external_id='Mr. Lion', id_type='US_OZ_CHARACTER', person_external_id_id=None)]) with address periods of type StatePersonAddressType.PHYSICAL_RESIDENCE that overlap.\nDate Ranges: [PotentiallyOpenDateRange(lower_bound_inclusive_date=datetime.date(2000, 1, 1), upper_bound_exclusive_date=datetime.date(2002, 1, 1)), PotentiallyOpenDateRange(lower_bound_inclusive_date=datetime.date(2001, 1, 1), upper_bound_exclusive_date=datetime.date(2021, 1, 1))] If this assumption is too strict for your state (e.g. we need multiple MAILING addresses), ping #platform-team to discuss!"
        ]

        # Mr. Lion decided he never left the wilderness.
        # Alas, he still can't have two physical residences.
        the_wilderness.address_end_date = None
        mr_lion.address_periods = [residence, the_wilderness]
        errors = validate_root_entity(mr_lion)
        assert errors == [
            "Found StatePerson(person_id=1, external_ids=[StatePersonExternalId(external_id='Mr. Lion', id_type='US_OZ_CHARACTER', person_external_id_id=None)]) with address periods of type StatePersonAddressType.PHYSICAL_RESIDENCE that overlap.\nDate Ranges: [PotentiallyOpenDateRange(lower_bound_inclusive_date=datetime.date(2000, 1, 1), upper_bound_exclusive_date=None), PotentiallyOpenDateRange(lower_bound_inclusive_date=datetime.date(2001, 1, 1), upper_bound_exclusive_date=datetime.date(2021, 1, 1))] If this assumption is too strict for your state (e.g. we need multiple MAILING addresses), ping #platform-team to discuss!"
        ]

        # We know he loves going into the wilderness though.
        # Fortunately we have another type to use during ingest.
        the_wilderness.address_type = StatePersonAddressType.PHYSICAL_OTHER
        mr_lion.address_periods = [residence, the_wilderness]
        errors = validate_root_entity(mr_lion)
        assert not any(errors)

    def test_adjacent_date_ranges_for_address_type_dont_throw(self) -> None:
        """Ensures address types with adjacent periods are not considered overlapping."""
        mr_lion = self._person()

        residence = state_entities.StatePersonAddressPeriod(
            state_code=self.STATE_CODE_VALUE,
            address_line_1="14 Yellow Brick Road",
            address_line_2="Unit 4401",
            address_city="Emerald City",
            address_start_date=date(2001, 1, 1),
            address_end_date=date(2021, 1, 1),
            address_type=StatePersonAddressType.PHYSICAL_RESIDENCE,
        )
        the_wilderness = state_entities.StatePersonAddressPeriod(
            state_code=self.STATE_CODE_VALUE,
            address_line_1="The Wilderness",
            address_start_date=date(2021, 1, 1),
            address_end_date=date(2022, 1, 1),
            address_type=StatePersonAddressType.PHYSICAL_RESIDENCE,
        )
        # Mr. Lion's apartment is right next to the wilderness and was
        # able to move the same day.
        mr_lion.address_periods = [residence, the_wilderness]
        errors = validate_root_entity(mr_lion)
        assert not any(errors)


class TestNormalizedStatePersonStaffRelationshipPeriodChecks(unittest.TestCase):
    """Tests for validating NormalizedStatePersonStaffRelationshipPeriod"""

    STATE_CODE_VALUE = "US_XX"
    STAFF_ID_1 = 1000
    STAFF_ID_1_EXTERNAL_ID_A = "STAFF_EXTERNAL_ID_1A"
    STAFF_ID_1_EXTERNAL_ID_B = "STAFF_EXTERNAL_ID_1B"
    STAFF_ID_2 = 2000
    STAFF_ID_2_EXTERNAL_ID = "STAFF_EXTERNAL_ID_2"

    DATE_1 = date(2019, 1, 1)
    DATE_2 = date(2020, 1, 1)
    DATE_3 = date(2021, 1, 1)

    def _person(self) -> normalized_entities.NormalizedStatePerson:
        person = normalized_entities.NormalizedStatePerson(
            state_code=self.STATE_CODE_VALUE,
            person_id=1,
            external_ids=[
                normalized_entities.NormalizedStatePersonExternalId(
                    person_external_id_id=1,
                    external_id="EXTERNAL_ID_A",
                    state_code=self.STATE_CODE_VALUE,
                    id_type="US_XX_STAFF_ID",
                    is_current_display_id_for_type=True,
                    id_active_from_datetime=datetime(2020, 1, 1),
                    id_active_to_datetime=None,
                ),
            ],
        )

        person.external_ids[0].person = person
        return person

    def test_valid_normalized_periods(self) -> None:
        person = self._person()

        person.staff_relationship_periods = [
            NormalizedStatePersonStaffRelationshipPeriod(
                state_code=self.STATE_CODE_VALUE,
                system_type=StateSystemType.SUPERVISION,
                system_type_raw_text=None,
                relationship_type=StatePersonStaffRelationshipType.SUPERVISING_OFFICER,
                relationship_type_raw_text=None,
                relationship_start_date=self.DATE_1,
                relationship_end_date_exclusive=self.DATE_2,
                location_external_id=None,
                relationship_priority=1,
                associated_staff_external_id=self.STAFF_ID_1_EXTERNAL_ID_A,
                associated_staff_external_id_type="US_XX_STAFF_ID",
                person_staff_relationship_period_id=9046224349876569075,
                person=person,
                associated_staff_id=self.STAFF_ID_1,
            ),
            NormalizedStatePersonStaffRelationshipPeriod(
                state_code=self.STATE_CODE_VALUE,
                system_type=StateSystemType.SUPERVISION,
                system_type_raw_text=None,
                relationship_type=StatePersonStaffRelationshipType.SUPERVISING_OFFICER,
                relationship_type_raw_text=None,
                relationship_start_date=self.DATE_2,
                relationship_end_date_exclusive=self.DATE_3,
                location_external_id=None,
                relationship_priority=1,
                associated_staff_external_id=self.STAFF_ID_2_EXTERNAL_ID,
                associated_staff_external_id_type="US_XX_STAFF_ID",
                person_staff_relationship_period_id=9059851427843950549,
                person=person,
                associated_staff_id=self.STAFF_ID_2,
            ),
            NormalizedStatePersonStaffRelationshipPeriod(
                state_code=self.STATE_CODE_VALUE,
                system_type=StateSystemType.SUPERVISION,
                system_type_raw_text=None,
                relationship_type=StatePersonStaffRelationshipType.SUPERVISING_OFFICER,
                relationship_type_raw_text=None,
                relationship_start_date=self.DATE_2,
                relationship_end_date_exclusive=self.DATE_3,
                location_external_id=None,
                relationship_priority=2,
                associated_staff_external_id=self.STAFF_ID_1_EXTERNAL_ID_A,
                associated_staff_external_id_type="US_XX_STAFF_ID",
                person_staff_relationship_period_id=9001502564037020168,
                person=person,
                associated_staff_id=self.STAFF_ID_1,
            ),
            NormalizedStatePersonStaffRelationshipPeriod(
                state_code=self.STATE_CODE_VALUE,
                system_type=StateSystemType.SUPERVISION,
                system_type_raw_text=None,
                relationship_type=StatePersonStaffRelationshipType.SUPERVISING_OFFICER,
                relationship_type_raw_text=None,
                relationship_start_date=self.DATE_3,
                relationship_end_date_exclusive=None,
                location_external_id=None,
                relationship_priority=1,
                associated_staff_external_id=self.STAFF_ID_2_EXTERNAL_ID,
                associated_staff_external_id_type="US_XX_STAFF_ID",
                person_staff_relationship_period_id=9049410191367390553,
                person=person,
                associated_staff_id=self.STAFF_ID_2,
            ),
        ]

        errors = validate_root_entity(person)
        self.assertEqual([], errors)

    def test_overlapping_same_staff_id_same_relationship_type(self) -> None:
        person = self._person()

        person.staff_relationship_periods = [
            NormalizedStatePersonStaffRelationshipPeriod(
                state_code=self.STATE_CODE_VALUE,
                system_type=StateSystemType.SUPERVISION,
                system_type_raw_text=None,
                relationship_type=StatePersonStaffRelationshipType.SUPERVISING_OFFICER,
                relationship_type_raw_text="A",
                relationship_start_date=self.DATE_2,
                relationship_end_date_exclusive=self.DATE_3,
                location_external_id=None,
                relationship_priority=1,
                associated_staff_external_id=self.STAFF_ID_1_EXTERNAL_ID_A,
                associated_staff_external_id_type="US_XX_STAFF_ID",
                person_staff_relationship_period_id=9059851427843950549,
                person=person,
                associated_staff_id=self.STAFF_ID_1,
            ),
            NormalizedStatePersonStaffRelationshipPeriod(
                state_code=self.STATE_CODE_VALUE,
                system_type=StateSystemType.SUPERVISION,
                system_type_raw_text=None,
                relationship_type=StatePersonStaffRelationshipType.SUPERVISING_OFFICER,
                relationship_type_raw_text="B",
                relationship_start_date=self.DATE_2,
                relationship_end_date_exclusive=self.DATE_3,
                location_external_id=None,
                # These periods are prioritized but we don't care because they still
                # have redundant info.
                relationship_priority=2,
                associated_staff_external_id=self.STAFF_ID_1_EXTERNAL_ID_B,
                associated_staff_external_id_type="US_XX_STAFF_ID",
                person_staff_relationship_period_id=9001502564037020168,
                person=person,
                associated_staff_id=self.STAFF_ID_1,
            ),
        ]

        errors = validate_root_entity(person)
        self.assertEqual(
            [
                "Found multiple (2) NormalizedStatePersonStaffRelationshipPeriod with "
                "relationship_type [StatePersonStaffRelationshipType.SUPERVISING_OFFICER] and "
                "staff_id [1000] on person [NormalizedStatePerson(person_id=1, "
                "external_ids=[NormalizedStatePersonExternalId(external_id='EXTERNAL_ID_A', "
                "id_type='US_XX_STAFF_ID', person_external_id_id=1)])] for date range "
                "[DateRange(lower_bound_inclusive_date=datetime.date(2020, 1, 1), "
                "upper_bound_exclusive_date=datetime.date(2021, 1, 1))]. Overlapping periods "
                "for the same staff member and relationship type should be collapsed / "
                "deduplicated."
            ],
            errors,
        )

    def test_overlapping_same_priority_same_relationship_type(self) -> None:
        person = self._person()

        person.staff_relationship_periods = [
            NormalizedStatePersonStaffRelationshipPeriod(
                state_code=self.STATE_CODE_VALUE,
                system_type=StateSystemType.SUPERVISION,
                system_type_raw_text=None,
                relationship_type=StatePersonStaffRelationshipType.SUPERVISING_OFFICER,
                relationship_type_raw_text=None,
                relationship_start_date=self.DATE_2,
                relationship_end_date_exclusive=self.DATE_3,
                location_external_id=None,
                relationship_priority=1,
                associated_staff_external_id=self.STAFF_ID_2_EXTERNAL_ID,
                associated_staff_external_id_type="US_XX_STAFF_ID",
                person_staff_relationship_period_id=9059851427843950549,
                person=person,
                associated_staff_id=self.STAFF_ID_2,
            ),
            NormalizedStatePersonStaffRelationshipPeriod(
                state_code=self.STATE_CODE_VALUE,
                system_type=StateSystemType.SUPERVISION,
                system_type_raw_text=None,
                relationship_type=StatePersonStaffRelationshipType.SUPERVISING_OFFICER,
                relationship_type_raw_text=None,
                relationship_start_date=self.DATE_2,
                relationship_end_date_exclusive=self.DATE_3,
                location_external_id=None,
                relationship_priority=1,
                associated_staff_external_id=self.STAFF_ID_1_EXTERNAL_ID_A,
                associated_staff_external_id_type="US_XX_STAFF_ID",
                person_staff_relationship_period_id=9001502564037020168,
                person=person,
                associated_staff_id=self.STAFF_ID_1,
            ),
        ]

        errors = validate_root_entity(person)
        self.assertEqual(
            [
                "Found multiple (2) NormalizedStatePersonStaffRelationshipPeriod with "
                "relationship_type [StatePersonStaffRelationshipType.SUPERVISING_OFFICER] and "
                "priority [1] on person [NormalizedStatePerson(person_id=1, "
                "external_ids=[NormalizedStatePersonExternalId(external_id='EXTERNAL_ID_A', "
                "id_type='US_XX_STAFF_ID', person_external_id_id=1)])] for date range "
                "[DateRange(lower_bound_inclusive_date=datetime.date(2020, 1, 1), "
                "upper_bound_exclusive_date=datetime.date(2021, 1, 1))]. If two staff have "
                "the same type of relationship with a person during a period of time, we must "
                "be able to prioritize that relationship."
            ],
            errors,
        )


class TestNormalizedPersonExternalIdChecks(unittest.TestCase):
    """Test that root entity checks specific to NormalizedStatePersonExternalId are
    valid.
    """

    def setUp(self) -> None:
        self.allowed_multiple_ids_patcher = patch(
            "recidiviz.pipelines.ingest.state.validator.person_external_id_types_with_allowed_multiples_per_person"
        )
        self.allowed_multiple_ids_patcher.start().return_value = [
            "US_XX_ID_TYPE",
            "US_XX_ID_TYPE_2",
        ]

    def tearDown(self) -> None:
        self.allowed_multiple_ids_patcher.stop()

    def _make_normalized_external_id(
        self,
        *,
        index: int,
        external_id: str,
        is_current_display_id_for_type: bool,
        id_type: str = "US_XX_ID_TYPE",
    ) -> NormalizedStatePersonExternalId:
        return NormalizedStatePersonExternalId(
            person_external_id_id=index,
            state_code=StateCode.US_XX.value,
            external_id=external_id,
            id_type=id_type,
            is_current_display_id_for_type=is_current_display_id_for_type,
            id_active_from_datetime=None,
            id_active_to_datetime=None,
        )

    def make_person(
        self,
        *,
        external_ids: list[NormalizedStatePersonExternalId],
    ) -> NormalizedStatePerson:
        """Helper to create a NormalizedStatePerson with given external IDs.

        external_ids: list of tuples (external_id, is_current_display_id_for_type)
        """
        person = NormalizedStatePerson(
            state_code=StateCode.US_XX.value,
            person_id=1,
            external_ids=external_ids,
        )
        for pei in person.external_ids:
            pei.person = person
        return person

    def test_valid_exactly_one_display_id(self) -> None:
        person = self.make_person(
            external_ids=[
                self._make_normalized_external_id(
                    index=1, external_id="ID1", is_current_display_id_for_type=False
                ),
                self._make_normalized_external_id(
                    index=2, external_id="ID2", is_current_display_id_for_type=True
                ),
                self._make_normalized_external_id(
                    index=3, external_id="ID3", is_current_display_id_for_type=False
                ),
            ]
        )
        errors: list[str] = list(validate_root_entity(person))
        self.assertEqual(errors, [])

    def test_error_multiple_ids_of_type_with_multiples_disallowed(self) -> None:
        person = self.make_person(
            external_ids=[
                self._make_normalized_external_id(
                    index=1,
                    external_id="ID1",
                    id_type="US_XX_NO_MULTIPLES_ALLOWED",
                    is_current_display_id_for_type=True,
                ),
                self._make_normalized_external_id(
                    index=2,
                    external_id="ID2",
                    id_type="US_XX_NO_MULTIPLES_ALLOWED",
                    is_current_display_id_for_type=False,
                ),
            ]
        )
        errors: list[str] = list(validate_root_entity(person))
        self.assertEqual(
            [
                "Duplicate external id types for [NormalizedStatePerson] with id [1]: "
                "US_XX_NO_MULTIPLES_ALLOWED"
            ],
            errors,
        )

    def test_error_no_display_id_set(self) -> None:
        person = self.make_person(
            external_ids=[
                self._make_normalized_external_id(
                    index=1, external_id="ID1", is_current_display_id_for_type=False
                ),
                self._make_normalized_external_id(
                    index=2, external_id="ID2", is_current_display_id_for_type=False
                ),
            ]
        )
        errors: list[str] = list(validate_root_entity(person))
        self.assertEqual(len(errors), 1)
        self.assertRegex(
            errors[0],
            r"Found no NormalizedStatePersonExternalId.*with type \[US_XX_ID_TYPE\].*"
            r"exactly one must be set",
        )

    def test_error_multiple_display_ids_set(self) -> None:
        person = self.make_person(
            external_ids=[
                self._make_normalized_external_id(
                    index=1, external_id="ID1", is_current_display_id_for_type=True
                ),
                self._make_normalized_external_id(
                    index=2, external_id="ID2", is_current_display_id_for_type=True
                ),
                self._make_normalized_external_id(
                    index=3, external_id="ID3", is_current_display_id_for_type=False
                ),
            ]
        )
        errors: list[str] = list(validate_root_entity(person))
        self.assertEqual(len(errors), 1)
        self.assertRegex(
            errors[0],
            r"Found multiple \(2\) NormalizedStatePersonExternalId.*with type "
            r"\[US_XX_ID_TYPE\].*exactly one must be set",
        )

    def test_valid_multiple_types_with_one_display_each(self) -> None:
        person = self.make_person(
            external_ids=[
                self._make_normalized_external_id(
                    index=1,
                    external_id="ID1",
                    id_type="US_XX_ID_TYPE",
                    is_current_display_id_for_type=True,
                ),
                self._make_normalized_external_id(
                    index=2,
                    external_id="ABC123",
                    id_type="US_XX_ID_TYPE_2",
                    is_current_display_id_for_type=True,
                ),
            ]
        )

        errors: list[str] = list(validate_root_entity(person))
        self.assertEqual(errors, [])

    def test_multiple_errors_for_multiple_types(self) -> None:
        person = self.make_person(
            external_ids=[
                # Type US_XX_ID_TYPE has no display ID
                self._make_normalized_external_id(
                    index=1,
                    external_id="ID1",
                    id_type="US_XX_ID_TYPE",
                    is_current_display_id_for_type=False,
                ),
                # Type US_XX_ID_TYPE_2 has two display IDs
                self._make_normalized_external_id(
                    index=2,
                    external_id="ABC123",
                    id_type="US_XX_ID_TYPE_2",
                    is_current_display_id_for_type=True,
                ),
                self._make_normalized_external_id(
                    index=3,
                    external_id="DEF456",
                    id_type="US_XX_ID_TYPE_2",
                    is_current_display_id_for_type=True,
                ),
            ]
        )

        errors: list[str] = list(validate_root_entity(person))
        self.assertEqual(
            [
                (
                    "Found multiple (2) NormalizedStatePersonExternalId on person "
                    "[NormalizedStatePerson(person_id=1, "
                    "external_ids=[NormalizedStatePersonExternalId(external_id='ID1', "
                    "id_type='US_XX_ID_TYPE', "
                    "person_external_id_id=1),NormalizedStatePersonExternalId(external_id='ABC123', "
                    "id_type='US_XX_ID_TYPE_2', "
                    "person_external_id_id=2),NormalizedStatePersonExternalId(external_id='DEF456', "
                    "id_type='US_XX_ID_TYPE_2', person_external_id_id=3)])] with type "
                    "[US_XX_ID_TYPE_2] that are designated as "
                    "is_current_display_id_for_type=True. If a person has any ids of a given "
                    "id_type, exactly one must be set as the display id."
                ),
                (
                    "Found no NormalizedStatePersonExternalId on person "
                    "[NormalizedStatePerson(person_id=1, "
                    "external_ids=[NormalizedStatePersonExternalId(external_id='ID1', "
                    "id_type='US_XX_ID_TYPE', "
                    "person_external_id_id=1),NormalizedStatePersonExternalId(external_id='ABC123', "
                    "id_type='US_XX_ID_TYPE_2', "
                    "person_external_id_id=2),NormalizedStatePersonExternalId(external_id='DEF456', "
                    "id_type='US_XX_ID_TYPE_2', person_external_id_id=3)])] with type "
                    "[US_XX_ID_TYPE] that are designated as is_current_display_id_for_type=True. "
                    "If a person has any ids of a given id_type, exactly one must be set as the "
                    "display id."
                ),
            ],
            sorted(errors),
        )
