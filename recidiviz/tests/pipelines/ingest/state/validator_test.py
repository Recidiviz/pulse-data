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

from recidiviz.common.constants.state.state_task_deadline import StateTaskType
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.schema_utils import (
    get_database_entity_by_table_name,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    get_all_entity_classes_in_module,
)
from recidiviz.persistence.entity.state import entities as entities_schema
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state.entities import (
    StatePersonExternalId,
    StateStaffExternalId,
    StateTaskDeadline,
)
from recidiviz.pipelines.ingest.state.validator import validate_root_entity


class TestEntityValidations(unittest.TestCase):
    """Tests validations functions"""

    def setUp(self) -> None:
        self.field_index = CoreEntityFieldIndex()

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

        error_messages = validate_root_entity(entity, self.field_index)
        self.assertTrue(len(list(error_messages)) == 0)

    def test_missing_external_id_state_staff_entities(self) -> None:
        entity = state_entities.StateStaff(
            state_code="US_XX", staff_id=1111, external_ids=[]
        )

        error_messages = validate_root_entity(entity, self.field_index)
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

        error_messages = validate_root_entity(entity, self.field_index)
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

        error_messages = validate_root_entity(entity, self.field_index)
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

        error_messages = validate_root_entity(entity, self.field_index)
        self.assertTrue(len(list(error_messages)) == 0)

    def test_missing_external_id_state_person_entities(self) -> None:
        entity = state_entities.StatePerson(
            state_code="US_XX", person_id=1111, external_ids=[]
        )

        error_messages = validate_root_entity(entity, self.field_index)
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

        error_messages = validate_root_entity(entity, self.field_index)
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

        error_messages = validate_root_entity(entity, self.field_index)
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

        error_messages = validate_root_entity(person, self.field_index)
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

        error_messages = validate_root_entity(person, self.field_index)
        expected_error_message = (
            "Found [2] state_task_deadline entities with (state_code=US_XX, "
            "task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION, "
            "task_subtype=None, "
            'task_metadata={"external_id": "00000001-111123-371006", "sentence_type": "INCARCERATION"}, '
            "update_datetime=2023-02-01 11:19:00). First 2 entities found:\n"
            "  * StateTaskDeadline(task_deadline_id=1)\n"
            "  * StateTaskDeadline(task_deadline_id=2)"
        )
        self.assertEqual(expected_error_message, one(error_messages))

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

        error_messages = validate_root_entity(person, self.field_index)
        expected_error_message = (
            "Found [2] state_task_deadline entities with (state_code=US_XX, "
            "task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION, "
            "task_subtype=my_subtype, "
            'task_metadata={"external_id": "00000001-111123-371006", "sentence_type": "INCARCERATION"}, '
            "update_datetime=2023-02-01 11:19:00). First 2 entities found:\n"
            "  * StateTaskDeadline(task_deadline_id=1)\n"
            "  * StateTaskDeadline(task_deadline_id=2)"
        )
        self.assertEqual(expected_error_message, one(error_messages))

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

        error_messages = validate_root_entity(person, self.field_index)
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

        error_messages = validate_root_entity(person, self.field_index)
        self.assertEqual(2, len(error_messages))
        self.assertRegex(
            error_messages[0],
            r"^Found \[StatePerson\] with id \[3111\] missing an external_id:",
        )

        expected_error_message = (
            "Found [2] state_task_deadline entities with (state_code=US_XX, "
            "task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION, "
            "task_subtype=None, "
            'task_metadata={"external_id": "00000001-111123-371006", "sentence_type": "INCARCERATION"}, '
            "update_datetime=2023-02-01 11:19:00). First 2 entities found:\n"
            "  * StateTaskDeadline(task_deadline_id=1)\n"
            "  * StateTaskDeadline(task_deadline_id=2)"
        )
        self.assertEqual(expected_error_message, error_messages[1])


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
