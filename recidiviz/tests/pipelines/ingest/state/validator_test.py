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

import sqlalchemy

from recidiviz.common.constants.state.state_task_deadline import StateTaskType
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.schema_utils import (
    get_database_entity_by_table_name,
)
from recidiviz.persistence.entity.entity_utils import get_all_entity_classes_in_module
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

    def test_valid_external_id_state_staff_entities(self) -> None:
        entities = [
            state_entities.StateStaff(
                state_code="US_XX",
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
        ]

        for entity in entities:
            validate_root_entity(entity)

    def test_missing_external_id_state_staff_entities(self) -> None:
        entities = [state_entities.StateStaff(state_code="US_XX", external_ids=[])]

        with self.assertRaisesRegex(
            ValueError,
            r"^Found \[StateStaff\] with id \[None\] missing an external_id:",
        ):
            for entity in entities:
                validate_root_entity(entity)

    def test_two_external_ids_same_type_state_staff_entities(self) -> None:
        entities = [
            state_entities.StateStaff(
                state_code="US_XX",
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
        ]

        with self.assertRaisesRegex(
            ValueError,
            r"Duplicate external id types for \[StateStaff\] with id "
            r"\[None\]: US_XX_EMPLOYEE",
        ):
            for entity in entities:
                validate_root_entity(entity)

    def test_two_external_ids_exact_same_state_staff_entities(self) -> None:
        entities = [
            state_entities.StateStaff(
                state_code="US_XX",
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
        ]

        with self.assertRaisesRegex(
            ValueError,
            r"Duplicate external id types for \[StateStaff\] with id "
            r"\[None\]: US_XX_EMPLOYEE",
        ):
            for entity in entities:
                validate_root_entity(entity)

    def test_valid_external_id_state_person_entities(self) -> None:
        entities = [
            state_entities.StatePerson(
                state_code="US_XX",
                external_ids=[
                    StatePersonExternalId(
                        external_id="100",
                        state_code="US_XX",
                        id_type="US_XX_EMPLOYEE",
                    ),
                ],
            )
        ]

        for entity in entities:
            validate_root_entity(entity)

    def test_missing_external_id_state_person_entities(self) -> None:
        entities = [state_entities.StatePerson(state_code="US_XX", external_ids=[])]

        with self.assertRaisesRegex(
            ValueError,
            r"^Found \[StatePerson\] with id \[None\] missing an external_id:",
        ):
            for entity in entities:
                validate_root_entity(entity)

    def test_two_external_ids_same_type_state_person_entities(self) -> None:
        entities = [
            state_entities.StatePerson(
                state_code="US_XX",
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
        ]

        with self.assertRaisesRegex(
            ValueError,
            r"Duplicate external id types for \[StatePerson\] with id "
            r"\[None\]: US_XX_EMPLOYEE",
        ):
            for entity in entities:
                validate_root_entity(entity)

    def test_two_external_ids_exact_same_state_person_entities(self) -> None:
        entities = [
            state_entities.StatePerson(
                state_code="US_XX",
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
        ]

        with self.assertRaisesRegex(
            ValueError,
            r"Duplicate external id types for \[StatePerson\] with id "
            r"\[None\]: US_XX_EMPLOYEE",
        ):
            for entity in entities:
                validate_root_entity(entity)

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

        validate_root_entity(person)

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
                eligible_date=date(2020, 9, 11),
                update_datetime=datetime(2023, 2, 1, 11, 19),
                task_metadata='{"external_id": "00000001-111123-371006", "sentence_type": "INCARCERATION"}',
                person=person,
            )
        )

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
        with self.assertRaisesRegex(
            ValueError,
            r"More than one state_task_deadline entity found for root entity \[person_id 3111\] with state_code=US_XX, task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION, task_subtype=None, update_datetime=2023-02-01 11:19:00, first entity found: \[task_deadline_id 2\]",
        ):
            validate_root_entity(person)


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
            schema_constraint_names = [
                arg.name
                for arg in schema_entity.__table_args__
                if isinstance(arg, sqlalchemy.UniqueConstraint)
            ]
            self.assertListEqual(constraint_names, schema_constraint_names)
