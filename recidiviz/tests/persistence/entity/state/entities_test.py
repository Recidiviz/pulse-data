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
"""Tests for state/entities.py"""
from datetime import date, datetime
from typing import ForwardRef, Optional
from unittest import TestCase

import attr
from more_itertools import one

from recidiviz.common.attr_mixins import attribute_field_type_reference_for_class
from recidiviz.common.attr_utils import is_non_optional_enum
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.constants.state.state_task_deadline import StateTaskType
from recidiviz.persistence.entity.base_entity import Entity, EnumEntity
from recidiviz.persistence.entity.core_entity import primary_key_name_from_cls
from recidiviz.persistence.entity.entity_utils import get_all_entity_classes_in_module
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.entity.state.entities import (
    StateAssessment,
    StateProgramAssignment,
    StateSupervisionContact,
    StateSupervisionPeriod,
    StateTaskDeadline,
)
from recidiviz.tests.persistence.entity.state.entities_test_utils import (
    generate_full_graph_state_person,
)


class TestStateEntities(TestCase):
    """Tests for state/entities.py"""

    def test_classes_have_eq_equal_false(self) -> None:
        for entity_class in get_all_entity_classes_in_module(entities):
            self.assertEqual(
                entity_class.__eq__,
                Entity.__eq__,
                f"Class [{entity_class}] has an __eq__ function "
                f"unequal to the base Entity class - did you "
                f"remember to set eq=False in the @attr.s "
                f"declaration?",
            )

    def test_all_entity_class_names_prefixed_with_state(self) -> None:
        for cls in get_all_entity_classes_in_module(entities):
            self.assertTrue(cls.__name__.startswith("State"))

    def test_all_entity_classes_have_expected_primary_id(self) -> None:
        for cls in get_all_entity_classes_in_module(entities):
            key_name = primary_key_name_from_cls(cls)
            self.assertTrue(
                key_name in attr.fields_dict(cls),
                f"Expected primary key field [{key_name}] not "
                f"defined for class [{cls}].",
            )
            attribute = attr.fields_dict(cls)[key_name]
            self.assertEqual(
                attribute.type,
                Optional[int],
                f"Unexpected type [{attribute.type}] for primary "
                f"key [{key_name}] of class [{cls}].",
            )

    def test_all_classes_have_a_non_optional_state_code(self) -> None:
        for cls in get_all_entity_classes_in_module(entities):
            self.assertTrue(
                "state_code" in attr.fields_dict(cls),
                f"Expected field |state_code| not defined for class [{cls}].",
            )
            attribute = attr.fields_dict(cls)["state_code"]
            self.assertEqual(
                attribute.type,
                str,
                f"Unexpected type [{attribute.type}] for "
                f"|state_code| field of class [{cls}].",
            )

    def test_all_enum_entity_class_structure(self) -> None:
        for cls in get_all_entity_classes_in_module(entities):
            if not issubclass(cls, EnumEntity):
                continue
            field_info_dict = attribute_field_type_reference_for_class(cls)

            if "external_id" in field_info_dict:
                raise ValueError(
                    f"Found EnumEntity [{cls.__name__}] with an external_id field. "
                    f"EnumEntity entities should not have an external_id."
                )

            enum_fields = {
                field_name
                for field_name, info in field_info_dict.items()
                if info.enum_cls
            }
            if len(enum_fields) > 1:
                raise ValueError(
                    f"Found more than one enum field on EnumEntity [{cls.__name__}]."
                )
            enum_field_name = one(enum_fields)
            attribute = attr.fields_dict(cls)[enum_field_name]
            self.assertTrue(
                is_non_optional_enum(attribute),
                f"Enum field [{enum_field_name}] on class [{cls.__name__}] should be "
                f"nonnull.",
            )

    def test_all_classes_have_person_or_staff_reference(self) -> None:
        classes_without_a_person_or_staff_ref = [
            entities.StatePerson,
            entities.StateStaff,
        ]

        for cls in get_all_entity_classes_in_module(entities):
            if cls in classes_without_a_person_or_staff_ref:
                continue
            has_person_ref = "person" in attr.fields_dict(cls)
            has_staff_ref = "staff" in attr.fields_dict(cls)
            self.assertTrue(
                has_person_ref or has_staff_ref,
                f"Expected field |person| or |staff| defined for class [{cls}].",
            )

            self.assertTrue(
                not has_person_ref or not has_staff_ref,
                f"Expected field |person| or |staff| defined for class [{cls}], but "
                f"not both.",
            )

            if has_person_ref:
                attribute = attr.fields_dict(cls)["person"]
                self.assertEqual(
                    attribute.type,
                    Optional[ForwardRef("StatePerson")],
                    f"Unexpected type [{attribute.type}] for |person| "
                    f"field of class [{cls}].",
                )
            if has_staff_ref:
                attribute = attr.fields_dict(cls)["staff"]
                self.assertEqual(
                    attribute.type,
                    Optional[ForwardRef("StateStaff")],
                    f"Unexpected type [{attribute.type}] for |staff| "
                    f"field of class [{cls}].",
                )

    def test_person_equality_no_backedges(self) -> None:
        person1 = generate_full_graph_state_person(set_back_edges=False)
        person2 = generate_full_graph_state_person(set_back_edges=False)

        self.assertEqual(person1, person2)

    def test_person_equality_with_backedges(self) -> None:
        person1 = generate_full_graph_state_person(set_back_edges=True)
        person2 = generate_full_graph_state_person(set_back_edges=True)

        self.assertEqual(person1, person2)

    def test_person_equality_ignore_list_ordering(self) -> None:
        person1 = generate_full_graph_state_person(set_back_edges=True)
        person2 = generate_full_graph_state_person(set_back_edges=True)

        self.assertTrue(len(person2.assessments) > 1)
        person2.assessments.reverse()

        self.assertEqual(person1, person2)

    def test_person_equality_list_items_differ(self) -> None:
        person1 = generate_full_graph_state_person(set_back_edges=True)
        person2 = generate_full_graph_state_person(set_back_edges=True)

        next(iter(person1.assessments)).state_code = "US_YY"

        self.assertNotEqual(person1, person2)

    def test_person_equality_list(self) -> None:
        """Test that we can compare a list of person Entity objects"""
        person1a = generate_full_graph_state_person(set_back_edges=True)
        person1b = generate_full_graph_state_person(set_back_edges=True)

        person2a = generate_full_graph_state_person(set_back_edges=False)
        person2b = generate_full_graph_state_person(set_back_edges=False)

        self.assertEqual([person1a, person2a], [person1b, person2b])

    # TODO(#1894): Add more detailed unit tests for entity_graph_eq (first
    #  defined in #1812)

    def test_post_attrs_valid_supervising_officer_staff_id_simple(self) -> None:
        _ = StateSupervisionPeriod(
            supervision_period_id=1000,
            state_code="US_XX",
            external_id="1111",
            start_date=date(2020, 1, 1),
            supervising_officer_staff_external_id="ABCDE",
            supervising_officer_staff_external_id_type="EMP-2",
        )

    def test_post_attrs_conducting_staff_id_simple(self) -> None:
        _ = StateAssessment(
            assessment_id=1000,
            state_code="US_XX",
            external_id="1111",
            conducting_staff_external_id="ABCDE",
            conducting_staff_external_id_type="EMP-2",
        )

    def test_check_constraint_for_contacting_staff_id_simple(self) -> None:
        _ = StateSupervisionContact(
            supervision_contact_id=1000,
            state_code="US_XX",
            external_id="1111",
            contacting_staff_external_id="ABCDE",
            contacting_staff_external_id_type="EMP-2",
        )

    def test_post_attrs_referring_staff_id_simple(self) -> None:
        _ = StateProgramAssignment(
            program_assignment_id=1000,
            state_code="US_XX",
            external_id="1111",
            referring_staff_external_id="ABCDE",
            referring_staff_external_id_type="EMP-2",
            participation_status=StateProgramAssignmentParticipationStatus.INTERNAL_UNKNOWN,
            participation_status_raw_text="X",
        )

    def test_post_attrs_state_task_deadline_simple(self) -> None:
        _ = StateTaskDeadline(
            state_code="US_XX",
            eligible_date=date(2012, 10, 15),
            due_date=date(2015, 7, 10),
            update_datetime=datetime(2022, 4, 8, 0, 0, 0),
            task_type=StateTaskType.DISCHARGE_FROM_SUPERVISION,
            sequence_num=None,
        )

    def test_post_attrs_state_task_deadline_eligible_before_due(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Found StateTaskDeadline StateTaskDeadline\(task_deadline_id=1000\) with  datetime 2015-06-15 after  datetime 2012-07-10.",
        ):
            _ = StateTaskDeadline(
                task_deadline_id=1000,
                state_code="US_XX",
                eligible_date=date(2015, 6, 15),
                due_date=date(2012, 7, 10),
                update_datetime=datetime(2022, 4, 8, 0, 0, 0),
                task_type=StateTaskType.DISCHARGE_FROM_SUPERVISION,
                sequence_num=None,
            )
