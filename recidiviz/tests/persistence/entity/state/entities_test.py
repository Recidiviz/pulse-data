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
from typing import Optional

# ForwardRef not defined in python 3.6 and mypy will complain if we don't
# surround with try-catch.
try:
    from typing import ForwardRef  # type: ignore
except ImportError as e:
    raise ImportError(e) from e

from unittest import TestCase

import attr

from recidiviz.persistence.entity.core_entity import primary_key_name_from_cls
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import get_all_entity_classes_in_module
from recidiviz.persistence.entity.state import entities
from recidiviz.tests.persistence.entity.state.entities_test_utils import (
    generate_full_graph_state_person,
)


class TestStateEntities(TestCase):
    """Tests for state/entities.py"""

    def test_classes_have_eq_equal_false(self):
        for entity_class in get_all_entity_classes_in_module(entities):
            self.assertEqual(
                entity_class.__eq__,
                Entity.__eq__,
                f"Class [{entity_class}] has an __eq__ function "
                f"unequal to the base Entity class - did you "
                f"remember to set eq=False in the @attr.s "
                f"declaration?",
            )

    def test_all_entity_class_names_prefixed_with_state(self):
        for cls in get_all_entity_classes_in_module(entities):
            self.assertTrue(cls.__name__.startswith("State"))

    def test_all_entity_classes_have_expected_primary_id(self):
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

    def test_all_classes_have_a_non_optional_state_code(self):
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

    def test_all_classes_have_person_reference(self):
        classes_without_a_person_ref = [
            entities.StatePerson,
            entities.StateAgent,
        ]

        for cls in get_all_entity_classes_in_module(entities):
            if cls in classes_without_a_person_ref:
                continue
            self.assertTrue(
                "person" in attr.fields_dict(cls),
                f"Expected field |person| not defined for class [{cls}].",
            )
            attribute = attr.fields_dict(cls)["person"]
            self.assertEqual(
                attribute.type,
                Optional[ForwardRef("StatePerson")],
                f"Unexpected type [{attribute.type}] for |person| "
                f"field of class [{cls}].",
            )

    def test_person_equality_no_backedges(self):
        person1 = generate_full_graph_state_person(set_back_edges=False)
        person2 = generate_full_graph_state_person(set_back_edges=False)

        self.assertEqual(person1, person2)

    def test_person_equality_with_backedges(self):
        person1 = generate_full_graph_state_person(set_back_edges=True)
        person2 = generate_full_graph_state_person(set_back_edges=True)

        self.assertEqual(person1, person2)

    def test_person_equality_ignore_list_ordering(self):
        person1 = generate_full_graph_state_person(set_back_edges=True)
        person2 = generate_full_graph_state_person(set_back_edges=True)

        self.assertTrue(len(person2.assessments) > 1)
        person2.assessments.reverse()

        self.assertEqual(person1, person2)

    def test_person_equality_list_items_differ(self) -> None:
        person1 = generate_full_graph_state_person(set_back_edges=True)
        person2 = generate_full_graph_state_person(set_back_edges=True)

        next(iter(person1.assessments)).state_code = "us_ny"

        self.assertNotEqual(person1, person2)

    def test_person_equality_list(self):
        """Test that we can compare a list of person Entity objects"""
        person1a = generate_full_graph_state_person(set_back_edges=True)
        person1b = generate_full_graph_state_person(set_back_edges=True)

        person2a = generate_full_graph_state_person(set_back_edges=False)
        person2b = generate_full_graph_state_person(set_back_edges=False)

        self.assertEqual([person1a, person2a], [person1b, person2b])

    # TODO(#1894): Add more detailed unit tests for entity_graph_eq (first
    #  defined in #1812)
