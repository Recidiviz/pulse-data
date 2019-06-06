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

from unittest import TestCase

import attr

from recidiviz.persistence.persistence_utils import primary_key_name_from_cls
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import \
    get_all_entity_classes_in_module
from recidiviz.persistence.entity.state import entities


class TestStateEntities(TestCase):
    """Tests for state/entities.py"""

    def test_classes_have_cmp_equal_false(self):
        for entity_class in get_all_entity_classes_in_module(entities):
            self.assertEqual(entity_class.__eq__, Entity.__eq__,
                             f"Class [{entity_class}] has an __eq__ function "
                             f"unequal to the base Entity class - did you "
                             f"remember to set cmp=False in the @attr.s "
                             f"declaration?")

    def test_all_entity_class_names_prefixed_with_state(self):
        for cls in get_all_entity_classes_in_module(entities):
            self.assertTrue(cls.__name__.startswith('State'))

    def test_all_entity_classes_have_expected_primary_id(self):
        for cls in get_all_entity_classes_in_module(entities):
            key_name = primary_key_name_from_cls(cls)
            self.assertTrue(key_name in attr.fields_dict(cls),
                            f"Expected primary key field [{key_name}] not "
                            f"defined for class [{cls}].")
            attribute = attr.fields_dict(cls)[key_name]
            self.assertEqual(attribute.type, Optional[int],
                             f"Unexpected type [{attribute.type}] for primary "
                             f"key [{key_name}] of class [{cls}].")

    def test_all_classes_have_a_non_optional_state_code(self):
        classes_without_a_state_code = [entities.StatePerson]

        for cls in get_all_entity_classes_in_module(entities):
            if cls in classes_without_a_state_code:
                continue
            self.assertTrue(
                'state_code' in attr.fields_dict(cls),
                f"Expected field |state_code| not defined for class [{cls}].")
            attribute = attr.fields_dict(cls)['state_code']
            self.assertEqual(attribute.type,
                             str,
                             f"Unexpected type [{attribute.type}] for "
                             f"|state_code| field of class [{cls}].")

    # TODO(1625): Add more detailed unit tests for entity_graph_eq (first
    #  defined in #1812)
