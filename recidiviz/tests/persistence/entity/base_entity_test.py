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

# pylint: disable=unused-import,wrong-import-order

"""Tests for base_entity.py"""
from unittest import TestCase

from recidiviz.persistence.entity import base_entity
from recidiviz.persistence.entity.base_entity import Entity, EnumEntity
from recidiviz.persistence.entity.entity_utils import get_all_entity_classes_in_module
from recidiviz.persistence.entity.state import entities as state_entities


class TestBaseEntities(TestCase):
    """Tests for base_entity.py"""

    def test_base_classes_have_eq_equal_false(self) -> None:
        for entity_class in get_all_entity_classes_in_module(base_entity):
            self.assertEqual(
                entity_class.__eq__,
                Entity.__eq__,
                f"Class [{entity_class}] has an __eq__ function "
                f"unequal to the base Entity class - did you "
                f"remember to set eq=False in the @attr.s "
                f"declaration?",
            )

    # TODO(#1894): Write unit tests for entity graph equality that reference the
    # schema defined in test_schema/test_entities.py.

    def test_enum_entity_helpers(self) -> None:
        self.assertEqual("race", state_entities.StatePersonRace.get_enum_field_name())
        self.assertEqual(
            "race_raw_text", state_entities.StatePersonRace.get_raw_text_field_name()
        )

        for entity_class in get_all_entity_classes_in_module(state_entities):
            if not issubclass(entity_class, EnumEntity):
                continue
            # These should not crash
            entity_class.get_enum_field_name()
            entity_class.get_raw_text_field_name()
