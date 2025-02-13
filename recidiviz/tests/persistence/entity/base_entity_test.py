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
import datetime
import unittest
from typing import no_type_check

import attr

from recidiviz.persistence.entity import base_entity
from recidiviz.persistence.entity.base_entity import (
    Entity,
    EnumEntity,
    UniqueConstraint,
    entity_graph_eq,
)
from recidiviz.persistence.entity.entity_utils import get_all_entity_classes_in_module
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.entity.state import entities as state_entities


class TestBaseEntities(unittest.TestCase):
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

    # MyPy is very helpful and doesn't let us get this far,
    # but it is nice to test we can't do this without it.
    @no_type_check
    def test_base_classes_are_subclass_only(self) -> None:
        with self.assertRaises(NotImplementedError):
            base_entity.Entity()
        with self.assertRaises(NotImplementedError):
            base_entity.EnumEntity()
        with self.assertRaises(NotImplementedError):
            base_entity.HasExternalIdEntity()
        with self.assertRaises(NotImplementedError):
            base_entity.HasMultipleExternalIdsEntity()

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


# TODO(#1894): Write unit tests for entity graph equality that reference the
# schema defined in test_schema/test_entities.py.
class TestEntityGraphEq(unittest.TestCase):
    """Tests the deep equality checks of two entities."""

    def test_entity_graph_eq_state_person_simple_case(self) -> None:
        person_1 = entities.StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=1,
        )
        person_2 = entities.StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=1,
        )
        self.assertTrue(entity_graph_eq(person_1, person_2))
        self.assertTrue(entity_graph_eq(None, None))
        self.assertFalse(entity_graph_eq(person_1, None))
        self.assertFalse(entity_graph_eq(None, person_2))


class TestUniqueConstraint(unittest.TestCase):
    """Tests for UniqueConstraint"""

    def test_unique_constraint_hashable(self) -> None:
        """Tests that UniqueConstraint can be added to a set for comparison."""
        constraints_1 = {
            UniqueConstraint(name="constraint_1", fields=["field_1", "field_2"])
        }

        constraints_2 = {
            UniqueConstraint(name="constraint_1", fields=["field_1", "field_2"])
        }

        self.assertEqual(constraints_1, constraints_2)


class TestEntityAssertDatetimeLessThan(unittest.TestCase):
    """Tests for the assert_datetime_less_than()"""

    def test_assert_datetime_less_than_or_equal(self) -> None:
        @attr.s(eq=False, kw_only=True)
        class ExampleEntity(Entity):
            start: datetime.datetime = attr.ib()
            end: datetime.datetime = attr.ib()

            def __attrs_post_init__(self) -> None:
                self.assert_datetime_less_than_or_equal(
                    self.start,
                    self.end,
                    before_description="start",
                    after_description="end",
                )

        _ = ExampleEntity(
            start=datetime.datetime(2022, 1, 1),
            end=datetime.datetime(2022, 1, 3),
        )

        # Exactly equal dates allowed
        _ = ExampleEntity(
            start=datetime.datetime(2022, 1, 1),
            end=datetime.datetime(2022, 1, 1),
        )

        with self.assertRaisesRegex(
            ValueError,
            r"Found ExampleEntity\(\) with start datetime "
            r"2022-01-01 00:00:00 after end datetime 2001-01-01 00:00:00.",
        ):
            _ = ExampleEntity(
                start=datetime.datetime(2022, 1, 1),
                end=datetime.datetime(2001, 1, 1),
            )

    def test_assert_datetime_less_than(self) -> None:
        @attr.s(eq=False, kw_only=True)
        class ExampleEntity(Entity):
            start: datetime.datetime = attr.ib()
            end: datetime.datetime = attr.ib()

            def __attrs_post_init__(self) -> None:
                self.assert_datetime_less_than(
                    self.start,
                    self.end,
                    before_description="start",
                    after_description="end",
                )

        _ = ExampleEntity(
            start=datetime.datetime(2022, 1, 1),
            end=datetime.datetime(2022, 1, 3),
        )

        with self.assertRaisesRegex(
            ValueError,
            r"Found ExampleEntity\(\) with start datetime "
            r"2001-01-01 00:00:00 equal to end datetime 2001-01-01 00:00:00.",
        ):
            _ = ExampleEntity(
                start=datetime.datetime(2001, 1, 1),
                end=datetime.datetime(2001, 1, 1),
            )

        with self.assertRaisesRegex(
            ValueError,
            r"Found ExampleEntity\(\) with start datetime "
            r"2022-01-01 00:00:00 after end datetime 2001-01-01 00:00:00.",
        ):
            _ = ExampleEntity(
                start=datetime.datetime(2022, 1, 1),
                end=datetime.datetime(2001, 1, 1),
            )

    def test_assert_datetime_less_than_multiple_dates(self) -> None:
        @attr.s(eq=False, kw_only=True)
        class ExampleEntity(Entity):
            START: datetime.datetime = attr.ib()
            END_1: datetime.datetime = attr.ib()
            END_2: datetime.datetime = attr.ib()

            def __attrs_post_init__(self) -> None:
                """An entity may have one or more datetime fields that are
                strictly after the 'start' datetime field.
                """
                self.assert_datetime_less_than_or_equal(
                    self.START,
                    self.END_1,
                    before_description="START",
                    after_description="END_1",
                )
                self.assert_datetime_less_than_or_equal(
                    self.START,
                    self.END_2,
                    before_description="START",
                    after_description="END_2",
                )

        _ = ExampleEntity(
            START=datetime.datetime(2022, 1, 1),
            END_1=datetime.datetime(2022, 1, 3),
            END_2=datetime.datetime(2022, 1, 6),
        )

        with self.assertRaisesRegex(
            ValueError,
            r"Found ExampleEntity\(\) with START datetime 2022-01-01 00:00:00 after "
            r"END_1 datetime 2001-01-01 00:00:00.",
        ):
            _ = ExampleEntity(
                START=datetime.datetime(2022, 1, 1),
                END_1=datetime.datetime(2001, 1, 1),
                END_2=datetime.datetime(2022, 1, 6),
            )

        with self.assertRaisesRegex(
            ValueError,
            r"Found ExampleEntity\(\) with START datetime 2022-01-01 00:00:00 after "
            r"END_2 datetime 1999-01-01 00:00:00.",
        ):
            _ = ExampleEntity(
                START=datetime.datetime(2022, 1, 1),
                END_1=datetime.datetime(2022, 1, 3),
                END_2=datetime.datetime(1999, 1, 1),
            )
