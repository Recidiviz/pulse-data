# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests for entity_deserialize.py."""
from enum import Enum
from typing import Optional
from unittest import TestCase

import attr

from recidiviz.common import attr_validators
from recidiviz.common.constants.state.state_person import StateRace
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_deserialize import (
    DeserializableEntityFieldValue,
    EntityFieldConverter,
    entity_deserialize,
)


@attr.s(eq=False)
class MyEntity(Entity):
    int_with_default: int = attr.ib(default=1, validator=attr_validators.is_int)
    bool_with_default: bool = attr.ib(default=True, validator=attr_validators.is_bool)
    str_with_default: str = attr.ib(default="default", validator=attr_validators.is_str)
    enum_with_default: StateRace = attr.ib(
        default=StateRace.EXTERNAL_UNKNOWN,
        validator=attr.validators.instance_of(StateRace),
    )

    opt_int: Optional[int] = attr.ib(default=None, validator=attr_validators.is_opt_int)
    opt_bool: Optional[bool] = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )
    opt_str: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)
    opt_enum: Optional[StateRace] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateRace)
    )


class MyEntityFactory:
    @staticmethod
    def deserialize(**kwargs: DeserializableEntityFieldValue) -> MyEntity:
        return entity_deserialize(
            MyEntity, converter_overrides={}, defaults={}, **kwargs
        )


class TestEntityDeserialize(TestCase):
    """Tests for entity_deserialize.py."""

    def test_entity_deserialize_not_entity(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Can only deserialize Entity classes with entity_deserialize\(\)",
        ):

            @attr.s
            class _NotAnEntity:
                an_int_field: int = attr.ib(default=1)

            _ = entity_deserialize(  # type: ignore[type-var]
                _NotAnEntity, converter_overrides={}, defaults={}, an_int_field="1"
            )

    def test_entity_deserialize_not_attr(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Can only deserialize attrs classes with entity_deserialize\(\)",
        ):

            class _NotAnAttr:
                def __init__(self) -> None:
                    self.an_int_field = 1

            _ = entity_deserialize(  # type: ignore[type-var]
                _NotAnAttr, converter_overrides={}, defaults={}, an_int_field="1"
            )

    def test_entity_deserialize_bad_arg(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Unexpected kwargs for class \[MyEntity\]: \{'bad_arg'\}",
        ):
            MyEntityFactory.deserialize(bad_arg="FOO")

    def test_entity_deserialize_use_normal_constructor(self) -> None:
        with self.assertRaises(TypeError):
            _ = MyEntity(int_with_default="3")  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            _ = MyEntity(opt_int="3")  # type: ignore[arg-type]

        with self.assertRaises(TypeError):
            _ = MyEntity(bool_with_default="True")  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            _ = MyEntity(opt_bool="True")  # type: ignore[arg-type]

        default_entity = MyEntity()
        expected_default_entity = MyEntity(
            int_with_default=1,
            bool_with_default=True,
            str_with_default="default",
            enum_with_default=StateRace.EXTERNAL_UNKNOWN,
            opt_int=None,
            opt_bool=None,
            opt_str=None,
            opt_enum=None,
        )

        self.assertEqual(default_entity, expected_default_entity)

    def test_entity_deserialize(self) -> None:
        expected_default_entity = MyEntity(
            int_with_default=1,
            bool_with_default=True,
            str_with_default="default",
            enum_with_default=StateRace.EXTERNAL_UNKNOWN,
            opt_int=None,
            opt_bool=None,
            opt_str=None,
            opt_enum=None,
        )

        self.assertEqual(expected_default_entity, MyEntityFactory.deserialize())

        self.assertEqual(
            attr.evolve(expected_default_entity, str_with_default="HELLO"),
            MyEntityFactory.deserialize(str_with_default="hello"),
        )
        self.assertEqual(
            attr.evolve(expected_default_entity, opt_str="HELLO"),
            MyEntityFactory.deserialize(opt_str="hello"),
        )

        self.assertEqual(
            attr.evolve(expected_default_entity, int_with_default=3),
            MyEntityFactory.deserialize(int_with_default="3"),
        )
        self.assertEqual(
            attr.evolve(expected_default_entity, opt_int=3),
            MyEntityFactory.deserialize(opt_int="3"),
        )

        self.assertEqual(
            attr.evolve(expected_default_entity, bool_with_default=False),
            MyEntityFactory.deserialize(bool_with_default="False"),
        )
        self.assertEqual(
            attr.evolve(expected_default_entity, opt_bool=False),
            MyEntityFactory.deserialize(opt_bool="False"),
        )

        self.assertEqual(
            attr.evolve(expected_default_entity, enum_with_default=StateRace.BLACK),
            MyEntityFactory.deserialize(enum_with_default=StateRace.BLACK),
        )
        self.assertEqual(
            attr.evolve(expected_default_entity, opt_enum=StateRace.BLACK),
            MyEntityFactory.deserialize(opt_enum=StateRace.BLACK),
        )

    def test_entity_deserialize_with_converter_overrides(self) -> None:
        def parse_int_and_double(int_str: str) -> int:
            return int(int_str) * 2

        def set_race_to_white(_race_enum: Enum) -> StateRace:
            return StateRace.WHITE

        @attr.s(eq=False)
        class MyEntityWithFieldOverrides(Entity):
            str_with_override: str = attr.ib(validator=attr_validators.is_str)
            int_with_override: int = attr.ib(validator=attr_validators.is_int)
            enum_with_override: StateRace = attr.ib(
                validator=attr.validators.instance_of(StateRace)
            )
            str_no_override: str = attr.ib(validator=attr_validators.is_str)
            int_no_override: int = attr.ib(validator=attr_validators.is_int)
            enum_no_override: StateRace = attr.ib(
                validator=attr.validators.instance_of(StateRace)
            )

        class MyEntityWithFieldOverridesFactory:
            @staticmethod
            def deserialize(
                **kwargs: DeserializableEntityFieldValue,
            ) -> MyEntityWithFieldOverrides:
                return entity_deserialize(
                    MyEntityWithFieldOverrides,
                    converter_overrides={
                        "str_with_override": EntityFieldConverter(str, str.lower),
                        "int_with_override": EntityFieldConverter(
                            str, parse_int_and_double
                        ),
                        "enum_with_override": EntityFieldConverter(
                            Enum, set_race_to_white
                        ),
                    },
                    defaults={},
                    **kwargs,
                )

        entity = MyEntityWithFieldOverridesFactory.deserialize(
            str_with_override="AbCd",
            int_with_override="3",
            enum_with_override=StateRace.BLACK,
            str_no_override="AbCd",
            int_no_override="3",
            enum_no_override=StateRace.BLACK,
        )

        self.assertEqual(
            entity,
            MyEntityWithFieldOverrides(
                str_with_override="abcd",
                int_with_override=6,
                enum_with_override=StateRace.WHITE,
                str_no_override="ABCD",
                int_no_override=3,
                enum_no_override=StateRace.BLACK,
            ),
        )

    def test_entity_deserialize_with_defaults(self) -> None:
        @attr.s(eq=False)
        class MyEntityWithFieldDefaults(Entity):
            str_with_deserialize_default: str = attr.ib(
                validator=attr_validators.is_str
            )
            enum_with_deserialize_default: StateRace = attr.ib(
                validator=attr.validators.instance_of(StateRace)
            )
            int_with_deserialize_default: int = attr.ib(
                validator=attr_validators.is_int,
            )

        class MyEntityWithFieldDefaultsFactory:
            @staticmethod
            def deserialize(
                **kwargs: DeserializableEntityFieldValue,
            ) -> MyEntityWithFieldDefaults:
                return entity_deserialize(
                    MyEntityWithFieldDefaults,
                    converter_overrides={},
                    defaults={
                        "str_with_deserialize_default": "deserialize_default",
                        "int_with_deserialize_default": 5,
                        "enum_with_deserialize_default": StateRace.WHITE,
                    },
                    **kwargs,
                )

        entity = MyEntityWithFieldDefaultsFactory.deserialize(
            str_with_deserialize_default=None,
            int_with_deserialize_default=None,
            enum_with_deserialize_default=None,
        )

        self.assertEqual(
            entity,
            MyEntityWithFieldDefaults(
                str_with_deserialize_default="deserialize_default",
                int_with_deserialize_default=5,
                enum_with_deserialize_default=StateRace.WHITE,
            ),
        )

    def test_entity_deserialize_subclass(self) -> None:
        @attr.s(eq=False)
        class MyEntitySubclass(MyEntity):
            subclass_field: int = attr.ib(default=0, validator=attr_validators.is_int)

        class MyEntitySubclassFactory:
            @staticmethod
            def deserialize(
                **kwargs: DeserializableEntityFieldValue,
            ) -> MyEntitySubclass:
                return entity_deserialize(
                    MyEntitySubclass, converter_overrides={}, defaults={}, **kwargs
                )

        subclass_entity = MyEntitySubclassFactory.deserialize(subclass_field="1234")
        self.assertIsInstance(subclass_entity, MyEntitySubclass)
        self.assertEqual(1234, subclass_entity.subclass_field)
