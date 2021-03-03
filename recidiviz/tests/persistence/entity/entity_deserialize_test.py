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
from typing import Optional, Union
from unittest import TestCase

import attr

from recidiviz.common import attr_validators
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.constants.enum_parser import EnumParser
from recidiviz.common.constants.person_characteristics import Race
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_deserialize import (
    entity_deserialize,
    EntityFieldConverter,
)


@attr.s(eq=False)
class MyEntity(Entity):
    int_with_default: int = attr.ib(default=1, validator=attr_validators.is_int)
    bool_with_default: bool = attr.ib(default=True, validator=attr_validators.is_bool)
    str_with_default: str = attr.ib(default="default", validator=attr_validators.is_str)
    enum_with_default: Race = attr.ib(
        default=Race.EXTERNAL_UNKNOWN, validator=attr.validators.instance_of(Race)
    )

    opt_int: Optional[int] = attr.ib(default=None, validator=attr_validators.is_opt_int)
    opt_bool: Optional[bool] = attr.ib(
        default=None, validator=attr_validators.is_opt_bool
    )
    opt_str: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)
    opt_enum: Optional[Race] = attr.ib(
        default=None, validator=attr_validators.is_opt(Race)
    )


class MyEntityFactory:
    @staticmethod
    def deserialize(**kwargs: Union[str, EnumParser]) -> MyEntity:
        return entity_deserialize(MyEntity, converter_overrides={}, **kwargs)


class TestEntityDeserialize(TestCase):
    """Tests for entity_deserialize.py."""

    def test_entity_deserialize_not_entity(self) -> None:
        with self.assertRaises(ValueError) as e:

            @attr.s
            class _NotAnEntity:
                an_int_field: int = attr.ib(default=1)

            _ = entity_deserialize(  # type: ignore[type-var]
                _NotAnEntity, converter_overrides={}, an_int_field="1"
            )

        self.assertTrue(
            str(e.exception).startswith(
                "Can only deserialize Entity classes with entity_deserialize()"
            )
        )

    def test_entity_deserialize_not_attr(self) -> None:
        with self.assertRaises(ValueError) as e:

            class _NotAnAttr:
                def __init__(self) -> None:
                    self.an_int_field = 1

            _ = entity_deserialize(  # type: ignore[type-var]
                _NotAnAttr, converter_overrides={}, an_int_field="1"
            )

        self.assertTrue(
            str(e.exception).startswith(
                "Can only deserialize attrs classes with entity_deserialize()"
            )
        )

    def test_entity_deserialize_use_normal_constructor(self) -> None:
        with self.assertRaises(TypeError):
            _ = MyEntity(int_with_default="3")  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            _ = MyEntity(opt_int="3")  # type: ignore[arg-type]

        with self.assertRaises(TypeError):
            _ = MyEntity(bool_with_default="True")  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            _ = MyEntity(opt_bool="True")  # type: ignore[arg-type]

        with self.assertRaises(TypeError):
            _ = MyEntity(
                enum_with_default=EnumParser(  # type: ignore[arg-type]
                    raw_text="BLACK",
                    enum_cls=Race,
                    enum_overrides=EnumOverrides.empty(),
                )
            )
        with self.assertRaises(TypeError):
            _ = MyEntity(
                opt_enum=EnumParser(  # type: ignore[arg-type]
                    raw_text="BLACK",
                    enum_cls=Race,
                    enum_overrides=EnumOverrides.empty(),
                )
            )

        default_entity = MyEntity()
        expected_default_entity = MyEntity(
            int_with_default=1,
            bool_with_default=True,
            str_with_default="default",
            enum_with_default=Race.EXTERNAL_UNKNOWN,
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
            enum_with_default=Race.EXTERNAL_UNKNOWN,
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

        enum_parser = EnumParser(
            raw_text="BLACK", enum_cls=Race, enum_overrides=EnumOverrides.empty()
        )
        self.assertEqual(
            attr.evolve(expected_default_entity, enum_with_default=Race.BLACK),
            MyEntityFactory.deserialize(enum_with_default=enum_parser),
        )
        self.assertEqual(
            attr.evolve(expected_default_entity, opt_enum=Race.BLACK),
            MyEntityFactory.deserialize(opt_enum=enum_parser),
        )

    def test_entity_deserialize_with_converter_overrides(self) -> None:
        def parse_int_and_double(int_str: str) -> int:
            return int(int_str) * 2

        def set_race_to_white(_race_enum_parser: EnumParser) -> Race:
            return Race.WHITE

        @attr.s(eq=False)
        class MyEntityWithFieldOverrides(Entity):
            str_with_override: str = attr.ib(validator=attr_validators.is_str)
            int_with_override: int = attr.ib(validator=attr_validators.is_int)
            enum_with_override: Race = attr.ib(
                validator=attr.validators.instance_of(Race)
            )
            str_no_override: str = attr.ib(validator=attr_validators.is_str)
            int_no_override: int = attr.ib(validator=attr_validators.is_int)
            enum_no_override: Race = attr.ib(
                validator=attr.validators.instance_of(Race)
            )

        class MyEntityWithFieldOverridesFactory:
            @staticmethod
            def deserialize(
                **kwargs: Union[str, EnumParser]
            ) -> MyEntityWithFieldOverrides:
                return entity_deserialize(
                    MyEntityWithFieldOverrides,
                    converter_overrides={
                        "str_with_override": EntityFieldConverter(str, str.lower),
                        "int_with_override": EntityFieldConverter(
                            str, parse_int_and_double
                        ),
                        "enum_with_override": EntityFieldConverter(
                            EnumParser, set_race_to_white
                        ),
                    },
                    **kwargs
                )

        entity = MyEntityWithFieldOverridesFactory.deserialize(
            str_with_override="AbCd",
            int_with_override="3",
            enum_with_override=EnumParser(
                raw_text="BLACK", enum_cls=Race, enum_overrides=EnumOverrides.empty()
            ),
            str_no_override="AbCd",
            int_no_override="3",
            enum_no_override=EnumParser(
                raw_text="BLACK", enum_cls=Race, enum_overrides=EnumOverrides.empty()
            ),
        )

        self.assertEqual(
            entity,
            MyEntityWithFieldOverrides(
                str_with_override="abcd",
                int_with_override=6,
                enum_with_override=Race.WHITE,
                str_no_override="ABCD",
                int_no_override=3,
                enum_no_override=Race.BLACK,
            ),
        )

    def test_entity_deserialize_subclass(self) -> None:
        @attr.s(eq=False)
        class MyEntitySubclass(MyEntity):
            subclass_field: int = attr.ib(default=0, validator=attr_validators.is_int)

        class MyEntitySubclassFactory:
            @staticmethod
            def deserialize(**kwargs: Union[str, EnumParser]) -> MyEntitySubclass:
                return entity_deserialize(
                    MyEntitySubclass, converter_overrides={}, **kwargs
                )

        subclass_entity = MyEntitySubclassFactory.deserialize(subclass_field="1234")
        self.assertIsInstance(subclass_entity, MyEntitySubclass)
        self.assertEqual(1234, subclass_entity.subclass_field)
