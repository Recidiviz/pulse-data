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
"""Tests for entity_decorators.py."""
from typing import Optional
from unittest import TestCase

import attr

from recidiviz.common import attr_validators
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.constants.enum_parser import EnumParser
from recidiviz.common.constants.person_characteristics import Race
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_decorators import add_deserialize_constructor

@add_deserialize_constructor()
@attr.s(eq=False)
class MyEntity(Entity):
    int_with_default: int = attr.ib(default=1, validator=attr_validators.is_int)
    bool_with_default: bool = attr.ib(default=True, validator=attr_validators.is_bool)
    str_with_default: str = attr.ib(default='default', validator=attr_validators.is_str)
    enum_with_default: Race = attr.ib(default=Race.EXTERNAL_UNKNOWN,
                                      validator=attr.validators.instance_of(Race))

    opt_int: Optional[int] = attr.ib(default=None, validator=attr_validators.is_opt_int)
    opt_bool: Optional[bool] = attr.ib(default=None, validator=attr_validators.is_opt_bool)
    opt_str: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)
    opt_enum: Optional[Race] = attr.ib(default=None, validator=attr_validators.is_opt(Race))


class TestEntityDecorators(TestCase):
    """Tests for entity_decorators.py."""
    def test_add_deserialize_constructor_not_entity(self) -> None:
        with self.assertRaises(ValueError) as e:
            @add_deserialize_constructor()
            @attr.s
            class _NotAnEntity:
                an_int_field: int = attr.ib(default=1)

        self.assertTrue(
            str(e.exception).startswith('Can only decorate Entity classes with @add_deserialize_constructor()'))

    def test_add_deserialize_constructor_not_attr(self) -> None:
        with self.assertRaises(ValueError) as e:
            @add_deserialize_constructor()
            class _NotAnAttr:
                def __init__(self) -> None:
                    self.an_int_field = 1

        self.assertTrue(
            str(e.exception).startswith('Can only decorate attrs classes with @add_deserialize_constructor()'))

    def test_add_deserialize_constructor_deserialize_fn_exists_already(self) -> None:
        with self.assertRaises(ValueError) as e:
            @add_deserialize_constructor()
            class _HasDeserializeEntity(Entity):
                an_int_field: int = attr.ib(default=1)

                @classmethod
                def deserialize(cls, int_str: str) -> int:
                    raise NotImplementedError()
        self.assertTrue(
            str(e.exception).startswith('Method |deserialize| already present for cls'))

    def test_add_deserialize_constructor_use_normal_constructor(self) -> None:
        with self.assertRaises(TypeError):
            _ = MyEntity(int_with_default='3')  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            _ = MyEntity(opt_int='3')  # type: ignore[arg-type]

        with self.assertRaises(TypeError):
            _ = MyEntity(bool_with_default='True')  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            _ = MyEntity(opt_bool='True')  # type: ignore[arg-type]

        with self.assertRaises(TypeError):
            _ = MyEntity(enum_with_default=EnumParser(  # type: ignore[arg-type]
                raw_text='BLACK', enum_cls=Race, enum_overrides=EnumOverrides.empty()))
        with self.assertRaises(TypeError):
            _ = MyEntity(opt_enum=EnumParser(  # type: ignore[arg-type]
                raw_text='BLACK', enum_cls=Race, enum_overrides=EnumOverrides.empty()))

        default_entity = MyEntity()
        expected_default_entity = MyEntity(
            int_with_default=1,
            bool_with_default=True,
            str_with_default="default",
            enum_with_default=Race.EXTERNAL_UNKNOWN,
            opt_int=None,
            opt_bool=None,
            opt_str=None,
            opt_enum=None
        )

        self.assertEqual(default_entity, expected_default_entity)

    def test_add_deserialize_constructor(self) -> None:
        expected_default_entity = MyEntity(
            int_with_default=1,
            bool_with_default=True,
            str_with_default="default",
            enum_with_default=Race.EXTERNAL_UNKNOWN,
            opt_int=None,
            opt_bool=None,
            opt_str=None,
            opt_enum=None
        )

        self.assertEqual(expected_default_entity, MyEntity.deserialize())  # type: ignore[attr-defined]

        self.assertEqual(attr.evolve(expected_default_entity, str_with_default="HELLO"),
                         MyEntity.deserialize(str_with_default='hello'))  # type: ignore[attr-defined]
        self.assertEqual(attr.evolve(expected_default_entity, opt_str="HELLO"),
                         MyEntity.deserialize(opt_str='hello'))  # type: ignore[attr-defined]

        self.assertEqual(attr.evolve(expected_default_entity, int_with_default=3),
                         MyEntity.deserialize(int_with_default='3'))  # type: ignore[attr-defined]
        self.assertEqual(attr.evolve(expected_default_entity, opt_int=3),
                         MyEntity.deserialize(opt_int='3'))  # type: ignore[attr-defined]

        self.assertEqual(attr.evolve(expected_default_entity, bool_with_default=False),
                         MyEntity.deserialize(bool_with_default='False'))  # type: ignore[attr-defined]
        self.assertEqual(attr.evolve(expected_default_entity, opt_bool=False),
                         MyEntity.deserialize(opt_bool='False'))  # type: ignore[attr-defined]

        enum_parser = EnumParser(
            raw_text='BLACK', enum_cls=Race, enum_overrides=EnumOverrides.empty())
        self.assertEqual(attr.evolve(expected_default_entity, enum_with_default=Race.BLACK),
                         MyEntity.deserialize(enum_with_default=enum_parser))  # type: ignore[attr-defined]
        self.assertEqual(attr.evolve(expected_default_entity, opt_enum=Race.BLACK),
                         MyEntity.deserialize(opt_enum=enum_parser))  # type: ignore[attr-defined]

    def test_add_deserialize_constructor_with_converter_overrides(self) -> None:
        def parse_int_and_double(int_str: str) -> int:
            return int(int_str) * 2

        @add_deserialize_constructor({
            'str_with_override': str.lower,
            'int_with_override': parse_int_and_double
        })
        @attr.s(eq=False)
        class MyEntityWithFieldOverrides(Entity):
            str_with_override: str = attr.ib(validator=attr_validators.is_str)
            int_with_override: int = attr.ib(validator=attr_validators.is_int)
            str_no_override: str = attr.ib(validator=attr_validators.is_str)
            int_no_override: int = attr.ib(validator=attr_validators.is_int)

        entity = MyEntityWithFieldOverrides.deserialize(  # type: ignore[attr-defined]
            str_with_override="AbCd",
            int_with_override="3",
            str_no_override="AbCd",
            int_no_override="3",
        )

        self.assertEqual(
            entity,
            MyEntityWithFieldOverrides(
                str_with_override="abcd",
                int_with_override=6,
                str_no_override="ABCD",
                int_no_override=3
            )
        )

    def test_add_deserialize_constructor_subclass(self) -> None:
        @attr.s(eq=False)
        class MyEntitySubclass(MyEntity):
            subclass_field: int = attr.ib(default=0, validator=attr_validators.is_int)

        subclass_entity = MyEntitySubclass.deserialize(subclass_field='1234')  # type: ignore[attr-defined]
        self.assertIsInstance(subclass_entity, MyEntitySubclass)
        self.assertEqual(1234, subclass_entity.subclass_field)
