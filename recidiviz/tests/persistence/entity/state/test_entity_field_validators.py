# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests code to validate fields specifically for state entities."""

import datetime
import unittest
from typing import List, Optional

import attr

from recidiviz.common import attr_validators
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state.entity_field_validators import pre_norm_opt
from recidiviz.persistence.entity.state.normalized_state_entity import (
    NormalizedStateEntity,
)


class Foo:
    BAR = "baz"


@attr.s()
class Example(Entity):
    an_int: Optional[int] = attr.ib(validator=pre_norm_opt(attr_validators.is_int))
    a_str: Optional[str] = attr.ib(validator=pre_norm_opt(attr_validators.is_str))
    a_foo: Optional[Foo] = attr.ib(
        validator=pre_norm_opt(attr.validators.instance_of(Foo))
    )
    always_optional_date: Optional[datetime.date] = attr.ib(
        validator=attr_validators.is_opt_date
    )
    always_optional_foo: Optional[Foo] = attr.ib(validator=attr_validators.is_opt(Foo))


class NormalizedExample(Example, NormalizedStateEntity):
    """Now all the fields should not be optional."""


@attr.s()
class CompoundExample(Entity):
    list_of_str: Optional[List[str]] = attr.ib(
        validator=pre_norm_opt(
            attr.validators.deep_iterable(
                member_validator=attr.validators.instance_of(str),
                iterable_validator=attr.validators.instance_of(list),
            )
        )
    )
    # attr.validator can be passed a list of validators, but individual elements
    # need to be compatible
    positive_int: Optional[int] = attr.ib(
        validator=[
            pre_norm_opt(attr_validators.is_int),
            pre_norm_opt(attr.validators.gt(0)),
        ]
    )
    # attr.validator can be passed a compound validator
    negative_int: Optional[int] = attr.ib(
        validator=pre_norm_opt(
            attr.validators.and_(attr_validators.is_int, attr.validators.lt(0))
        )
    )
    always_optional_foo: Optional[Foo] = attr.ib(validator=attr_validators.is_opt(Foo))
    always_optional_date: Optional[datetime.date] = attr.ib(
        validator=attr_validators.is_opt_date
    )


class NormalizedCompoundExample(CompoundExample, NormalizedStateEntity):
    """Now all the fields should not be optional."""


class TestPreNormOptionalValidator(unittest.TestCase):
    """Tests that the pre_norm_opt validator works as expected."""

    def test_pre_norm_is_actually_optional(self) -> None:
        _ = Example(
            an_int=None,
            a_str=None,
            a_foo=None,
            always_optional_foo=None,
            always_optional_date=None,
        )
        _ = Example(
            an_int=None,
            a_str="string",
            a_foo=Foo(),
            always_optional_foo=None,
            always_optional_date=None,
        )
        _ = Example(
            an_int=2,
            a_str=None,
            a_foo=Foo(),
            always_optional_foo=None,
            always_optional_date=None,
        )
        _ = Example(
            an_int=2,
            a_str="string",
            a_foo=None,
            always_optional_foo=None,
            always_optional_date=None,
        )
        _ = Example(
            an_int=2,
            a_str="string",
            a_foo=Foo(),
            always_optional_foo=None,
            always_optional_date=None,
        )
        _ = Example(
            an_int=2,
            a_str="string",
            a_foo=Foo(),
            always_optional_foo=Foo(),
            always_optional_date=None,
        )

        _ = CompoundExample(
            list_of_str=None,
            positive_int=None,
            negative_int=None,
            always_optional_foo=None,
            always_optional_date=None,
        )
        _ = CompoundExample(
            list_of_str=["abc"],
            positive_int=None,
            negative_int=None,
            always_optional_foo=None,
            always_optional_date=None,
        )
        _ = CompoundExample(
            list_of_str=None,
            positive_int=1,
            negative_int=None,
            always_optional_foo=None,
            always_optional_date=None,
        )
        _ = CompoundExample(
            list_of_str=None,
            positive_int=None,
            negative_int=-1,
            always_optional_foo=None,
            always_optional_date=None,
        )
        _ = CompoundExample(
            list_of_str=["abc"],
            positive_int=1,
            negative_int=-1,
            always_optional_foo=None,
            always_optional_date=None,
        )

    def test_normalized_is_not_optional(self) -> None:
        _ = NormalizedExample(
            an_int=2,
            a_str="string",
            a_foo=Foo(),
            always_optional_foo=None,
            always_optional_date=None,
        )
        _ = NormalizedExample(
            an_int=2,
            a_str="string",
            a_foo=Foo(),
            always_optional_foo=Foo(),
            always_optional_date=None,
        )
        with self.assertRaisesRegex(TypeError, "must be <class 'int'>"):
            _ = NormalizedExample(
                an_int=None,
                a_str="string",
                a_foo=Foo(),
                always_optional_foo=None,
                always_optional_date=None,
            )
        with self.assertRaisesRegex(TypeError, "must be <class 'str'>"):
            _ = NormalizedExample(
                an_int=2,
                a_str=None,
                a_foo=Foo(),
                always_optional_foo=None,
                always_optional_date=None,
            )
        with self.assertRaisesRegex(TypeError, "'a_foo' must be <class "):
            _ = NormalizedExample(
                an_int=2,
                a_str="string",
                a_foo=None,
                always_optional_foo=None,
                always_optional_date=None,
            )

        _ = NormalizedCompoundExample(
            list_of_str=["abc"],
            positive_int=1,
            negative_int=-1,
            always_optional_foo=None,
            always_optional_date=None,
        )
        with self.assertRaisesRegex(TypeError, "'list_of_str' must be "):
            _ = NormalizedCompoundExample(
                list_of_str=None,
                positive_int=1,
                negative_int=-1,
                always_optional_foo=None,
                always_optional_date=None,
            )
        with self.assertRaisesRegex(TypeError, "'positive_int' must be "):
            _ = NormalizedCompoundExample(
                list_of_str=["abc"],
                positive_int=None,
                negative_int=-1,
                always_optional_foo=None,
                always_optional_date=None,
            )
        with self.assertRaisesRegex(TypeError, "'negative_int' must be "):
            _ = NormalizedCompoundExample(
                list_of_str=["abc"],
                positive_int=1,
                negative_int=None,
                always_optional_foo=None,
                always_optional_date=None,
            )

        # Check that the underlying validators still work.
        with self.assertRaisesRegex(ValueError, "'positive_int' must be > 0: -42"):
            _ = NormalizedCompoundExample(
                list_of_str=["abc"],
                positive_int=-42,
                negative_int=-1,
                always_optional_foo=None,
                always_optional_date=None,
            )

        with self.assertRaisesRegex(ValueError, "'negative_int' must be < 0: 42"):
            _ = NormalizedCompoundExample(
                list_of_str=["abc"],
                positive_int=42,
                negative_int=42,
                always_optional_foo=Foo(),
                always_optional_date=None,
            )
