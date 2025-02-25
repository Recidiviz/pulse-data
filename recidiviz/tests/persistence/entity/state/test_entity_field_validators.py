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
from recidiviz.persistence.entity.state.entity_field_validators import (
    appears_with,
    parsing_opt_only,
)
from recidiviz.persistence.entity.state.normalized_state_entity import (
    NormalizedStateEntity,
)


class Foo:
    BAR = "baz"


@attr.s()
class Example(Entity):
    an_int: Optional[int] = attr.ib(validator=parsing_opt_only(attr_validators.is_int))
    a_str: Optional[str] = attr.ib(validator=parsing_opt_only(attr_validators.is_str))
    a_foo: Optional[Foo] = attr.ib(
        validator=parsing_opt_only(attr.validators.instance_of(Foo))
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
        validator=parsing_opt_only(
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
            parsing_opt_only(attr_validators.is_int),
            parsing_opt_only(attr.validators.gt(0)),
        ]
    )
    # attr.validator can be passed a compound validator
    negative_int: Optional[int] = attr.ib(
        validator=parsing_opt_only(
            attr.validators.and_(attr_validators.is_int, attr.validators.lt(0))
        )
    )
    always_optional_foo: Optional[Foo] = attr.ib(validator=attr_validators.is_opt(Foo))
    always_optional_date: Optional[datetime.date] = attr.ib(
        validator=attr_validators.is_opt_date
    )


class NormalizedCompoundExample(CompoundExample, NormalizedStateEntity):
    """Now all the fields should not be optional."""


class TestParsingOptionalOnlyValidator(unittest.TestCase):
    """Tests that the parsing_opt_only validator works as expected."""

    def test_parsing_only_is_actually_optional(self) -> None:
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

    def test_parsing_only_on_normalized_entity(self) -> None:
        with self.assertRaisesRegex(
            TypeError,
            r"Cannot use a parsing_opt_only\(\) validator on NormalizedStateEntity "
            r"classes",
        ):
            _ = NormalizedExample(
                an_int=2,
                a_str="string",
                a_foo=Foo(),
                always_optional_foo=None,
                always_optional_date=None,
            )

    def test_parsing_only_underlying_validators(self) -> None:

        # Check that the underlying validators still work.
        _ = CompoundExample(
            list_of_str=["abc"],
            positive_int=2,
            negative_int=-1,
            always_optional_foo=Foo(),
            always_optional_date=None,
        )
        with self.assertRaisesRegex(TypeError, "'positive_int' must be <class 'int'>"):
            _ = CompoundExample(
                list_of_str=["abc"],
                positive_int="str",  # type: ignore[arg-type]
                negative_int=-1,
                always_optional_foo=None,
                always_optional_date=None,
            )

        with self.assertRaisesRegex(TypeError, "'list_of_str' must be <class 'list'>"):
            _ = CompoundExample(
                list_of_str=2,  # type: ignore[arg-type]
                positive_int=1,
                negative_int=-1,
                always_optional_foo=None,
                always_optional_date=None,
            )

        with self.assertRaisesRegex(TypeError, "'negative_int' must be <class 'int'>"):
            _ = CompoundExample(
                list_of_str=["abc"],
                positive_int=1,
                negative_int="str",  # type: ignore[arg-type]
                always_optional_foo=None,
                always_optional_date=None,
            )

        with self.assertRaisesRegex(ValueError, "'positive_int' must be > 0: -42"):
            _ = CompoundExample(
                list_of_str=["abc"],
                positive_int=-42,
                negative_int=-1,
                always_optional_foo=None,
                always_optional_date=None,
            )

        with self.assertRaisesRegex(ValueError, "'negative_int' must be < 0: 42"):
            _ = CompoundExample(
                list_of_str=["abc"],
                positive_int=42,
                negative_int=42,
                always_optional_foo=Foo(),
                always_optional_date=None,
            )


@attr.s
class _ProperTestEntity:
    """
    Used in TestAppearsWith
    """

    first_field: Optional[str] = attr.ib(
        default=None,
        validator=[
            attr_validators.is_opt_str,
            appears_with("second_field"),
        ],
    )

    second_field: Optional[str] = attr.ib(
        default=None,
        validator=[
            attr_validators.is_opt_str,
            appears_with("first_field"),
        ],
    )


@attr.s
class _OnlyOneFieldHasAppearsWith:
    """
    Used in TestAppearsWith
    """

    first_field: Optional[str] = attr.ib(
        default=None,
        validator=[
            attr_validators.is_opt_str,
            appears_with("second_field"),
        ],
    )
    second_field: Optional[str] = attr.ib(
        default=None,
        validator=[
            attr_validators.is_opt_str,
        ],
    )


@attr.s
class _Missingfirstfieldattribute:
    """
    Used in TestAppearsWith
    """

    second_field: Optional[str] = attr.ib(
        default=None,
        validator=[
            attr_validators.is_opt_str,
            appears_with("first_field"),
        ],
    )


class TestAssertAppearsWith(unittest.TestCase):
    """
    Tests for appears_with validator.
    """

    def test_first_field_none(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            f"Fields of {_ProperTestEntity}: 'first_field' and 'second_field' must both be set or both be None. "
            "Current values: first_field=None, second_field=value2",
        ):
            _ProperTestEntity(second_field="value2")

    def test_second_field_none(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            f"Fields of {_ProperTestEntity}: 'first_field' and 'second_field' must both be set or both be None. "
            "Current values: first_field=value1, second_field=None",
        ):
            _ProperTestEntity(first_field="value1")

    def test_field2_validator(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "Field second_field does not have 'appears_with' validator",
        ):
            _OnlyOneFieldHasAppearsWith(first_field="value1", second_field="value2")

    def test_field1_attribute(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            f"first_field is currently not an attribute of {_Missingfirstfieldattribute}. "
            f"Fields 'first_field' and 'second_field' should both be attributes of {_Missingfirstfieldattribute}",
        ):
            _Missingfirstfieldattribute(second_field="value2")

    def test_valid_field_values(self) -> None:
        # These don't crash
        _ = _ProperTestEntity()  # both fields are none
        _ = _ProperTestEntity(
            first_field="value1", second_field="value2"
        )  # both fields are non null
