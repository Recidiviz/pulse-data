# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests for attr_utils.py."""
import unittest
from datetime import date, datetime
from enum import Enum
from typing import ForwardRef, List, Optional

import attr
from more_itertools import one

from recidiviz.common.attr_utils import (
    get_enum_cls,
    get_non_flat_attribute_class_name,
    get_non_optional_type,
    is_date,
    is_datetime,
    is_enum,
    is_float,
    is_forward_ref,
    is_int,
    is_list,
    is_non_optional_enum,
    is_optional_type,
    is_str,
)
from recidiviz.utils.types import non_optional


class FakeEnum(Enum):
    A = "A"
    B = "B"


class AttrUtilsTest(unittest.TestCase):
    """Tests for attr_utils.py."""

    def _check_no_utils_crash(self, attribute: attr.Attribute) -> None:
        """Tests that none of the attr_utils helpers crash for the given attribute."""
        mutually_exclusive: List[bool] = [
            is_str(attribute),
            is_int(attribute),
            is_float(attribute),
            is_date(attribute),
            is_enum(attribute),
            is_list(attribute),
            is_forward_ref(attribute),
        ]

        try:
            one(result for result in mutually_exclusive if result is True)
        except ValueError as e:
            raise ValueError(
                f"Expected to find exactly one mutually exclusive field classifier "
                f"that returned True for attribute [{attribute}] "
            ) from e

        _ = is_datetime(attribute)
        _ = get_non_flat_attribute_class_name(attribute)
        _ = get_enum_cls(attribute)

    def test_enum_fields(self) -> None:
        @attr.define
        class MyAttrClass:
            my_opt_enum: Optional[FakeEnum]
            my_opt_enum_other: FakeEnum | None
            my_nonnull_enum: FakeEnum

        fields_dict = attr.fields_dict(MyAttrClass)

        my_opt_enum_attribute = fields_dict["my_opt_enum"]
        my_opt_enum_other_attribute = fields_dict["my_opt_enum_other"]
        my_nonnull_enum_attribute = fields_dict["my_nonnull_enum"]

        self.assertTrue(is_enum(my_opt_enum_attribute))
        self.assertTrue(is_enum(my_opt_enum_other_attribute))
        self.assertTrue(is_enum(my_nonnull_enum_attribute))

        self.assertFalse(is_non_optional_enum(my_opt_enum_attribute))
        self.assertFalse(is_non_optional_enum(my_opt_enum_other_attribute))
        self.assertTrue(is_non_optional_enum(my_nonnull_enum_attribute))

        self.assertEqual(FakeEnum, get_enum_cls(my_opt_enum_attribute))
        self.assertEqual(FakeEnum, get_enum_cls(my_opt_enum_other_attribute))
        self.assertEqual(FakeEnum, get_enum_cls(my_nonnull_enum_attribute))

        for attribute in fields_dict.values():
            self._check_no_utils_crash(attribute)

    def test_str_fields(self) -> None:
        @attr.define
        class MyAttrClass:
            my_opt_str: Optional[str]
            my_opt_str_other: str | None
            my_nonnull_str: str

        fields_dict = attr.fields_dict(MyAttrClass)

        my_opt_str_attribute = fields_dict["my_opt_str"]
        my_opt_str_other_attribute = fields_dict["my_opt_str_other"]
        my_nonnull_str_attribute = fields_dict["my_nonnull_str"]

        self.assertTrue(is_str(my_opt_str_attribute))
        self.assertTrue(is_str(my_opt_str_other_attribute))
        self.assertTrue(is_str(my_nonnull_str_attribute))

        self.assertTrue(is_optional_type(non_optional(my_opt_str_attribute.type)))
        self.assertTrue(is_optional_type(non_optional(my_opt_str_other_attribute.type)))
        self.assertFalse(is_optional_type(non_optional(my_nonnull_str_attribute.type)))

        self.assertEqual(None, get_enum_cls(my_opt_str_attribute))
        self.assertEqual(None, get_enum_cls(my_nonnull_str_attribute))

        self.assertEqual(
            str, get_non_optional_type(non_optional(my_opt_str_attribute.type))
        )
        self.assertEqual(
            str, get_non_optional_type(non_optional(my_opt_str_other_attribute.type))
        )

        for attribute in fields_dict.values():
            self._check_no_utils_crash(attribute)

    def test_int_fields(self) -> None:
        @attr.define
        class MyAttrClass:
            my_opt_int: Optional[int]
            my_opt_int_other: int | None
            my_nonnull_int: int

        fields_dict = attr.fields_dict(MyAttrClass)

        my_opt_int_attribute = fields_dict["my_opt_int"]
        my_opt_int_other_attribute = fields_dict["my_opt_int_other"]
        my_nonnull_int_attribute = fields_dict["my_nonnull_int"]

        self.assertTrue(is_int(my_opt_int_attribute))
        self.assertTrue(is_int(my_opt_int_other_attribute))
        self.assertTrue(is_int(my_nonnull_int_attribute))

        self.assertTrue(is_optional_type(non_optional(my_opt_int_attribute.type)))
        self.assertTrue(is_optional_type(non_optional(my_opt_int_other_attribute.type)))
        self.assertFalse(is_optional_type(non_optional(my_nonnull_int_attribute.type)))

        self.assertEqual(
            int, get_non_optional_type(non_optional(my_opt_int_attribute.type))
        )
        self.assertEqual(
            int, get_non_optional_type(non_optional(my_opt_int_other_attribute.type))
        )

        for attribute in fields_dict.values():
            self._check_no_utils_crash(attribute)

    def test_float_fields(self) -> None:
        @attr.define
        class MyAttrClass:
            my_opt_float: Optional[float]
            my_opt_float_other: float | None
            my_nonnull_float: float

        fields_dict = attr.fields_dict(MyAttrClass)

        my_opt_float_attribute = fields_dict["my_opt_float"]
        my_opt_float_other_attribute = fields_dict["my_opt_float_other"]
        my_nonnull_float_attribute = fields_dict["my_nonnull_float"]

        self.assertTrue(is_float(my_opt_float_attribute))
        self.assertTrue(is_float(my_opt_float_other_attribute))
        self.assertTrue(is_float(my_nonnull_float_attribute))

        self.assertTrue(is_optional_type(non_optional(my_opt_float_attribute.type)))
        self.assertTrue(
            is_optional_type(non_optional(my_opt_float_other_attribute.type))
        )
        self.assertFalse(
            is_optional_type(non_optional(my_nonnull_float_attribute.type))
        )

        self.assertEqual(
            float, get_non_optional_type(non_optional(my_opt_float_attribute.type))
        )
        self.assertEqual(
            float,
            get_non_optional_type(non_optional(my_opt_float_other_attribute.type)),
        )

        for attribute in fields_dict.values():
            self._check_no_utils_crash(attribute)

    def test_date_fields(self) -> None:
        @attr.define
        class MyAttrClass:
            my_opt_date: Optional[date]
            my_opt_date_other: date | None
            my_nonnull_date: date

        fields_dict = attr.fields_dict(MyAttrClass)

        my_opt_date_attribute = fields_dict["my_opt_date"]
        my_opt_date_other_attribute = fields_dict["my_opt_date_other"]
        my_nonnull_date_attribute = fields_dict["my_nonnull_date"]

        self.assertTrue(is_date(my_opt_date_attribute))
        self.assertTrue(is_date(my_opt_date_other_attribute))
        self.assertTrue(is_date(my_nonnull_date_attribute))

        self.assertTrue(is_optional_type(non_optional(my_opt_date_attribute.type)))
        self.assertTrue(
            is_optional_type(non_optional(my_opt_date_other_attribute.type))
        )
        self.assertFalse(is_optional_type(non_optional(my_nonnull_date_attribute.type)))

        self.assertEqual(
            date, get_non_optional_type(non_optional(my_opt_date_attribute.type))
        )
        self.assertEqual(
            date, get_non_optional_type(non_optional(my_opt_date_other_attribute.type))
        )

        for attribute in fields_dict.values():
            self._check_no_utils_crash(attribute)

    def test_datetime_fields(self) -> None:
        @attr.define
        class MyAttrClass:
            my_opt_datetime: Optional[datetime]
            my_opt_datetime_other: datetime | None
            my_nonnull_datetime: datetime

        fields_dict = attr.fields_dict(MyAttrClass)

        my_opt_datetime_attribute = fields_dict["my_opt_datetime"]
        my_opt_datetime_other_attribute = fields_dict["my_opt_datetime_other"]
        my_nonnull_datetime_attribute = fields_dict["my_nonnull_datetime"]

        self.assertTrue(is_datetime(my_opt_datetime_attribute))
        self.assertTrue(is_datetime(my_opt_datetime_other_attribute))
        self.assertTrue(is_datetime(my_nonnull_datetime_attribute))

        self.assertTrue(is_optional_type(non_optional(my_opt_datetime_attribute.type)))
        self.assertTrue(
            is_optional_type(non_optional(my_opt_datetime_other_attribute.type))
        )
        self.assertFalse(
            is_optional_type(non_optional(my_nonnull_datetime_attribute.type))
        )

        self.assertEqual(
            datetime,
            get_non_optional_type(non_optional(my_opt_datetime_attribute.type)),
        )
        self.assertEqual(
            datetime,
            get_non_optional_type(non_optional(my_opt_datetime_other_attribute.type)),
        )

        for attribute in fields_dict.values():
            self._check_no_utils_crash(attribute)

    def test_forward_ref_fields(self) -> None:
        @attr.define
        class MyAttrClass:
            my_opt_forward_ref: Optional["AnotherClass"]
            # Note: As of Python 3.11, this syntax is not supported:
            # my_opt_datetime_other: "AnotherClass" | None
            my_nonnull_forward_ref: "AnotherClass"

        @attr.define
        class AnotherClass:
            pass

        fields_dict = attr.fields_dict(MyAttrClass)

        my_opt_forward_ref_attribute = fields_dict["my_opt_forward_ref"]
        my_nonnull_forward_ref_attribute = fields_dict["my_nonnull_forward_ref"]

        self.assertTrue(is_forward_ref(my_opt_forward_ref_attribute))
        self.assertTrue(is_forward_ref(my_nonnull_forward_ref_attribute))

        self.assertTrue(
            is_optional_type(non_optional(my_opt_forward_ref_attribute.type))
        )
        self.assertFalse(
            is_optional_type(non_optional(my_nonnull_forward_ref_attribute.type))
        )

        self.assertEqual(
            ForwardRef("AnotherClass"),
            get_non_optional_type(non_optional(my_opt_forward_ref_attribute.type)),
        )
        self.assertEqual(
            "AnotherClass",
            get_non_optional_type(non_optional(my_nonnull_forward_ref_attribute.type)),
        )

        self.assertEqual(
            "AnotherClass",
            get_non_flat_attribute_class_name(my_opt_forward_ref_attribute),
        )
        self.assertEqual(
            "AnotherClass",
            get_non_flat_attribute_class_name(my_nonnull_forward_ref_attribute),
        )

        for attribute in fields_dict.values():
            self._check_no_utils_crash(attribute)

    def test_list_fields(self) -> None:
        @attr.define
        class ChildClass:
            pass

        @attr.define
        class MyAttrClass:
            my_opt_list: Optional[List["AnotherChildClass"]]
            my_opt_list_other: Optional[List[ChildClass]]
            my_nonnull_list: List["AnotherChildClass"]
            my_nonnull_list_other: List[ChildClass]

        @attr.define
        class AnotherChildClass:
            pass

        fields_dict = attr.fields_dict(MyAttrClass)

        my_opt_list_attribute = fields_dict["my_opt_list"]
        my_opt_list_other_attribute = fields_dict["my_opt_list_other"]
        my_nonnull_list_attribute = fields_dict["my_nonnull_list"]
        my_nonnull_list_attribute_other = fields_dict["my_nonnull_list_other"]

        self.assertTrue(is_list(my_opt_list_attribute))
        self.assertTrue(is_list(my_opt_list_other_attribute))
        self.assertTrue(is_list(my_nonnull_list_attribute))
        self.assertTrue(is_list(my_nonnull_list_attribute_other))

        self.assertTrue(is_optional_type(non_optional(my_opt_list_attribute.type)))
        self.assertTrue(
            is_optional_type(non_optional(my_opt_list_other_attribute.type))
        )
        self.assertFalse(is_optional_type(non_optional(my_nonnull_list_attribute.type)))
        self.assertFalse(
            is_optional_type(non_optional(my_nonnull_list_attribute_other.type))
        )

        self.assertEqual(
            List[ForwardRef("AnotherChildClass")],  # type: ignore[misc]
            get_non_optional_type(non_optional(my_opt_list_attribute.type)),
        )
        self.assertEqual(
            List[ChildClass],
            get_non_optional_type(non_optional(my_opt_list_other_attribute.type)),
        )
        self.assertEqual(
            List[ForwardRef("AnotherChildClass")],  # type: ignore[misc]
            get_non_optional_type(non_optional(my_nonnull_list_attribute.type)),
        )
        self.assertEqual(
            List[ChildClass],
            get_non_optional_type(non_optional(my_nonnull_list_attribute_other.type)),
        )

        for attribute in fields_dict.values():
            self._check_no_utils_crash(attribute)
