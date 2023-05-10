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
from enum import Enum
from typing import Optional

import attr

from recidiviz.common.attr_utils import get_enum_cls, is_enum, is_non_optional_enum


class FakeEnum(Enum):
    A = "A"
    B = "B"


class AttrUtilsTest(unittest.TestCase):
    """Tests for attr_utils.py."""

    def test_enum_fields(self) -> None:
        @attr.define
        class MyAttrClass:
            my_enum: Optional[FakeEnum]
            my_nonnull_enum: FakeEnum

        fields_dict = attr.fields_dict(MyAttrClass)

        my_enum_attribute = fields_dict["my_enum"]
        my_nonnull_enum_attribute = fields_dict["my_nonnull_enum"]

        self.assertTrue(is_enum(my_enum_attribute))
        self.assertTrue(is_enum(my_nonnull_enum_attribute))

        self.assertFalse(is_non_optional_enum(my_enum_attribute))
        self.assertTrue(is_non_optional_enum(my_nonnull_enum_attribute))

        self.assertEqual(FakeEnum, get_enum_cls(my_enum_attribute))
        self.assertEqual(FakeEnum, get_enum_cls(my_nonnull_enum_attribute))
