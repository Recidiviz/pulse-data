# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for column_types.py."""
import enum
import unittest

from sqlalchemy import Column, Enum

from recidiviz.persistence.database.column_types import StringBackedEnum


class _Color(enum.Enum):
    RED = "RED"
    GREEN = "GREEN"
    BLUE = "BLUE"


class StringBackedEnumTest(unittest.TestCase):
    """Tests for StringBackedEnum."""

    def test_returns_plain_sqlalchemy_enum(self) -> None:
        column_type = StringBackedEnum(_Color)
        self.assertIs(type(column_type), Enum)

    def test_is_string_backed_not_native(self) -> None:
        column_type = StringBackedEnum(_Color)
        self.assertFalse(column_type.native_enum)
        self.assertFalse(column_type.create_constraint)

    def test_captures_enum_values(self) -> None:
        column_type = StringBackedEnum(_Color)
        self.assertEqual(["RED", "GREEN", "BLUE"], column_type.enums)
        self.assertIs(_Color, column_type.enum_class)

    def test_has_fixed_default_length(self) -> None:
        # A fixed width means the VARCHAR does not depend on the enum's values, so
        # adding or removing values never requires a migration.
        self.assertEqual(255, StringBackedEnum(_Color).length)
        self.assertEqual(64, StringBackedEnum(_Color, length=64).length)

    def test_defaults_can_be_overridden(self) -> None:
        column_type = StringBackedEnum(_Color, create_constraint=True)
        self.assertTrue(column_type.create_constraint)

    def test_extra_kwargs_pass_through(self) -> None:
        column_type = StringBackedEnum(_Color, name="color")
        self.assertEqual("color", column_type.name)

    def test_usable_as_column_type(self) -> None:
        column = Column("color", StringBackedEnum(_Color))
        self.assertIs(type(column.type), Enum)
        self.assertFalse(column.type.native_enum)
