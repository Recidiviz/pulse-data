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
import datetime
import enum
import unittest

from sqlalchemy import Column, DateTime, Enum
from sqlalchemy.engine.default import DefaultDialect

from recidiviz.persistence.database.column_types import StringBackedEnum, UTCDateTime


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


_DIALECT = DefaultDialect()
UTC_DATETIME = datetime.datetime(2026, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
NAIVE_DATETIME = datetime.datetime(2026, 1, 1, 12, 0, 0)


class UTCDateTimeTest(unittest.TestCase):
    """Tests for UTCDateTime."""

    def test_result_value_attaches_utc(self) -> None:
        # A naive value read from the DB (stored as UTC wall-clock) comes back
        # UTC-aware.
        result = UTCDateTime().process_result_value(NAIVE_DATETIME, dialect=_DIALECT)
        self.assertEqual(
            UTC_DATETIME,
            result,
        )
        assert result is not None
        self.assertIs(datetime.timezone.utc, result.tzinfo)

    def test_result_value_none_passes_through(self) -> None:
        self.assertIsNone(UTCDateTime().process_result_value(None, dialect=_DIALECT))

    def test_bind_param_strips_utc_aware_to_naive(self) -> None:
        result = UTCDateTime().process_bind_param(UTC_DATETIME, dialect=_DIALECT)
        self.assertEqual(NAIVE_DATETIME, result)
        assert result is not None
        self.assertIsNone(result.tzinfo)

    def test_bind_param_converts_non_utc_to_utc(self) -> None:
        # A non-UTC aware value is shifted to UTC before its tzinfo is dropped.
        plus_five = datetime.timezone(datetime.timedelta(hours=5))
        result = UTCDateTime().process_bind_param(
            datetime.datetime(2026, 1, 1, 12, 0, 0, tzinfo=plus_five),
            dialect=_DIALECT,
        )
        self.assertEqual(datetime.datetime(2026, 1, 1, 7, 0, 0), result)
        assert result is not None
        self.assertIsNone(result.tzinfo)

    def test_bind_param_rejects_naive(self) -> None:
        # A naive value on write is a bug: the app layer must hand over UTC-aware
        # datetimes so the stored value's zone is unambiguous.
        with self.assertRaisesRegex(ValueError, r"requires a timezone-aware datetime"):
            UTCDateTime().process_bind_param(NAIVE_DATETIME, dialect=_DIALECT)

    def test_bind_param_none_passes_through(self) -> None:
        self.assertIsNone(UTCDateTime().process_bind_param(None, dialect=_DIALECT))

    def test_roundtrip_preserves_utc_value(self) -> None:
        original = UTC_DATETIME
        column_type = UTCDateTime()
        stored = column_type.process_bind_param(original, dialect=_DIALECT)
        loaded = column_type.process_result_value(stored, dialect=_DIALECT)
        self.assertEqual(original, loaded)

    def test_impl_is_naive_datetime(self) -> None:
        # impl is a plain DateTime, so the column's DDL -- and therefore generated
        # migrations -- is identical to a vanilla DateTime column.
        self.assertIs(DateTime, UTCDateTime.impl)

    def test_usable_as_column_type(self) -> None:
        column = Column("ts", UTCDateTime())
        self.assertIsInstance(column.type, UTCDateTime)
