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
"""Tests for BuildableAttr base class."""

import unittest
from typing import Optional

from recidiviz.common.constants.entity_enum import EntityEnum, EnumParsingError
from recidiviz.common.constants.enum_overrides import EnumOverrides


class FakeEntityEnum(EntityEnum):
    BANANA = "BANANA"
    STRAWBERRY = "STRAWBERRY"
    PASSION_FRUIT = "PASSION FRUIT"

    @staticmethod
    def _get_default_map():
        return {
            "BANANA": FakeEntityEnum.BANANA,
            "STRAWBERRY": FakeEntityEnum.STRAWBERRY,
            "PASSION FRUIT": FakeEntityEnum.PASSION_FRUIT,
        }


class EntityEnumTest(unittest.TestCase):
    """Tests for EntityEnum class."""

    def testParse_InvalidString_throwsEnumParsingError(self):
        with self.assertRaises(EnumParsingError):
            FakeEntityEnum.parse("invalid", EnumOverrides.empty())

    def testParse_NoOverrides_UsesDefaultMap(self):
        self.assertEqual(
            FakeEntityEnum.parse("banana", EnumOverrides.empty()), FakeEntityEnum.BANANA
        )

    def testParse_WithNoneOverride_IgnoresDefaultMap(self):
        overrides_builder = EnumOverrides.Builder()
        overrides_builder.ignore("BANANA", FakeEntityEnum)
        overrides = overrides_builder.build()
        self.assertEqual(FakeEntityEnum.parse("banana", overrides), None)

    def testParse_WithOverrides_UsesOverrides(self):
        overrides_builder = EnumOverrides.Builder()
        overrides_builder.add("BAN", FakeEntityEnum.BANANA)
        overrides = overrides_builder.build()
        self.assertEqual(FakeEntityEnum.parse("ban", overrides), FakeEntityEnum.BANANA)

    def testParse_WithPunctuation(self):
        self.assertEqual(
            FakeEntityEnum.parse('"PASSION"-FRUIT.', EnumOverrides.empty()),
            FakeEntityEnum.PASSION_FRUIT,
        )

    def testCanParse_Invalid(self):
        self.assertFalse(FakeEntityEnum.can_parse("invalid", EnumOverrides.empty()))

    def testCanParse_Valid(self):
        self.assertTrue(FakeEntityEnum.can_parse("banana", EnumOverrides.empty()))

    def testCanParse_WithNoneOverride(self):
        overrides_builder = EnumOverrides.Builder()
        overrides_builder.ignore("BANANA", FakeEntityEnum)
        overrides = overrides_builder.build()
        self.assertTrue(FakeEntityEnum.can_parse("banana", overrides))

    def testCanParse_WithOverrides(self):
        overrides_builder = EnumOverrides.Builder()
        overrides_builder.ignore("BAN", FakeEntityEnum)
        overrides = overrides_builder.build()
        self.assertTrue(FakeEntityEnum.can_parse("ban", overrides))

    def testParseFromCanonicalString_Valid(self):
        self.assertEqual(
            FakeEntityEnum.parse_from_canonical_string(FakeEntityEnum.BANANA.value),
            FakeEntityEnum.BANANA,
        )

    def testParseFromCanonicalString_Valid_ValueDifferent(self):
        self.assertEqual(
            FakeEntityEnum.parse_from_canonical_string(
                FakeEntityEnum.PASSION_FRUIT.value
            ),
            FakeEntityEnum.PASSION_FRUIT,
        )

    def testParseFromCanonicalString_None(self):
        self.assertEqual(FakeEntityEnum.parse_from_canonical_string(None), None)

    def testParseFromCanonicalString_Invalid(self):
        with self.assertRaises(EnumParsingError):
            FakeEntityEnum.parse_from_canonical_string("invalid")

    def testParseFromCanonicalString_Lowercase(self):
        with self.assertRaises(EnumParsingError):
            FakeEntityEnum.parse_from_canonical_string("strawberry")

    def testMapperErrorsCaughtAndThrownAsEntityMatchingError(self):
        def very_bad_mapper_that_asserts(_raw_text: str) -> Optional[FakeEntityEnum]:
            raise ValueError("Something bad happened!")

        overrides_builder = EnumOverrides.Builder()
        overrides_builder.add_mapper(very_bad_mapper_that_asserts, FakeEntityEnum)

        overrides = overrides_builder.build()

        with self.assertRaises(EnumParsingError):
            FakeEntityEnum.parse("A STRING TO PARSE", overrides)
