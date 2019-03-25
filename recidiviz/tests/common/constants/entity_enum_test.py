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

from recidiviz.common.constants.entity_enum import EntityEnum, EnumParsingError
from recidiviz.common.constants.enum_overrides import EnumOverrides


class FakeEntityEnum(EntityEnum):
    BANANA = 'banana'
    STRAWBERRY = 'strawberry'
    PASSION_FRUIT = 'passion fruit'

    @staticmethod
    def _get_default_map():
        return {'BANANA': FakeEntityEnum.BANANA,
                'STRAWBERRY': FakeEntityEnum.STRAWBERRY,
                'PASSION FRUIT': FakeEntityEnum.PASSION_FRUIT}


class EntityEnumTest(unittest.TestCase):
    """Tests for EntityEnum class."""

    def testParse_InvalidString_throwsEnumParsingError(self):
        with self.assertRaises(EnumParsingError):
            FakeEntityEnum.parse('invalid', EnumOverrides.empty())

    def testParse_NoOverrides_UsesDefaultMap(self):
        self.assertEqual(FakeEntityEnum.parse('banana', EnumOverrides.empty()),
                         FakeEntityEnum.BANANA)

    def testParse_WithNoneOverride_IgnoresDefaultMap(self):
        overrides_builder = EnumOverrides.Builder()
        overrides_builder.ignore('BANANA', FakeEntityEnum)
        overrides = overrides_builder.build()
        self.assertEqual(FakeEntityEnum.parse('banana', overrides), None)

    def testParse_WithOverrides_UsesOverrides(self):
        overrides_builder = EnumOverrides.Builder()
        overrides_builder.add('BAN', FakeEntityEnum.BANANA)
        overrides = overrides_builder.build()
        self.assertEqual(FakeEntityEnum.parse('ban', overrides),
                         FakeEntityEnum.BANANA)

    def testParse_WithPunctuation(self):
        self.assertEqual(FakeEntityEnum.parse('"PASSION"-FRUIT.',
                                              EnumOverrides.empty()),
                         FakeEntityEnum.PASSION_FRUIT)

    def testCanParse_Invalid(self):
        self.assertFalse(
            FakeEntityEnum.can_parse('invalid', EnumOverrides.empty()))

    def testCanParse_Valid(self):
        self.assertTrue(
            FakeEntityEnum.can_parse('banana', EnumOverrides.empty()))

    def testCanParse_WithNoneOverride(self):
        overrides_builder = EnumOverrides.Builder()
        overrides_builder.ignore('BANANA', FakeEntityEnum)
        overrides = overrides_builder.build()
        self.assertTrue(FakeEntityEnum.can_parse('banana', overrides))

    def testCanParse_WithOverrides(self):
        overrides_builder = EnumOverrides.Builder()
        overrides_builder.ignore('BAN', FakeEntityEnum)
        overrides = overrides_builder.build()
        self.assertTrue(FakeEntityEnum.can_parse('ban', overrides))
