# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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
    """Tests for MappableEnum class."""

    def testParse_InvalidString_throwsEnumParsingError(self):
        with self.assertRaises(EnumParsingError):
            FakeEntityEnum.parse('invalid', {})

    def testParse_NoOverrides_UsesDefaultMap(self):
        self.assertEqual(FakeEntityEnum.parse('banana', {}),
                         FakeEntityEnum.BANANA)

    def testParse_WithNoneOverride_IgnoresDefaultMap(self):
        overrides = {'BANANA': None}
        self.assertEqual(FakeEntityEnum.parse('banana', overrides), None)

    def testParse_WithOverrides_UsesOverrides(self):
        overrides = {'BAN': FakeEntityEnum.BANANA}
        self.assertEqual(FakeEntityEnum.parse('ban', overrides),
                         FakeEntityEnum.BANANA)

    def testParse_WithPunctuation(self):
        self.assertEqual(FakeEntityEnum.parse('"PASSION"-FRUIT.', {}),
                         FakeEntityEnum.PASSION_FRUIT)

    def testParses_Invalid(self):
        self.assertFalse(FakeEntityEnum.can_parse('invalid', {}))

    def testParses_Valid(self):
        self.assertTrue(FakeEntityEnum.can_parse('banana', {}))

    def testParses_WithNoneOverride(self):
        overrides = {'BANANA': None}
        self.assertTrue(FakeEntityEnum.can_parse('banana', overrides))

    def testParses_WithOverrides(self):
        overrides = {'BAN': FakeEntityEnum.BANANA}
        self.assertTrue(FakeEntityEnum.can_parse('ban', overrides))
