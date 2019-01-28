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

from recidiviz.common.constants.mappable_enum import MappableEnum, \
    EnumParsingError


class FakeMappableEnum(MappableEnum):
    BANANA = 'banana'
    STRAWBERRY = 'strawberry'
    PASSION_FRUIT = 'passion fruit'

    @staticmethod
    def _get_default_map():
        return {'BANANA': FakeMappableEnum.BANANA,
                'STRAWBERRY': FakeMappableEnum.STRAWBERRY,
                'PASSION FRUIT': FakeMappableEnum.PASSION_FRUIT}


class MappableEnumTest(unittest.TestCase):
    """Tests for MappableEnum class."""

    def testFromStr_InvalidString_throwsEnumParsingError(self):
        with self.assertRaises(EnumParsingError):
            FakeMappableEnum.parse('invalid', {})

    def testFromStr_NoOverrides_UsesDefaultMap(self):
        self.assertEqual(FakeMappableEnum.parse('banana', {}),
                         FakeMappableEnum.BANANA)

    def testFromStr_WithNoneOverride_IgnoresDefaultMap(self):
        overrides = {'BANANA': None}
        self.assertEqual(FakeMappableEnum.parse('banana', overrides), None)

    def testFromStr_WithOverrides_UsesOverrides(self):
        overrides = {'BAN': FakeMappableEnum.BANANA}
        self.assertEqual(FakeMappableEnum.parse('ban', overrides),
                         FakeMappableEnum.BANANA)

    def testParses_Invalid(self):
        self.assertFalse(FakeMappableEnum.can_parse('invalid', {}))

    def testParses_Valid(self):
        self.assertTrue(FakeMappableEnum.can_parse('banana', {}))

    def testParses_WithNoneOverride(self):
        overrides = {'BANANA': None}
        self.assertTrue(FakeMappableEnum.can_parse('banana', overrides))

    def testFromStr_WithOverrides(self):
        overrides = {'BAN': FakeMappableEnum.BANANA}
        self.assertTrue(FakeMappableEnum.can_parse('ban', overrides))
