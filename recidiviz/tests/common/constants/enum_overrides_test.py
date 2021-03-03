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
"""Tests for EnumOverrides class."""

import unittest

from recidiviz.common.constants.bond import BondStatus, BondType
from recidiviz.common.constants.county.charge import ChargeClass, ChargeDegree
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.constants.person_characteristics import Race, Ethnicity


class EnumOverridesTest(unittest.TestCase):
    """Tests for EnumOverrides class."""

    def test_add(self):
        overrides_builder = EnumOverrides.Builder()
        overrides_builder.add("A", Race.ASIAN)
        overrides_builder.add("A", ChargeDegree.FIRST)

        overrides = overrides_builder.build()

        self.assertIsNone(overrides.parse("A", ChargeClass))
        self.assertEqual(overrides.parse("A", Race), Race.ASIAN)
        self.assertEqual(overrides.parse("A", ChargeDegree), ChargeDegree.FIRST)

    def test_add_fromDifferentEnum(self):
        overrides_builder = EnumOverrides.Builder()

        overrides = overrides_builder.build()

        self.assertIsNone(overrides.parse("LATINO", Ethnicity))

    def test_add_mapper(self):
        is_pending = lambda s: BondStatus.PENDING if s.startswith("PENDING") else None

        overrides_builder = EnumOverrides.Builder()
        overrides_builder.add_mapper(is_pending, BondStatus)

        overrides = overrides_builder.build()

        self.assertIsNone(overrides.parse("PEND", BondStatus))
        self.assertEqual(overrides.parse("PENDING", BondStatus), BondStatus.PENDING)
        self.assertEqual(
            overrides.parse("PENDING - WAITING TO SEE MAGISTRATE", BondStatus),
            BondStatus.PENDING,
        )

    def test_ignore(self):
        overrides_builder = EnumOverrides.Builder()
        overrides_builder.ignore("A", ChargeClass)

        overrides = overrides_builder.build()

        self.assertTrue(overrides.should_ignore("A", ChargeClass))
        self.assertFalse(overrides.should_ignore("A", BondType))

    def test_ignoreWithPredicate(self):
        overrides_builder = EnumOverrides.Builder()
        overrides_builder.ignore_with_predicate(
            lambda s: s.startswith("NO"), ChargeClass
        )

        overrides = overrides_builder.build()

        self.assertTrue(overrides.should_ignore("NONE", ChargeClass))
