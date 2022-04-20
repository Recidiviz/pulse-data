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

from recidiviz.common.constants.county.bond import BondStatus, BondType
from recidiviz.common.constants.county.charge import ChargeClass, ChargeDegree
from recidiviz.common.constants.county.person_characteristics import Ethnicity, Race
from recidiviz.common.constants.enum_overrides import EnumOverrides


class EnumOverridesTest(unittest.TestCase):
    """Tests for EnumOverrides class."""

    def test_add(self) -> None:
        overrides_builder = EnumOverrides.Builder()
        overrides_builder.add("A", Race.ASIAN)
        overrides_builder.add("A", ChargeDegree.FIRST)
        overrides_builder.add("b", Race.BLACK, normalize_label=False)
        overrides_builder.add("h", Ethnicity.HISPANIC)

        overrides = overrides_builder.build()

        self.assertIsNone(overrides.parse("A", ChargeClass))

        self.assertEqual(overrides.parse("A", Race), Race.ASIAN)
        self.assertIsNone(overrides.parse("a", Race))

        self.assertEqual(overrides.parse("A", ChargeDegree), ChargeDegree.FIRST)
        self.assertIsNone(overrides.parse("a", ChargeDegree))

        self.assertIsNone(overrides.parse("B", Race))
        self.assertEqual(overrides.parse("b", Race), Race.BLACK)

        self.assertEqual(overrides.parse("H", Ethnicity), Ethnicity.HISPANIC)
        self.assertIsNone(overrides.parse("h", Ethnicity))

    def test_add_fromDifferentEnum(self) -> None:
        overrides_builder = EnumOverrides.Builder()

        overrides = overrides_builder.build()

        self.assertIsNone(overrides.parse("LATINO", Ethnicity))

    def test_add_mapper_fn(self) -> None:
        is_pending = lambda s: BondStatus.PENDING if s.startswith("PENDING") else None

        overrides_builder = EnumOverrides.Builder()
        overrides_builder.add_mapper_fn(is_pending, BondStatus)

        overrides = overrides_builder.build()

        self.assertIsNone(overrides.parse("PEND", BondStatus))
        self.assertEqual(overrides.parse("PENDING", BondStatus), BondStatus.PENDING)
        self.assertEqual(
            overrides.parse("PENDING - WAITING TO SEE MAGISTRATE", BondStatus),
            BondStatus.PENDING,
        )

    def test_ignore(self) -> None:
        overrides_builder = EnumOverrides.Builder()
        overrides_builder.ignore("A", ChargeClass)
        overrides_builder.ignore("u", Ethnicity, normalize_label=False)
        overrides_builder.ignore("x", Ethnicity)

        overrides = overrides_builder.build()

        self.assertTrue(overrides.should_ignore("A", ChargeClass))
        self.assertFalse(overrides.should_ignore("A", BondType))

        self.assertFalse(overrides.should_ignore("a", ChargeClass))
        self.assertFalse(overrides.should_ignore("a", BondType))

        self.assertFalse(overrides.should_ignore("U", Ethnicity))
        self.assertTrue(overrides.should_ignore("u", Ethnicity))

        self.assertTrue(overrides.should_ignore("X", Ethnicity))
        self.assertFalse(overrides.should_ignore("x", Ethnicity))

    def test_parse_ignored(self) -> None:
        overrides_builder = EnumOverrides.Builder()
        overrides_builder.add("U", Ethnicity.EXTERNAL_UNKNOWN)
        overrides_builder.ignore("u", Ethnicity, normalize_label=False)
        overrides_builder.ignore("x", Ethnicity)

        overrides = overrides_builder.build()

        self.assertEqual(overrides.parse("U", Ethnicity), Ethnicity.EXTERNAL_UNKNOWN)
        self.assertIsNone(overrides.parse("u", Ethnicity))
        self.assertIsNone(overrides.parse("x", Ethnicity))
        self.assertIsNone(overrides.parse("X", Ethnicity))

    def test_ignoreWithPredicate(self) -> None:
        overrides_builder = EnumOverrides.Builder()
        overrides_builder.ignore_with_predicate(
            lambda s: s.startswith("NO"), ChargeClass
        )

        overrides = overrides_builder.build()

        self.assertTrue(overrides.should_ignore("NONE", ChargeClass))
        self.assertFalse(overrides.should_ignore("None", ChargeClass))

    def test_double_add_fails(self) -> None:
        overrides_builder = EnumOverrides.Builder()
        overrides_builder.add("A", ChargeDegree.FIRST)
        with self.assertRaises(ValueError):
            overrides_builder.add("A", ChargeDegree.SECOND)

    def test_overwrite_double_add_succeeds(self) -> None:
        overrides_builder = EnumOverrides.Builder()
        overrides_builder.add("A", ChargeDegree.FIRST)
        overrides_builder.add("A", ChargeDegree.SECOND, force_overwrite=True)

    def test_double_add_different_case(self) -> None:
        overrides_builder = EnumOverrides.Builder()
        overrides_builder.add("A", ChargeDegree.FIRST)
        overrides_builder.add("a", ChargeDegree.FIRST, normalize_label=False)

        overrides = overrides_builder.build()

        self.assertEqual(
            overrides.parse("A", ChargeDegree), overrides.parse("a", ChargeDegree)
        )
