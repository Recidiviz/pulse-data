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

from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.constants.state.state_charge import (
    StateChargeClassificationType,
    StateChargeStatus,
)
from recidiviz.common.constants.state.state_drug_screen import (
    StateDrugScreenResult,
    StateDrugScreenSampleType,
)
from recidiviz.common.constants.state.state_person import StateEthnicity, StateRace


class EnumOverridesTest(unittest.TestCase):
    """Tests for EnumOverrides class."""

    def test_add(self) -> None:
        overrides_builder = EnumOverrides.Builder()
        overrides_builder.add("A", StateRace.ASIAN)
        overrides_builder.add("A", StateChargeStatus.PENDING)
        overrides_builder.add("b", StateRace.BLACK, normalize_label=False)
        overrides_builder.add("h", StateEthnicity.HISPANIC)

        overrides = overrides_builder.build()

        self.assertIsNone(overrides.parse("A", StateChargeClassificationType))

        self.assertEqual(overrides.parse("A", StateRace), StateRace.ASIAN)
        self.assertIsNone(overrides.parse("a", StateRace))

        self.assertEqual(
            overrides.parse("A", StateChargeStatus), StateChargeStatus.PENDING
        )
        self.assertIsNone(overrides.parse("a", StateChargeStatus))

        self.assertIsNone(overrides.parse("B", StateRace))
        self.assertEqual(overrides.parse("b", StateRace), StateRace.BLACK)

        self.assertEqual(overrides.parse("H", StateEthnicity), StateEthnicity.HISPANIC)
        self.assertIsNone(overrides.parse("h", StateEthnicity))

    def test_add_fromDifferentEnum(self) -> None:
        overrides_builder = EnumOverrides.Builder()

        overrides = overrides_builder.build()

        self.assertIsNone(overrides.parse("LATINO", StateEthnicity))

    def test_add_mapper_fn(self) -> None:
        is_pending = (
            # pylint: disable=unnecessary-lambda-assignment
            lambda s: StateDrugScreenResult.POSITIVE
            if s.startswith("POSITIVE")
            else None
        )

        overrides_builder = EnumOverrides.Builder()
        overrides_builder.add_mapper_fn(is_pending, StateDrugScreenResult)

        overrides = overrides_builder.build()

        self.assertIsNone(overrides.parse("POSIT", StateDrugScreenResult))
        self.assertEqual(
            overrides.parse("POSITIVE", StateDrugScreenResult),
            StateDrugScreenResult.POSITIVE,
        )
        self.assertEqual(
            overrides.parse("POSITIVE - MARIJUANA", StateDrugScreenResult),
            StateDrugScreenResult.POSITIVE,
        )

    def test_ignore(self) -> None:
        overrides_builder = EnumOverrides.Builder()
        overrides_builder.ignore("A", StateChargeClassificationType)
        overrides_builder.ignore("u", StateEthnicity, normalize_label=False)
        overrides_builder.ignore("x", StateEthnicity)

        overrides = overrides_builder.build()

        self.assertTrue(overrides.should_ignore("A", StateChargeClassificationType))
        self.assertFalse(overrides.should_ignore("A", StateDrugScreenSampleType))

        self.assertFalse(overrides.should_ignore("a", StateChargeClassificationType))
        self.assertFalse(overrides.should_ignore("a", StateDrugScreenSampleType))

        self.assertFalse(overrides.should_ignore("U", StateEthnicity))
        self.assertTrue(overrides.should_ignore("u", StateEthnicity))

        self.assertTrue(overrides.should_ignore("X", StateEthnicity))
        self.assertFalse(overrides.should_ignore("x", StateEthnicity))

    def test_parse_ignored(self) -> None:
        overrides_builder = EnumOverrides.Builder()
        overrides_builder.add("U", StateEthnicity.EXTERNAL_UNKNOWN)
        overrides_builder.ignore("u", StateEthnicity, normalize_label=False)
        overrides_builder.ignore("x", StateEthnicity)

        overrides = overrides_builder.build()

        self.assertEqual(
            overrides.parse("U", StateEthnicity), StateEthnicity.EXTERNAL_UNKNOWN
        )
        self.assertIsNone(overrides.parse("u", StateEthnicity))
        self.assertIsNone(overrides.parse("x", StateEthnicity))
        self.assertIsNone(overrides.parse("X", StateEthnicity))

    def test_ignoreWithPredicate(self) -> None:
        overrides_builder = EnumOverrides.Builder()
        overrides_builder.ignore_with_predicate(
            lambda s: s.startswith("NO"), StateChargeClassificationType
        )

        overrides = overrides_builder.build()

        self.assertTrue(overrides.should_ignore("NONE", StateChargeClassificationType))
        self.assertFalse(overrides.should_ignore("None", StateChargeClassificationType))

    def test_double_add_fails(self) -> None:
        overrides_builder = EnumOverrides.Builder()
        overrides_builder.add("A", StateChargeStatus.PENDING)
        with self.assertRaises(ValueError):
            overrides_builder.add("A", StateChargeStatus.CONVICTED)

    def test_overwrite_double_add_succeeds(self) -> None:
        overrides_builder = EnumOverrides.Builder()
        overrides_builder.add("A", StateChargeStatus.PENDING)
        overrides_builder.add("A", StateChargeStatus.CONVICTED, force_overwrite=True)

    def test_double_add_different_case(self) -> None:
        overrides_builder = EnumOverrides.Builder()
        overrides_builder.add("A", StateChargeStatus.PENDING)
        overrides_builder.add("a", StateChargeStatus.PENDING, normalize_label=False)

        overrides = overrides_builder.build()

        self.assertEqual(
            overrides.parse("A", StateChargeStatus),
            overrides.parse("a", StateChargeStatus),
        )
