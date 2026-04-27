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
"""Tests for recidiviz/common/demographics.py and its backwards-compatible aliases."""
import unittest

import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum
from recidiviz.common.constants.state.state_person import (
    StateEthnicity,
    StateGender,
    StateRace,
    StateSex,
)
from recidiviz.common.demographics import Ethnicity, Gender, Race, Sex
from recidiviz.common.entity_enum import EntityEnum


class TestDemographicsAliases(unittest.TestCase):
    """Verifies that State* names in state_person.py are true aliases of the
    canonical classes in demographics.py, not independent redefinitions."""

    def test_state_entity_enum_is_entity_enum(self) -> None:
        self.assertIs(StateEntityEnum, EntityEnum)

    def test_state_ethnicity_is_ethnicity(self) -> None:
        self.assertIs(StateEthnicity, Ethnicity)

    def test_state_gender_is_gender(self) -> None:
        self.assertIs(StateGender, Gender)

    def test_state_race_is_race(self) -> None:
        self.assertIs(StateRace, Race)

    def test_state_sex_is_sex(self) -> None:
        self.assertIs(StateSex, Sex)

    def test_enum_members_are_identical(self) -> None:
        self.assertIs(Ethnicity.HISPANIC, StateEthnicity.HISPANIC)
        self.assertIs(Gender.MALE, StateGender.MALE)
        self.assertIs(Race.BLACK, StateRace.BLACK)
        self.assertIs(Sex.FEMALE, StateSex.FEMALE)

    def test_isinstance_works_across_aliases(self) -> None:
        self.assertIsInstance(Ethnicity.HISPANIC, StateEthnicity)
        self.assertIsInstance(Gender.MALE, StateGender)
        self.assertIsInstance(Race.BLACK, StateRace)
        self.assertIsInstance(Sex.FEMALE, StateSex)
        self.assertIsInstance(Gender.MALE, EntityEnum)
        self.assertIsInstance(Gender.MALE, StateEntityEnum)


class TestDemographicsStringValues(unittest.TestCase):
    """Verifies that canonical string values are consistent between
    demographics_strings.py and their re-exports in enum_canonical_strings.py."""

    def test_ethnicity_string_values(self) -> None:
        self.assertEqual(Ethnicity.HISPANIC.value, "HISPANIC")
        self.assertEqual(Ethnicity.NOT_HISPANIC.value, "NOT_HISPANIC")

    def test_gender_string_values(self) -> None:
        self.assertEqual(Gender.FEMALE.value, "FEMALE")
        self.assertEqual(Gender.MALE.value, "MALE")
        self.assertEqual(Gender.NON_BINARY.value, "NON_BINARY")

    def test_race_string_values(self) -> None:
        self.assertEqual(Race.BLACK.value, "BLACK")
        self.assertEqual(Race.WHITE.value, "WHITE")
        self.assertEqual(
            Race.AMERICAN_INDIAN_ALASKAN_NATIVE.value, "AMERICAN_INDIAN_ALASKAN_NATIVE"
        )

    def test_sex_string_values(self) -> None:
        self.assertEqual(Sex.FEMALE.value, "FEMALE")
        self.assertEqual(Sex.MALE.value, "MALE")
        self.assertEqual(Sex.OTHER.value, "OTHER")

    def test_enum_canonical_strings_reexports_match(self) -> None:
        self.assertEqual(
            state_enum_strings.state_ethnicity_hispanic, Ethnicity.HISPANIC.value
        )
        self.assertEqual(state_enum_strings.state_gender_female, Gender.FEMALE.value)
        self.assertEqual(state_enum_strings.state_gender_male, Gender.MALE.value)
        self.assertEqual(state_enum_strings.state_race_black, Race.BLACK.value)
        self.assertEqual(state_enum_strings.state_sex_female, Sex.FEMALE.value)
