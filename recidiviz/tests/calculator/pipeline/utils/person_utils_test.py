# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Tests the functions in the person_utils file."""
import unittest
from datetime import date

import pytest

from recidiviz.calculator.pipeline.utils.person_utils import build_person_metadata, \
    determine_prioritized_race_or_ethnicity, StateRaceEthnicityPopulationCounts, PersonMetadata
from recidiviz.common.constants.person_characteristics import Gender, Race, Ethnicity
from recidiviz.persistence.entity.state.entities import StatePerson, StatePersonRace, StatePersonEthnicity


class TestBuildPersonMetadata(unittest.TestCase):
    """Tests the build_person_metadata function."""
    def setUp(self) -> None:
        self.state_race_ethnicity_population_counts = [
            StateRaceEthnicityPopulationCounts(
                state_code='US_XX',
                race_or_ethnicity=Race.WHITE.value,
                population_count=100,
                representation_priority=2
            ),
            StateRaceEthnicityPopulationCounts(
                state_code='US_XX',
                race_or_ethnicity=Race.BLACK.value,
                population_count=80,
                representation_priority=1
            )
        ]

    def test_build_person_metadata(self):
        person = StatePerson.new_with_defaults(
            state_code='US_XX', person_id=12345,
            birthdate=date(1984, 8, 31), gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_XX', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(state_code='US_XX', ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        person_metadata = build_person_metadata(person, self.state_race_ethnicity_population_counts)

        expected_person_metadata = PersonMetadata(prioritized_race_or_ethnicity=Race.WHITE.value)

        self.assertEqual(expected_person_metadata, person_metadata)

    def test_build_person_metadata_no_race_ethnicity(self):
        person = StatePerson.new_with_defaults(
            state_code='US_XX', person_id=12345,
            birthdate=date(1984, 8, 31), gender=Gender.FEMALE)

        person_metadata = build_person_metadata(person, self.state_race_ethnicity_population_counts)

        expected_person_metadata = PersonMetadata()

        self.assertEqual(expected_person_metadata, person_metadata)


class TestDeterminePrioritizedRaceOrEthnicity(unittest.TestCase):
    """Tests the determine_prioritized_race_or_ethnicity function."""
    def setUp(self) -> None:
        self.state_race_ethnicity_population_counts = [
            StateRaceEthnicityPopulationCounts(
                state_code='US_XX',
                race_or_ethnicity=Race.ASIAN.value,
                population_count=65,
                representation_priority=1
            ),
            StateRaceEthnicityPopulationCounts(
                state_code='US_XX',
                race_or_ethnicity=Ethnicity.HISPANIC.value,
                population_count=70,
                representation_priority=2
            ),
            StateRaceEthnicityPopulationCounts(
                state_code='US_XX',
                race_or_ethnicity=Race.BLACK.value,
                population_count=80,
                representation_priority=3
            ),
            StateRaceEthnicityPopulationCounts(
                state_code='US_XX',
                race_or_ethnicity=Race.WHITE.value,
                population_count=100,
                representation_priority=4
            )
        ]

    def test_determine_prioritized_race_or_ethnicity_race(self):
        person = StatePerson.new_with_defaults(
            state_code='US_XX', person_id=12345,
            birthdate=date(1984, 8, 31), gender=Gender.FEMALE)

        race_white = StatePersonRace.new_with_defaults(state_code='US_XX', race=Race.WHITE)
        race_black = StatePersonRace.new_with_defaults(state_code='US_XX', race=Race.BLACK)

        person.races = [race_white, race_black]

        prioritized_race_ethnicity = determine_prioritized_race_or_ethnicity(
            person, self.state_race_ethnicity_population_counts)

        expected_output = Race.BLACK.value

        self.assertEqual(expected_output, prioritized_race_ethnicity)

    def test_determine_prioritized_race_or_ethnicity_ethnicity(self):
        person = StatePerson.new_with_defaults(
            state_code='US_XX', person_id=12345,
            birthdate=date(1984, 8, 31), gender=Gender.FEMALE)

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_XX', ethnicity=Ethnicity.HISPANIC
        )

        person.ethnicities = [ethnicity]

        prioritized_race_ethnicity = determine_prioritized_race_or_ethnicity(
            person, self.state_race_ethnicity_population_counts)

        expected_output = Ethnicity.HISPANIC.value

        self.assertEqual(expected_output, prioritized_race_ethnicity)

    def test_determine_prioritized_race_or_ethnicity_no_races(self):
        person = StatePerson.new_with_defaults(
            state_code='US_XX', person_id=12345,
            birthdate=date(1984, 8, 31), gender=Gender.FEMALE)

        person.races = []

        prioritized_race_ethnicity = determine_prioritized_race_or_ethnicity(
            person, self.state_race_ethnicity_population_counts)

        self.assertIsNone(prioritized_race_ethnicity)

    def test_determine_prioritized_race_or_ethnicity_unsupported_state(self):
        person = StatePerson.new_with_defaults(
            state_code='US_NOT_SUPPORTED', person_id=12345,
            birthdate=date(1984, 8, 31), gender=Gender.FEMALE)

        race_white = StatePersonRace.new_with_defaults(state_code='US_NOT_SUPPORTED', race=Race.WHITE)
        race_black = StatePersonRace.new_with_defaults(state_code='US_NOT_SUPPORTED', race=Race.BLACK)

        person.races = [race_white, race_black]

        with pytest.raises(ValueError):
            _ = determine_prioritized_race_or_ethnicity(
                person, self.state_race_ethnicity_population_counts)

    def test_determine_prioritized_race_or_ethnicity_unsupported_race(self):
        person = StatePerson.new_with_defaults(
            state_code='US_XX', person_id=12345,
            birthdate=date(1984, 8, 31), gender=Gender.FEMALE)

        # We want to raise an error if a person has a race or ethnicity that isn't in the state prioritization
        race_unsupported = StatePersonRace.new_with_defaults(
            state_code='US_XX', race=Race.NATIVE_HAWAIIAN_PACIFIC_ISLANDER)

        person.races = [race_unsupported]

        with pytest.raises(ValueError):
            _ = determine_prioritized_race_or_ethnicity(
                person, self.state_race_ethnicity_population_counts)

    def test_determine_prioritized_race_or_ethnicity_external_unknown(self):
        person = StatePerson.new_with_defaults(
            state_code='US_XX', person_id=12345,
            birthdate=date(1984, 8, 31), gender=Gender.FEMALE)

        race_unsupported = StatePersonRace.new_with_defaults(
            state_code='US_XX', race=Race.EXTERNAL_UNKNOWN)

        person.races = [race_unsupported]

        prioritized_race_ethnicity = determine_prioritized_race_or_ethnicity(
            person, self.state_race_ethnicity_population_counts)

        self.assertIsNone(prioritized_race_ethnicity)
