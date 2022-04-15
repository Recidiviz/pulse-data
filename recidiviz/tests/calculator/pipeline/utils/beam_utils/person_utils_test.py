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

from recidiviz.calculator.pipeline.metrics.utils.metric_utils import PersonMetadata
from recidiviz.calculator.pipeline.utils.beam_utils.person_utils import (
    StateRaceEthnicityPopulationCounts,
    _build_person_metadata,
    _determine_prioritized_race_or_ethnicity,
)
from recidiviz.common.constants.state.state_person import (
    StateEthnicity,
    StateGender,
    StateRace,
)
from recidiviz.persistence.entity.state.entities import (
    StatePerson,
    StatePersonEthnicity,
    StatePersonRace,
)


class TestBuildPersonMetadata(unittest.TestCase):
    """Tests the _build_person_metadata function."""

    def setUp(self) -> None:
        self.state_race_ethnicity_population_counts = [
            StateRaceEthnicityPopulationCounts(
                state_code="US_XX",
                race_or_ethnicity=StateRace.WHITE.value,
                population_count=100,
                representation_priority=2,
            ),
            StateRaceEthnicityPopulationCounts(
                state_code="US_XX",
                race_or_ethnicity=StateRace.BLACK.value,
                population_count=80,
                representation_priority=1,
            ),
        ]

    def test_build_person_metadata(self):
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(
            state_code="US_XX", race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=StateEthnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        person_metadata = _build_person_metadata(
            person, self.state_race_ethnicity_population_counts
        )

        expected_person_metadata = PersonMetadata(
            prioritized_race_or_ethnicity=StateRace.WHITE.value
        )

        self.assertEqual(expected_person_metadata, person_metadata)

    def test_build_person_metadata_no_race_ethnicity(self):
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        person_metadata = _build_person_metadata(
            person, self.state_race_ethnicity_population_counts
        )

        expected_person_metadata = PersonMetadata()

        self.assertEqual(expected_person_metadata, person_metadata)


class TestDeterminePrioritizedRaceOrEthnicity(unittest.TestCase):
    """Tests the _determine_prioritized_race_or_ethnicity function."""

    def setUp(self) -> None:
        self.state_race_ethnicity_population_counts = [
            StateRaceEthnicityPopulationCounts(
                state_code="US_XX",
                race_or_ethnicity=StateRace.ASIAN.value,
                population_count=65,
                representation_priority=1,
            ),
            StateRaceEthnicityPopulationCounts(
                state_code="US_XX",
                race_or_ethnicity=StateEthnicity.HISPANIC.value,
                population_count=70,
                representation_priority=2,
            ),
            StateRaceEthnicityPopulationCounts(
                state_code="US_XX",
                race_or_ethnicity=StateRace.BLACK.value,
                population_count=80,
                representation_priority=3,
            ),
            StateRaceEthnicityPopulationCounts(
                state_code="US_XX",
                race_or_ethnicity=StateRace.WHITE.value,
                population_count=100,
                representation_priority=4,
            ),
        ]

    def test_determine_prioritized_race_or_ethnicity_race(self):
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race_white = StatePersonRace.new_with_defaults(
            state_code="US_XX", race=StateRace.WHITE
        )
        race_black = StatePersonRace.new_with_defaults(
            state_code="US_XX", race=StateRace.BLACK
        )

        person.races = [race_white, race_black]

        prioritized_race_ethnicity = _determine_prioritized_race_or_ethnicity(
            person, self.state_race_ethnicity_population_counts
        )

        expected_output = StateRace.BLACK.value

        self.assertEqual(expected_output, prioritized_race_ethnicity)

    def test_determine_prioritized_race_or_ethnicity_ethnicity(self):
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=StateEthnicity.HISPANIC
        )

        person.ethnicities = [ethnicity]

        prioritized_race_ethnicity = _determine_prioritized_race_or_ethnicity(
            person, self.state_race_ethnicity_population_counts
        )

        expected_output = StateEthnicity.HISPANIC.value

        self.assertEqual(expected_output, prioritized_race_ethnicity)

    def test_determine_prioritized_race_or_ethnicity_no_races(self):
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        person.races = []

        prioritized_race_ethnicity = _determine_prioritized_race_or_ethnicity(
            person, self.state_race_ethnicity_population_counts
        )

        self.assertIsNone(prioritized_race_ethnicity)

    def test_determine_prioritized_race_or_ethnicity_unsupported_state(self):
        person = StatePerson.new_with_defaults(
            state_code="US_NOT_SUPPORTED",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race_white = StatePersonRace.new_with_defaults(
            state_code="US_NOT_SUPPORTED", race=StateRace.WHITE
        )
        race_black = StatePersonRace.new_with_defaults(
            state_code="US_NOT_SUPPORTED", race=StateRace.BLACK
        )

        person.races = [race_white, race_black]

        with self.assertRaises(ValueError):
            _ = _determine_prioritized_race_or_ethnicity(
                person, self.state_race_ethnicity_population_counts
            )

    def test_determine_prioritized_race_or_ethnicity_unsupported_race(self):
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        # We want to raise an error if a person has a race or ethnicity that isn't in the state prioritization
        race_unsupported = StatePersonRace.new_with_defaults(
            state_code="US_XX", race=StateRace.NATIVE_HAWAIIAN_PACIFIC_ISLANDER
        )

        person.races = [race_unsupported]

        with self.assertRaises(ValueError):
            _ = _determine_prioritized_race_or_ethnicity(
                person, self.state_race_ethnicity_population_counts
            )

    def test_determine_prioritized_race_or_ethnicity_external_unknown(self):
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race_unsupported = StatePersonRace.new_with_defaults(
            state_code="US_XX", race=StateRace.EXTERNAL_UNKNOWN
        )

        person.races = [race_unsupported]

        prioritized_race_ethnicity = _determine_prioritized_race_or_ethnicity(
            person, self.state_race_ethnicity_population_counts
        )

        self.assertIsNone(prioritized_race_ethnicity)
