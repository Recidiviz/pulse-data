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
"""Tests for supervision/calculator.py."""
# pylint: disable=unused-import,wrong-import-order

from datetime import date

from recidiviz.calculator.pipeline.supervision import calculator
from recidiviz.calculator.pipeline.supervision.supervision_month import \
    NonRevocationReturnSupervisionMonth
from recidiviz.common.constants.person_characteristics import Gender, Race, \
    Ethnicity
from recidiviz.common.constants.state.state_supervision import \
    StateSupervisionType
from recidiviz.persistence.entity.state.entities import StatePerson, \
    StatePersonRace, StatePersonEthnicity


class TestMapSupervisionCombinations:
    """Tests the map_supervision_combinations function."""

    def test_map_supervision_combinations(self):
        """Tests the map_supervision_combinations function."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='CA',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='CA',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_months = [
            NonRevocationReturnSupervisionMonth('AK', 2018, 3,
                                                StateSupervisionType.PAROLE)
        ]

        inclusions = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
            'release_facility': True,
            'stay_length_bucket': True
        }

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_months, inclusions
        )

        # 64 combinations of demographics, methodology, and supervision type
        # * 1 month = 64 metrics
        assert len(supervision_combinations) == 64
        assert all(value == 1 for _combination, value
                   in supervision_combinations)

    def test_map_supervision_combinations_multiple_months(self):
        """Tests the map_supervision_combinations function where the person
        was on supervision for multiple months."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='CA',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='CA',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_months = [
            NonRevocationReturnSupervisionMonth('AK', 2018, 3,
                                                StateSupervisionType.PAROLE),
            NonRevocationReturnSupervisionMonth('AK', 2018, 4,
                                                StateSupervisionType.PAROLE),
            NonRevocationReturnSupervisionMonth('AK', 2018, 5,
                                                StateSupervisionType.PAROLE),
            NonRevocationReturnSupervisionMonth('AK', 2018, 6,
                                                StateSupervisionType.PAROLE),
        ]

        inclusions = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
            'release_facility': True,
            'stay_length_bucket': True
        }

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_months, inclusions
        )

        # 64 combinations of demographics, methodology, and supervision type
        # * 4 month = 256 metrics
        assert len(supervision_combinations) == 256
        assert all(value == 1 for _combination, value
                   in supervision_combinations)

    def test_map_supervision_combinations_overlapping_months(self):
        """Tests the map_supervision_combinations function where the person
        was serving multiple supervision sentences simultaneously in a given
        month."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='CA',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='CA',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_months = [
            NonRevocationReturnSupervisionMonth('AK', 2018, 3,
                                                StateSupervisionType.PROBATION),
            NonRevocationReturnSupervisionMonth('AK', 2018, 3,
                                                StateSupervisionType.PROBATION)
        ]

        inclusions = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
            'release_facility': True,
            'stay_length_bucket': True
        }

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_months, inclusions
        )

        # Person-based metrics:
        # 32 combinations of demographics and supervision type
        # * 1 month = 32 person-based metrics
        # Event-based metrics:
        # 32 combinations of demographics and supervision type
        # * 2 months = 64 event-based metrics
        assert len(supervision_combinations) == 96
        assert all(value == 1 for _combination, value
                   in supervision_combinations)


class TestCharacteristicCombinations:
    """Tests the characteristic_combinations function."""

    def test_characteristic_combinations(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='CA',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='CA',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_month = NonRevocationReturnSupervisionMonth(
            'AK', 2018, 3, StateSupervisionType.PAROLE)

        inclusions = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
        }

        combinations = calculator.characteristic_combinations(
            person, supervision_month, inclusions)

        # 32 combinations of demographics and supervision type
        assert len(combinations) == 32

    def test_characteristic_combinations_exclude_age(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='CA',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='CA',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_month = NonRevocationReturnSupervisionMonth(
            'AK', 2018, 3, StateSupervisionType.PAROLE)

        inclusions = {
            'age_bucket': False,
            'gender': True,
            'race': True,
            'ethnicity': True,
        }

        combinations = calculator.characteristic_combinations(
            person, supervision_month, inclusions)

        # 16 combinations of demographics and supervision type
        assert len(combinations) == 16

        for combo in combinations:
            assert combo.get('age_bucket') is None

    def test_characteristic_combinations_exclude_gender(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='CA',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='CA',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_month = NonRevocationReturnSupervisionMonth(
            'AK', 2018, 3, StateSupervisionType.PAROLE)

        inclusions = {
            'age_bucket': True,
            'gender': False,
            'race': True,
            'ethnicity': True,
        }

        combinations = calculator.characteristic_combinations(
            person, supervision_month, inclusions)

        # 16 combinations of demographics and supervision type
        assert len(combinations) == 16

        for combo in combinations:
            assert combo.get('gender') is None

    def test_characteristic_combinations_exclude_race(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='CA',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='CA',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_month = NonRevocationReturnSupervisionMonth(
            'AK', 2018, 3, StateSupervisionType.PAROLE)

        inclusions = {
            'age_bucket': True,
            'gender': True,
            'race': False,
            'ethnicity': True,
        }

        combinations = calculator.characteristic_combinations(
            person, supervision_month, inclusions)

        # 16 combinations of demographics and supervision type
        assert len(combinations) == 16

        for combo in combinations:
            assert combo.get('race') is None

    def test_characteristic_combinations_exclude_ethnicity(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='CA',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='CA',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_month = NonRevocationReturnSupervisionMonth(
            'AK', 2018, 3, StateSupervisionType.PAROLE)

        inclusions = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': False,
        }

        combinations = calculator.characteristic_combinations(
            person, supervision_month, inclusions)

        # 16 combinations of demographics and supervision type
        assert len(combinations) == 16

        for combo in combinations:
            assert combo.get('ethnicity') is None

    def test_characteristic_combinations_exclude_multiple(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='CA',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='CA',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_month = NonRevocationReturnSupervisionMonth(
            'AK', 2018, 3, StateSupervisionType.PAROLE)

        inclusions = {
            'age_bucket': False,
            'gender': True,
            'race': True,
            'ethnicity': False,
        }

        combinations = calculator.characteristic_combinations(
            person, supervision_month, inclusions)

        # 8 combinations of demographics and supervision type
        assert len(combinations) == 8

        for combo in combinations:
            assert combo.get('age_bucket') is None
            assert combo.get('ethnicity') is None
