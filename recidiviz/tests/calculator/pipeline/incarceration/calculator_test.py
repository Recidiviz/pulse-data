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
"""Tests for incarceration/calculator.py."""
# pylint: disable=unused-import,wrong-import-order
import unittest
from datetime import date
from typing import List, Dict, Set, Tuple

from recidiviz.calculator.pipeline.incarceration.incarceration_event import \
    IncarcerationEvent, IncarcerationAdmissionEvent, IncarcerationReleaseEvent
from recidiviz.calculator.pipeline.incarceration import calculator
from recidiviz.calculator.pipeline.utils.metric_utils import \
    MetricMethodologyType
from recidiviz.common.constants.person_characteristics import Gender, Race, \
    Ethnicity
from recidiviz.persistence.entity.state.entities import StatePerson, \
    StatePersonRace, StatePersonEthnicity
from recidiviz.tests.calculator.calculator_test_utils import \
    demographic_metric_combos_count_for_person

ALL_INCLUSIONS_DICT = {
    'age_bucket': True,
    'gender': True,
    'race': True,
    'ethnicity': True,
}

CALCULATION_METHODOLOGIES = len(MetricMethodologyType)


class TestMapIncarcerationCombinations(unittest.TestCase):
    """Tests the map_incarceration_combinations function."""

    def test_map_incarceration_combinations(self):
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

        incarceration_event = IncarcerationAdmissionEvent(
            state_code='CA',
            year=2000,
            month=3,
            facility='SAN QUENTIN'
        )

        incarceration_events = [incarceration_event]

        incarceration_combinations = calculator.map_incarceration_combinations(
            person,
            incarceration_events,
            ALL_INCLUSIONS_DICT
        )

        expected_combinations_count = expected_metric_combos_count(
            person, incarceration_events, ALL_INCLUSIONS_DICT)

        self.assertEqual(expected_combinations_count,
                         len(incarceration_combinations))
        assert all(value == 1 for _combination, value
                   in incarceration_combinations)

        for combo, _ in incarceration_combinations:
            assert combo.get('year') == 2000

    def test_map_incarceration_combinations_both_types(self):
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

        incarceration_events = [
            IncarcerationAdmissionEvent(
                state_code='CA',
                year=2000,
                month=3,
                facility='SAN QUENTIN'
            ),
            IncarcerationReleaseEvent(
                state_code='CA',
                year=2003,
                month=9,
                facility='SAN QUENTIN'
            )
        ]

        incarceration_combinations = calculator.map_incarceration_combinations(
            person,
            incarceration_events,
            ALL_INCLUSIONS_DICT
        )

        expected_combinations_count = expected_metric_combos_count(
            person, incarceration_events, ALL_INCLUSIONS_DICT)

        self.assertEqual(expected_combinations_count,
                         len(incarceration_combinations))
        assert all(value == 1 for _combination, value
                   in incarceration_combinations)

    def test_map_incarceration_combinations_two_admissions_same_month(self):
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

        incarceration_events = [
            IncarcerationAdmissionEvent(
                state_code='CA',
                year=2000,
                month=3,
                facility='SAN QUENTIN'
            ),
            IncarcerationAdmissionEvent(
                state_code='CA',
                year=2000,
                month=3,
                facility='SAN QUENTIN'
            )
        ]

        incarceration_combinations = calculator.map_incarceration_combinations(
            person,
            incarceration_events,
            ALL_INCLUSIONS_DICT
        )

        expected_combinations_count = expected_metric_combos_count(
            person, incarceration_events, ALL_INCLUSIONS_DICT)

        self.assertEqual(expected_combinations_count,
                         len(incarceration_combinations))
        assert all(value == 1 for _combination, value
                   in incarceration_combinations)

    def test_map_incarceration_combinations_two_releases_same_month(self):
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

        incarceration_events = [
            IncarcerationReleaseEvent(
                state_code='CA',
                year=2010,
                month=9,
                facility='FACILITY 33'
            ),
            IncarcerationReleaseEvent(
                state_code='CA',
                year=2010,
                month=9,
                facility='FACILITY 33'
            )
        ]

        incarceration_combinations = calculator.map_incarceration_combinations(
            person,
            incarceration_events,
            ALL_INCLUSIONS_DICT
        )

        expected_combinations_count = expected_metric_combos_count(
            person, incarceration_events, ALL_INCLUSIONS_DICT)

        self.assertEqual(expected_combinations_count,
                         len(incarceration_combinations))
        assert all(value == 1 for _combination, value
                   in incarceration_combinations)


class TestCharacteristicCombinations(unittest.TestCase):
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

        incarceration_event = IncarcerationAdmissionEvent(
            state_code='CA',
            year=2000,
            month=3,
            facility='SAN QUENTIN'
        )

        combinations = calculator.characteristic_combinations(
            person, incarceration_event, ALL_INCLUSIONS_DICT)

        # 32 combinations of demographics
        assert len(combinations) == 32

    def test_characteristic_combinations_no_facility(self):
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

        incarceration_event = IncarcerationAdmissionEvent(
            state_code='CA',
            year=2000,
            month=3,
        )

        combinations = calculator.characteristic_combinations(
            person, incarceration_event, ALL_INCLUSIONS_DICT)

        # 32 combinations of demographics
        assert len(combinations) == 16


def demographic_metric_combos_count_for_person_incarceration(
        person: StatePerson,
        inclusions: Dict[str, bool]) -> int:
    """Returns the number of possible demographic metric combinations for a
    given person, given the metric inclusions list."""

    total_metric_combos = demographic_metric_combos_count_for_person(
        person, inclusions
    )

    # Facility is always included
    total_metric_combos *= 2

    return total_metric_combos


def expected_metric_combos_count(
        person: StatePerson,
        incarceration_events: List[IncarcerationEvent],
        inclusions: Dict[str, bool],
        with_methodologies: bool = True) -> int:
    """Calculates the expected number of characteristic combinations given
    the person, the incarceration events, and the dimensions that should
    be included in the explosion of feature combinations."""

    demographic_metric_combos = \
        demographic_metric_combos_count_for_person_incarceration(
            person, inclusions)

    # Some test cases above use a different call that doesn't take methodology
    # into account as a dimension
    methodology_multiplier = 1
    if with_methodologies:
        methodology_multiplier *= CALCULATION_METHODOLOGIES

    num_incarceration_events = len(incarceration_events)

    admission_events = [
        event for event in incarceration_events
        if isinstance(event, IncarcerationAdmissionEvent)
    ]

    num_duplicated_admission_months = 0
    admission_months: Set[Tuple[int, int]] = set()

    for admission_event in admission_events:
        if (admission_event.year,
                admission_event.month) in admission_months:
            num_duplicated_admission_months += 1

        admission_months.add((admission_event.year,
                              admission_event.month))

    release_events = [
        event for event in incarceration_events
        if isinstance(event, IncarcerationReleaseEvent)
    ]

    num_duplicated_release_months = 0
    release_months: Set[Tuple[int, int]] = set()

    for release_event in release_events:
        if (release_event.year,
                release_event.month) in release_months:
            num_duplicated_release_months += 1

        release_months.add((release_event.year,
                            release_event.month))

    incarceration_event_combos = \
        demographic_metric_combos * methodology_multiplier * \
        num_incarceration_events

    duplicated_admission_combos = int(
        demographic_metric_combos *
        num_duplicated_admission_months
    )

    duplicated_release_combos = int(
        demographic_metric_combos *
        num_duplicated_release_months
    )

    incarceration_event_combos -= \
        (duplicated_admission_combos + duplicated_release_combos)

    return incarceration_event_combos
