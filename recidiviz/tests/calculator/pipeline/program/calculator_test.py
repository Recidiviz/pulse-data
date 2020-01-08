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
"""Tests for program/calculator.py."""
# pylint: disable=unused-import,wrong-import-order
import unittest
from datetime import date
from typing import List, Dict, Set, Tuple

import recidiviz.calculator.pipeline.utils.calculator_utils
from recidiviz.calculator.pipeline.program import calculator
from recidiviz.calculator.pipeline.program.program_event import \
    ProgramReferralEvent, ProgramEvent
from recidiviz.calculator.pipeline.utils.metric_utils import \
    MetricMethodologyType
from recidiviz.common.constants.person_characteristics import Gender, Race, \
    Ethnicity
from recidiviz.common.constants.state.state_assessment import \
    StateAssessmentType
from recidiviz.common.constants.state.state_supervision import \
    StateSupervisionType
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


class TestMapProgramCombinations(unittest.TestCase):
    """Tests the map_program_combinations function."""

    def test_map_program_combinations(self):
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

        program_events = [
            ProgramReferralEvent(
                state_code='CA',
                program_id='XXX',
                year=2009,
                month=10,
            ),
            ProgramReferralEvent(
                state_code='CA',
                program_id='ZZZ',
                year=2010,
                month=2,
            )
        ]

        supervision_combinations = calculator.map_program_combinations(
            person, program_events, ALL_INCLUSIONS_DICT
        )

        expected_combinations_count = expected_metric_combos_count(
            person, program_events, ALL_INCLUSIONS_DICT)

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == 1 for _combination, value
                   in supervision_combinations)

    def test_map_program_combinations_full_info(self):
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

        program_events = [
            ProgramReferralEvent(
                state_code='CA',
                program_id='XXX',
                year=2009,
                month=10,
                supervision_type=StateSupervisionType.PAROLE,
                assessment_score=22,
                assessment_type=StateAssessmentType.LSIR,
                supervising_officer_external_id='OFFICERZ',
                supervising_district_external_id='135'
            )
        ]

        supervision_combinations = calculator.map_program_combinations(
            person, program_events, ALL_INCLUSIONS_DICT
        )

        expected_combinations_count = expected_metric_combos_count(
            person, program_events, ALL_INCLUSIONS_DICT)

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == 1 for _combination, value
                   in supervision_combinations)

    def test_map_program_combinations_multiple_supervision_types(self):
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

        program_events = [
            ProgramReferralEvent(
                state_code='CA',
                program_id='XXX',
                year=2009,
                month=10,
                supervision_type=StateSupervisionType.PAROLE,
                assessment_score=22,
                assessment_type=StateAssessmentType.LSIR,
                supervising_officer_external_id='OFFICERZ',
                supervising_district_external_id='135'
            ),
            ProgramReferralEvent(
                state_code='CA',
                program_id='XXX',
                year=2009,
                month=10,
                supervision_type=StateSupervisionType.PROBATION,
                assessment_score=22,
                assessment_type=StateAssessmentType.LSIR,
                supervising_officer_external_id='OFFICERZ',
                supervising_district_external_id='135'
            )
        ]

        supervision_combinations = calculator.map_program_combinations(
            person, program_events, ALL_INCLUSIONS_DICT
        )

        expected_combinations_count = expected_metric_combos_count(
            person, program_events, ALL_INCLUSIONS_DICT)

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == 1 for _combination, value
                   in supervision_combinations)


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

        program_event = ProgramEvent(
            state_code='CA',
            program_id='XXX',
            year=2009,
            month=10,
        )

        combinations = calculator.characteristic_combinations(
            person, program_event, ALL_INCLUSIONS_DICT)

        # 32 combinations of demographics
        assert len(combinations) == 32

    def test_characteristic_combinations_referral_no_extra_set(self):
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

        program_event = ProgramReferralEvent(
            state_code='CA',
            program_id='XXX',
            year=2009,
            month=10
        )

        combinations = calculator.characteristic_combinations(
            person, program_event, ALL_INCLUSIONS_DICT)

        expected_combinations_count = expected_metric_combos_count(
            person, [program_event], ALL_INCLUSIONS_DICT,
            with_methodologies=False)

        self.assertEqual(expected_combinations_count, len(combinations))

    def test_characteristic_combinations_referral(self):
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

        program_event = ProgramReferralEvent(
            state_code='CA',
            program_id='XXX',
            year=2009,
            month=10,
            supervision_type=StateSupervisionType.PROBATION,
            assessment_score=25,
            assessment_type=StateAssessmentType.LSIR,
            supervising_officer_external_id='OFFICER',
            supervising_district_external_id='DISTRICT'
        )

        combinations = calculator.characteristic_combinations(
            person, program_event, ALL_INCLUSIONS_DICT)

        expected_combinations_count = expected_metric_combos_count(
            person, [program_event], ALL_INCLUSIONS_DICT,
            with_methodologies=False)

        self.assertEqual(expected_combinations_count, len(combinations))

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

        inclusions = {
            **ALL_INCLUSIONS_DICT,
            'age_bucket': False,
        }

        program_event = ProgramEvent(
            state_code='CA',
            program_id='XXX',
            year=2009,
            month=10,
        )

        combinations = calculator.characteristic_combinations(
            person, program_event, inclusions)

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

        program_event = ProgramEvent(
            state_code='CA',
            program_id='XXX',
            year=2009,
            month=10,
        )

        inclusions = {
            **ALL_INCLUSIONS_DICT,
            'gender': False,
        }

        combinations = calculator.characteristic_combinations(
            person, program_event, inclusions)

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

        program_event = ProgramEvent(
            state_code='CA',
            program_id='XXX',
            year=2009,
            month=10,
        )

        inclusions = {
            **ALL_INCLUSIONS_DICT,
            'race': False,
        }

        combinations = calculator.characteristic_combinations(
            person, program_event, inclusions)

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

        program_event = ProgramEvent(
            state_code='CA',
            program_id='XXX',
            year=2009,
            month=10,
        )

        inclusions = {
            **ALL_INCLUSIONS_DICT,
            'ethnicity': False,
        }

        combinations = calculator.characteristic_combinations(
            person, program_event, inclusions)

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

        program_event = ProgramEvent(
            state_code='CA',
            program_id='XXX',
            year=2009,
            month=10,
        )

        inclusions = {
            **ALL_INCLUSIONS_DICT,
            'age_bucket': False,
            'ethnicity': False,
        }

        combinations = calculator.characteristic_combinations(
            person, program_event, inclusions)

        # 8 combinations of demographics and supervision type
        assert len(combinations) == 8

        for combo in combinations:
            assert combo.get('age_bucket') is None
            assert combo.get('ethnicity') is None


def demographic_metric_combos_count_for_person_program(
        person: StatePerson,
        inclusions: Dict[str, bool]) -> int:
    """Returns the number of possible demographic metric combinations for a
    given person, given the metric inclusions list."""

    total_metric_combos = demographic_metric_combos_count_for_person(
        person, inclusions
    )

    return total_metric_combos


def expected_metric_combos_count(
        person: StatePerson,
        program_events: List[ProgramEvent],
        inclusions: Dict[str, bool],
        with_methodologies: bool = True) -> int:
    """Calculates the expected number of characteristic combinations given
    the person, the program events, and the dimensions that should
    be included in the explosion of feature combinations."""

    demographic_metric_combos = \
        demographic_metric_combos_count_for_person_program(
            person, inclusions)

    # Some test cases above use a different call that doesn't take methodology
    # into account as a dimension
    methodology_multiplier = 1
    if with_methodologies:
        methodology_multiplier *= CALCULATION_METHODOLOGIES

    referral_events = [
        bucket for bucket in program_events
        if isinstance(bucket, ProgramReferralEvent)
    ]
    num_referral_events = len(referral_events)

    num_duplicated_referral_months = 0
    months: Set[Tuple[int, int]] = set()

    for referral_event in referral_events:
        if (referral_event.year,
                referral_event.month) in months:
            num_duplicated_referral_months += 1
        if referral_event.month:
            months.add((referral_event.year,
                        referral_event.month))

    # Calculate total combos for program referrals
    referral_dimension_multiplier = 1
    if referral_events and referral_events[0].program_id:
        referral_dimension_multiplier *= 2
    if referral_events and referral_events[0].supervision_type:
        referral_dimension_multiplier *= 2
    if referral_events and referral_events[0].assessment_score:
        referral_dimension_multiplier *= 2
    if referral_events and referral_events[0].assessment_type:
        referral_dimension_multiplier *= 2
    if referral_events and referral_events[0].supervising_officer_external_id:
        referral_dimension_multiplier *= 2
    if referral_events and referral_events[0].supervising_district_external_id:
        referral_dimension_multiplier *= 2

    program_referral_combos = \
        demographic_metric_combos * methodology_multiplier * \
        num_referral_events * referral_dimension_multiplier

    # Referral metrics removed in person-based de-duplication that don't
    # specify supervision type
    duplicated_referral_combos = \
        int(demographic_metric_combos / 2 *
            num_duplicated_referral_months *
            referral_dimension_multiplier)

    # Remove the combos that don't specify supervision type for the duplicated
    # referral months
    program_referral_combos -= duplicated_referral_combos

    return program_referral_combos
