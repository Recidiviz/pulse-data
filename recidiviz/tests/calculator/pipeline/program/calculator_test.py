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

from freezegun import freeze_time

from recidiviz.calculator.pipeline.program import calculator
from recidiviz.calculator.pipeline.program.program_event import \
    ProgramReferralEvent, ProgramEvent
from recidiviz.calculator.pipeline.utils import calculator_utils
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
    demographic_metric_combos_count_for_person, combo_has_enum_value_for_key

ALL_INCLUSIONS_DICT = {
    'age_bucket': True,
    'gender': True,
    'race': True,
    'ethnicity': True,
}

CALCULATION_METHODOLOGIES = len(MetricMethodologyType)


class TestMapProgramCombinations(unittest.TestCase):
    """Tests the map_program_combinations function."""

    # Freezing time to before the events so none of them fall into the
    # relevant metric periods
    @freeze_time('2012-11-02')
    def test_map_program_combinations(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_ND',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_ND',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        program_events = [
            ProgramReferralEvent(
                state_code='US_ND',
                event_date=date(2019, 10, 10),
                program_id='XXX'
            ),
            ProgramReferralEvent(
                state_code='US_ND',
                event_date=date(2019, 2, 2),
                program_id='ZZZ'
            )
        ]

        supervision_combinations = calculator.map_program_combinations(
            person, program_events, ALL_INCLUSIONS_DICT, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, program_events, ALL_INCLUSIONS_DICT)

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == 1 for _combination, value
                   in supervision_combinations)

    @freeze_time('2007-11-02')
    def test_map_program_combinations_full_info(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_ND',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_ND',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        program_events = [
            ProgramReferralEvent(
                state_code='US_ND',
                event_date=date(2009, 10, 31),
                program_id='XXX',
                supervision_type=StateSupervisionType.PAROLE,
                assessment_score=22,
                assessment_type=StateAssessmentType.LSIR,
                supervising_officer_external_id='OFFICERZ',
                supervising_district_external_id='135'
            )
        ]

        supervision_combinations = calculator.map_program_combinations(
            person, program_events, ALL_INCLUSIONS_DICT, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, program_events, ALL_INCLUSIONS_DICT)

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == 1 for _combination, value
                   in supervision_combinations)

    @freeze_time('2007-11-02')
    def test_map_program_combinations_full_info_probation(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_ND',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_ND',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        program_events = [
            ProgramReferralEvent(
                state_code='US_ND',
                event_date=date(2009, 10, 31),
                program_id='XXX',
                supervision_type=StateSupervisionType.PROBATION,
                assessment_score=22,
                assessment_type=StateAssessmentType.LSIR,
                supervising_officer_external_id='OFFICERZ',
                supervising_district_external_id='135'
            )
        ]

        supervision_combinations = calculator.map_program_combinations(
            person, program_events, ALL_INCLUSIONS_DICT, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, program_events, ALL_INCLUSIONS_DICT)

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == 1 for _combination, value
                   in supervision_combinations)

    @freeze_time('2007-11-02')
    def test_map_program_combinations_multiple_supervision_types(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_ND',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_ND',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        program_events = [
            ProgramReferralEvent(
                state_code='US_ND',
                event_date=date(2009, 10, 7),
                program_id='XXX',
                supervision_type=StateSupervisionType.PAROLE,
                assessment_score=22,
                assessment_type=StateAssessmentType.LSIR,
                supervising_officer_external_id='OFFICERZ',
                supervising_district_external_id='135'
            ),
            ProgramReferralEvent(
                state_code='US_ND',
                event_date=date(2009, 10, 7),
                program_id='XXX',
                supervision_type=StateSupervisionType.PROBATION,
                assessment_score=22,
                assessment_type=StateAssessmentType.LSIR,
                supervising_officer_external_id='OFFICERZ',
                supervising_district_external_id='135'
            )
        ]

        supervision_combinations = calculator.map_program_combinations(
            person, program_events, ALL_INCLUSIONS_DICT, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, program_events, ALL_INCLUSIONS_DICT,
            duplicated_months_different_supervision_types=True)

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == 1 for _combination, value
                   in supervision_combinations)
        parole_combos = 0
        probation_combos = 0
        for combo, _ in supervision_combinations:
            if combo.get('supervision_type') \
                and combo_has_enum_value_for_key(
                        combo, 'methodology', MetricMethodologyType.PERSON):
                # Ensure that all person-based metrics are of type parole
                if combo.get('supervision_type') == \
                        StateSupervisionType.PAROLE:
                    parole_combos += 1
                elif combo.get('supervision_type') == \
                        StateSupervisionType.PROBATION:
                    probation_combos += 1

        # Assert that there are the same number of parole and probation
        # person-based combinations
        assert parole_combos == probation_combos

    @freeze_time('2007-12-30')
    def test_map_program_combinations_relevant_periods(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_ND',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_ND',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        program_events = [
            ProgramReferralEvent(
                state_code='US_ND',
                event_date=date(2007, 12, 7),
                program_id='XXX',
                supervision_type=StateSupervisionType.PAROLE,
                assessment_score=22,
                assessment_type=StateAssessmentType.LSIR,
                supervising_officer_external_id='OFFICERZ',
                supervising_district_external_id='135'
            )
        ]

        supervision_combinations = calculator.map_program_combinations(
            person, program_events, ALL_INCLUSIONS_DICT, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, program_events, ALL_INCLUSIONS_DICT,
            len(calculator_utils.METRIC_PERIOD_MONTHS))

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == 1 for _combination, value
                   in supervision_combinations)

    @freeze_time('2007-12-30')
    def test_map_program_combinations_relevant_periods_duplicates(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_ND',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_ND',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        program_events = [
            ProgramReferralEvent(
                state_code='US_ND',
                event_date=date(2007, 12, 7),
                program_id='XXX',
                supervision_type=StateSupervisionType.PAROLE,
                assessment_score=22,
                assessment_type=StateAssessmentType.LSIR,
                supervising_officer_external_id='OFFICERZ',
                supervising_district_external_id='135'
            ),
            ProgramReferralEvent(
                state_code='US_ND',
                event_date=date(2007, 12, 11),
                program_id='XXX',
                supervision_type=StateSupervisionType.PAROLE,
                assessment_score=22,
                assessment_type=StateAssessmentType.LSIR,
                supervising_officer_external_id='OFFICERZ',
                supervising_district_external_id='135'
            )
        ]

        supervision_combinations = calculator.map_program_combinations(
            person, program_events, ALL_INCLUSIONS_DICT, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, program_events, ALL_INCLUSIONS_DICT,
            len(calculator_utils.METRIC_PERIOD_MONTHS))

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == 1 for _combination, value
                   in supervision_combinations)

    @freeze_time('2007-12-30')
    def test_map_program_combinations_relevant_periods_multiple_supervisions(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_ND',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_ND',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        program_events = [
            ProgramReferralEvent(
                state_code='US_ND',
                event_date=date(2007, 1, 7),
                program_id='XXX',
                supervision_type=StateSupervisionType.PAROLE,
                assessment_score=22,
                assessment_type=StateAssessmentType.LSIR,
                supervising_officer_external_id='OFFICERZ',
                supervising_district_external_id='135'
            ),
            ProgramReferralEvent(
                state_code='US_ND',
                event_date=date(2007, 1, 11),
                program_id='XXX',
                supervision_type=StateSupervisionType.PROBATION,
                assessment_score=22,
                assessment_type=StateAssessmentType.LSIR,
                supervising_officer_external_id='OFFICERZ',
                supervising_district_external_id='135'
            )
        ]

        supervision_combinations = calculator.map_program_combinations(
            person, program_events, ALL_INCLUSIONS_DICT, calculation_month_limit=-1
        )

        relevant_periods = [36, 12]

        expected_combinations_count = expected_metric_combos_count(
            person, program_events, ALL_INCLUSIONS_DICT,
            len(relevant_periods),
            duplicated_months_different_supervision_types=True)

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == 1 for _combination, value
                   in supervision_combinations)
        parole_combos = 0
        probation_combos = 0
        for combo, _ in supervision_combinations:
            if combo.get('supervision_type') \
                and combo_has_enum_value_for_key(
                        combo, 'methodology', MetricMethodologyType.PERSON):
                # Ensure that all person-based metrics are of type parole
                if combo.get('supervision_type') == \
                        StateSupervisionType.PAROLE:
                    parole_combos += 1
                elif combo.get('supervision_type') == \
                        StateSupervisionType.PROBATION:
                    probation_combos += 1

        # Assert that there are the same number of parole and probation
        # person-based combinations
        assert parole_combos == probation_combos

    @freeze_time('2012-11-30')
    def test_map_program_combinations_calculation_month_limit_1(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_ND',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_ND',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        included_event = ProgramReferralEvent(
            state_code='US_ND',
            event_date=date(2012, 11, 10),
            program_id='XXX'
        )

        not_included_event = ProgramReferralEvent(
            state_code='US_ND',
            event_date=date(2000, 2, 2),
            program_id='ZZZ'
        )

        program_events = [included_event, not_included_event]

        supervision_combinations = calculator.map_program_combinations(
            person, program_events, ALL_INCLUSIONS_DICT, calculation_month_limit=1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, [included_event], ALL_INCLUSIONS_DICT, len(calculator_utils.METRIC_PERIOD_MONTHS))

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == 1 for _combination, value
                   in supervision_combinations)

    @freeze_time('2012-12-31')
    def test_map_program_combinations_calculation_month_limit_36(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_ND',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_ND',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        included_event = ProgramReferralEvent(
            state_code='US_ND',
            event_date=date(2012, 12, 10),
            program_id='XXX'
        )

        not_included_event = ProgramReferralEvent(
            state_code='US_ND',
            event_date=date(2009, 12, 10),
            program_id='ZZZ'
        )

        program_events = [included_event, not_included_event]

        supervision_combinations = calculator.map_program_combinations(
            person, program_events, ALL_INCLUSIONS_DICT, calculation_month_limit=36
        )

        expected_combinations_count = expected_metric_combos_count(
            person, [included_event], ALL_INCLUSIONS_DICT, len(calculator_utils.METRIC_PERIOD_MONTHS))

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == 1 for _combination, value
                   in supervision_combinations)

    @freeze_time('2012-12-31')
    def test_map_program_combinations_calculation_month_limit_36_include(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_ND',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_ND',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        same_month_event = ProgramReferralEvent(
            state_code='US_ND',
            event_date=date(2012, 12, 10),
            program_id='XXX'
        )

        old_month_event = ProgramReferralEvent(
            state_code='US_ND',
            event_date=date(2010, 1, 10),
            program_id='ZZZ'
        )

        program_events = [same_month_event, old_month_event]

        supervision_combinations = calculator.map_program_combinations(
            person, program_events, ALL_INCLUSIONS_DICT, calculation_month_limit=36
        )

        expected_combinations_count = expected_metric_combos_count(
            person, [same_month_event], ALL_INCLUSIONS_DICT, len(calculator_utils.METRIC_PERIOD_MONTHS))

        expected_combinations_count += expected_metric_combos_count(
            person, [old_month_event], ALL_INCLUSIONS_DICT, 1)

        # Hack to remove the double counted person-based referral in the 36 metric period window
        expected_combinations_count -= (demographic_metric_combos_count_for_person_program(person, ALL_INCLUSIONS_DICT)
                                        * 2)

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

        race = StatePersonRace.new_with_defaults(state_code='US_ND',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_ND',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        program_event = ProgramEvent(
            state_code='US_ND',
            program_id='XXX',
            event_date=date(2009, 10, 1)
        )

        combinations = calculator.characteristic_combinations(
            person, program_event, ALL_INCLUSIONS_DICT)

        # 32 combinations of demographics
        assert len(combinations) == 32

    def test_characteristic_combinations_referral_no_extra_set(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_ND',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_ND',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        program_event = ProgramReferralEvent(
            state_code='US_ND',
            program_id='XXX',
            event_date=date(2009, 10, 1)
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

        race = StatePersonRace.new_with_defaults(state_code='US_ND',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_ND',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        program_event = ProgramReferralEvent(
            state_code='US_ND',
            program_id='XXX',
            event_date=date(2009, 10, 1),
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

        race = StatePersonRace.new_with_defaults(state_code='US_ND',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_ND',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        inclusions = {
            **ALL_INCLUSIONS_DICT,
            'age_bucket': False,
        }

        program_event = ProgramEvent(
            state_code='US_ND',
            program_id='XXX',
            event_date=date(2009, 10, 1)
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

        race = StatePersonRace.new_with_defaults(state_code='US_ND',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_ND',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        program_event = ProgramEvent(
            state_code='US_ND',
            program_id='XXX',
            event_date=date(2009, 10, 1)
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

        race = StatePersonRace.new_with_defaults(state_code='US_ND',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_ND',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        program_event = ProgramEvent(
            state_code='US_ND',
            program_id='XXX',
            event_date=date(2009, 10, 1)
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

        race = StatePersonRace.new_with_defaults(state_code='US_ND',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_ND',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        program_event = ProgramEvent(
            state_code='US_ND',
            program_id='XXX',
            event_date=date(2009, 10, 1)
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

        race = StatePersonRace.new_with_defaults(state_code='US_ND',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_ND',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        program_event = ProgramEvent(
            state_code='US_ND',
            program_id='XXX',
            event_date=date(2009, 10, 1)
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


class TestIncludeReferralInCount(unittest.TestCase):
    """Tests the include_referral_in_count function."""
    def test_include_referral_in_count(self):
        """Tests the include_referral_in_count function when the referral
        should be included."""

        combo = {
            'metric_type': 'REFERRAL'
        }

        program_event = ProgramReferralEvent(
            state_code='US_ND',
            event_date=date(2020, 1, 3),
            program_id='XXX'
        )

        end_date = date(2020, 1, 31)

        include = calculator.include_referral_in_count(
            combo, program_event, end_date, [program_event])

        self.assertTrue(include)

    def test_include_referral_in_count_last_of_many(self):
        """Tests the include_referral_in_count function when the referral
        should be included because it is the last one before the end of the
        time period."""

        combo = {
            'metric_type': 'REFERRAL'
        }

        program_event_1 = ProgramReferralEvent(
            state_code='US_ND',
            event_date=date(2020, 1, 3),
            program_id='XXX'
        )

        program_event_2 = ProgramReferralEvent(
            state_code='US_ND',
            event_date=date(2020, 1, 9),
            program_id='XXX'
        )

        program_event_3 = ProgramReferralEvent(
            state_code='US_ND',
            event_date=date(2020, 1, 27),
            program_id='XXX'
        )

        events_in_period = [program_event_1, program_event_2, program_event_3]

        end_date = date(2020, 1, 31)

        include = calculator.include_referral_in_count(
            combo, program_event_3, end_date, events_in_period)

        self.assertTrue(include)

    def test_include_referral_in_count_last_of_many_unsorted(self):
        """Tests the include_referral_in_count function when the referral
        should be included because it is the last one before the end of the
        time period."""

        combo = {
            'metric_type': 'REFERRAL'
        }

        program_event_1 = ProgramReferralEvent(
            state_code='US_ND',
            event_date=date(2017, 2, 3),
            program_id='XXX'
        )

        program_event_2 = ProgramReferralEvent(
            state_code='US_ND',
            event_date=date(2020, 1, 30),
            program_id='XXX'
        )

        program_event_3 = ProgramReferralEvent(
            state_code='US_ND',
            event_date=date(2018, 3, 11),
            program_id='XXX'
        )

        events_in_period = [program_event_1, program_event_2, program_event_3]

        end_date = date(2020, 1, 31)

        include = calculator.include_referral_in_count(
            combo, program_event_2, end_date, events_in_period)

        self.assertTrue(include)

    def test_include_referral_in_count_supervision_type_unset(self):
        """Tests the include_referral_in_count function when there are two
        events in the same month, but of different supervision types, and the
        combo does not specify the supervision type."""

        combo = {
            'metric_type': 'REFERRAL'
        }

        program_event_1 = ProgramReferralEvent(
            state_code='US_ND',
            event_date=date(2020, 1, 3),
            program_id='XXX',
            supervision_type=StateSupervisionType.PROBATION
        )

        program_event_2 = ProgramReferralEvent(
            state_code='US_ND',
            event_date=date(2020, 1, 9),
            program_id='XXX',
            supervision_type=StateSupervisionType.PAROLE
        )

        events_in_period = [program_event_1, program_event_2]

        end_date = date(2020, 1, 31)

        include_first = calculator.include_referral_in_count(
            combo, program_event_1, end_date, events_in_period)

        self.assertFalse(include_first)

        include_second = calculator.include_referral_in_count(
            combo, program_event_2, end_date, events_in_period)

        self.assertTrue(include_second)

    def test_include_referral_in_count_supervision_type_set(self):
        """Tests the include_referral_in_count function when there are two
        events in the same month, but of different supervision types, and the
        combo does specify the supervision type."""

        combo = {
            'metric_type': 'REFERRAL',
            'supervision_type': StateSupervisionType.PROBATION
        }

        program_event_1 = ProgramReferralEvent(
            state_code='US_ND',
            event_date=date(2020, 1, 3),
            program_id='XXX',
            supervision_type=StateSupervisionType.PROBATION
        )

        program_event_2 = ProgramReferralEvent(
            state_code='US_ND',
            event_date=date(2020, 1, 9),
            program_id='XXX',
            supervision_type=StateSupervisionType.PAROLE
        )

        events_in_period = [program_event_1, program_event_2]

        end_date = date(2020, 1, 31)

        include_first = calculator.include_referral_in_count(
            combo, program_event_1, end_date, events_in_period)

        self.assertTrue(include_first)


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
        num_relevant_periods: int = 0,
        with_methodologies: bool = True,
        duplicated_months_different_supervision_types: bool = False) -> int:
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
        if (referral_event.event_date.year,
                referral_event.event_date.month) in months:
            num_duplicated_referral_months += 1
        if referral_event.event_date.month:
            months.add((referral_event.event_date.year,
                        referral_event.event_date.month))

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

    if num_relevant_periods > 0:
        program_referral_combos += \
            demographic_metric_combos * \
            (num_referral_events - num_duplicated_referral_months) * \
            referral_dimension_multiplier * \
            num_relevant_periods

        if duplicated_months_different_supervision_types:
            program_referral_combos += \
             int(demographic_metric_combos *
                 num_duplicated_referral_months *
                 referral_dimension_multiplier *
                 num_relevant_periods / 2)

    # Referral metrics removed in person-based de-duplication
    duplicated_referral_combos = \
        int(demographic_metric_combos *
            num_duplicated_referral_months *
            referral_dimension_multiplier)

    if duplicated_months_different_supervision_types:
        # If the duplicated months have different supervision types, then
        # don't remove the supervision-type-specific combos
        duplicated_referral_combos = int(duplicated_referral_combos / 2)

    # Remove the combos for the duplicated referral months
    program_referral_combos -= duplicated_referral_combos

    return program_referral_combos
