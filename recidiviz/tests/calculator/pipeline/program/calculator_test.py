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
"""Tests for program/calculator.py."""
# pylint: disable=unused-import,wrong-import-order
import unittest
from datetime import date
from typing import List, Set, Tuple

from freezegun import freeze_time

from recidiviz.calculator.pipeline.program import calculator
from recidiviz.calculator.pipeline.program.metrics import ProgramMetricType, ProgramReferralMetric, \
    ProgramParticipationMetric
from recidiviz.calculator.pipeline.program.program_event import \
    ProgramReferralEvent, ProgramEvent, ProgramParticipationEvent
from recidiviz.calculator.pipeline.utils import calculator_utils
from recidiviz.calculator.pipeline.utils.metric_utils import \
    MetricMethodologyType
from recidiviz.calculator.pipeline.utils.person_utils import PersonMetadata
from recidiviz.common.constants.person_characteristics import Gender, Race, \
    Ethnicity
from recidiviz.common.constants.state.state_assessment import \
    StateAssessmentType
from recidiviz.common.constants.state.state_program_assignment import StateProgramAssignmentParticipationStatus
from recidiviz.common.constants.state.state_supervision import \
    StateSupervisionType
from recidiviz.common.date import last_day_of_month
from recidiviz.persistence.entity.state.entities import StatePerson, \
    StatePersonRace, StatePersonEthnicity
from recidiviz.tests.calculator.calculator_test_utils import combo_has_enum_value_for_key

ALL_METRICS_INCLUSIONS_DICT = {
    metric_type: True for metric_type in ProgramMetricType
}

_DEFAULT_PERSON_METADATA = PersonMetadata(prioritized_race_or_ethnicity='BLACK')

CALCULATION_METHODOLOGIES = len(MetricMethodologyType)


class TestMapProgramCombinations(unittest.TestCase):
    """Tests the map_program_combinations function."""

    # Freezing time to years after the events so none of them fall into the
    # relevant metric periods
    @freeze_time('2030-11-02')
    def test_map_program_combinations(self):
        person = StatePerson.new_with_defaults(state_code='US_ND', person_id=12345,
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
            ProgramParticipationEvent(
                state_code='US_ND',
                event_date=date(2019, 2, 2),
                program_id='ZZZ'
            )
        ]

        program_combinations = calculator.map_program_combinations(
            person, program_events, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA
        )

        expected_combinations_count = expected_metric_combos_count(program_events)

        self.assertEqual(expected_combinations_count, len(program_combinations))
        assert all(value == 1 for _combination, value in program_combinations)

    def test_map_program_combinations_full_info(self):
        person = StatePerson.new_with_defaults(state_code='US_ND', person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_ND',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_ND',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        event_date = date(2009, 10, 31)

        program_events = [
            ProgramReferralEvent(
                state_code='US_ND',
                event_date=event_date,
                program_id='XXX',
                supervision_type=StateSupervisionType.PAROLE,
                assessment_score=22,
                assessment_type=StateAssessmentType.LSIR,
                supervising_officer_external_id='OFFICERZ',
                supervising_district_external_id='135'
            ),
            ProgramParticipationEvent(
                state_code='US_ND',
                event_date=event_date,
                program_id='XXX',
                program_location_id='YYY',
                supervision_type=StateSupervisionType.PAROLE,
            )
        ]

        program_combinations = calculator.map_program_combinations(
            person, program_events, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month='2009-10',
            calculation_month_count=1,
            person_metadata=_DEFAULT_PERSON_METADATA
        )

        expected_combinations_count = expected_metric_combos_count(program_events)

        self.assertEqual(expected_combinations_count, len(program_combinations))
        assert all(value == 1 for _combination, value in program_combinations)

    def test_map_program_combinations_full_info_probation(self):
        person = StatePerson.new_with_defaults(state_code='US_ND', person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_ND',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_ND',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        event_date = date(2009, 10, 31)

        program_events = [
            ProgramReferralEvent(
                state_code='US_ND',
                event_date=event_date,
                program_id='XXX',
                supervision_type=StateSupervisionType.PROBATION,
                assessment_score=22,
                assessment_type=StateAssessmentType.LSIR,
                supervising_officer_external_id='OFFICERZ',
                supervising_district_external_id='135'
            ),
            ProgramParticipationEvent(
                state_code='US_ND',
                event_date=event_date,
                program_id='XXX',
                program_location_id='YYY',
                supervision_type=StateSupervisionType.PROBATION,
            )
        ]

        program_combinations = calculator.map_program_combinations(
            person, program_events, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month='2009-10',
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA
        )

        expected_combinations_count = expected_metric_combos_count(program_events)

        self.assertEqual(expected_combinations_count, len(program_combinations))
        assert all(value == 1 for _combination, value in program_combinations)

    def test_map_program_combinations_multiple_supervision_types(self):
        person = StatePerson.new_with_defaults(state_code='US_ND', person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_ND',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_ND',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        event_date = date(2009, 10, 7)

        program_events = [
            ProgramReferralEvent(
                state_code='US_ND',
                event_date=event_date,
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
            ),
            ProgramParticipationEvent(
                state_code='US_ND',
                event_date=event_date,
                program_id='XXX',
                program_location_id='YYY',
                supervision_type=StateSupervisionType.PAROLE,
            ),
            ProgramParticipationEvent(
                state_code='US_ND',
                event_date=event_date,
                program_id='XXX',
                program_location_id='YYY',
                supervision_type=StateSupervisionType.PROBATION,
            )
        ]

        program_combinations = calculator.map_program_combinations(
            person, program_events, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month='2009-10',
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA
        )

        expected_combinations_count = expected_metric_combos_count(
            program_events, duplicated_months_different_supervision_types=True)

        self.assertEqual(expected_combinations_count, len(program_combinations))
        assert all(value == 1 for _combination, value in program_combinations)

        parole_combos = 0
        probation_combos = 0
        for combo, _ in program_combinations:
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
        person = StatePerson.new_with_defaults(state_code='US_ND', person_id=12345,
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

        program_combinations = calculator.map_program_combinations(
            person, program_events, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA
        )

        expected_combinations_count = expected_metric_combos_count(
            program_events, len(calculator_utils.METRIC_PERIOD_MONTHS))

        self.assertEqual(expected_combinations_count, len(program_combinations))
        assert all(value == 1 for _combination, value in program_combinations)

    @freeze_time('2007-12-30')
    def test_map_program_combinations_relevant_periods_duplicates(self):
        person = StatePerson.new_with_defaults(state_code='US_ND', person_id=12345,
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

        program_combinations = calculator.map_program_combinations(
            person, program_events, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA
        )

        expected_combinations_count = expected_metric_combos_count(
            program_events, len(calculator_utils.METRIC_PERIOD_MONTHS))

        self.assertEqual(expected_combinations_count, len(program_combinations))
        assert all(value == 1 for _combination, value in program_combinations)

    @freeze_time('2007-12-30')
    def test_map_program_combinations_relevant_periods_multiple_supervisions(self):
        person = StatePerson.new_with_defaults(state_code='US_ND', person_id=12345,
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

        program_combinations = calculator.map_program_combinations(
            person, program_events, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA
        )

        relevant_periods = [36, 12]

        expected_combinations_count = expected_metric_combos_count(
            program_events, len(relevant_periods),
            duplicated_months_different_supervision_types=True)

        self.assertEqual(expected_combinations_count, len(program_combinations))
        assert all(value == 1 for _combination, value in program_combinations)
        parole_combos = 0
        probation_combos = 0
        for combo, _ in program_combinations:
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
    def test_map_program_combinations_calculation_month_count_1(self):
        person = StatePerson.new_with_defaults(state_code='US_ND', person_id=12345,
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

        program_combinations = calculator.map_program_combinations(
            person, program_events, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=1,
            person_metadata=_DEFAULT_PERSON_METADATA
        )

        expected_combinations_count = expected_metric_combos_count(
            [included_event], len(calculator_utils.METRIC_PERIOD_MONTHS))

        self.assertEqual(expected_combinations_count, len(program_combinations))
        assert all(value == 1 for _combination, value in program_combinations)

    @freeze_time('2012-12-31')
    def test_map_program_combinations_calculation_month_count_36(self):
        person = StatePerson.new_with_defaults(state_code='US_ND', person_id=12345,
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

        program_combinations = calculator.map_program_combinations(
            person, program_events, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=36,
            person_metadata=_DEFAULT_PERSON_METADATA
        )

        expected_combinations_count = expected_metric_combos_count(
            [included_event], len(calculator_utils.METRIC_PERIOD_MONTHS))

        self.assertEqual(expected_combinations_count, len(program_combinations))
        assert all(value == 1 for _combination, value in program_combinations)

    @freeze_time('2012-12-31')
    def test_map_program_combinations_calculation_month_count_36_include(self):
        person = StatePerson.new_with_defaults(state_code='US_ND', person_id=12345,
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

        program_combinations = calculator.map_program_combinations(
            person, program_events, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=36,
            person_metadata=_DEFAULT_PERSON_METADATA
        )

        expected_combinations_count = expected_metric_combos_count(
            [same_month_event], len(calculator_utils.METRIC_PERIOD_MONTHS))

        expected_combinations_count += expected_metric_combos_count([old_month_event], 1)

        # Subtract the duplicated count for the metric_period_months = 36 person-based dict
        expected_combinations_count -= 1

        self.assertEqual(expected_combinations_count, len(program_combinations))
        assert all(value == 1 for _combination, value in program_combinations)


class TestCharacteristicsDict(unittest.TestCase):
    """Tests the characteristics_dict function."""

    def setUp(self) -> None:
        self.person = StatePerson.new_with_defaults(state_code='US_ND', person_id=12345,
                                                    birthdate=date(1984, 8, 31),
                                                    gender=Gender.FEMALE)

        race_white = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)
        race_black = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.BLACK)

        self.person.races = [race_white, race_black]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        self.person.ethnicities = [ethnicity]

    def test_characteristics_dict_program_referral_event(self):

        program_referral_event = ProgramReferralEvent(
            state_code='US_ND',
            program_id='XXX',
            event_date=date(2009, 10, 1),
            supervision_type=StateSupervisionType.PAROLE,
            participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
            assessment_score=23,
            assessment_type=StateAssessmentType.LSIR,
            supervising_officer_external_id='OFFICER',
            supervising_district_external_id='DISTRICT',
        )

        characteristic_dict = calculator.characteristics_dict(self.person,
                                                              program_referral_event,
                                                              ProgramReferralMetric,
                                                              _DEFAULT_PERSON_METADATA)

        expected_output = {
            'program_id': 'XXX',
            'age_bucket': '25-29',
            'gender': Gender.FEMALE,
            'race': [Race.WHITE, Race.BLACK],
            'ethnicity': [Ethnicity.NOT_HISPANIC],
            'person_id': 12345,
            'supervision_type': StateSupervisionType.PAROLE,
            'participation_status': StateProgramAssignmentParticipationStatus.IN_PROGRESS,
            'assessment_score_bucket': '0-23',
            'assessment_type': StateAssessmentType.LSIR,
            'supervising_officer_external_id': 'OFFICER',
            'supervising_district_external_id': 'DISTRICT',
            'prioritized_race_or_ethnicity': _DEFAULT_PERSON_METADATA.prioritized_race_or_ethnicity
        }

        self.assertEqual(expected_output, characteristic_dict)

    def test_characteristics_dict_program_participation_event(self):

        program_participation_event = ProgramParticipationEvent(
            state_code='US_ND',
            program_id='XXX',
            program_location_id='YYY',
            event_date=date(2009, 10, 1),
            supervision_type=StateSupervisionType.PAROLE,
        )

        characteristic_dict = calculator.characteristics_dict(self.person,
                                                              program_participation_event,
                                                              ProgramParticipationMetric,
                                                              _DEFAULT_PERSON_METADATA)

        expected_output = {
            'date_of_participation': program_participation_event.event_date,
            'program_id': 'XXX',
            'age_bucket': '25-29',
            'gender': Gender.FEMALE,
            'race': [Race.WHITE, Race.BLACK],
            'ethnicity': [Ethnicity.NOT_HISPANIC],
            'person_id': 12345,
            'supervision_type': StateSupervisionType.PAROLE,
            'program_location_id': 'YYY',
            'prioritized_race_or_ethnicity': _DEFAULT_PERSON_METADATA.prioritized_race_or_ethnicity
        }

        self.assertEqual(expected_output, characteristic_dict)


class TestIncludeEventInCount(unittest.TestCase):
    """Tests the include_referral_in_count function."""
    def test_include_event_in_count(self):
        """Tests the include_event_in_count function when the referral should be included."""

        combo = {
            'metric_type': 'PROGRAM_PARTICIPATION'
        }

        program_event = ProgramParticipationEvent(
            state_code='US_ND',
            event_date=date(2020, 1, 3),
            program_id='XXX'
        )

        end_date = date(2020, 1, 31)

        include = calculator.include_event_in_count(combo, program_event, end_date, [program_event])

        self.assertTrue(include)

    def test_include_participation_event_in_count_multiple_same_day(self):
        """Tests the include_event_in_count function when the participation event should be included because its
        program_location_id alphabetically before the other program_location_ids."""

        combo = {
            'metric_type': 'PROGRAM_PARTICIPATION'
        }

        program_event_1 = ProgramParticipationEvent(
            state_code='US_ND',
            event_date=date(2020, 1, 3),
            program_id='XXX',
            program_location_id='A'
        )

        program_event_2 = ProgramParticipationEvent(
            state_code='US_ND',
            event_date=date(2020, 1, 3),
            program_id='XXX',
            program_location_id='B'
        )

        program_event_3 = ProgramParticipationEvent(
            state_code='US_ND',
            event_date=date(2020, 1, 3),
            program_id='XXX',
            program_location_id='C'
        )

        end_date = date(2020, 1, 31)

        program_events = [program_event_1, program_event_2, program_event_3]

        include = calculator.include_event_in_count(combo, program_event_1, end_date, program_events)

        self.assertTrue(include)

    def test_include_participation_event_in_count_multiple_same_day_do_not_include(self):
        """Tests the include_event_in_count function when the participation event should not be included because its
        program_location_id is not alphabetically before the other program_location_ids."""

        combo = {
            'metric_type': 'PROGRAM_PARTICIPATION'
        }

        program_event_1 = ProgramParticipationEvent(
            state_code='US_ND',
            event_date=date(2020, 1, 3),
            program_id='XXX',
            program_location_id='Z'
        )

        program_event_2 = ProgramParticipationEvent(
            state_code='US_ND',
            event_date=date(2020, 1, 3),
            program_id='XXX',
            program_location_id='B'
        )

        program_event_3 = ProgramParticipationEvent(
            state_code='US_ND',
            event_date=date(2020, 1, 3),
            program_id='XXX',
            program_location_id='C'
        )

        end_date = date(2020, 1, 31)

        program_events = [program_event_1, program_event_2, program_event_3]

        include = calculator.include_event_in_count(combo, program_event_1, end_date, program_events)

        self.assertFalse(include)

    def test_include_event_in_count_last_of_many(self):
        """Tests the include_event_in_count function when the referral should be included because it is the last one
        before the end of the time period."""

        combo = {
            'metric_type': 'PROGRAM_REFERRAL'
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

        include = calculator.include_event_in_count(
            combo, program_event_3, end_date, events_in_period)

        self.assertTrue(include)

    def test_include_event_in_count_last_of_many_unsorted(self):
        """Tests the include_event_in_count function when the event should be included because it is the last one before
        the end of the time period."""

        combo = {
            'metric_type': 'PROGRAM_REFERRAL'
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

        include = calculator.include_event_in_count(
            combo, program_event_2, end_date, events_in_period)

        self.assertTrue(include)

    def test_include_event_in_count_supervision_type_unset(self):
        """Tests the include_event_in_count function when there are two events in the same month, but of different
        supervision types, and the combo does not specify the supervision type."""

        combo = {
            'metric_type': 'PROGRAM_REFERRAL'
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

        include_first = calculator.include_event_in_count(
            combo, program_event_1, end_date, events_in_period)

        self.assertFalse(include_first)

        include_second = calculator.include_event_in_count(
            combo, program_event_2, end_date, events_in_period)

        self.assertTrue(include_second)

    def test_include_event_in_count_supervision_type_set(self):
        """Tests the include_event_in_count function when there are two events in the same month, but of different
        supervision types, and the combo does specify the supervision type."""

        combo = {
            'metric_type': 'PROGRAM_REFERRAL',
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

        include_first = calculator.include_event_in_count(
            combo, program_event_1, end_date, events_in_period)

        self.assertTrue(include_first)


def expected_metric_combos_count(
        program_events: List[ProgramEvent],
        num_relevant_periods: int = 0,
        with_methodologies: bool = True,
        duplicated_months_different_supervision_types: bool = False) -> int:
    """Calculates the expected number of characteristic combinations given the program events, the number of relevant
    periods, and other indications of what happened."""
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

    duplication_multiplier = 0 if duplicated_months_different_supervision_types else 1

    program_referral_combos = (num_referral_events + (
        num_referral_events - duplication_multiplier*num_duplicated_referral_months)*(
            num_relevant_periods + 1))

    participation_events = [
        bucket for bucket in program_events
        if isinstance(bucket, ProgramParticipationEvent)
    ]
    num_participation_events = len(participation_events)

    num_duplicated_participation_days = 0
    days: Set[date] = set()

    for participation_event in participation_events:
        if participation_event.event_date in days:
            num_duplicated_participation_days += 1
        days.add(participation_event.event_date)

    program_participation_combos = (num_participation_events + (
        num_participation_events - duplication_multiplier*num_duplicated_participation_days))

    return program_referral_combos + program_participation_combos
