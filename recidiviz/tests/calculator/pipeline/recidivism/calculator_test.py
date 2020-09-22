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

# pylint: disable=unused-import,wrong-import-order

"""Tests for recidivism/calculator.py."""
import unittest
from datetime import date
from typing import Dict, List

from dateutil.relativedelta import relativedelta
from freezegun import freeze_time

from recidiviz.calculator.pipeline.recidivism import calculator
from recidiviz.calculator.pipeline.recidivism.calculator \
    import FOLLOW_UP_PERIODS
from recidiviz.calculator.pipeline.recidivism.release_event import \
    ReleaseEvent, RecidivismReleaseEvent, NonRecidivismReleaseEvent
from recidiviz.calculator.pipeline.utils.metric_utils import \
    MetricMethodologyType
from recidiviz.calculator.pipeline.recidivism.release_event import \
    ReincarcerationReturnType
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodSupervisionType
from recidiviz.common.constants.state.state_supervision_violation import \
    StateSupervisionViolationType
from recidiviz.persistence.entity.state.entities import StatePerson, Gender,\
    StatePersonRace, Race, StatePersonEthnicity, Ethnicity
from recidiviz.calculator.pipeline.recidivism.metrics import \
    ReincarcerationRecidivismMetricType as MetricType, ReincarcerationRecidivismCountMetric, \
    ReincarcerationRecidivismRateMetric

_COUNTY_OF_RESIDENCE = 'county'


class TestReincarcerations(unittest.TestCase):
    """Tests the reincarcerations() function in the calculator."""
    def test_reincarcerations(self):
        release_date = date.today()
        original_admission_date = release_date - relativedelta(years=4)
        reincarceration_date = release_date + relativedelta(years=3)
        second_release_date = reincarceration_date + relativedelta(years=1)

        first_event = RecidivismReleaseEvent(
            'CA', original_admission_date, release_date, 'Sing Sing',
            _COUNTY_OF_RESIDENCE, reincarceration_date, 'Sing Sing',
            ReincarcerationReturnType.NEW_ADMISSION)
        second_event = NonRecidivismReleaseEvent(
            'CA', reincarceration_date, second_release_date, 'Sing Sing',
            _COUNTY_OF_RESIDENCE)
        release_events = {2018: [first_event], 2022: [second_event]}

        expected_reincarcerations = {reincarceration_date:
                                     {'release_date': first_event.release_date,
                                      'return_type': first_event.return_type,
                                      'from_supervision_type':
                                      first_event.from_supervision_type,
                                      'source_violation_type': None}}

        reincarcerations = calculator.reincarcerations(release_events)
        self.assertEqual(expected_reincarcerations, reincarcerations)

    def test_reincarcerations_empty(self):
        reincarcerations = calculator.reincarcerations({})
        self.assertEqual({}, reincarcerations)


class TestReincarcerationsInWindow(unittest.TestCase):
    """Tests the reincarcerations_in_window() function in the calculator."""

    def test_reincarcerations_in_window(self):
        # Too early
        release_2012 = date(2012, 4, 30)
        # Just right
        release_2016 = date(2016, 5, 13)
        release_2020 = date(2020, 11, 20)
        release_2021 = date(2021, 5, 13)
        # Too late
        release_2022 = date(2022, 5, 13)

        reincarceration = {'return_type': ReincarcerationReturnType.NEW_ADMISSION,
                           'from_supervision_type': None}

        all_reincarcerations = {release_2012: reincarceration,
                                release_2016: reincarceration,
                                release_2020: reincarceration,
                                release_2021: reincarceration,
                                release_2022: reincarceration}

        start_date = date(2016, 5, 13)

        reincarcerations = calculator.reincarcerations_in_window(
            start_date, start_date +
            relativedelta(years=6), all_reincarcerations)
        self.assertEqual(3, len(reincarcerations))

    def test_reincarcerations_in_window_all_early(self):
        # Too early
        release_2012 = date(2012, 4, 30)
        release_2016 = date(2016, 5, 13)
        release_2020 = date(2020, 11, 20)
        release_2021 = date(2021, 5, 13)
        release_2022 = date(2022, 5, 13)

        reincarceration = {'return_type': ReincarcerationReturnType.NEW_ADMISSION,
                           'from_supervision_type': None}

        all_reincarcerations = {release_2012: reincarceration,
                                release_2016: reincarceration,
                                release_2020: reincarceration,
                                release_2021: reincarceration,
                                release_2022: reincarceration}

        start_date = date(2026, 5, 13)

        reincarcerations = calculator.reincarcerations_in_window(
            start_date, start_date +
            relativedelta(years=6), all_reincarcerations)

        self.assertEqual([], reincarcerations)

    def test_reincarcerations_in_window_all_late(self):
        # Too late
        release_2012 = date(2012, 4, 30)
        release_2016 = date(2016, 5, 13)
        release_2020 = date(2020, 11, 20)
        release_2021 = date(2021, 5, 13)
        release_2022 = date(2022, 5, 13)

        reincarceration = {'return_type': ReincarcerationReturnType.NEW_ADMISSION,
                           'from_supervision_type': None}

        all_reincarcerations = {release_2012: reincarceration,
                                release_2016: reincarceration,
                                release_2020: reincarceration,
                                release_2021: reincarceration,
                                release_2022: reincarceration}

        start_date = date(2006, 5, 13)

        reincarcerations = calculator.reincarcerations_in_window(
            start_date, start_date +
            relativedelta(years=5), all_reincarcerations)

        self.assertEqual([], reincarcerations)

    def test_reincarcerations_in_window_with_revocation_returns(self):
        # Too early
        release_2012 = date(2012, 4, 30)
        # Just right
        release_2016 = date(2016, 5, 13)
        release_2020 = date(2020, 11, 20)
        release_2021 = date(2021, 5, 13)
        # Too late
        release_2022 = date(2022, 5, 13)

        revocation_reincarceration = {
            'return_type':
                ReincarcerationReturnType.REVOCATION,
            'from_supervision_type':
                StateSupervisionPeriodSupervisionType.PAROLE}

        new_admission_reincarceration = {
            'return_type': ReincarcerationReturnType.NEW_ADMISSION,
            'from_supervision_type': None}

        all_reincarcerations = {release_2012: new_admission_reincarceration,
                                release_2016: revocation_reincarceration,
                                release_2020: revocation_reincarceration,
                                release_2021: new_admission_reincarceration,
                                release_2022: new_admission_reincarceration}

        start_date = date(2016, 5, 13)

        reincarcerations = calculator.reincarcerations_in_window(
            start_date, start_date +
            relativedelta(years=6), all_reincarcerations)

        self.assertEqual(3, len(reincarcerations))
        self.assertEqual(ReincarcerationReturnType.REVOCATION, reincarcerations[0].get('return_type'))
        self.assertEqual(StateSupervisionPeriodSupervisionType.PAROLE, reincarcerations[0].get('from_supervision_type'))
        self.assertEqual(ReincarcerationReturnType.REVOCATION, reincarcerations[1].get('return_type'))
        self.assertEqual(StateSupervisionPeriodSupervisionType.PAROLE, reincarcerations[1].get('from_supervision_type'))
        self.assertEqual(ReincarcerationReturnType.NEW_ADMISSION, reincarcerations[2].get('return_type'))
        self.assertIsNone(reincarcerations[2].get('from_supervision_type'))


class TestEarliestRecidivatedFollowUpPeriod(unittest.TestCase):
    """Tests the earliest_recidivated_follow_up_period() function in the calculator."""

    def test_earliest_recidivated_follow_up_period_later_month_in_year(self):
        release_date = date(2012, 4, 20)
        reincarceration_date = date(2016, 5, 13)

        earliest_period = calculator.earliest_recidivated_follow_up_period(
            release_date, reincarceration_date)
        self.assertEqual(5, earliest_period)

    def test_earliest_recidivated_follow_up_period_same_month_in_year_later_day(self):
        release_date = date(2012, 4, 20)
        reincarceration_date = date(2016, 4, 21)

        earliest_period = calculator.earliest_recidivated_follow_up_period(
            release_date, reincarceration_date)
        self.assertEqual(5, earliest_period)

    def test_earliest_recidivated_follow_up_period_same_month_in_year_earlier_day(self):
        release_date = date(2012, 4, 20)
        reincarceration_date = date(2016, 4, 19)

        earliest_period = calculator.earliest_recidivated_follow_up_period(
            release_date, reincarceration_date)
        self.assertEqual(4, earliest_period)

    def test_earliest_recidivated_follow_up_period_same_month_in_year_same_day(self):
        release_date = date(2012, 4, 20)
        reincarceration_date = date(2016, 4, 20)

        earliest_period = calculator.earliest_recidivated_follow_up_period(
            release_date, reincarceration_date)
        self.assertEqual(4, earliest_period)

    def test_earliest_recidivated_follow_up_period_earlier_month_in_year(self):
        release_date = date(2012, 4, 20)
        reincarceration_date = date(2016, 3, 31)

        earliest_period = calculator.earliest_recidivated_follow_up_period(
            release_date, reincarceration_date)
        self.assertEqual(4, earliest_period)

    def test_earliest_recidivated_follow_up_period_same_year(self):
        release_date = date(2012, 4, 20)
        reincarceration_date = date(2012, 5, 13)

        earliest_period = calculator.earliest_recidivated_follow_up_period(
            release_date, reincarceration_date)
        self.assertEqual(1, earliest_period)

    def test_earliest_recidivated_follow_up_period_no_reincarceration(self):
        release_date = date(2012, 4, 30)

        earliest_period = calculator.earliest_recidivated_follow_up_period(
            release_date, None)
        self.assertIsNone(earliest_period)


class TestStayLengthFromEvent(unittest.TestCase):
    """Tests the built-in stay_length function on the ReleaseEvent class."""
    def test_stay_length_from_event_earlier_month_and_date(self):
        original_admission_date = date(2013, 6, 17)
        release_date = date(2014, 4, 15)
        event = ReleaseEvent('CA', original_admission_date, release_date,
                             'Sing Sing')

        self.assertEqual(9, event.stay_length)

    def test_stay_length_from_event_same_month_earlier_date(self):
        original_admission_date = date(2013, 6, 17)
        release_date = date(2014, 6, 16)
        event = ReleaseEvent('NH', original_admission_date, release_date,
                             'Sing Sing')

        self.assertEqual(11, event.stay_length)

    def test_stay_length_from_event_same_month_same_date(self):
        original_admission_date = date(2013, 6, 17)
        release_date = date(2014, 6, 17)
        event = ReleaseEvent('TX', original_admission_date, release_date,
                             'Sing Sing')

        self.assertEqual(12, event.stay_length)

    def test_stay_length_from_event_same_month_later_date(self):
        original_admission_date = date(2013, 6, 17)
        release_date = date(2014, 6, 18)
        event = ReleaseEvent('UT', original_admission_date, release_date,
                             'Sing Sing')

        self.assertEqual(12, event.stay_length)

    def test_stay_length_from_event_later_month(self):
        original_admission_date = date(2013, 6, 17)
        release_date = date(2014, 8, 11)
        event = ReleaseEvent('HI', original_admission_date, release_date, 'Sing Sing')

        self.assertEqual(13, event.stay_length)

    def test_stay_length_from_event_original_admission_date_unknown(self):
        release_date = date(2014, 7, 11)
        event = ReleaseEvent('MT', None, release_date, 'Sing Sing')

        self.assertIsNone(event.stay_length)

    def test_stay_length_from_event_release_date_unknown(self):
        original_admission_date = date(2014, 7, 11)
        event = ReleaseEvent('UT', original_admission_date, None, 'Sing Sing')

        self.assertIsNone(event.stay_length)

    def test_stay_length_from_event_both_dates_unknown(self):
        event = ReleaseEvent('NH', None, None, 'Sing Sing')

        self.assertIsNone(event.stay_length)


class TestStayLengthBucket(unittest.TestCase):
    """Tests the built-in stay_length_bucket attribute on the ReleaseEvent class."""
    def setUp(self) -> None:
        self.months_to_bucket_map = {
            11: '<12',
            12: '12-24',
            20: '12-24',
            24: '24-36',
            30: '24-36',
            36: '36-48',
            40: '36-48',
            48: '48-60',
            50: '48-60',
            60: '60-72',
            70: '60-72',
            72: '72-84',
            80: '72-84',
            84: '84-96',
            96: '96-108',
            100: '96-108',
            108: '108-120',
            110: '108-120',
            120: '120<',
            130: '120<',
        }

    def test_stay_length_bucket(self):
        original_admission_date = date(1903, 6, 17)

        for months, bucket in self.months_to_bucket_map.items():
            release_date = original_admission_date + relativedelta(months=months)
            event = ReleaseEvent('CA', original_admission_date, release_date, 'Sing Sing')
            self.assertEqual(bucket, event.stay_length_bucket)


class TestRecidivismValueForMetric(unittest.TestCase):
    """Tests the recidivism_value_for_metric() function in the calculator."""
    def test_recidivism_value_for_metric(self):
        combo = {'age': '<25', 'race': 'black', 'gender': 'female'}

        value = calculator.recidivism_value_for_metric(combo, None, None, None)

        self.assertEqual(1, value)

    def test_recidivism_value_for_metric_new_admission(self):
        combo = {'age': '<25', 'race': 'black', 'gender': 'female',
                 'return_type': ReincarcerationReturnType.NEW_ADMISSION}

        value = calculator.recidivism_value_for_metric(
            combo, ReincarcerationReturnType.NEW_ADMISSION, None, None)

        self.assertEqual(1, value)

    def test_recidivism_value_for_metric_not_new_admission(self):
        combo = {'age': '<25', 'race': 'black', 'gender': 'female',
                 'return_type': ReincarcerationReturnType.NEW_ADMISSION}

        value = calculator.recidivism_value_for_metric(
            combo, ReincarcerationReturnType.REVOCATION, None, None)

        self.assertEqual(0, value)

    def test_recidivism_value_for_metric_parole_revocation(self):
        combo = {'age': '<25', 'race': 'black', 'gender': 'female',
                 'return_type': ReincarcerationReturnType.REVOCATION,
                 'from_supervision_type':
                     StateSupervisionPeriodSupervisionType.PAROLE}

        value = calculator.recidivism_value_for_metric(
            combo, ReincarcerationReturnType.REVOCATION,
            StateSupervisionPeriodSupervisionType.PAROLE, None)

        self.assertEqual(1, value)

    def test_recidivism_value_for_metric_probation_revocation(self):
        combo = {'age': '<25', 'race': 'black', 'gender': 'female',
                 'return_type': ReincarcerationReturnType.REVOCATION,
                 'from_supervision_type':
                     StateSupervisionPeriodSupervisionType.PROBATION}

        value = calculator.recidivism_value_for_metric(
            combo, ReincarcerationReturnType.REVOCATION,
            StateSupervisionPeriodSupervisionType.PROBATION, None)

        self.assertEqual(1, value)

    def test_recidivism_value_for_metric_parole_revocation_source_violation(self):
        combo = {'age': '<25', 'race': 'black', 'gender': 'female',
                 'return_type': ReincarcerationReturnType.REVOCATION,
                 'from_supervision_type':
                     StateSupervisionPeriodSupervisionType.PAROLE,
                 'source_violation_type': StateSupervisionViolationType.TECHNICAL}

        value = calculator.recidivism_value_for_metric(
            combo, ReincarcerationReturnType.REVOCATION,
            StateSupervisionPeriodSupervisionType.PAROLE,
            StateSupervisionViolationType.TECHNICAL)

        self.assertEqual(1, value)

    def test_recidivism_value_for_metric_probation_revocation_source_violation(self):
        combo = {'age': '<25', 'race': 'black', 'gender': 'female',
                 'return_type': ReincarcerationReturnType.REVOCATION,
                 'from_supervision_type':
                     StateSupervisionPeriodSupervisionType.PROBATION,
                 'source_violation_type': StateSupervisionViolationType.FELONY}

        value = calculator.recidivism_value_for_metric(
            combo, ReincarcerationReturnType.REVOCATION,
            StateSupervisionPeriodSupervisionType.PROBATION,
            StateSupervisionViolationType.FELONY)

        self.assertEqual(1, value)

    def test_recidivism_value_for_metric_not_revocation(self):
        combo = {'age': '<25', 'race': 'black', 'gender': 'female',
                 'return_type': ReincarcerationReturnType.REVOCATION,
                 'from_supervision_type':
                     StateSupervisionPeriodSupervisionType.PROBATION}

        value = calculator.recidivism_value_for_metric(
            combo, ReincarcerationReturnType.NEW_ADMISSION,
            StateSupervisionPeriodSupervisionType.PROBATION, None)

        self.assertEqual(0, value)

    def test_recidivism_value_for_metric_not_supervision_type(self):
        combo = {'age': '<25', 'race': 'black', 'gender': 'female',
                 'return_type': ReincarcerationReturnType.REVOCATION,
                 'from_supervision_type':
                     StateSupervisionPeriodSupervisionType.PROBATION}

        value = calculator.recidivism_value_for_metric(
            combo, ReincarcerationReturnType.REVOCATION,
            StateSupervisionPeriodSupervisionType.PAROLE, None)

        self.assertEqual(0, value)

    def test_recidivism_value_for_metric_not_source_violation_type(self):
        combo = {'age': '<25', 'race': 'black', 'gender': 'female',
                 'return_type': ReincarcerationReturnType.REVOCATION,
                 'from_supervision_type':
                     StateSupervisionPeriodSupervisionType.PROBATION,
                 'source_violation_type': StateSupervisionViolationType.FELONY}

        value = calculator.recidivism_value_for_metric(
            combo, ReincarcerationReturnType.REVOCATION,
            StateSupervisionPeriodSupervisionType.PAROLE,
            StateSupervisionViolationType.TECHNICAL)

        self.assertEqual(0, value)


ALL_METRIC_INCLUSIONS_DICT = {
    MetricType.REINCARCERATION_COUNT: True,
    MetricType.REINCARCERATION_RATE: True
}


class TestMapRecidivismCombinations(unittest.TestCase):
    """Tests the map_recidivism_combinations function."""

    RECIDIVISM_METHODOLOGIES = len(MetricMethodologyType)

    def expected_metric_combos_count(self, release_events_by_cohort: Dict[int, List[ReleaseEvent]]) -> int:
        """Calculates the expected number of characteristic combinations given the release events."""
        all_release_events = [
            re
            for re_list in release_events_by_cohort.values()
            for re in re_list
        ]

        recidivism_release_events = [
            re for re in all_release_events if
            isinstance(re, RecidivismReleaseEvent)
        ]

        num_events_with_multiple_releases_in_year = 0
        for _, events in release_events_by_cohort.items():
            if len(events) > 1:
                num_events_with_multiple_releases_in_year += (len(events) - 1)

        expected_rate_metrics = self.RECIDIVISM_METHODOLOGIES * len(FOLLOW_UP_PERIODS) * len(all_release_events)

        # Duplicated combos for multiple releases in the same year
        expected_rate_metrics -= (
            len(FOLLOW_UP_PERIODS) * num_events_with_multiple_releases_in_year
        )

        expected_count_metrics = (len(recidivism_release_events) * self.RECIDIVISM_METHODOLOGIES)

        return expected_rate_metrics + expected_count_metrics

    @freeze_time('2100-01-01')
    def test_map_recidivism_combinations(self):
        """Tests the map_recidivism_combinations function where there is
        recidivism."""
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

        release_events_by_cohort = {
            2008: [RecidivismReleaseEvent(
                'CA', date(2005, 7, 19), date(2008, 9, 19), 'Hudson',
                _COUNTY_OF_RESIDENCE,
                date(2014, 5, 12), 'Upstate',
                ReincarcerationReturnType.NEW_ADMISSION)]
        }

        days_at_liberty = (date(2014, 5, 12) - date(2008, 9, 19)).days

        recidivism_combinations = calculator.map_recidivism_combinations(
            person, release_events_by_cohort, ALL_METRIC_INCLUSIONS_DICT)

        expected_combos_count = self.expected_metric_combos_count(release_events_by_cohort)

        self.assertEqual(expected_combos_count, len(recidivism_combinations))
        self.assertEqual(len(set(id(combination_dict) for combination_dict, _ in recidivism_combinations)),
                         len(recidivism_combinations))

        for combination, value in recidivism_combinations:
            if combination.get('metric_type') == MetricType.REINCARCERATION_RATE and \
                    combination.get('follow_up_period') <= 5 or \
                    combination.get('return_type') == \
                    ReincarcerationReturnType.REVOCATION:
                self.assertEqual(0, value)
            else:
                self.assertEqual(1, value)
                if (combination.get('metric_type') == MetricType.REINCARCERATION_COUNT
                        and combination.get('person_id') is not None):
                    assert combination.get('days_at_liberty') == days_at_liberty

    def test_map_recidivism_combinations_multiple_in_period(self):
        """Tests the map_recidivism_combinations function where there are multiple instances of recidivism within a
        follow-up period."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='CA',
                                                 race=Race.BLACK)
        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='CA',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        release_events_by_cohort = {
            1908: [RecidivismReleaseEvent(
                'CA', date(1905, 7, 19), date(1908, 9, 19), 'Hudson',
                _COUNTY_OF_RESIDENCE,
                date(1910, 8, 12), 'Upstate',
                ReincarcerationReturnType.NEW_ADMISSION)],
            1912: [RecidivismReleaseEvent(
                'CA', date(1910, 8, 12), date(1912, 8, 19), 'Upstate',
                _COUNTY_OF_RESIDENCE,
                date(1914, 7, 15), 'Sing Sing',
                ReincarcerationReturnType.NEW_ADMISSION)]
        }

        days_at_liberty_1 = (date(1910, 8, 12) - date(1908, 9, 19)).days
        days_at_liberty_2 = (date(1914, 7, 15) - date(1912, 8, 19)).days

        recidivism_combinations = calculator.map_recidivism_combinations(
            person, release_events_by_cohort, ALL_METRIC_INCLUSIONS_DICT)

        # For the first event:
        #   For the first 5 periods:
        #       5 periods * 2 methodologies = 10 metrics
        #   For the second 5 periods, there is an additional event-based count:
        #       5 periods * (1 person-based + 2 event-based) = 15 metrics
        #
        #   Person-level metrics: 1 count window * 2 methodologies = 2
        #
        # For the second event:
        #   10 periods * 2 methodologies = 20 metrics
        #
        # Person-level metrics: 1 count window * 2 methodologies = 2

        expected_count = (10 + 15 + 2 + 20 + 2)

        # Multiplied by 2 to include the county of residence field
        assert len(recidivism_combinations) == expected_count

        for combination, value in recidivism_combinations:
            if combination.get('metric_type') == MetricType.REINCARCERATION_RATE and \
                    combination.get('follow_up_period') < 2 or \
                    combination.get('return_type') == \
                    ReincarcerationReturnType.REVOCATION:
                self.assertEqual(0, value)
            else:
                self.assertEqual(1, value)

                if (combination.get('metric_type') == MetricType.REINCARCERATION_COUNT
                        and combination.get('person_id') is not None):
                    if combination.get('year') == 1910:
                        self.assertEqual(days_at_liberty_1, combination.get('days_at_liberty'))
                    else:
                        self.assertEqual(days_at_liberty_2, combination.get('days_at_liberty'))

    def test_map_recidivism_combinations_multiple_releases_in_year(self):
        """Tests the map_recidivism_combinations function where there are multiple releases in the same year."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='CA',
                                                 race=Race.BLACK)
        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='CA',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        release_events_by_cohort = {
            1908: [
                RecidivismReleaseEvent(
                    'CA', date(1905, 7, 19), date(1908, 1, 19), 'Hudson',
                    _COUNTY_OF_RESIDENCE,
                    date(1908, 5, 12), 'Upstate',
                    ReincarcerationReturnType.NEW_ADMISSION),
                NonRecidivismReleaseEvent(
                    'CA', date(1908, 5, 12), date(1908, 8, 19), 'Upstate',
                    _COUNTY_OF_RESIDENCE)
            ],
        }

        days_at_liberty_1 = (date(1908, 5, 12) - date(1908, 1, 19)).days

        recidivism_combinations = calculator.map_recidivism_combinations(
            person, release_events_by_cohort, ALL_METRIC_INCLUSIONS_DICT)

        expected_combos_count = self.expected_metric_combos_count(release_events_by_cohort)

        self.assertEqual(expected_combos_count, len(recidivism_combinations))
        self.assertEqual(len(set(id(combination_dict) for combination_dict, _ in recidivism_combinations)),
                         len(recidivism_combinations))

        for combination, value in recidivism_combinations:
            if combination.get('return_type') == ReincarcerationReturnType.REVOCATION:
                self.assertEqual(0, value)
            elif combination.get('metric_type') == MetricType.REINCARCERATION_COUNT:
                self.assertEqual(1, value)

                if combination.get('person_id') is not None:
                    self.assertEqual(days_at_liberty_1, combination.get('days_at_liberty'))

            elif combination.get('metric_type') != MetricType.REINCARCERATION_RATE \
                    or combination.get('methodology') == MetricMethodologyType.PERSON:
                if value == 0:
                    print(combination)
                self.assertEqual(1, value)

    def test_map_recidivism_combinations_no_recidivism(self):
        """Tests the map_recidivism_combinations function where there is no
        recidivism."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='CA',
                                                 race=Race.BLACK)
        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='CA',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        release_events_by_cohort = {
            2008: [NonRecidivismReleaseEvent('CA', date(2005, 7, 19),
                                             date(2008, 9, 19), 'Hudson',
                                             _COUNTY_OF_RESIDENCE)]
        }

        recidivism_combinations = calculator.map_recidivism_combinations(
            person, release_events_by_cohort, ALL_METRIC_INCLUSIONS_DICT)

        expected_combos_count = self.expected_metric_combos_count(release_events_by_cohort)

        self.assertEqual(expected_combos_count, len(recidivism_combinations))
        self.assertEqual(len(set(id(combination_dict) for combination_dict, _ in recidivism_combinations)),
                         len(recidivism_combinations))
        self.assertTrue(all(value == 0 for _combination, value
                            in recidivism_combinations))

    def test_map_recidivism_combinations_recidivated_after_last_period(self):
        """Tests the map_recidivism_combinations function where there is
        recidivism but it occurred after the last follow-up period we track."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='CA',
                                                 race=Race.BLACK)
        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='CA',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        release_events_by_cohort = {
            1998: [RecidivismReleaseEvent(
                'CA', date(1995, 7, 19), date(1998, 9, 19), 'Hudson',
                _COUNTY_OF_RESIDENCE,
                date(2008, 10, 12), 'Upstate',
                ReincarcerationReturnType.NEW_ADMISSION)]
        }

        days_at_liberty = (date(2008, 10, 12) - date(1998, 9, 19)).days

        recidivism_combinations = calculator.map_recidivism_combinations(
            person, release_events_by_cohort, ALL_METRIC_INCLUSIONS_DICT)

        expected_combos_count = self.expected_metric_combos_count(release_events_by_cohort)

        self.assertEqual(expected_combos_count, len(recidivism_combinations))
        self.assertEqual(len(set(id(combination_dict) for combination_dict, _ in recidivism_combinations)),
                         len(recidivism_combinations))

        assert all(value == 0 for _combination, value
                   in recidivism_combinations if
                   _combination['metric_type'] == MetricType.REINCARCERATION_RATE)
        assert all(value == 1 for _combination, value
                   in recidivism_combinations if
                   _combination['metric_type'] == MetricType.REINCARCERATION_COUNT and
                   _combination.get('return_type') !=
                   ReincarcerationReturnType.REVOCATION)
        assert all(_combination.get('days_at_liberty') == days_at_liberty
                   for _combination, _ in recidivism_combinations
                   if _combination['metric_type'] == MetricType.REINCARCERATION_COUNT
                   and _combination.get('person_id') is not None)

    def test_map_recidivism_combinations_multiple_races(self):
        """Tests the map_recidivism_combinations function where there is
        recidivism, and the person has more than one race."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race_white = StatePersonRace.new_with_defaults(state_code='CA',
                                                       race=Race.WHITE)

        race_black = StatePersonRace.new_with_defaults(state_code='MT',
                                                       race=Race.BLACK)

        person.races = [race_white, race_black]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='CA',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        release_events_by_cohort = {
            2008: [RecidivismReleaseEvent(
                'CA', date(2005, 7, 19), date(2008, 9, 19), 'Hudson',
                _COUNTY_OF_RESIDENCE,
                date(2014, 5, 12), 'Upstate',
                ReincarcerationReturnType.NEW_ADMISSION)]
        }

        days_at_liberty = (date(2014, 5, 12) - date(2008, 9, 19)).days

        recidivism_combinations = calculator.map_recidivism_combinations(
            person, release_events_by_cohort, ALL_METRIC_INCLUSIONS_DICT)

        expected_combos_count = self.expected_metric_combos_count(release_events_by_cohort)

        self.assertEqual(expected_combos_count, len(recidivism_combinations))
        self.assertEqual(len(set(id(combination_dict) for combination_dict, _ in recidivism_combinations)),
                         len(recidivism_combinations))

        for combination, value in recidivism_combinations:
            if combination.get('metric_type') == MetricType.REINCARCERATION_RATE and \
                    combination.get('follow_up_period') <= 5 or \
                    combination.get('return_type') == \
                    ReincarcerationReturnType.REVOCATION:
                self.assertEqual(0, value)
            elif (combination.get('metric_type') == MetricType.REINCARCERATION_COUNT
                  and combination.get('person_id') is not None):
                self.assertEqual(1, value)
                assert combination.get('days_at_liberty') == days_at_liberty
            else:
                self.assertEqual(1, value)

    def test_map_recidivism_combinations_multiple_ethnicities(self):
        """Tests the map_recidivism_combinations function where there is
        recidivism, and the person has more than one ethnicity."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='CA',
                                                 race=Race.BLACK)
        person.races = [race]

        ethnicity_hispanic = StatePersonEthnicity.new_with_defaults(
            state_code='CA',
            ethnicity=Ethnicity.HISPANIC)

        ethnicity_not_hispanic = StatePersonEthnicity.new_with_defaults(
            state_code='MT',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity_hispanic, ethnicity_not_hispanic]

        release_events_by_cohort = {
            2008: [RecidivismReleaseEvent(
                'CA', date(2005, 7, 19), date(2008, 9, 19), 'Hudson',
                _COUNTY_OF_RESIDENCE,
                date(2014, 5, 12), 'Upstate',
                ReincarcerationReturnType.NEW_ADMISSION)]
        }

        days_at_liberty = (date(2014, 5, 12) - date(2008, 9, 19)).days

        recidivism_combinations = calculator.map_recidivism_combinations(
            person, release_events_by_cohort, ALL_METRIC_INCLUSIONS_DICT)

        expected_combos_count = self.expected_metric_combos_count(release_events_by_cohort)

        self.assertEqual(expected_combos_count, len(recidivism_combinations))
        self.assertEqual(len(set(id(combination_dict) for combination_dict, _ in recidivism_combinations)),
                         len(recidivism_combinations))

        for combination, value in recidivism_combinations:
            if combination.get('metric_type') == MetricType.REINCARCERATION_RATE and \
                    combination.get('follow_up_period') <= 5 or \
                    combination.get('return_type') == \
                    ReincarcerationReturnType.REVOCATION:
                self.assertEqual(0, value)
            else:
                self.assertEqual(1, value)

                if (combination.get('metric_type') == MetricType.REINCARCERATION_COUNT
                        and combination.get('person_id') is not None):
                    assert combination.get('days_at_liberty') == days_at_liberty

    def test_map_recidivism_combinations_multiple_races_ethnicities(self):
        """Tests the map_recidivism_combinations function where there is
        recidivism, and the person has multiple races and multiple
        ethnicities."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race_white = StatePersonRace.new_with_defaults(state_code='CA',
                                                       race=Race.WHITE)

        race_black = StatePersonRace.new_with_defaults(state_code='MT',
                                                       race=Race.BLACK)

        person.races = [race_white, race_black]

        ethnicity_hispanic = StatePersonEthnicity.new_with_defaults(
            state_code='CA',
            ethnicity=Ethnicity.HISPANIC)

        ethnicity_not_hispanic = StatePersonEthnicity.new_with_defaults(
            state_code='MT',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity_hispanic, ethnicity_not_hispanic]

        release_events_by_cohort = {
            2008: [RecidivismReleaseEvent(
                'CA', date(2005, 7, 19), date(2008, 9, 19), 'Hudson',
                _COUNTY_OF_RESIDENCE,
                date(2014, 5, 12), 'Upstate',
                ReincarcerationReturnType.NEW_ADMISSION)]
        }

        days_at_liberty = (date(2014, 5, 12) - date(2008, 9, 19)).days

        recidivism_combinations = calculator.map_recidivism_combinations(
            person, release_events_by_cohort, ALL_METRIC_INCLUSIONS_DICT)

        expected_combos_count = self.expected_metric_combos_count(release_events_by_cohort)

        self.assertEqual(expected_combos_count, len(recidivism_combinations))
        self.assertEqual(len(set(id(combination_dict) for combination_dict, _ in recidivism_combinations)),
                         len(recidivism_combinations))

        for combination, value in recidivism_combinations:
            if combination.get('metric_type') == MetricType.REINCARCERATION_RATE and \
                    combination.get('follow_up_period') <= 5 or \
                    combination.get('return_type') == \
                    ReincarcerationReturnType.REVOCATION:
                self.assertEqual(0, value)
            else:
                self.assertEqual(1, value)

                if (combination.get('metric_type') == MetricType.REINCARCERATION_COUNT
                        and combination.get('person_id') is not None):
                    assert combination.get('days_at_liberty') == days_at_liberty

    def test_map_recidivism_combinations_revocation_parole(self):
        """Tests the map_recidivism_combinations function where there is
        recidivism, and they returned from a revocation of parole."""
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

        release_events_by_cohort = {
            2008: [RecidivismReleaseEvent(
                'CA', date(2005, 7, 19), date(2008, 9, 19), 'Hudson',
                _COUNTY_OF_RESIDENCE,
                date(2014, 5, 12), 'Upstate',
                ReincarcerationReturnType.REVOCATION,
                from_supervision_type=
                StateSupervisionPeriodSupervisionType.PAROLE)]
        }

        days_at_liberty = (date(2014, 5, 12) - date(2008, 9, 19)).days

        recidivism_combinations = calculator.map_recidivism_combinations(
            person, release_events_by_cohort, ALL_METRIC_INCLUSIONS_DICT)
        expected_combos_count = self.expected_metric_combos_count(release_events_by_cohort)

        self.assertEqual(expected_combos_count, len(recidivism_combinations))
        self.assertEqual(len(set(id(combination_dict) for combination_dict, _ in recidivism_combinations)),
                         len(recidivism_combinations))

        for combination, value in recidivism_combinations:
            if combination.get('metric_type') == MetricType.REINCARCERATION_RATE and \
                    combination.get('follow_up_period') <= 5 or \
                    combination.get('return_type') == \
                    ReincarcerationReturnType.NEW_ADMISSION or \
                    combination.get('from_supervision_type') == \
                    StateSupervisionPeriodSupervisionType.PROBATION or \
                    combination.get('source_violation_type') is not None:
                self.assertEqual(0, value)
            else:
                self.assertEqual(1, value)

                if (combination.get('metric_type') == MetricType.REINCARCERATION_COUNT
                        and combination.get('person_id') is not None):
                    assert combination.get('days_at_liberty') == days_at_liberty

    def test_map_recidivism_combinations_revocation_probation(self):
        """Tests the map_recidivism_combinations function where there is
        recidivism, and they returned from a revocation of parole."""
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

        release_events_by_cohort = {
            2008: [RecidivismReleaseEvent(
                'CA', date(2005, 7, 19), date(2008, 9, 19), 'Hudson',
                _COUNTY_OF_RESIDENCE,
                date(2014, 5, 12), 'Upstate',
                ReincarcerationReturnType.REVOCATION,
                from_supervision_type=
                StateSupervisionPeriodSupervisionType.PROBATION)]
        }

        days_at_liberty = (date(2014, 5, 12) - date(2008, 9, 19)).days

        recidivism_combinations = calculator.map_recidivism_combinations(
            person, release_events_by_cohort, ALL_METRIC_INCLUSIONS_DICT)

        expected_combos_count = self.expected_metric_combos_count(release_events_by_cohort)

        self.assertEqual(expected_combos_count, len(recidivism_combinations))
        self.assertEqual(len(set(id(combination_dict) for combination_dict, _ in recidivism_combinations)),
                         len(recidivism_combinations))

        for combination, value in recidivism_combinations:
            if combination.get('metric_type') == MetricType.REINCARCERATION_RATE and \
                    combination.get('follow_up_period') <= 5 or \
                    combination.get('return_type') == ReincarcerationReturnType.NEW_ADMISSION or \
                    combination.get('from_supervision_type') in {StateSupervisionPeriodSupervisionType.DUAL,
                                                                 StateSupervisionPeriodSupervisionType.PAROLE} or \
                    combination.get('source_violation_type') is not None:
                self.assertEqual(0, value)
            else:
                self.assertEqual(1, value)

                if (combination.get('metric_type') == MetricType.REINCARCERATION_COUNT
                        and combination.get('person_id') is not None):
                    assert combination.get('days_at_liberty') == days_at_liberty

    def test_map_recidivism_combinations_technical_revocation_parole(self):
        """Tests the map_recidivism_combinations function where there is
        recidivism, and they returned from a technical violation that resulted
        in the revocation of parole."""
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

        release_events_by_cohort = {
            2008: [RecidivismReleaseEvent(
                'CA', date(2005, 7, 19), date(2008, 9, 19), 'Hudson',
                _COUNTY_OF_RESIDENCE,
                date(2014, 5, 12), 'Upstate',
                ReincarcerationReturnType.REVOCATION,
                from_supervision_type=
                StateSupervisionPeriodSupervisionType.PAROLE,
                source_violation_type=StateSupervisionViolationType.TECHNICAL)]
        }

        days_at_liberty = (date(2014, 5, 12) - date(2008, 9, 19)).days

        recidivism_combinations = calculator.map_recidivism_combinations(
            person, release_events_by_cohort, ALL_METRIC_INCLUSIONS_DICT)

        expected_combos_count = self.expected_metric_combos_count(release_events_by_cohort)

        self.assertEqual(expected_combos_count, len(recidivism_combinations))
        self.assertEqual(len(set(id(combination_dict) for combination_dict, _ in recidivism_combinations)),
                         len(recidivism_combinations))

        for combination, value in recidivism_combinations:
            if combination.get('metric_type') == MetricType.REINCARCERATION_RATE and \
                    combination.get('follow_up_period') <= 5:
                self.assertEqual(0, value)
            elif combination.get('return_type') == \
                    ReincarcerationReturnType.NEW_ADMISSION or \
                    combination.get('from_supervision_type') == \
                    StateSupervisionPeriodSupervisionType.PROBATION:
                self.assertEqual(0, value)
            elif combination.get('from_supervision_type') is None or \
                    combination.get('from_supervision_type') == \
                    StateSupervisionPeriodSupervisionType.PAROLE:
                if combination.get('source_violation_type') not in \
                        [None, StateSupervisionViolationType.TECHNICAL]:
                    self.assertEqual(0, value)
                else:
                    self.assertEqual(1, value)
            else:
                self.assertEqual(1, value)

                if (combination.get('metric_type') == MetricType.REINCARCERATION_COUNT
                        and combination.get('person_id') is not None):
                    assert combination.get('days_at_liberty') == days_at_liberty

    def test_map_recidivism_combinations_count_metric_buckets(self):
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

        release_events_by_cohort = {
            2008: [RecidivismReleaseEvent(
                'CA', date(2005, 7, 19), date(2008, 9, 19), 'Hudson',
                _COUNTY_OF_RESIDENCE,
                date(2014, 5, 12), 'Upstate',
                ReincarcerationReturnType.NEW_ADMISSION)]
        }

        recidivism_combinations = calculator.map_recidivism_combinations(
            person, release_events_by_cohort, ALL_METRIC_INCLUSIONS_DICT)

        expected_combos_count = self.expected_metric_combos_count(release_events_by_cohort)

        self.assertEqual(expected_combos_count, len(recidivism_combinations))
        self.assertEqual(len(set(id(combination_dict) for combination_dict, _ in recidivism_combinations)),
                         len(recidivism_combinations))

        for combo, value in recidivism_combinations:
            if combo['metric_type'] == MetricType.REINCARCERATION_COUNT:
                assert combo['year'] == 2014
                assert combo['month'] == 5

                if combo.get('return_type') != \
                        ReincarcerationReturnType.REVOCATION:
                    self.assertEqual(1, value)
                else:
                    self.assertEqual(0, value)

    def test_map_recidivism_combinations_count_metric_no_recidivism(self):
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

        release_events_by_cohort = {
            2008: [NonRecidivismReleaseEvent('CA', date(2005, 7, 19),
                                             date(2008, 9, 19), 'Hudson')]
        }

        recidivism_combinations = calculator.map_recidivism_combinations(
            person, release_events_by_cohort, ALL_METRIC_INCLUSIONS_DICT)

        assert all(value == 0 for _combination, value
                   in recidivism_combinations)
        assert all(_combination['metric_type'] ==
                   MetricType.REINCARCERATION_RATE for _combination, value
                   in recidivism_combinations)

    @freeze_time('1914-09-30')
    def test_map_recidivism_combinations_count_relevant_periods(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1884, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='CA',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='CA',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        release_events_by_cohort = {
            1908: [RecidivismReleaseEvent(
                'TX', date(1905, 7, 19), date(1908, 9, 19), 'Hudson',
                _COUNTY_OF_RESIDENCE,
                date(1914, 3, 12), 'Upstate',
                ReincarcerationReturnType.NEW_ADMISSION)],
            1914: [RecidivismReleaseEvent(
                'TX', date(1914, 3, 12), date(1914, 7, 3), 'Hudson',
                _COUNTY_OF_RESIDENCE,
                date(1914, 9, 1), 'Upstate',
                ReincarcerationReturnType.NEW_ADMISSION)]
        }

        days_at_liberty_1 = (date(1914, 3, 12) - date(1908, 9, 19)).days
        days_at_liberty_2 = (date(1914, 9, 1) - date(1914, 7, 3)).days

        recidivism_combinations = calculator.map_recidivism_combinations(
            person, release_events_by_cohort, ALL_METRIC_INCLUSIONS_DICT)

        # For the first event:
        #   For the first 5 periods:
        #       5 periods * 2 methodologies = 10 metrics
        #   For the next 2 periods:
        #       2 periods * (1 person-based + 2 event-based) = 6 metrics
        #
        #   Count + liberty person-level metrics: 1 count window * 2 methodologies = 2
        #
        # For the second event:
        #   2 methodologies * 1 period = 2
        #
        #   Person-level metrics: (1 count window * 2 methodologies) = 2
        #
        #   Relevant metric_period_months person-level count metrics:
        #   (6 event-based + 4 person-based) = 10

        expected_combos_count = (10 + 6 + 2 + 2 + 2 + 10)

        self.assertEqual(expected_combos_count, len(recidivism_combinations))
        self.assertEqual(len(set(id(combination_dict) for combination_dict, _ in recidivism_combinations)),
                         len(recidivism_combinations))

        for combo, value in recidivism_combinations:
            if combo['metric_type'] == MetricType.REINCARCERATION_COUNT:

                assert combo['year'] == 1914
                assert combo['month'] in (3, 9)

                self.assertEqual(1, value)

                if combo.get('metric_period_months') > 1:
                    assert combo['year'] == 1914
                    assert combo['month'] == 9

                if combo.get('metric_type') == MetricType.REINCARCERATION_COUNT and combo.get('person_id') is not None:
                    if combo['month'] == 3:
                        assert combo.get('days_at_liberty') == days_at_liberty_1
                    else:
                        if combo.get('metric_period_months') == 1:
                            assert combo.get('days_at_liberty') == days_at_liberty_2
                        else:
                            assert combo.get('days_at_liberty') in (days_at_liberty_1, days_at_liberty_2)

    def test_map_recidivism_combinations_count_twice_in_month(self):
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

        release_events_by_cohort = {
            1908: [RecidivismReleaseEvent(
                'CA', date(1905, 7, 19), date(1908, 9, 19), 'Hudson',
                _COUNTY_OF_RESIDENCE,
                date(1914, 3, 12), 'Upstate',
                ReincarcerationReturnType.NEW_ADMISSION)],
            1914: [RecidivismReleaseEvent(
                'CA', date(1914, 3, 12), date(1914, 3, 19), 'Upstate',
                _COUNTY_OF_RESIDENCE,
                date(1914, 3, 30), 'Upstate',
                ReincarcerationReturnType.NEW_ADMISSION)]
        }

        days_at_liberty_1 = (date(1914, 3, 12) - date(1908, 9, 19)).days
        days_at_liberty_2 = (date(1914, 3, 30) - date(1914, 3, 19)).days

        recidivism_combinations = calculator.map_recidivism_combinations(
            person, release_events_by_cohort, ALL_METRIC_INCLUSIONS_DICT)

        # For the first event:
        #   For the first 5 periods:
        #       5 periods * 2 methodologies = 10 metrics
        #   For the second 5 periods
        #       5 periods * (1 person-based + 2 event-based) = 15 metrics
        #
        #   Person-level metrics: 1 count window * 2 methodologies = 2
        #
        # For the second event:
        #   10 periods * 2 methodologies = 20 metrics
        #
        # Person-level metrics: 1 event-based count window = 1

        expected_combos_count = (10 + 15 + 2 + 20 + 1)

        self.assertEqual(expected_combos_count, len(recidivism_combinations))
        self.assertEqual(len(set(id(combination_dict) for combination_dict, _ in recidivism_combinations)),
                         len(recidivism_combinations))

        for combo, value in recidivism_combinations:
            if combo['metric_type'] == MetricType.REINCARCERATION_COUNT:
                assert combo['year'] == 1914
                assert combo['month'] == 3

                self.assertEqual(1, value)

            if combo.get('metric_type') == MetricType.REINCARCERATION_COUNT and combo.get('person_id') is not None:
                if combo['release_facility'] == 'Hudson':
                    assert combo.get('days_at_liberty') == days_at_liberty_1
                else:
                    assert combo.get('days_at_liberty') == days_at_liberty_2


class TestCharacteristicCombinations(unittest.TestCase):
    """Tests the characteristic_combinations function."""
    def test_characteristic_combinations_rate(self):
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

        release_event = RecidivismReleaseEvent(
            'CA', date(2005, 7, 19), date(2008, 9, 19), 'Hudson',
            _COUNTY_OF_RESIDENCE,
            date(2014, 5, 12), 'Upstate',
            ReincarcerationReturnType.NEW_ADMISSION)

        characteristic_dict = calculator.characteristics_dict(person,
                                                              release_event,
                                                              ReincarcerationRecidivismRateMetric)

        expected_output = {'county_of_residence': 'county',
                           'release_cohort': 2008,
                           'release_facility': 'Hudson',
                           'stay_length_bucket': '36-48',
                           'age_bucket': '<25',
                           'gender': Gender.FEMALE,
                           'race': [Race.WHITE],
                           'ethnicity': [Ethnicity.NOT_HISPANIC],
                           'person_id': 12345,
                           'return_type': ReincarcerationReturnType.NEW_ADMISSION
                           }

        self.assertEqual(expected_output, characteristic_dict)

    def test_characteristic_combinations_count(self):
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

        release_event = RecidivismReleaseEvent(
            state_code='CA', original_admission_date=date(2005, 7, 19), release_date=date(2008, 9, 19),
            release_facility='Hudson', county_of_residence=_COUNTY_OF_RESIDENCE,
            reincarceration_date=date(2014, 5, 12), reincarceration_facility='Upstate',
            return_type=ReincarcerationReturnType.NEW_ADMISSION)

        characteristic_dict = calculator.characteristics_dict(person,
                                                              release_event,
                                                              ReincarcerationRecidivismCountMetric)

        expected_output = {'county_of_residence': 'county',
                           'release_facility': 'Hudson',
                           'stay_length_bucket': '36-48',
                           'age_bucket': '<25',
                           'gender': Gender.FEMALE,
                           'race': [Race.WHITE],
                           'ethnicity': [Ethnicity.NOT_HISPANIC],
                           'person_id': 12345,
                           'return_type': ReincarcerationReturnType.NEW_ADMISSION,
                           'days_at_liberty': (date(2014, 5, 12) - date(2008, 9, 19)).days,
                           'reincarceration_date': date(2014, 5, 12)
                           }

        self.assertEqual(expected_output, characteristic_dict)


def test_relevant_follow_up_periods():
    today = date(2018, 1, 26)

    assert calculator.relevant_follow_up_periods(
        date(2015, 1, 5), today, calculator.FOLLOW_UP_PERIODS) == [1, 2, 3, 4]
    assert calculator.relevant_follow_up_periods(
        date(2015, 1, 26), today, calculator.FOLLOW_UP_PERIODS) == [1, 2, 3, 4]
    assert calculator.relevant_follow_up_periods(
        date(2015, 1, 27), today, calculator.FOLLOW_UP_PERIODS) == [1, 2, 3]
    assert calculator.relevant_follow_up_periods(
        date(2016, 1, 5), today, calculator.FOLLOW_UP_PERIODS) == [1, 2, 3]
    assert calculator.relevant_follow_up_periods(
        date(2017, 4, 10), today, calculator.FOLLOW_UP_PERIODS) == [1]
    assert calculator.relevant_follow_up_periods(
        date(2018, 1, 5), today, calculator.FOLLOW_UP_PERIODS) == [1]
    assert calculator.relevant_follow_up_periods(
        date(2018, 2, 5), today, calculator.FOLLOW_UP_PERIODS) == []
