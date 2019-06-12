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


from datetime import date
from datetime import datetime

from dateutil.relativedelta import relativedelta

from recidiviz.calculator.recidivism import calculator, ReleaseEvent, \
    RecidivismReleaseEvent, NonRecidivismReleaseEvent
from recidiviz.calculator.recidivism.metrics import RecidivismMethodologyType
from recidiviz.calculator.recidivism.release_event import \
    ReincarcerationReturnType
from recidiviz.persistence.entity.state.entities import StatePerson, Gender


def test_reincarceration_dates():
    release_date = date.today()
    original_admission_date = release_date - relativedelta(years=4)
    reincarceration_date = release_date + relativedelta(years=3)
    second_release_date = reincarceration_date + relativedelta(years=1)

    first_event = RecidivismReleaseEvent(
        original_admission_date, release_date, 'Sing Sing',
        reincarceration_date, 'Sing Sing')
    second_event = NonRecidivismReleaseEvent(
        reincarceration_date, second_release_date, 'Sing Sing')
    release_events = {2018: [first_event], 2022: [second_event]}

    release_dates = calculator.reincarceration_dates(release_events)
    assert release_dates == [reincarceration_date]


def test_reincarceration_dates_empty():
    release_dates = calculator.reincarceration_dates({})
    assert release_dates == []


def test_count_releases_in_window():
    # Too early
    release_2012 = date(2012, 4, 30)
    # Just right
    release_2016 = date(2016, 5, 13)
    release_2020 = date(2020, 11, 20)
    release_2021 = date(2021, 5, 13)
    # Too late
    release_2022 = date(2022, 5, 13)
    all_reincarceration_dates = [release_2012, release_2016, release_2020,
                                 release_2021, release_2022]

    start_date = date(2016, 5, 13)

    reincarcerations = calculator.count_reincarcerations_in_window(
        start_date, 6, all_reincarceration_dates)
    assert reincarcerations == 3


def test_count_releases_in_window_all_early():
    # Too early
    release_2012 = date(2012, 4, 30)
    release_2016 = date(2016, 5, 13)
    release_2020 = date(2020, 11, 20)
    release_2021 = date(2021, 5, 13)
    release_2022 = date(2022, 5, 13)
    all_reincarceration_dates = [release_2012, release_2016, release_2020,
                                 release_2021, release_2022]

    start_date = date(2026, 5, 13)

    reincarcerations = calculator.count_reincarcerations_in_window(
        start_date, 6, all_reincarceration_dates)
    assert reincarcerations == 0


def test_count_releases_in_window_all_late():
    # Too late
    release_2012 = date(2012, 4, 30)
    release_2016 = date(2016, 5, 13)
    release_2020 = date(2020, 11, 20)
    release_2021 = date(2021, 5, 13)
    release_2022 = date(2022, 5, 13)
    all_reincarceration_dates = [release_2012, release_2016, release_2020,
                                 release_2021, release_2022]

    start_date = date(2006, 5, 13)

    reincarcerations = calculator.count_reincarcerations_in_window(
        start_date, 5, all_reincarceration_dates)
    assert reincarcerations == 0


def test_earliest_recidivated_follow_up_period_later_month_in_year():
    release_date = date(2012, 4, 20)
    reincarceration_date = date(2016, 5, 13)

    earliest_period = calculator.earliest_recidivated_follow_up_period(
        release_date, reincarceration_date)
    assert earliest_period == 5


def test_earliest_recidivated_follow_up_period_same_month_in_year_later_day():
    release_date = date(2012, 4, 20)
    reincarceration_date = date(2016, 4, 21)

    earliest_period = calculator.earliest_recidivated_follow_up_period(
        release_date, reincarceration_date)
    assert earliest_period == 5


def test_earliest_recidivated_follow_up_period_same_month_in_year_earlier_day():
    release_date = date(2012, 4, 20)
    reincarceration_date = date(2016, 4, 19)

    earliest_period = calculator.earliest_recidivated_follow_up_period(
        release_date, reincarceration_date)
    assert earliest_period == 4


def test_earliest_recidivated_follow_up_period_same_month_in_year_same_day():
    release_date = date(2012, 4, 20)
    reincarceration_date = date(2016, 4, 20)

    earliest_period = calculator.earliest_recidivated_follow_up_period(
        release_date, reincarceration_date)
    assert earliest_period == 4


def test_earliest_recidivated_follow_up_period_earlier_month_in_year():
    release_date = date(2012, 4, 20)
    reincarceration_date = date(2016, 3, 31)

    earliest_period = calculator.earliest_recidivated_follow_up_period(
        release_date, reincarceration_date)
    assert earliest_period == 4


def test_earliest_recidivated_follow_up_period_same_year():
    release_date = date(2012, 4, 20)
    reincarceration_date = date(2012, 5, 13)

    earliest_period = calculator.earliest_recidivated_follow_up_period(
        release_date, reincarceration_date)
    assert earliest_period == 1


def test_earliest_recidivated_follow_up_period_no_reincarceration():
    release_date = date(2012, 4, 30)

    earliest_period = calculator.earliest_recidivated_follow_up_period(
        release_date, None)
    assert earliest_period is None


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


def test_age_at_date_earlier_month():
    birthdate = date(1989, 6, 17)
    check_date = date(2014, 4, 15)
    person = StatePerson.new_with_defaults(birthdate=birthdate)

    assert calculator.age_at_date(person, check_date) == 24


def test_age_at_date_same_month_earlier_date():
    birthdate = date(1989, 6, 17)
    check_date = date(2014, 6, 16)
    person = StatePerson.new_with_defaults(birthdate=birthdate)

    assert calculator.age_at_date(person, check_date) == 24


def test_age_at_date_same_month_same_date():
    birthdate = date(1989, 6, 17)
    check_date = date(2014, 6, 17)
    person = StatePerson.new_with_defaults(birthdate=birthdate)

    assert calculator.age_at_date(person, check_date) == 25


def test_age_at_date_same_month_later_date():
    birthdate = date(1989, 6, 17)
    check_date = date(2014, 6, 18)
    person = StatePerson.new_with_defaults(birthdate=birthdate)

    assert calculator.age_at_date(person, check_date) == 25


def test_age_at_date_later_month():
    birthdate = date(1989, 6, 17)
    check_date = date(2014, 7, 11)
    person = StatePerson.new_with_defaults(birthdate=birthdate)

    assert calculator.age_at_date(person, check_date) == 25


def test_age_at_date_birthdate_unknown():
    assert calculator.age_at_date(
        StatePerson.new_with_defaults(), datetime.today()) is None


def test_age_bucket():
    assert calculator.age_bucket(24) == '<25'
    assert calculator.age_bucket(27) == '25-29'
    assert calculator.age_bucket(30) == '30-34'
    assert calculator.age_bucket(39) == '35-39'
    assert calculator.age_bucket(40) == '40<'


def test_stay_length_from_event_earlier_month_and_date():
    original_admission_date = date(2013, 6, 17)
    release_date = date(2014, 4, 15)
    event = ReleaseEvent(original_admission_date, release_date, 'Sing Sing')

    assert calculator.stay_length_from_event(event) == 9


def test_stay_length_from_event_same_month_earlier_date():
    original_admission_date = date(2013, 6, 17)
    release_date = date(2014, 6, 16)
    event = ReleaseEvent(original_admission_date, release_date, 'Sing Sing')

    assert calculator.stay_length_from_event(event) == 11


def test_stay_length_from_event_same_month_same_date():
    original_admission_date = date(2013, 6, 17)
    release_date = date(2014, 6, 17)
    event = ReleaseEvent(original_admission_date, release_date, 'Sing Sing')

    assert calculator.stay_length_from_event(event) == 12


def test_stay_length_from_event_same_month_later_date():
    original_admission_date = date(2013, 6, 17)
    release_date = date(2014, 6, 18)
    event = ReleaseEvent(original_admission_date, release_date, 'Sing Sing')

    assert calculator.stay_length_from_event(event) == 12


def test_stay_length_from_event_later_month():
    original_admission_date = date(2013, 6, 17)
    release_date = date(2014, 8, 11)
    event = ReleaseEvent(original_admission_date, release_date, 'Sing Sing')

    assert calculator.stay_length_from_event(event) == 13


def test_stay_length_from_event_original_admission_date_unknown():
    release_date = date(2014, 7, 11)
    event = ReleaseEvent(None, release_date, 'Sing Sing')
    assert calculator.stay_length_from_event(event) is None


def test_stay_length_from_event_release_date_unknown():
    original_admission_date = date(2014, 7, 11)
    event = ReleaseEvent(original_admission_date, None, 'Sing Sing')
    assert calculator.stay_length_from_event(event) is None


def test_stay_length_from_event_both_dates_unknown():
    event = ReleaseEvent(None, None, 'Sing Sing')
    assert calculator.stay_length_from_event(event) is None


def test_stay_length_bucket():
    assert calculator.stay_length_bucket(None) is None
    assert calculator.stay_length_bucket(11) == '<12'
    assert calculator.stay_length_bucket(12) == '12-24'
    assert calculator.stay_length_bucket(20) == '12-24'
    assert calculator.stay_length_bucket(24) == '24-36'
    assert calculator.stay_length_bucket(30) == '24-36'
    assert calculator.stay_length_bucket(36) == '36-48'
    assert calculator.stay_length_bucket(40) == '36-48'
    assert calculator.stay_length_bucket(48) == '48-60'
    assert calculator.stay_length_bucket(50) == '48-60'
    assert calculator.stay_length_bucket(60) == '60-72'
    assert calculator.stay_length_bucket(70) == '60-72'
    assert calculator.stay_length_bucket(72) == '72-84'
    assert calculator.stay_length_bucket(80) == '72-84'
    assert calculator.stay_length_bucket(84) == '84-96'
    assert calculator.stay_length_bucket(96) == '96-108'
    assert calculator.stay_length_bucket(100) == '96-108'
    assert calculator.stay_length_bucket(108) == '108-120'
    assert calculator.stay_length_bucket(110) == '108-120'
    assert calculator.stay_length_bucket(120) == '120<'
    assert calculator.stay_length_bucket(130) == '120<'


def test_for_characteristics():
    characteristics = {'race': 'black', 'sex': 'female', 'age': '<25'}
    combinations = calculator.for_characteristics(characteristics)

    assert combinations == [{},
                            {'race': 'black'},
                            {'sex': 'female'},
                            {'age': '<25'},
                            {'race': 'black', 'sex': 'female'},
                            {'age': '<25', 'race': 'black'},
                            {'age': '<25', 'sex': 'female'},
                            {'age': '<25', 'race': 'black', 'sex': 'female'}]


def test_for_characteristics_one_characteristic():
    characteristics = {'sex': 'male'}
    combinations = calculator.for_characteristics(characteristics)

    assert combinations == [{}, {'sex': 'male'}]


def test_augment_combination():
    combo = {'age': '<25', 'race': 'black', 'sex': 'female'}
    augmented = calculator.augment_combination(combo,
                                               RecidivismMethodologyType.EVENT,
                                               8)

    assert augmented == {'age': '<25',
                         'follow_up_period': 8,
                         'methodology': RecidivismMethodologyType.EVENT,
                         'race': 'black',
                         'sex': 'female'}
    assert augmented != combo


class TestMapRecidivismCombinations:
    """Tests the map_recidivism_combinations function."""

    def test_map_recidivism_combinations(self):
        """Tests the map_recidivism_combinations function where there is
        recidivism."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        release_events_by_cohort = {
            2008: [RecidivismReleaseEvent(
                date(2005, 7, 19), date(2008, 9, 19), 'Hudson',
                date(2014, 5, 12), 'Upstate',
                ReincarcerationReturnType.NEW_ADMISSION)]
        }

        recidivism_combinations = calculator.map_recidivism_combinations(
            person, release_events_by_cohort)

        # 32 combinations of demographics, facility, and stay length
        # * 2 methodologies * 10 periods = 640 metrics
        # TODO(1781): Update this number once races and ethnicities are
        #  implemented
        assert len(recidivism_combinations) == 640

        for combination, value in recidivism_combinations:
            if combination['follow_up_period'] <= 5:
                assert value == 0
            else:
                assert value == 1

    def test_map_recidivism_combinations_multiple_in_period(self):
        """Tests the map_recidivism_combinations function where there are
        multiple instances of recidivism within a follow-up period."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        release_events_by_cohort = {
            1908: [RecidivismReleaseEvent(
                date(1905, 7, 19), date(1908, 9, 19), 'Hudson',
                date(1910, 8, 12), 'Upstate',
                ReincarcerationReturnType.NEW_ADMISSION)],
            1912: [RecidivismReleaseEvent(
                date(1910, 8, 12), date(1912, 8, 19), 'Upstate',
                date(1914, 7, 12), 'Sing Sing',
                ReincarcerationReturnType.NEW_ADMISSION)]
        }

        recidivism_combinations = calculator.map_recidivism_combinations(
            person, release_events_by_cohort)

        # For the first event:
        #   For the first 5 periods:
        #       32 combinations of characteristics
        #       * 2 methodologies * 5 periods = 320 metrics
        #   For the second 5 periods, there is an additional event-based count:
        #       32 combinations of demographics, facility, and stay length
        #       * (2 methodologies + 1 more instance) * 5 periods = 480 metrics
        #
        # For the second event:
        #   32 combinations * 2 methodologies * 10 periods = 640 metrics
        # TODO(1781): Update this number once races and ethnicities are
        #  implemented
        assert len(recidivism_combinations) == (320 + 480 + 640)

        for combination, value in recidivism_combinations:
            if combination['follow_up_period'] < 2:
                assert value == 0
            else:
                assert value == 1

    def test_map_recidivism_combinations_no_recidivism(self):
        """Tests the map_recidivism_combinations function where there is no
        recidivism."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        release_events_by_cohort = {
            2008: [NonRecidivismReleaseEvent(date(2005, 7, 19),
                                             date(2008, 9, 19), 'Hudson')]
        }

        recidivism_combinations = calculator.map_recidivism_combinations(
            person, release_events_by_cohort)

        # 32 combinations of demographics, facility, and stay length
        # * 2 methodologies * 10 periods = 640 metrics
        # TODO(1781): Update this number once races and ethnicities are
        #  implemented
        assert len(recidivism_combinations) == 320
        assert all(value == 0 for _combination, value
                   in recidivism_combinations)

    def test_map_recidivism_combinations_recidivated_after_last_period(self):
        """Tests the map_recidivism_combinations function where there is
        recidivism but it occurred after the last follow-up period we track."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        release_events_by_cohort = {
            2008: [RecidivismReleaseEvent(
                date(2005, 7, 19), date(2008, 9, 19), 'Hudson',
                date(2018, 10, 12), 'Upstate',
                ReincarcerationReturnType.NEW_ADMISSION)]
        }

        recidivism_combinations = calculator.map_recidivism_combinations(
            person, release_events_by_cohort)

        # 32 combinations of demographics, facility, and stay length
        # * 2 methodologies * 10 periods = 640 metrics
        # TODO(1781): Update this number once races and ethnicities are
        #  implemented
        assert len(recidivism_combinations) == 640
        assert all(value == 0 for _combination, value
                   in recidivism_combinations)
