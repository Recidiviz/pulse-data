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
from more_itertools import one

from recidiviz.calculator.recidivism import calculator
from recidiviz.calculator.recidivism.release_event import ReleaseEvent, \
    RecidivismReleaseEvent, NonRecidivismReleaseEvent, ReincarcerationReturnType
from recidiviz.calculator.recidivism.metrics import RecidivismMethodologyType
from recidiviz.calculator.recidivism.release_event import \
    ReincarcerationReturnType, ReincarcerationReturnFromSupervisionType
from recidiviz.common.constants.state.state_supervision_violation import \
    StateSupervisionViolationType
from recidiviz.persistence.entity.state.entities import StatePerson, Gender,\
    StatePersonRace, Race, StatePersonEthnicity, Ethnicity


def test_reincarcerations():
    release_date = date.today()
    original_admission_date = release_date - relativedelta(years=4)
    reincarceration_date = release_date + relativedelta(years=3)
    second_release_date = reincarceration_date + relativedelta(years=1)

    first_event = RecidivismReleaseEvent(
        'CA', original_admission_date, release_date, 'Sing Sing',
        reincarceration_date, 'Sing Sing',
        ReincarcerationReturnType.NEW_ADMISSION)
    second_event = NonRecidivismReleaseEvent(
        'CA', reincarceration_date, second_release_date, 'Sing Sing')
    release_events = {2018: [first_event], 2022: [second_event]}

    expected_reincarcerations = {reincarceration_date:
                                 {'return_type': first_event.return_type,
                                  'from_supervision_type':
                                  first_event.from_supervision_type,
                                  'source_violation_type': None}}

    reincarcerations = calculator.reincarcerations(release_events)
    assert reincarcerations == expected_reincarcerations


def test_reincarcerations_empty():
    reincarcerations = calculator.reincarcerations({})
    assert reincarcerations == {}


def test_releases_in_window():
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
    assert len(reincarcerations) == 3


def test_releases_in_window_all_early():
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

    assert reincarcerations == []


def test_releases_in_window_all_late():
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

    assert reincarcerations == []


def test_releases_in_window_with_revocation_returns():
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
            ReincarcerationReturnFromSupervisionType.PAROLE}

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
    assert len(reincarcerations) == 3

    assert reincarcerations[0].get('return_type') == \
        ReincarcerationReturnType.REVOCATION
    assert reincarcerations[0].get('from_supervision_type') == \
        ReincarcerationReturnFromSupervisionType.PAROLE
    assert reincarcerations[1].get('return_type') == \
        ReincarcerationReturnType.REVOCATION
    assert reincarcerations[1].get('from_supervision_type') == \
        ReincarcerationReturnFromSupervisionType.PAROLE
    assert reincarcerations[2].get('return_type') == \
        ReincarcerationReturnType.NEW_ADMISSION
    assert reincarcerations[2].get('from_supervision_type') is None


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
    event = ReleaseEvent('CA', original_admission_date, release_date,
                         'Sing Sing')

    assert calculator.stay_length_from_event(event) == 9


def test_stay_length_from_event_same_month_earlier_date():
    original_admission_date = date(2013, 6, 17)
    release_date = date(2014, 6, 16)
    event = ReleaseEvent('NH', original_admission_date, release_date,
                         'Sing Sing')

    assert calculator.stay_length_from_event(event) == 11


def test_stay_length_from_event_same_month_same_date():
    original_admission_date = date(2013, 6, 17)
    release_date = date(2014, 6, 17)
    event = ReleaseEvent('TX', original_admission_date, release_date,
                         'Sing Sing')

    assert calculator.stay_length_from_event(event) == 12


def test_stay_length_from_event_same_month_later_date():
    original_admission_date = date(2013, 6, 17)
    release_date = date(2014, 6, 18)
    event = ReleaseEvent('UT', original_admission_date, release_date,
                         'Sing Sing')

    assert calculator.stay_length_from_event(event) == 12


def test_stay_length_from_event_later_month():
    original_admission_date = date(2013, 6, 17)
    release_date = date(2014, 8, 11)
    event = ReleaseEvent('HI', original_admission_date, release_date,
                         'Sing Sing')

    assert calculator.stay_length_from_event(event) == 13


def test_stay_length_from_event_original_admission_date_unknown():
    release_date = date(2014, 7, 11)
    event = ReleaseEvent('MT', None, release_date, 'Sing Sing')
    assert calculator.stay_length_from_event(event) is None


def test_stay_length_from_event_release_date_unknown():
    original_admission_date = date(2014, 7, 11)
    event = ReleaseEvent('UT', original_admission_date, None, 'Sing Sing')
    assert calculator.stay_length_from_event(event) is None


def test_stay_length_from_event_both_dates_unknown():
    event = ReleaseEvent('NH', None, None, 'Sing Sing')
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


def test_for_characteristics_races_ethnicities_one_race():
    race_white = StatePersonRace.new_with_defaults(state_code='CA',
                                                   race=Race.WHITE)

    characteristics = {'gender': 'female', 'age': '<25'}

    combinations = calculator.for_characteristics_races_ethnicities(
        [race_white], [], characteristics)

    assert combinations == [{},
                            {'gender': 'female'},
                            {'age': '<25'},
                            {'gender': 'female', 'age': '<25'},
                            {'race': Race.WHITE},
                            {'race': Race.WHITE, 'gender': 'female'},
                            {'age': '<25', 'race': Race.WHITE},
                            {'age': '<25', 'race': Race.WHITE,
                             'gender': 'female'}]


def test_for_characteristics_races_ethnicities_two_races():
    race_white = StatePersonRace.new_with_defaults(state_code='CA',
                                                   race=Race.WHITE)

    race_black = StatePersonRace.new_with_defaults(state_code='MT',
                                                   race=Race.BLACK)

    characteristics = {'gender': 'female', 'age': '<25'}

    combinations = calculator.for_characteristics_races_ethnicities(
        [race_white, race_black], [], characteristics)

    assert combinations == [{},
                            {'gender': 'female'},
                            {'age': '<25'},
                            {'gender': 'female', 'age': '<25'},
                            {'race': Race.WHITE},
                            {'race': Race.WHITE, 'gender': 'female'},
                            {'age': '<25', 'race': Race.WHITE},
                            {'age': '<25', 'race': Race.WHITE,
                             'gender': 'female'},
                            {'race': Race.BLACK},
                            {'race': Race.BLACK, 'gender': 'female'},
                            {'age': '<25', 'race': Race.BLACK},
                            {'age': '<25', 'race': Race.BLACK,
                             'gender': 'female'}]


def test_for_characteristics_races_ethnicities_one_ethnicity():
    ethnicity_hispanic = StatePersonEthnicity.new_with_defaults(
        state_code='CA',
        ethnicity=Ethnicity.HISPANIC)

    characteristics = {'gender': 'female', 'age': '<25'}

    combinations = calculator.for_characteristics_races_ethnicities(
        [], [ethnicity_hispanic], characteristics)

    assert combinations == [{},
                            {'gender': 'female'},
                            {'age': '<25'},
                            {'gender': 'female', 'age': '<25'},
                            {'ethnicity': Ethnicity.HISPANIC},
                            {'ethnicity': Ethnicity.HISPANIC,
                             'gender': 'female'},
                            {'age': '<25', 'ethnicity': Ethnicity.HISPANIC},
                            {'age': '<25', 'ethnicity': Ethnicity.HISPANIC,
                             'gender': 'female'}]


def test_for_characteristics_races_ethnicities_multiple_ethnicities():
    ethnicity_hispanic = StatePersonEthnicity.new_with_defaults(
        state_code='CA',
        ethnicity=Ethnicity.HISPANIC)

    ethnicity_not_hispanic = StatePersonEthnicity.new_with_defaults(
        state_code='FL',
        ethnicity=Ethnicity.NOT_HISPANIC)

    characteristics = {'gender': 'female', 'age': '<25'}

    combinations = calculator.for_characteristics_races_ethnicities(
        [], [ethnicity_hispanic, ethnicity_not_hispanic], characteristics)

    assert combinations == [{},
                            {'gender': 'female'},
                            {'age': '<25'},
                            {'gender': 'female', 'age': '<25'},
                            {'ethnicity': Ethnicity.HISPANIC},
                            {'ethnicity': Ethnicity.HISPANIC,
                             'gender': 'female'},
                            {'age': '<25', 'ethnicity': Ethnicity.HISPANIC},
                            {'age': '<25', 'ethnicity': Ethnicity.HISPANIC,
                             'gender': 'female'},
                            {'ethnicity': Ethnicity.NOT_HISPANIC},
                            {'ethnicity': Ethnicity.NOT_HISPANIC,
                             'gender': 'female'},
                            {'age': '<25', 'ethnicity': Ethnicity.NOT_HISPANIC},
                            {'age': '<25', 'ethnicity': Ethnicity.NOT_HISPANIC,
                             'gender': 'female'}
                            ]


def test_for_characteristics_races_ethnicities_one_race_one_ethnicity():
    race_white = StatePersonRace.new_with_defaults(state_code='CA',
                                                   race=Race.WHITE)

    ethnicity_hispanic = StatePersonEthnicity.new_with_defaults(
        state_code='CA',
        ethnicity=Ethnicity.HISPANIC)

    characteristics = {'gender': 'female', 'age': '<25'}

    combinations = calculator.for_characteristics_races_ethnicities(
        [race_white], [ethnicity_hispanic], characteristics)

    assert combinations == [{},
                            {'gender': 'female'},
                            {'age': '<25'},
                            {'gender': 'female', 'age': '<25'},
                            {'race': Race.WHITE},
                            {'race': Race.WHITE, 'gender': 'female'},
                            {'age': '<25', 'race': Race.WHITE},
                            {'age': '<25', 'race': Race.WHITE,
                             'gender': 'female'},
                            {'ethnicity': Ethnicity.HISPANIC},
                            {'ethnicity': Ethnicity.HISPANIC,
                             'gender': 'female'},
                            {'age': '<25', 'ethnicity': Ethnicity.HISPANIC},
                            {'age': '<25', 'ethnicity': Ethnicity.HISPANIC,
                             'gender': 'female'},
                            {'race': Race.WHITE,
                             'ethnicity': Ethnicity.HISPANIC},
                            {'race': Race.WHITE,
                             'ethnicity': Ethnicity.HISPANIC,
                             'gender': 'female'},
                            {'age': '<25', 'race': Race.WHITE,
                             'ethnicity': Ethnicity.HISPANIC},
                            {'race': Race.WHITE,
                             'ethnicity': Ethnicity.HISPANIC,
                             'age': '<25',
                             'gender': 'female'},
                            ]


def test_for_characteristics_races_ethnicities_multiple_races_one_ethnicity():
    race_white = StatePersonRace.new_with_defaults(state_code='CA',
                                                   race=Race.WHITE)

    race_black = StatePersonRace.new_with_defaults(state_code='CA',
                                                   race=Race.BLACK)

    ethnicity_hispanic = StatePersonEthnicity.new_with_defaults(
        state_code='CA',
        ethnicity=Ethnicity.HISPANIC)

    characteristics = {'age': '<25'}

    combinations = calculator.for_characteristics_races_ethnicities(
        [race_white, race_black], [ethnicity_hispanic], characteristics)

    assert combinations == [{},
                            {'age': '<25'},
                            {'race': Race.WHITE},
                            {'age': '<25', 'race': Race.WHITE},
                            {'race': Race.BLACK},
                            {'age': '<25', 'race': Race.BLACK},
                            {'ethnicity': Ethnicity.HISPANIC},
                            {'age': '<25', 'ethnicity': Ethnicity.HISPANIC},
                            {'race': Race.WHITE,
                             'ethnicity': Ethnicity.HISPANIC},
                            {'age': '<25', 'race': Race.WHITE,
                             'ethnicity': Ethnicity.HISPANIC},
                            {'race': Race.BLACK,
                             'ethnicity': Ethnicity.HISPANIC},
                            {'age': '<25', 'race': Race.BLACK,
                             'ethnicity': Ethnicity.HISPANIC}
                            ]


def test_for_characteristics_races_ethnicities_one_race_multiple_ethnicities():
    race_white = StatePersonRace.new_with_defaults(state_code='CA',
                                                   race=Race.WHITE)

    ethnicity_hispanic = StatePersonEthnicity.new_with_defaults(
        state_code='CA',
        ethnicity=Ethnicity.HISPANIC)

    ethnicity_not_hispanic = StatePersonEthnicity.new_with_defaults(
        state_code='CA',
        ethnicity=Ethnicity.NOT_HISPANIC)

    characteristics = {'age': '<25'}

    combinations = calculator.for_characteristics_races_ethnicities(
        [race_white], [ethnicity_hispanic, ethnicity_not_hispanic],
        characteristics)

    assert combinations == [{},
                            {'age': '<25'},
                            {'race': Race.WHITE},
                            {'age': '<25', 'race': Race.WHITE},
                            {'ethnicity': Ethnicity.HISPANIC},
                            {'age': '<25', 'ethnicity': Ethnicity.HISPANIC},
                            {'ethnicity': Ethnicity.NOT_HISPANIC},
                            {'age': '<25', 'ethnicity': Ethnicity.NOT_HISPANIC},
                            {'race': Race.WHITE,
                             'ethnicity': Ethnicity.HISPANIC},
                            {'age': '<25', 'race': Race.WHITE,
                             'ethnicity': Ethnicity.HISPANIC},
                            {'race': Race.WHITE,
                             'ethnicity': Ethnicity.NOT_HISPANIC},
                            {'age': '<25', 'race': Race.WHITE,
                             'ethnicity': Ethnicity.NOT_HISPANIC}
                            ]


def test_for_characteristics_races_ethnicities_no_races_or_ethnicities():
    characteristics = {'age': '<25'}

    combinations = calculator.for_characteristics_races_ethnicities(
        [], [], characteristics)

    assert combinations == [{}, {'age': '<25'}]


def test_for_characteristics_races_ethnicities_multiple_races_and_ethnicities():
    race_white = StatePersonRace.new_with_defaults(state_code='CA',
                                                   race=Race.WHITE)

    race_black = StatePersonRace.new_with_defaults(state_code='CA',
                                                   race=Race.BLACK)

    ethnicity_hispanic = StatePersonEthnicity.new_with_defaults(
        state_code='CA',
        ethnicity=Ethnicity.HISPANIC)

    ethnicity_not_hispanic = StatePersonEthnicity.new_with_defaults(
        state_code='CA',
        ethnicity=Ethnicity.NOT_HISPANIC)

    characteristics = {'age': '<25'}

    combinations = calculator.for_characteristics_races_ethnicities(
        [race_white, race_black], [ethnicity_hispanic, ethnicity_not_hispanic],
        characteristics)

    assert combinations == [{},
                            {'age': '<25'},
                            {'race': Race.WHITE},
                            {'age': '<25', 'race': Race.WHITE},
                            {'race': Race.BLACK},
                            {'age': '<25', 'race': Race.BLACK},
                            {'ethnicity': Ethnicity.HISPANIC},
                            {'age': '<25', 'ethnicity': Ethnicity.HISPANIC},
                            {'ethnicity': Ethnicity.NOT_HISPANIC},
                            {'age': '<25', 'ethnicity': Ethnicity.NOT_HISPANIC},
                            {'race': Race.WHITE,
                             'ethnicity': Ethnicity.HISPANIC},
                            {'age': '<25', 'race': Race.WHITE,
                             'ethnicity': Ethnicity.HISPANIC},
                            {'race': Race.WHITE,
                             'ethnicity': Ethnicity.NOT_HISPANIC},
                            {'age': '<25', 'race': Race.WHITE,
                             'ethnicity': Ethnicity.NOT_HISPANIC},
                            {'race': Race.BLACK,
                             'ethnicity': Ethnicity.HISPANIC},
                            {'age': '<25', 'race': Race.BLACK,
                             'ethnicity': Ethnicity.HISPANIC},
                            {'race': Race.BLACK,
                             'ethnicity': Ethnicity.NOT_HISPANIC},
                            {'age': '<25', 'race': Race.BLACK,
                             'ethnicity': Ethnicity.NOT_HISPANIC}
                            ]


def test_for_characteristics():
    characteristics = {'race': 'black', 'gender': 'female', 'age': '<25'}
    combinations = calculator.for_characteristics(characteristics)

    assert combinations == [{},
                            {'race': 'black'},
                            {'gender': 'female'},
                            {'age': '<25'},
                            {'race': 'black', 'gender': 'female'},
                            {'age': '<25', 'race': 'black'},
                            {'age': '<25', 'gender': 'female'},
                            {'age': '<25', 'race': 'black', 'gender': 'female'}]


def test_for_characteristics_one_characteristic():
    characteristics = {'gender': 'male'}
    combinations = calculator.for_characteristics(characteristics)

    assert combinations == [{}, {'gender': 'male'}]


def test_augmented_combo_list_methodologies():
    base_combo = {'age': '<25', 'race': 'black', 'gender': 'female'}

    person_combo_list = calculator.augmented_combo_list(
        base_combo, 'CA', RecidivismMethodologyType.PERSON, 8)

    for combo in person_combo_list:
        assert combo['methodology'] == RecidivismMethodologyType.PERSON
        assert combo['follow_up_period'] == 8

    event_combo_list = calculator.augmented_combo_list(
        base_combo, 'CA', RecidivismMethodologyType.EVENT, 8)

    for combo in event_combo_list:
        assert combo['methodology'] == RecidivismMethodologyType.EVENT
        assert combo['follow_up_period'] == 8


def test_augmented_combo_list_return_info():
    """Tests that all return_type and from_supervision_type values are being
    covered."""
    base_combo = {'age': '<25', 'race': 'black', 'gender': 'female'}

    combo_list = calculator.augmented_combo_list(
        base_combo, 'CA', RecidivismMethodologyType.PERSON, 8)

    parameter_list = {}

    for return_type in ReincarcerationReturnType:
        parameter_list[return_type] = False

    for from_supervision_type in ReincarcerationReturnFromSupervisionType:
        parameter_list[from_supervision_type] = False

    for combo in combo_list:
        assert combo['methodology'] == RecidivismMethodologyType.PERSON
        assert combo['follow_up_period'] == 8

        return_type = combo.get('return_type')

        if return_type:
            parameter_list[return_type] = True

        from_supervision_type = combo.get('from_supervision_type')

        if from_supervision_type:
            parameter_list[from_supervision_type] = True

    for value in parameter_list.values():
        assert value


def test_recidivism_value_for_metric():
    combo = {'age': '<25', 'race': 'black', 'gender': 'female'}

    value = calculator.recidivism_value_for_metric(combo, None, None, None)

    assert value == 1


def test_recidivism_value_for_metric_new_admission():
    combo = {'age': '<25', 'race': 'black', 'gender': 'female',
             'return_type': ReincarcerationReturnType.NEW_ADMISSION}

    value = calculator.recidivism_value_for_metric(
        combo, ReincarcerationReturnType.NEW_ADMISSION, None, None)

    assert value == 1


def test_recidivism_value_for_metric_not_new_admission():
    combo = {'age': '<25', 'race': 'black', 'gender': 'female',
             'return_type': ReincarcerationReturnType.NEW_ADMISSION}

    value = calculator.recidivism_value_for_metric(
        combo, ReincarcerationReturnType.REVOCATION, None, None)

    assert value == 0


def test_recidivism_value_for_metric_parole_revocation():
    combo = {'age': '<25', 'race': 'black', 'gender': 'female',
             'return_type': ReincarcerationReturnType.REVOCATION,
             'from_supervision_type':
                 ReincarcerationReturnFromSupervisionType.PAROLE}

    value = calculator.recidivism_value_for_metric(
        combo, ReincarcerationReturnType.REVOCATION,
        ReincarcerationReturnFromSupervisionType.PAROLE, None)

    assert value == 1


def test_recidivism_value_for_metric_probation_revocation():
    combo = {'age': '<25', 'race': 'black', 'gender': 'female',
             'return_type': ReincarcerationReturnType.REVOCATION,
             'from_supervision_type':
                 ReincarcerationReturnFromSupervisionType.PROBATION}

    value = calculator.recidivism_value_for_metric(
        combo, ReincarcerationReturnType.REVOCATION,
        ReincarcerationReturnFromSupervisionType.PROBATION, None)

    assert value == 1


def test_recidivism_value_for_metric_parole_revocation_source_violation():
    combo = {'age': '<25', 'race': 'black', 'gender': 'female',
             'return_type': ReincarcerationReturnType.REVOCATION,
             'from_supervision_type':
                 ReincarcerationReturnFromSupervisionType.PAROLE,
             'source_violation_type': StateSupervisionViolationType.TECHNICAL}

    value = calculator.recidivism_value_for_metric(
        combo, ReincarcerationReturnType.REVOCATION,
        ReincarcerationReturnFromSupervisionType.PAROLE,
        StateSupervisionViolationType.TECHNICAL)

    assert value == 1


def test_recidivism_value_for_metric_probation_revocation_source_violation():
    combo = {'age': '<25', 'race': 'black', 'gender': 'female',
             'return_type': ReincarcerationReturnType.REVOCATION,
             'from_supervision_type':
                 ReincarcerationReturnFromSupervisionType.PROBATION,
             'source_violation_type': StateSupervisionViolationType.FELONY}

    value = calculator.recidivism_value_for_metric(
        combo, ReincarcerationReturnType.REVOCATION,
        ReincarcerationReturnFromSupervisionType.PROBATION,
        StateSupervisionViolationType.FELONY)

    assert value == 1


def test_recidivism_value_for_metric_not_revocation():
    combo = {'age': '<25', 'race': 'black', 'gender': 'female',
             'return_type': ReincarcerationReturnType.REVOCATION,
             'from_supervision_type':
                 ReincarcerationReturnFromSupervisionType.PROBATION}

    value = calculator.recidivism_value_for_metric(
        combo, ReincarcerationReturnType.NEW_ADMISSION,
        ReincarcerationReturnFromSupervisionType.PROBATION, None)

    assert value == 0


def test_recidivism_value_for_metric_not_supervision_type():
    combo = {'age': '<25', 'race': 'black', 'gender': 'female',
             'return_type': ReincarcerationReturnType.REVOCATION,
             'from_supervision_type':
                 ReincarcerationReturnFromSupervisionType.PROBATION}

    value = calculator.recidivism_value_for_metric(
        combo, ReincarcerationReturnType.REVOCATION,
        ReincarcerationReturnFromSupervisionType.PAROLE, None)

    assert value == 0


def test_recidivism_value_for_metric_not_source_violation_type():
    combo = {'age': '<25', 'race': 'black', 'gender': 'female',
             'return_type': ReincarcerationReturnType.REVOCATION,
             'from_supervision_type':
                 ReincarcerationReturnFromSupervisionType.PROBATION,
             'source_violation_type': StateSupervisionViolationType.FELONY}

    value = calculator.recidivism_value_for_metric(
        combo, ReincarcerationReturnType.REVOCATION,
        ReincarcerationReturnFromSupervisionType.PAROLE,
        StateSupervisionViolationType.TECHNICAL)

    assert value == 0


def test_augment_combination():
    combo = {'age': '<25', 'race': 'black', 'gender': 'female'}

    parameters = {'A': 'a', 'B': 9}

    augmented = calculator.augment_combination(combo, parameters)

    assert augmented == {'age': '<25',
                         'A': 'a', 'B': 9,
                         'race': 'black',
                         'gender': 'female'}
    assert augmented != combo


class TestMapRecidivismCombinations:
    """Tests the map_recidivism_combinations function."""

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
                date(2014, 5, 12), 'Upstate',
                ReincarcerationReturnType.NEW_ADMISSION)]
        }

        inclusions = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
            'release_facility': True,
            'stay_length_bucket': True
        }

        recidivism_combinations = calculator.map_recidivism_combinations(
            person, release_events_by_cohort, inclusions)

        # 64 combinations of demographics, facility, & stay length
        # * 40 combinations of methodology/return type/supervision type
        # * 10 periods +
        # 64 * 2 count windows * 40 combos of methodology etc. = 30720 metrics
        assert len(recidivism_combinations) == 30720

        for combination, value in recidivism_combinations:
            if combination.get('metric_type') == 'rate' and \
                    combination.get('follow_up_period') <= 5 or \
                    combination.get('return_type') == \
                    ReincarcerationReturnType.REVOCATION:
                assert value == 0
            else:
                assert value == 1

    def test_map_recidivism_combinations_multiple_in_period(self):
        """Tests the map_recidivism_combinations function where there are
        multiple instances of recidivism within a follow-up period."""
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
                date(1910, 8, 12), 'Upstate',
                ReincarcerationReturnType.NEW_ADMISSION)],
            1912: [RecidivismReleaseEvent(
                'CA', date(1910, 8, 12), date(1912, 8, 19), 'Upstate',
                date(1914, 7, 12), 'Sing Sing',
                ReincarcerationReturnType.NEW_ADMISSION)]
        }

        inclusions = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
            'release_facility': True,
            'stay_length_bucket': True
        }

        recidivism_combinations = calculator.map_recidivism_combinations(
            person, release_events_by_cohort, inclusions)

        # For the first event:
        #   For the first 5 periods:
        #       64 combinations of characteristics
        #       * 40 combinations of methodology/return type/supervision type
        #       * 5 periods + 64 * 2 count windows * 40 combos of
        #       methodology etc.= 17920 metrics
        #   For the second 5 periods, there is an additional event-based count:
        #       64 combinations of characteristics
        #       * (40 combinations of methodology/return type/supervision type
        #           + 20 more instances) * 5 periods = 19200 metrics
        #
        # For the second event:
        #   64 combinations * 40 combos * 10 periods +
        #   64 combos * 2 count windows * 40 combos of methodology etc.
        #   = 30720 metrics
        assert len(recidivism_combinations) == (17920 + 19200 + 30720)

        for combination, value in recidivism_combinations:
            if combination.get('metric_type') == 'rate' and \
                    combination.get('follow_up_period') < 2 or \
                    combination.get('return_type') == \
                    ReincarcerationReturnType.REVOCATION:
                assert value == 0
            else:
                assert value == 1

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
                                             date(2008, 9, 19), 'Hudson')]
        }

        inclusions = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
            'release_facility': True,
            'stay_length_bucket': True
        }

        recidivism_combinations = calculator.map_recidivism_combinations(
            person, release_events_by_cohort, inclusions)

        # 64 combinations of demographics, facility, & stay length
        # * 40 combinations of methodology/return type/supervision type
        # * 10 periods + 0 count windows = 25600 metrics

        assert len(recidivism_combinations) == 25600
        assert all(value == 0 for _combination, value
                   in recidivism_combinations)

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
            2008: [RecidivismReleaseEvent(
                'CA', date(2005, 7, 19), date(2008, 9, 19), 'Hudson',
                date(2018, 10, 12), 'Upstate',
                ReincarcerationReturnType.NEW_ADMISSION)]
        }

        inclusions = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
            'release_facility': True,
            'stay_length_bucket': True
        }

        recidivism_combinations = calculator.map_recidivism_combinations(
            person, release_events_by_cohort, inclusions)

        # 64 combinations of demographics, facility, & stay length
        # * 40 combinations of methodology/return type/supervision type
        # * 10 periods + 0 count metrics. = 25600 metrics
        assert len(recidivism_combinations) == 25600
        assert all(value == 0 for _combination, value
                   in recidivism_combinations if
                   _combination['metric_type'] == 'rate')
        assert all(value == 1 for _combination, value
                   in recidivism_combinations if
                   _combination['metric_type'] == 'count' and
                   _combination.get('return_type') !=
                   ReincarcerationReturnType.REVOCATION)

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
                date(2014, 5, 12), 'Upstate',
                ReincarcerationReturnType.NEW_ADMISSION)]
        }

        inclusions = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
            'release_facility': True,
            'stay_length_bucket': True
        }

        recidivism_combinations = calculator.map_recidivism_combinations(
            person, release_events_by_cohort, inclusions)

        # 96 combinations of demographics, facility, & stay length
        # * 40 combinations of methodology/return type/supervision type
        # * 10 periods + 96 * 2 count windows * 40 combos of methodology etc.
        # = 46080 metrics
        assert len(recidivism_combinations) == 46080

        for combination, value in recidivism_combinations:
            if combination.get('metric_type') == 'rate' and \
                    combination.get('follow_up_period') <= 5 or \
                    combination.get('return_type') == \
                    ReincarcerationReturnType.REVOCATION:
                assert value == 0
            else:
                assert value == 1

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
                date(2014, 5, 12), 'Upstate',
                ReincarcerationReturnType.NEW_ADMISSION)]
        }

        inclusions = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
            'release_facility': True,
            'stay_length_bucket': True
        }

        recidivism_combinations = calculator.map_recidivism_combinations(
            person, release_events_by_cohort, inclusions)

        # 96 combinations of demographics, facility, & stay length
        # * 40 combinations of methodology/return type/supervision type
        # * 10 periods + 96 * 2 count windows * 40 combos of methodology etc.
        # = 46080 metrics
        assert len(recidivism_combinations) == 46080

        for combination, value in recidivism_combinations:
            if combination.get('metric_type') == 'rate' and \
                    combination.get('follow_up_period') <= 5 or \
                    combination.get('return_type') == \
                    ReincarcerationReturnType.REVOCATION:
                assert value == 0
            else:
                assert value == 1

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
                date(2014, 5, 12), 'Upstate',
                ReincarcerationReturnType.NEW_ADMISSION)]
        }

        inclusions = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
            'release_facility': True,
            'stay_length_bucket': True
        }

        recidivism_combinations = calculator.map_recidivism_combinations(
            person, release_events_by_cohort, inclusions)

        # 144 combinations of demographics, facility, & stay length
        # * 40 combinations of methodology/return type/supervision type
        # * 10 periods + 144 * 2 count windows * 40 combos of methodology etc.
        # = 69120 metrics
        assert len(recidivism_combinations) == 69120

        for combination, value in recidivism_combinations:
            if combination.get('metric_type') == 'rate' and \
                    combination.get('follow_up_period') <= 5 or \
                    combination.get('return_type') == \
                    ReincarcerationReturnType.REVOCATION:
                assert value == 0
            else:
                assert value == 1

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
                date(2014, 5, 12), 'Upstate',
                ReincarcerationReturnType.REVOCATION,
                from_supervision_type=
                ReincarcerationReturnFromSupervisionType.PAROLE)]
        }

        inclusions = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
            'release_facility': True,
            'stay_length_bucket': True
        }

        recidivism_combinations = calculator.map_recidivism_combinations(
            person, release_events_by_cohort, inclusions)

        # 64 combinations of demographics, facility, & stay length
        # * 40 combinations of methodology/return type/supervision type
        # * 10 periods + 64 * 2 count windows * 40 combos of methodology etc.
        # = 30720 metrics
        assert len(recidivism_combinations) == 30720

        for combination, value in recidivism_combinations:
            if combination.get('metric_type') == 'rate' and \
                    combination.get('follow_up_period') <= 5 or \
                    combination.get('return_type') == \
                    ReincarcerationReturnType.NEW_ADMISSION or \
                    combination.get('from_supervision_type') == \
                    ReincarcerationReturnFromSupervisionType.PROBATION or \
                    combination.get('source_violation_type') is not None:
                assert value == 0
            else:
                assert value == 1

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
                date(2014, 5, 12), 'Upstate',
                ReincarcerationReturnType.REVOCATION,
                from_supervision_type=
                ReincarcerationReturnFromSupervisionType.PROBATION)]
        }

        inclusions = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
            'release_facility': True,
            'stay_length_bucket': True
        }

        recidivism_combinations = calculator.map_recidivism_combinations(
            person, release_events_by_cohort, inclusions)

        # 64 combinations of demographics, facility, & stay length
        # * 40 combinations of methodology/return type/supervision type
        # * 10 periods + 64 * 2 count windows * 40 combos of methodology etc.
        # = 30720 metrics
        assert len(recidivism_combinations) == 30720

        for combination, value in recidivism_combinations:
            if combination.get('metric_type') == 'rate' and \
                    combination.get('follow_up_period') <= 5 or \
                    combination.get('return_type') == \
                    ReincarcerationReturnType.NEW_ADMISSION or \
                    combination.get('from_supervision_type') == \
                    ReincarcerationReturnFromSupervisionType.PAROLE or \
                    combination.get('source_violation_type') is not None:
                assert value == 0
            else:
                assert value == 1

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
                date(2014, 5, 12), 'Upstate',
                ReincarcerationReturnType.REVOCATION,
                from_supervision_type=
                ReincarcerationReturnFromSupervisionType.PAROLE,
                source_violation_type=StateSupervisionViolationType.TECHNICAL)]
        }

        inclusions = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
            'release_facility': True,
            'stay_length_bucket': True
        }

        recidivism_combinations = calculator.map_recidivism_combinations(
            person, release_events_by_cohort, inclusions)

        # 64 combinations of demographics, facility, & stay length
        # * 40 combinations of methodology/return type/supervision type
        # * 10 periods + 64 * 2 count windows * 40 combos of methodology etc.
        # = 30720 metrics
        assert len(recidivism_combinations) == 30720

        for combination, value in recidivism_combinations:
            if combination.get('metric_type') == 'rate' and \
                    combination.get('follow_up_period') <= 5:
                assert value == 0
            elif combination.get('return_type') == \
                    ReincarcerationReturnType.NEW_ADMISSION or \
                    combination.get('from_supervision_type') == \
                    ReincarcerationReturnFromSupervisionType.PROBATION:
                assert value == 0
            elif combination.get('from_supervision_type') is None or \
                    combination.get('from_supervision_type') == \
                    ReincarcerationReturnFromSupervisionType.PAROLE:
                if combination.get('source_violation_type') not in \
                        [None, StateSupervisionViolationType.TECHNICAL]:
                    assert value == 0
                else:
                    assert value == 1
            else:
                assert value == 1

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
                date(2014, 5, 12), 'Upstate',
                ReincarcerationReturnType.NEW_ADMISSION)]
        }

        inclusions = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
            'release_facility': True,
            'stay_length_bucket': True
        }

        recidivism_combinations = calculator.map_recidivism_combinations(
            person, release_events_by_cohort, inclusions)

        num_count_metrics = 0
        num_year_metrics = 0
        num_month_metrics = 0

        for combo, value in recidivism_combinations:
            if combo['metric_type'] == 'count':
                num_count_metrics += 1

                assert combo['start_date'].year == 2014
                if combo['start_date'].month == 5:
                    num_month_metrics += 1
                    # Month bucket
                    assert combo['start_date'] == date(2014, 5, 1)
                    assert combo['end_date'] == date(2014, 5, 31)
                else:
                    num_year_metrics += 1
                    # Year bucket
                    assert combo['start_date'] == date(2014, 1, 1)
                    assert combo['end_date'] == date(2014, 12, 31)
                if combo.get('return_type') != \
                        ReincarcerationReturnType.REVOCATION:
                    assert value == 1
                else:
                    assert value == 0

        # Expected count metrics: 64 * 2 count windows * 40 combos of
        # methodology etc. = 3840
        # Half of those should be year metrics, and half should be month metrics
        assert num_count_metrics == 5120
        assert num_year_metrics == 2560
        assert num_month_metrics == 2560

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

        inclusions = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
            'release_facility': True,
            'stay_length_bucket': True
        }

        recidivism_combinations = calculator.map_recidivism_combinations(
            person, release_events_by_cohort, inclusions)

        assert all(value == 0 for _combination, value
                   in recidivism_combinations)
        assert all(_combination['metric_type'] == 'rate' for _combination, value
                   in recidivism_combinations)

    def test_map_recidivism_combos_count_metric_return_after_last_period(self):
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
            1990: [RecidivismReleaseEvent(
                'CA', date(1985, 7, 19), date(1990, 9, 19), 'Hudson',
                date(2014, 5, 12), 'Upstate',
                ReincarcerationReturnType.NEW_ADMISSION)]
        }

        inclusions = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
            'release_facility': True,
            'stay_length_bucket': True
        }

        recidivism_combinations = calculator.map_recidivism_combinations(
            person, release_events_by_cohort, inclusions)

        assert all(value == 0 for _combination, value
                   in recidivism_combinations)
        assert all(_combination['metric_type'] == 'rate' for _combination, value
                   in recidivism_combinations)

    def test_map_recidivism_combinations_count_twice_in_year(self):
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
                'TX', date(1905, 7, 19), date(1908, 9, 19), 'Hudson',
                date(1914, 3, 12), 'Upstate',
                ReincarcerationReturnType.NEW_ADMISSION)],
            1914: [RecidivismReleaseEvent(
                'TX', date(1914, 3, 12), date(1914, 7, 3), 'Hudson',
                date(1914, 9, 1), 'Upstate',
                ReincarcerationReturnType.NEW_ADMISSION)]
        }

        inclusions = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
            'release_facility': True,
            'stay_length_bucket': True
        }

        recidivism_combinations = calculator.map_recidivism_combinations(
            person, release_events_by_cohort, inclusions)

        # For the first event:
        #   For the first 5 periods:
        #       64 combinations of characteristics
        #       * 40 combinations of methodology/return type/supervision type
        #       * 5 periods = 12800 metrics
        #   For the second 5 periods
        #       64 combinations of characteristics
        #       * (40 combinations of methodology/return type/supervision type
        #           + 20 more instances) * 5 periods = 19200 metrics
        #
        #   Count window: 64 * 2 count windows * 40 combos = 5120
        #
        # For the second event:
        #   64 combinations * 40 combos * 10 periods +
        #   64 * 2 count windows * 20 event-based-combos  +
        #   64 * 1 count window (month) * 20 person-based-combos = 29440 metrics
        assert len(recidivism_combinations) == (12800 + 19200 + 5120 + 29440)

        num_count_metrics = 0
        num_person_year_metrics = 0
        num_event_year_metrics = 0
        num_march_month_metrics = 0
        num_sept_month_metrics = 0

        for combo, value in recidivism_combinations:
            if combo['metric_type'] == 'count':
                num_count_metrics += 1

                assert combo['start_date'].year == 1914
                if combo['start_date'].month == 3:
                    num_march_month_metrics += 1
                    # March month bucket
                    assert combo['start_date'] == date(1914, 3, 1)
                    assert combo['end_date'] == date(1914, 3, 31)
                elif combo['start_date'].month == 9:
                    num_sept_month_metrics += 1
                    # September month bucket
                    assert combo['start_date'] == date(1914, 9, 1)
                    assert combo['end_date'] == date(1914, 9, 30)
                else:
                    if combo['methodology'] == RecidivismMethodologyType.EVENT:
                        num_event_year_metrics += 1
                    else:
                        num_person_year_metrics += 1

                    # Year bucket
                    assert combo['start_date'] == date(1914, 1, 1)
                    assert combo['end_date'] == date(1914, 12, 31)
                if combo.get('return_type') != \
                        ReincarcerationReturnType.REVOCATION:
                    assert value == 1
                else:
                    assert value == 0

        assert num_count_metrics == (5120 + 3840)
        assert num_person_year_metrics == 1280
        assert num_event_year_metrics == 2560
        assert num_march_month_metrics == 2560
        assert num_sept_month_metrics == 2560

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
                date(1914, 3, 12), 'Upstate',
                ReincarcerationReturnType.NEW_ADMISSION)],
            1914: [RecidivismReleaseEvent(
                'CA', date(1914, 3, 12), date(1914, 3, 19), 'Hudson',
                date(1914, 3, 30), 'Upstate',
                ReincarcerationReturnType.NEW_ADMISSION)]
        }

        inclusions = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
            'release_facility': True,
            'stay_length_bucket': True
        }

        recidivism_combinations = calculator.map_recidivism_combinations(
            person, release_events_by_cohort, inclusions)

        # For the first event:
        #   For the first 5 periods:
        #       64 combinations of characteristics
        #       * 40 combinations of methodology/return type/supervision type
        #       * 5 periods = 12800 metrics
        #   For the second 5 periods
        #       64 combinations of characteristics
        #       * (40 combinations of methodology/return type/supervision type
        #           + 20 more instances) * 5 periods = 19200 metrics
        #
        #   Count window: 64 * 2 count windows * 40 combos = 5120
        #
        # For the second event:
        #   64 combinations * 40 combos * 10 periods +
        #   64 * 2 count windows * 20 event-based-combos  +
        #   64 * 0 person-based-combos = 28160 metrics
        assert len(recidivism_combinations) == (12800 + 19200 + 5120 + 28160)

        num_count_metrics = 0
        num_person_year_metrics = 0
        num_event_year_metrics = 0
        num_person_month_metrics = 0
        num_event_month_metrics = 0
        num_march_month_metrics = 0

        for combo, value in recidivism_combinations:
            if combo['metric_type'] == 'count':
                num_count_metrics += 1

                assert combo['start_date'].year == 1914
                if combo['start_date'].month == 3:
                    num_march_month_metrics += 1
                    # March month bucket
                    assert combo['start_date'] == date(1914, 3, 1)
                    assert combo['end_date'] == date(1914, 3, 31)

                    if combo['methodology'] == RecidivismMethodologyType.EVENT:
                        num_event_month_metrics += 1
                    else:
                        num_person_month_metrics += 1

                else:
                    if combo['methodology'] == RecidivismMethodologyType.EVENT:
                        num_event_year_metrics += 1
                    else:
                        num_person_year_metrics += 1

                    # Year bucket
                    assert combo['start_date'] == date(1914, 1, 1)
                    assert combo['end_date'] == date(1914, 12, 31)
                if combo.get('return_type') != \
                        ReincarcerationReturnType.REVOCATION:
                    assert value == 1
                else:
                    assert value == 0

        assert num_count_metrics == (5120 + 2560)
        assert num_person_year_metrics == 1280
        assert num_event_year_metrics == 2560
        assert num_march_month_metrics == (2560 + 1280)
        assert num_person_month_metrics == 1280
        assert num_event_month_metrics == 2560


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

        release_event = RecidivismReleaseEvent(
            'CA', date(2005, 7, 19), date(2008, 9, 19), 'Hudson',
            date(2014, 5, 12), 'Upstate',
            ReincarcerationReturnType.NEW_ADMISSION)

        inclusions = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
            'release_facility': True,
            'stay_length_bucket': True
        }

        combinations = calculator.characteristic_combinations(
            person, release_event, inclusions)

        # 64 combinations of demographics, facility, & stay length
        assert len(combinations) == 64

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

        release_event = RecidivismReleaseEvent(
            'CA', date(2005, 7, 19), date(2008, 9, 19), 'Hudson',
            date(2014, 5, 12), 'Upstate',
            ReincarcerationReturnType.NEW_ADMISSION)

        inclusions = {
            'age_bucket': False,
            'gender': True,
            'race': True,
            'ethnicity': True,
            'release_facility': True,
            'stay_length_bucket': True
        }

        combinations = calculator.characteristic_combinations(
            person, release_event, inclusions)

        # 32 combinations of demographics, facility, & stay length
        assert len(combinations) == 32

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

        release_event = RecidivismReleaseEvent(
            'CA', date(2005, 7, 19), date(2008, 9, 19), 'Hudson',
            date(2014, 5, 12), 'Upstate',
            ReincarcerationReturnType.NEW_ADMISSION)

        inclusions = {
            'age_bucket': True,
            'gender': False,
            'race': True,
            'ethnicity': True,
            'release_facility': True,
            'stay_length_bucket': True
        }

        combinations = calculator.characteristic_combinations(
            person, release_event, inclusions)

        # 32 combinations of demographics, facility, & stay length
        assert len(combinations) == 32

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

        release_event = RecidivismReleaseEvent(
            'CA', date(2005, 7, 19), date(2008, 9, 19), 'Hudson',
            date(2014, 5, 12), 'Upstate',
            ReincarcerationReturnType.NEW_ADMISSION)

        inclusions = {
            'age_bucket': True,
            'gender': True,
            'race': False,
            'ethnicity': True,
            'release_facility': True,
            'stay_length_bucket': True
        }

        combinations = calculator.characteristic_combinations(
            person, release_event, inclusions)

        # 32 combinations of demographics, facility, & stay length
        assert len(combinations) == 32

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

        release_event = RecidivismReleaseEvent(
            'CA', date(2005, 7, 19), date(2008, 9, 19), 'Hudson',
            date(2014, 5, 12), 'Upstate',
            ReincarcerationReturnType.NEW_ADMISSION)

        inclusions = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': False,
            'release_facility': True,
            'stay_length_bucket': True
        }

        combinations = calculator.characteristic_combinations(
            person, release_event, inclusions)

        # 32 combinations of demographics, facility, & stay length
        assert len(combinations) == 32

        for combo in combinations:
            assert combo.get('ethnicity') is None

    def test_characteristic_combinations_exclude_release_facility(self):
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
            date(2014, 5, 12), 'Upstate',
            ReincarcerationReturnType.NEW_ADMISSION)

        inclusions = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
            'release_facility': False,
            'stay_length_bucket': True
        }

        combinations = calculator.characteristic_combinations(
            person, release_event, inclusions)

        # 32 combinations of demographics, facility, & stay length
        assert len(combinations) == 32

        for combo in combinations:
            assert combo.get('release_facility') is None

    def test_characteristic_combinations_exclude_stay_length(self):
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
            date(2014, 5, 12), 'Upstate',
            ReincarcerationReturnType.NEW_ADMISSION)

        inclusions = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
            'release_facility': True,
            'stay_length_bucket': False
        }

        combinations = calculator.characteristic_combinations(
            person, release_event, inclusions)

        # 32 combinations of demographics, facility, & stay length
        assert len(combinations) == 32

        for combo in combinations:
            assert combo.get('stay_length') is None

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

        release_event = RecidivismReleaseEvent(
            'CA', date(2005, 7, 19), date(2008, 9, 19), 'Hudson',
            date(2014, 5, 12), 'Upstate',
            ReincarcerationReturnType.NEW_ADMISSION)

        inclusions = {
            'age_bucket': False,
            'gender': True,
            'race': True,
            'ethnicity': False,
            'release_facility': True,
            'stay_length_bucket': False
        }

        combinations = calculator.characteristic_combinations(
            person, release_event, inclusions)

        # 8 combinations of demographics, facility, & stay length
        assert len(combinations) == 8

        for combo in combinations:
            assert combo.get('age_bucket') is None
            assert combo.get('ethnicity') is None
            assert combo.get('stay_length_bucket') is None

    def test_characteristic_combinations_exclude_all(self):
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
            date(2014, 5, 12), 'Upstate',
            ReincarcerationReturnType.NEW_ADMISSION)

        inclusions = {
            'age_bucket': False,
            'gender': False,
            'race': False,
            'ethnicity': False,
            'release_facility': False,
            'stay_length_bucket': False
        }

        combinations = calculator.characteristic_combinations(
            person, release_event, inclusions)

        #  combinations of demographics, facility, & stay length
        assert len(combinations) == 1
        assert one(combinations) == {}
