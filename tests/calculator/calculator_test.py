# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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


import pytest
from datetime import date
from datetime import datetime
from dateutil.relativedelta import relativedelta
from google.appengine.ext import ndb
from google.appengine.ext import testbed

from ..context import calculator
from calculator import calculator
from scraper.us_ny.us_ny_record import UsNyRecord
from models.inmate import Inmate
from models.inmate_facility_snapshot import InmateFacilitySnapshot


def test_first_entrance():
    today = date.today()
    record = UsNyRecord(custody_date=today)
    assert calculator.first_entrance(record) == today


def test_subsequent_entrance():
    today = date.today()
    two_years_ago = today - relativedelta(years=2)
    record = UsNyRecord(custody_date=two_years_ago, last_custody_date=today)
    assert calculator.subsequent_entrance(record) == today


def test_final_release():
    today = date.today()
    record = UsNyRecord(is_released=True, last_release_date=today)
    assert calculator.final_release(record) == today


def test_final_release_incarcerated():
    record = UsNyRecord(is_released=False)
    assert calculator.final_release(record) is None


def test_first_facility():
    now = datetime.utcnow()
    record = UsNyRecord(id="parent-record")
    unrelated_record = UsNyRecord(id="unrelated-record")
    most_recent_snapshot_overall = InmateFacilitySnapshot(parent=unrelated_record.key, snapshot_date=now,
                                                          facility="Washington")
    most_recent_snapshot_for_record = InmateFacilitySnapshot(parent=record.key, snapshot_date=now, facility="Marcy")
    less_recent_snapshot_for_record = InmateFacilitySnapshot(parent=record.key, snapshot_date=now, facility="Greene")

    first_facility = calculator.first_facility(record, [most_recent_snapshot_overall, most_recent_snapshot_for_record,
                                                        less_recent_snapshot_for_record])
    assert first_facility == "Greene"


def test_first_facility_none_for_record():
    now = datetime.utcnow()
    record = UsNyRecord(id="parent-record")
    unrelated_record = UsNyRecord(id="unrelated-record")
    most_recent_snapshot_overall = InmateFacilitySnapshot(parent=unrelated_record.key, snapshot_date=now,
                                                          facility="Washington")
    least_recent_snapshot_overall = InmateFacilitySnapshot(parent=unrelated_record.key, snapshot_date=now,
                                                           facility="Greene")

    first_facility = calculator.first_facility(record, [most_recent_snapshot_overall, least_recent_snapshot_overall])
    assert first_facility is None


def test_first_facility_none_at_all():
    record = UsNyRecord(id="parent-record")

    first_facility = calculator.first_facility(record, [])
    assert first_facility is None


def test_last_facility():
    now = datetime.utcnow()
    record = UsNyRecord(id="parent-record")
    unrelated_record = UsNyRecord(id="unrelated-record")
    most_recent_snapshot_overall = InmateFacilitySnapshot(parent=unrelated_record.key, snapshot_date=now,
                                                          facility="Washington")
    most_recent_snapshot_for_record = InmateFacilitySnapshot(parent=record.key, snapshot_date=now, facility="Marcy")
    less_recent_snapshot_for_record = InmateFacilitySnapshot(parent=record.key, snapshot_date=now, facility="Greene")

    last_facility = calculator.last_facility(record, [most_recent_snapshot_overall, most_recent_snapshot_for_record,
                                                      less_recent_snapshot_for_record])
    assert last_facility == "Marcy"


def test_last_facility_none_for_record():
    now = datetime.utcnow()
    record = UsNyRecord(id="parent-record")
    unrelated_record = UsNyRecord(id="unrelated-record")
    most_recent_snapshot_overall = InmateFacilitySnapshot(parent=unrelated_record.key, snapshot_date=now,
                                                          facility="Washington")
    least_recent_snapshot_overall = InmateFacilitySnapshot(parent=unrelated_record.key, snapshot_date=now,
                                                           facility="Greene")

    last_facility = calculator.last_facility(record, [most_recent_snapshot_overall, least_recent_snapshot_overall])
    assert last_facility is None


def test_last_facility_none_at_all():
    record = UsNyRecord(id="parent-record")

    last_facility = calculator.last_facility(record, [])
    assert last_facility is None


def test_release_dates():
    release_date = date.today()
    original_entry_date = release_date - relativedelta(years=4)
    reincarceration_date = release_date + relativedelta(years=3)
    second_release_date = reincarceration_date + relativedelta(years=1)

    first_event = calculator.RecidivismEvent.recidivism_event(original_entry_date, release_date, "Sing Sing",
                                                              reincarceration_date, "Sing Sing", False)
    second_event = calculator.RecidivismEvent.non_recidivism_event(reincarceration_date, second_release_date,
                                                                   "Sing Sing")
    recidivism_events = {2018: first_event, 2022: second_event}

    release_dates = calculator.release_dates(recidivism_events)
    assert release_dates == [release_date, second_release_date]


def test_release_dates_empty():
    release_dates = calculator.release_dates({})
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
    all_release_dates = [release_2012, release_2016, release_2020, release_2021, release_2022]

    start_date = date(2016, 5, 13)

    releases = calculator.count_releases_in_window(start_date, 6, all_release_dates)
    assert releases == 3


def test_count_releases_in_window_all_early():
    # Too early
    release_2012 = date(2012, 4, 30)
    release_2016 = date(2016, 5, 13)
    release_2020 = date(2020, 11, 20)
    release_2021 = date(2021, 5, 13)
    release_2022 = date(2022, 5, 13)
    all_release_dates = [release_2012, release_2016, release_2020, release_2021, release_2022]

    start_date = date(2026, 5, 13)

    releases = calculator.count_releases_in_window(start_date, 6, all_release_dates)
    assert releases == 0


def test_count_releases_in_window_all_late():
    # Too late
    release_2012 = date(2012, 4, 30)
    release_2016 = date(2016, 5, 13)
    release_2020 = date(2020, 11, 20)
    release_2021 = date(2021, 5, 13)
    release_2022 = date(2022, 5, 13)
    all_release_dates = [release_2012, release_2016, release_2020, release_2021, release_2022]

    start_date = date(2006, 5, 13)

    releases = calculator.count_releases_in_window(start_date, 5, all_release_dates)
    assert releases == 0


def test_earliest_recidivated_follow_up_period_later_month_in_year():
    release_date = date(2012, 4, 20)
    reincarceration_date = date(2016, 5, 13)

    earliest_period = calculator.earliest_recidivated_follow_up_period(release_date, reincarceration_date)
    assert earliest_period == 5


def test_earliest_recidivated_follow_up_period_same_month_in_year_later_day():
    release_date = date(2012, 4, 20)
    reincarceration_date = date(2016, 4, 21)

    earliest_period = calculator.earliest_recidivated_follow_up_period(release_date, reincarceration_date)
    assert earliest_period == 5


def test_earliest_recidivated_follow_up_period_same_month_in_year_earlier_day():
    release_date = date(2012, 4, 20)
    reincarceration_date = date(2016, 4, 19)

    earliest_period = calculator.earliest_recidivated_follow_up_period(release_date, reincarceration_date)
    assert earliest_period == 4


def test_earliest_recidivated_follow_up_period_same_month_in_year_same_day():
    release_date = date(2012, 4, 20)
    reincarceration_date = date(2016, 4, 20)

    earliest_period = calculator.earliest_recidivated_follow_up_period(release_date, reincarceration_date)
    assert earliest_period == 4


def test_earliest_recidivated_follow_up_period_earlier_month_in_year():
    release_date = date(2012, 4, 20)
    reincarceration_date = date(2016, 3, 31)

    earliest_period = calculator.earliest_recidivated_follow_up_period(release_date, reincarceration_date)
    assert earliest_period == 4


def test_earliest_recidivated_follow_up_period_same_year():
    release_date = date(2012, 4, 20)
    reincarceration_date = date(2012, 5, 13)

    earliest_period = calculator.earliest_recidivated_follow_up_period(release_date, reincarceration_date)
    assert earliest_period == 1


def test_earliest_recidivated_follow_up_period_no_reincarceration():
    release_date = date(2012, 4, 30)

    earliest_period = calculator.earliest_recidivated_follow_up_period(release_date, None)
    assert earliest_period is None


def test_relevant_follow_up_periods():
    today = date(2018, 1, 26)

    assert calculator.relevant_follow_up_periods(date(2015, 1, 5), today,
                                                 calculator.FOLLOW_UP_PERIODS) == [1, 2, 3, 4]
    assert calculator.relevant_follow_up_periods(date(2015, 1, 26), today,
                                                 calculator.FOLLOW_UP_PERIODS) == [1, 2, 3, 4]
    assert calculator.relevant_follow_up_periods(date(2015, 1, 27), today,
                                                 calculator.FOLLOW_UP_PERIODS) == [1, 2, 3]
    assert calculator.relevant_follow_up_periods(date(2016, 1, 5), today,
                                                 calculator.FOLLOW_UP_PERIODS) == [1, 2, 3]
    assert calculator.relevant_follow_up_periods(date(2017, 4, 10), today,
                                                 calculator.FOLLOW_UP_PERIODS) == [1]
    assert calculator.relevant_follow_up_periods(date(2018, 1, 5), today,
                                                 calculator.FOLLOW_UP_PERIODS) == [1]
    assert calculator.relevant_follow_up_periods(date(2018, 2, 5), today,
                                                 calculator.FOLLOW_UP_PERIODS) == []


def test_age_at_date_earlier_month():
    birthday = date(1989, 6, 17)
    check_date = date(2014, 4, 15)
    inmate = Inmate(birthday=birthday)

    assert calculator.age_at_date(inmate, check_date) == 24


def test_age_at_date_same_month_earlier_date():
    birthday = date(1989, 6, 17)
    check_date = date(2014, 6, 16)
    inmate = Inmate(birthday=birthday)

    assert calculator.age_at_date(inmate, check_date) == 24


def test_age_at_date_same_month_same_date():
    birthday = date(1989, 6, 17)
    check_date = date(2014, 6, 17)
    inmate = Inmate(birthday=birthday)

    assert calculator.age_at_date(inmate, check_date) == 25


def test_age_at_date_same_month_later_date():
    birthday = date(1989, 6, 17)
    check_date = date(2014, 6, 18)
    inmate = Inmate(birthday=birthday)

    assert calculator.age_at_date(inmate, check_date) == 25


def test_age_at_date_later_month():
    birthday = date(1989, 6, 17)
    check_date = date(2014, 7, 11)
    inmate = Inmate(birthday=birthday)

    assert calculator.age_at_date(inmate, check_date) == 25


def test_age_at_date_birthday_unknown():
    assert calculator.age_at_date(Inmate(), datetime.today()) is None


def test_age_bucket():
    assert calculator.age_bucket(24) == "<25"
    assert calculator.age_bucket(27) == "25-29"
    assert calculator.age_bucket(30) == "30-34"
    assert calculator.age_bucket(39) == "35-39"
    assert calculator.age_bucket(40) == "40<"


def test_characteristic_combinations():
    characteristics = {"race": "black", "sex": "female", "age": "<25"}
    combinations = calculator.characteristic_combinations(characteristics)

    assert combinations == [{},
                            {'age': '<25'}, {'race': 'black'}, {'sex': 'female'},
                            {'age': '<25', 'race': 'black'}, {'age': '<25', 'sex': 'female'},
                            {'race': 'black', 'sex': 'female'},
                            {'age': '<25', 'race': 'black', 'sex': 'female'}]


def test_characteristic_combinations_one_characteristic():
    characteristics = {"sex": "male"}
    combinations = calculator.characteristic_combinations(characteristics)

    assert combinations == [{}, {'sex': 'male'}]


def test_augment_combination():
    combo = {'age': '<25', 'race': 'black', 'sex': 'female'}
    augmented = calculator.augment_combination(combo, 'EVENT', 8)

    assert augmented == {'age': '<25', 'follow_up_period': 8, 'methodology': 'EVENT', 'race': 'black', 'sex': 'female'}
    assert augmented != combo


class TestFindRecidivism(object):

    def setup_method(self, test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        ndb.get_context().clear_cache()

    def teardown_method(self, test_method):
        self.testbed.deactivate()

    def test_find_recidivism(self):
        inmate = Inmate(id="test-inmate")
        inmate.put()

        initial_incarceration = record(inmate.key, True, date(2008, 11, 20), date(2010, 12, 4))
        initial_incarceration_first_snapshot = snapshot(initial_incarceration.key, datetime(2009, 6, 17), "Sing Sing")
        initial_incarceration_second_snapshot = snapshot(initial_incarceration.key, datetime(2010, 10, 17),
                                                         "Adirondack")

        first_reincarceration = record(inmate.key, True, date(2011, 4, 5), date(2014, 4, 14))
        first_reincarceration_first_snapshot = snapshot(first_reincarceration.key, datetime(2012, 10, 15), "Adirondack")
        first_reincarceration_second_snapshot = snapshot(first_reincarceration.key, datetime(2013, 10, 15), "Upstate")

        subsequent_reincarceration = record(inmate.key, False, date(2017, 1, 4))
        subsequent_reincarceration_snapshot = snapshot(subsequent_reincarceration.key, datetime(2017, 10, 15),
                                                       "Downstate")

        recidivism_events_by_cohort = calculator.find_recidivism(inmate)

        assert len(recidivism_events_by_cohort) == 2

        assert recidivism_events_by_cohort[2010] == calculator.RecidivismEvent.recidivism_event(
            initial_incarceration.custody_date, initial_incarceration.last_release_date,
            initial_incarceration_second_snapshot.facility, first_reincarceration.custody_date,
            first_reincarceration_first_snapshot.facility, False)

        assert recidivism_events_by_cohort[2014] == calculator.RecidivismEvent.recidivism_event(
            first_reincarceration.custody_date, first_reincarceration.last_release_date,
            first_reincarceration_second_snapshot.facility, subsequent_reincarceration.custody_date,
            subsequent_reincarceration_snapshot.facility, False)

    def test_find_recidivism_no_records_at_all(self):
        inmate = Inmate(id="test-inmate")
        inmate.put()

        recidivism_events_by_cohort = calculator.find_recidivism(inmate)

        assert not recidivism_events_by_cohort

    def test_find_recidivism_no_recidivism_after_first(self):
        inmate = Inmate(id="test-inmate")
        inmate.put()

        initial_incarceration = record(inmate.key, True, date(2008, 11, 20), date(2010, 12, 4))
        initial_incarceration_first_snapshot = snapshot(initial_incarceration.key, datetime(2009, 6, 17), "Sing Sing")
        initial_incarceration_second_snapshot = snapshot(initial_incarceration.key, datetime(2010, 10, 17),
                                                         "Adirondack")

        recidivism_events_by_cohort = calculator.find_recidivism(inmate)

        assert len(recidivism_events_by_cohort) == 1

        assert recidivism_events_by_cohort[2010] == calculator.RecidivismEvent.non_recidivism_event(
            initial_incarceration.custody_date, initial_incarceration.last_release_date,
            initial_incarceration_second_snapshot.facility)

    def test_find_recidivism_still_incarcerated_on_first(self):
        inmate = Inmate(id="test-inmate")
        inmate.put()

        initial_incarceration = record(inmate.key, False, date(2008, 11, 20))
        initial_incarceration_first_snapshot = snapshot(initial_incarceration.key, datetime(2009, 6, 17), "Sing Sing")

        recidivism_events_by_cohort = calculator.find_recidivism(inmate)

        assert not recidivism_events_by_cohort

    def test_find_recidivism_invalid_no_entry_date(self):
        inmate = Inmate(id="test-inmate")
        inmate.put()

        initial_incarceration = record(inmate.key, True, None, date(2010, 12, 4))
        initial_incarceration_first_snapshot = snapshot(initial_incarceration.key, datetime(2009, 6, 17), "Sing Sing")
        initial_incarceration_second_snapshot = snapshot(initial_incarceration.key, datetime(2010, 10, 17),
                                                         "Adirondack")

        first_reincarceration = record(inmate.key, True, date(2011, 4, 5), date(2014, 4, 14))
        first_reincarceration_first_snapshot = snapshot(first_reincarceration.key, datetime(2012, 10, 15), "Adirondack")
        first_reincarceration_second_snapshot = snapshot(first_reincarceration.key, datetime(2013, 10, 15), "Upstate")

        subsequent_reincarceration = record(inmate.key, False, date(2017, 1, 4))
        subsequent_reincarceration_snapshot = snapshot(subsequent_reincarceration.key, datetime(2017, 10, 15),
                                                       "Downstate")

        recidivism_events_by_cohort = calculator.find_recidivism(inmate)

        # Only one event. The 2010 event should be discarded because of its lack of a custody date.
        assert len(recidivism_events_by_cohort) == 1

        assert recidivism_events_by_cohort[2014] == calculator.RecidivismEvent.recidivism_event(
            first_reincarceration.custody_date, first_reincarceration.last_release_date,
            first_reincarceration_second_snapshot.facility, subsequent_reincarceration.custody_date,
            subsequent_reincarceration_snapshot.facility, False)

    def test_find_recidivism_invalid_released_but_no_release_date(self):
        inmate = Inmate(id="test-inmate")
        inmate.put()

        initial_incarceration = record(inmate.key, True, date(2008, 11, 20), None)
        initial_incarceration_first_snapshot = snapshot(initial_incarceration.key, datetime(2009, 6, 17), "Sing Sing")
        initial_incarceration_second_snapshot = snapshot(initial_incarceration.key, datetime(2010, 10, 17),
                                                         "Adirondack")

        first_reincarceration = record(inmate.key, True, date(2011, 4, 5), date(2014, 4, 14))
        first_reincarceration_first_snapshot = snapshot(first_reincarceration.key, datetime(2012, 10, 15), "Adirondack")
        first_reincarceration_second_snapshot = snapshot(first_reincarceration.key, datetime(2013, 10, 15), "Upstate")

        subsequent_reincarceration = record(inmate.key, False, date(2017, 1, 4))
        subsequent_reincarceration_snapshot = snapshot(subsequent_reincarceration.key, datetime(2017, 10, 15),
                                                       "Downstate")

        recidivism_events_by_cohort = calculator.find_recidivism(inmate)

        # Only one event. The 2010 event should be discarded because of its lack of a release date though released=True.
        assert len(recidivism_events_by_cohort) == 1

        assert recidivism_events_by_cohort[2014] == calculator.RecidivismEvent.recidivism_event(
            first_reincarceration.custody_date, first_reincarceration.last_release_date,
            first_reincarceration_second_snapshot.facility, subsequent_reincarceration.custody_date,
            subsequent_reincarceration_snapshot.facility, False)

    def test_find_recidivism_invalid_released_but_no_entry_date_on_reincarceration(self):
        inmate = Inmate(id="test-inmate")
        inmate.put()

        initial_incarceration = record(inmate.key, True, date(2008, 11, 20), date(2010, 12, 4))
        initial_incarceration_first_snapshot = snapshot(initial_incarceration.key, datetime(2009, 6, 17), "Sing Sing")
        initial_incarceration_second_snapshot = snapshot(initial_incarceration.key, datetime(2010, 10, 17),
                                                         "Adirondack")

        first_reincarceration = record(inmate.key, True, None, date(2014, 4, 14))
        first_reincarceration_first_snapshot = snapshot(first_reincarceration.key, datetime(2012, 10, 15), "Adirondack")
        first_reincarceration_second_snapshot = snapshot(first_reincarceration.key, datetime(2013, 10, 15), "Upstate")

        subsequent_reincarceration = record(inmate.key, False, date(2017, 1, 4))
        subsequent_reincarceration_snapshot = snapshot(subsequent_reincarceration.key, datetime(2017, 10, 15),
                                                       "Downstate")

        recidivism_events_by_cohort = calculator.find_recidivism(inmate)

        # Only one event. The 2014 event should be discarded because of its lack of a release date though released=True.
        # We wind up with an event that shows recidivism in 2017 after release in 2010 because the middle event that
        # has no custody date due to some record keeping error gets sorted to the front by our Datastore query.
        assert len(recidivism_events_by_cohort) == 1

        assert recidivism_events_by_cohort[2010] == calculator.RecidivismEvent.recidivism_event(
            initial_incarceration.custody_date, initial_incarceration.last_release_date,
            initial_incarceration_second_snapshot.facility, subsequent_reincarceration.custody_date,
            subsequent_reincarceration_snapshot.facility, False)


class TestMapRecidivismCombinations(object):

    def setup_method(self, test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        ndb.get_context().clear_cache()

    def teardown_method(self, test_method):
        self.testbed.deactivate()

    def test_map_recidivism_combinations(self):
        inmate = Inmate(id="test-inmate", birthday=date(1984, 8, 31), race="white", sex="female")

        recidivism_events_by_cohort = {
            2008: calculator.RecidivismEvent.recidivism_event(date(2005, 7, 19), date(2008, 9, 19), "Hudson",
                                                              date(2014, 5, 12), "Upstate", False)
        }

        recidivism_combinations = calculator.map_recidivism_combinations(inmate, recidivism_events_by_cohort)

        # 16 combinations of demographics and facility * 2 methodologies * 10 periods = 320 metrics
        assert len(recidivism_combinations) == 320

        for combination, value in recidivism_combinations:
            if combination['follow_up_period'] <= 5:
                assert value == 0
            else:
                assert value == 1

    def test_map_recidivism_combinations_no_recidivism(self):
        inmate = Inmate(id="test-inmate", birthday=date(1984, 8, 31), race="white", sex="female")

        recidivism_events_by_cohort = {
            2008: calculator.RecidivismEvent.non_recidivism_event(date(2005, 7, 19), date(2008, 9, 19), "Hudson")
        }

        recidivism_combinations = calculator.map_recidivism_combinations(inmate, recidivism_events_by_cohort)

        # 16 combinations of demographics and facility * 2 methodologies * 10 periods = 320 metrics
        assert len(recidivism_combinations) == 320
        assert all(value == 0 for _combination, value in recidivism_combinations)

    def test_map_recidivism_combinations_recidivated_after_last_follow_up_period(self):
        inmate = Inmate(id="test-inmate", birthday=date(1984, 8, 31), race="white", sex="female")

        recidivism_events_by_cohort = {
            2008: calculator.RecidivismEvent.recidivism_event(date(2005, 7, 19), date(2008, 9, 19), "Hudson",
                                                              date(2018, 10, 12), "Upstate", False)
        }

        recidivism_combinations = calculator.map_recidivism_combinations(inmate, recidivism_events_by_cohort)

        # 16 combinations of demographics and facility * 2 methodologies * 10 periods = 320 metrics
        assert len(recidivism_combinations) == 320
        assert all(value == 0 for _combination, value in recidivism_combinations)


def record(parent_key, is_released, custody_date, last_release_date=None):
    new_record = UsNyRecord(parent=parent_key, is_released=is_released, custody_date=custody_date,
                            last_release_date=last_release_date)
    new_record.put()
    return new_record


def snapshot(parent_key, snapshot_date, facility):
    new_snapshot = InmateFacilitySnapshot(parent=parent_key, snapshot_date=snapshot_date, facility=facility)
    new_snapshot.put()
    return new_snapshot
