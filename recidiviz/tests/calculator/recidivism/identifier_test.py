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

# pylint: disable=unused-import,wrong-import-order

"""Tests for recidivism/identifier.py."""


from datetime import date
from datetime import datetime

import pytest

from dateutil.relativedelta import relativedelta
from google.appengine.ext import ndb
from google.appengine.ext import testbed

from recidiviz.calculator.recidivism import identifier
from recidiviz.calculator.recidivism import recidivism_event
from recidiviz.ingest.us_ny.us_ny_record import UsNyRecord
from recidiviz.models.person import Person
from recidiviz.models.snapshot import Snapshot


def test_first_entrance():
    today = date.today()
    new_record = UsNyRecord(custody_date=today)
    assert identifier.first_entrance(new_record) == today


def test_subsequent_entrance():
    today = date.today()
    two_years_ago = today - relativedelta(years=2)
    new_record = UsNyRecord(custody_date=two_years_ago, last_custody_date=today)
    assert identifier.subsequent_entrance(new_record) == today


def test_final_release():
    today = date.today()
    new_record = UsNyRecord(is_released=True, latest_release_date=today)
    assert identifier.final_release(new_record) == today


def test_final_release_incarcerated():
    new_record = UsNyRecord(is_released=False)
    assert identifier.final_release(new_record) is None


def test_first_facility():
    now = datetime.utcnow()
    new_record = UsNyRecord(id="parent-record")
    unrelated_record = UsNyRecord(id="unrelated-record")

    most_recent_snapshot_overall = Snapshot(parent=unrelated_record.key,
                                            created_on=now,
                                            latest_facility="Washington")
    most_recent_snapshot_for_record = Snapshot(parent=new_record.key,
                                               created_on=now,
                                               latest_facility="Marcy")
    less_recent_snapshot_for_record = Snapshot(parent=new_record.key,
                                               created_on=now,
                                               latest_facility="Greene")

    first_facility = identifier.first_facility(new_record, [
        most_recent_snapshot_overall, most_recent_snapshot_for_record,
        less_recent_snapshot_for_record])

    assert first_facility == "Greene"


def test_first_facility_none_for_record():
    now = datetime.utcnow()
    new_record = UsNyRecord(id="parent-record")
    unrelated_record = UsNyRecord(id="unrelated-record")

    most_recent_snapshot_overall = Snapshot(parent=unrelated_record.key,
                                            created_on=now,
                                            latest_facility="Washington")
    least_recent_snapshot_overall = Snapshot(parent=unrelated_record.key,
                                             created_on=now,
                                             latest_facility="Greene")

    first_facility = identifier.first_facility(new_record, [
        most_recent_snapshot_overall, least_recent_snapshot_overall])

    assert first_facility is None


def test_first_facility_none_at_all():
    new_record = UsNyRecord(id="parent-record")

    first_facility = identifier.first_facility(new_record, [])
    assert first_facility is None


def test_last_facility():
    now = datetime.utcnow()
    new_record = UsNyRecord(id="parent-record")
    unrelated_record = UsNyRecord(id="unrelated-record")

    most_recent_snapshot_overall = Snapshot(parent=unrelated_record.key,
                                            created_on=now,
                                            latest_facility="Washington")
    most_recent_snapshot_for_record = Snapshot(parent=new_record.key,
                                               created_on=now,
                                               latest_facility="Marcy")
    less_recent_snapshot_for_record = Snapshot(parent=new_record.key,
                                               created_on=now,
                                               latest_facility="Greene")

    last_facility = identifier.last_facility(new_record, [
        most_recent_snapshot_overall, most_recent_snapshot_for_record,
        less_recent_snapshot_for_record])

    assert last_facility == "Marcy"


def test_last_facility_none_for_record():
    now = datetime.utcnow()
    new_record = UsNyRecord(id="parent-record")
    unrelated_record = UsNyRecord(id="unrelated-record")
    most_recent_snapshot_overall = Snapshot(parent=unrelated_record.key,
                                            created_on=now,
                                            latest_facility="Washington")
    least_recent_snapshot_overall = Snapshot(parent=unrelated_record.key,
                                             created_on=now,
                                             latest_facility="Greene")

    last_facility = identifier.last_facility(new_record, [
        most_recent_snapshot_overall, least_recent_snapshot_overall])

    assert last_facility is None


def test_last_facility_none_at_all():
    new_record = UsNyRecord(id="parent-record")

    last_facility = identifier.last_facility(new_record, [])
    assert last_facility is None


class TestFindRecidivism(object):
    """Tests for the find_recidivism function."""

    def setup_method(self, _test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        context = ndb.get_context()
        context.set_memcache_policy(False)
        context.clear_cache()

    def teardown_method(self, _test_method):
        self.testbed.deactivate()

    def test_find_recidivism(self):
        """Tests the find_recidivism function path where the person did
        recidivate."""
        person = Person(id="test-person")
        person.put()

        initial_incarceration = record(person.key, True, date(2008, 11, 20),
                                       date(2010, 12, 4))
        snapshot(initial_incarceration.key, datetime(2009, 6, 17), "Sing Sing")
        initial_incarceration_second_snapshot = snapshot(
            initial_incarceration.key, datetime(2010, 10, 17), "Adirondack")

        first_reincarceration = record(person.key, True, date(2011, 4, 5),
                                       date(2014, 4, 14))
        first_reincarceration_first_snapshot = snapshot(
            first_reincarceration.key, datetime(2012, 10, 15), "Adirondack")
        first_reincarceration_second_snapshot = snapshot(
            first_reincarceration.key, datetime(2013, 10, 15), "Upstate")

        subsequent_reincarceration = record(person.key, False, date(2017, 1, 4))
        subsequent_reincarceration_snapshot = snapshot(
            subsequent_reincarceration.key, datetime(2017, 10, 15), "Downstate")

        recidivism_events_by_cohort = identifier.find_recidivism(person)

        assert len(recidivism_events_by_cohort) == 2

        assert recidivism_events_by_cohort[2010] == recidivism_event.\
            RecidivismEvent.recidivism_event(
                initial_incarceration.custody_date,
                initial_incarceration.latest_release_date,
                initial_incarceration_second_snapshot.latest_facility,
                first_reincarceration.custody_date,
                first_reincarceration_first_snapshot.latest_facility,
                False)

        assert recidivism_events_by_cohort[2014] == recidivism_event.\
            RecidivismEvent.recidivism_event(
                first_reincarceration.custody_date,
                first_reincarceration.latest_release_date,
                first_reincarceration_second_snapshot.latest_facility,
                subsequent_reincarceration.custody_date,
                subsequent_reincarceration_snapshot.latest_facility,
                False)

    def test_find_recidivism_no_records_at_all(self):
        """Tests the find_recidivism function when the person has no records."""
        person = Person(id="test-person")
        person.put()

        recidivism_events_by_cohort = identifier.find_recidivism(person)

        assert not recidivism_events_by_cohort

    def test_find_recidivism_no_recidivism_after_first(self):
        """Tests the find_recidivism function where the person did not
        recidivate."""
        person = Person(id="test-person")
        person.put()

        initial_incarceration = record(person.key, True, date(2008, 11, 20),
                                       date(2010, 12, 4))
        snapshot(initial_incarceration.key, datetime(2009, 6, 17), "Sing Sing")
        initial_incarceration_second_snapshot = snapshot(
            initial_incarceration.key, datetime(2010, 10, 17), "Adirondack")

        recidivism_events_by_cohort = identifier.find_recidivism(person)

        assert len(recidivism_events_by_cohort) == 1

        assert recidivism_events_by_cohort[2010] == recidivism_event.\
            RecidivismEvent.non_recidivism_event(
                initial_incarceration.custody_date,
                initial_incarceration.latest_release_date,
                initial_incarceration_second_snapshot.latest_facility)

    def test_find_recidivism_still_incarcerated_on_first(self):
        """Tests the find_recidivism function where the person is still
        incarcerated on their very first record."""
        person = Person(id="test-person")
        person.put()

        initial_incarceration = record(person.key, False, date(2008, 11, 20))
        snapshot(initial_incarceration.key, datetime(2009, 6, 17), "Sing Sing")

        recidivism_events_by_cohort = identifier.find_recidivism(person)

        assert not recidivism_events_by_cohort

    def test_find_recidivism_invalid_no_entry_date(self):
        """Tests the find_recidivism function error handling when the record
        is invalid because it has no entry date."""
        person = Person(id="test-person")
        person.put()

        initial_incarceration = record(person.key, True, None,
                                       date(2010, 12, 4))
        snapshot(initial_incarceration.key, datetime(2009, 6, 17), "Sing Sing")
        snapshot(initial_incarceration.key, datetime(2010, 10, 17),
                 "Adirondack")

        first_reincarceration = record(person.key, True, date(2011, 4, 5),
                                       date(2014, 4, 14))
        snapshot(first_reincarceration.key, datetime(2012, 10, 15),
                 "Adirondack")
        first_reincarceration_second_snapshot = snapshot(
            first_reincarceration.key, datetime(2013, 10, 15), "Upstate")

        subsequent_reincarceration = record(person.key, False, date(2017, 1, 4))
        subsequent_reincarceration_snapshot = snapshot(
            subsequent_reincarceration.key, datetime(2017, 10, 15), "Downstate")

        recidivism_events_by_cohort = identifier.find_recidivism(person)

        # Only one event. The 2010 event should be discarded
        # because of its lack of a custody date.
        assert len(recidivism_events_by_cohort) == 1

        assert recidivism_events_by_cohort[2014] == recidivism_event.\
            RecidivismEvent.recidivism_event(
                first_reincarceration.custody_date,
                first_reincarceration.latest_release_date,
                first_reincarceration_second_snapshot.latest_facility,
                subsequent_reincarceration.custody_date,
                subsequent_reincarceration_snapshot.latest_facility,
                False)

    def test_find_recidivism_invalid_released_but_no_release_date(self):
        """Tests the find_recidivism function error handling when the record
        is invalid because it has no release date but is marked as released."""
        person = Person(id="test-person")
        person.put()

        initial_incarceration = record(person.key, True, date(2008, 11, 20),
                                       None)
        snapshot(initial_incarceration.key, datetime(2009, 6, 17), "Sing Sing")
        snapshot(initial_incarceration.key, datetime(2010, 10, 17),
                 "Adirondack")

        first_reincarceration = record(person.key, True, date(2011, 4, 5),
                                       date(2014, 4, 14))
        snapshot(first_reincarceration.key, datetime(2012, 10, 15),
                 "Adirondack")
        first_reincarceration_second_snapshot = snapshot(
            first_reincarceration.key, datetime(2013, 10, 15), "Upstate")

        subsequent_reincarceration = record(person.key, False, date(2017, 1, 4))
        subsequent_reincarceration_snapshot = snapshot(
            subsequent_reincarceration.key, datetime(2017, 10, 15), "Downstate")

        recidivism_events_by_cohort = identifier.find_recidivism(person)

        # Only one event. The 2010 event should be discarded
        # because of its lack of a release date although released=True.
        assert len(recidivism_events_by_cohort) == 1

        assert recidivism_events_by_cohort[2014] == recidivism_event.\
            RecidivismEvent.recidivism_event(
                first_reincarceration.custody_date,
                first_reincarceration.latest_release_date,
                first_reincarceration_second_snapshot.latest_facility,
                subsequent_reincarceration.custody_date,
                subsequent_reincarceration_snapshot.latest_facility,
                False)

    def test_find_recidivism_invalid_released_but_no_entry_date_on_reincarceration(self):  # pylint: disable=line-too-long
        """Tests the find_recidivism function error handling when the record
        cannot be counted because the subsequent record indicates recidivism
        but itself has no entry date."""
        person = Person(id="test-person")
        person.put()

        initial_incarceration = record(person.key, True, date(2008, 11, 20),
                                       date(2010, 12, 4))
        snapshot(initial_incarceration.key, datetime(2009, 6, 17), "Sing Sing")
        initial_incarceration_second_snapshot = snapshot(
            initial_incarceration.key, datetime(2010, 10, 17), "Adirondack")

        first_reincarceration = record(person.key, True, None,
                                       date(2014, 4, 14))
        snapshot(first_reincarceration.key, datetime(2012, 10, 15),
                 "Adirondack")
        snapshot(first_reincarceration.key, datetime(2013, 10, 15), "Upstate")

        subsequent_reincarceration = record(person.key, False, date(2017, 1, 4))
        subsequent_reincarceration_snapshot = snapshot(
            subsequent_reincarceration.key, datetime(2017, 10, 15), "Downstate")

        recidivism_events_by_cohort = identifier.find_recidivism(person)

        # Only one event. The 2014 event should be discarded because of its
        # lack of a release date though released=True.
        # We wind up with an event that shows recidivism in 2017 after release
        # in 2010 because the middle event that has no custody date due to some
        # record-keeping error gets sorted to the front by our Datastore query.
        assert len(recidivism_events_by_cohort) == 1

        assert recidivism_events_by_cohort[2010] == recidivism_event.\
            RecidivismEvent.recidivism_event(
                initial_incarceration.custody_date,
                initial_incarceration.latest_release_date,
                initial_incarceration_second_snapshot.latest_facility,
                subsequent_reincarceration.custody_date,
                subsequent_reincarceration_snapshot.latest_facility,
                False)


def record(parent_key, is_released, custody_date, latest_release_date=None):
    new_record = UsNyRecord(parent=parent_key,
                            is_released=is_released,
                            custody_date=custody_date,
                            latest_release_date=latest_release_date)
    new_record.put()
    return new_record


def snapshot(parent_key, snapshot_date, facility):
    new_snapshot = Snapshot(parent=parent_key,
                            created_on=snapshot_date,
                            latest_facility=facility)
    new_snapshot.put()
    return new_snapshot
