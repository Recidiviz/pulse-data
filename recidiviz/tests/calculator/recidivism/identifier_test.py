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

from recidiviz.calculator.recidivism import identifier
from recidiviz.calculator.recidivism import recidivism_event


def test_first_entrance():
    today = date.today()
    new_record = FakeRecord(custody_date=today)
    assert identifier.first_entrance(new_record) == today


def test_subsequent_entrance():
    today = date.today()
    two_years_ago = today - relativedelta(years=2)
    new_record = FakeRecord(custody_date=two_years_ago, last_custody_date=today)
    assert identifier.subsequent_entrance(new_record) == today


def test_final_release():
    today = date.today()
    new_record = FakeRecord(is_released=True, latest_release_date=today)
    assert identifier.final_release(new_record) == today


def test_final_release_incarcerated():
    new_record = FakeRecord(is_released=False)
    assert identifier.final_release(new_record) is None


def test_first_facility():
    now = datetime.utcnow()
    new_record = FakeRecord(key="parent-record")
    unrelated_record = FakeRecord(key="unrelated-record")

    most_recent_snapshot_overall = FakeSnapshot(parent_key=unrelated_record.key,
                                                created_on=now,
                                                latest_facility="Washington")
    most_recent_snapshot_for_record = FakeSnapshot(parent_key=new_record.key,
                                                   created_on=now,
                                                   latest_facility="Marcy")
    less_recent_snapshot_for_record = FakeSnapshot(parent_key=new_record.key,
                                                   created_on=now,
                                                   latest_facility="Greene")

    first_facility = identifier.first_facility(new_record, [
        most_recent_snapshot_overall, most_recent_snapshot_for_record,
        less_recent_snapshot_for_record])

    assert first_facility == "Greene"


def test_first_facility_none_for_record():
    now = datetime.utcnow()
    new_record = FakeRecord(key="parent-record")
    unrelated_record = FakeRecord(key="unrelated-record")

    most_recent_snapshot_overall = FakeSnapshot(parent_key=unrelated_record.key,
                                                created_on=now,
                                                latest_facility="Washington")
    least_recent_snapshot_overall = FakeSnapshot(
        parent_key=unrelated_record.key, created_on=now,
        latest_facility="Greene")

    first_facility = identifier.first_facility(new_record, [
        most_recent_snapshot_overall, least_recent_snapshot_overall])

    assert first_facility is None


def test_first_facility_none_at_all():
    new_record = FakeRecord(key="parent-record")

    first_facility = identifier.first_facility(new_record, [])
    assert first_facility is None


def test_last_facility():
    now = datetime.utcnow()
    new_record = FakeRecord(key="parent-record")
    unrelated_record = FakeRecord(key="unrelated-record")

    most_recent_snapshot_overall = FakeSnapshot(parent_key=unrelated_record.key,
                                                created_on=now,
                                                latest_facility="Washington")
    most_recent_snapshot_for_record = FakeSnapshot(parent_key=new_record.key,
                                                   created_on=now,
                                                   latest_facility="Marcy")
    less_recent_snapshot_for_record = FakeSnapshot(parent_key=new_record.key,
                                                   created_on=now,
                                                   latest_facility="Greene")

    last_facility = identifier.last_facility(new_record, [
        most_recent_snapshot_overall, most_recent_snapshot_for_record,
        less_recent_snapshot_for_record])

    assert last_facility == "Marcy"


def test_last_facility_none_for_record():
    now = datetime.utcnow()
    new_record = FakeRecord(key="parent-record")
    unrelated_record = FakeRecord(key="unrelated-record")
    most_recent_snapshot_overall = FakeSnapshot(parent_key=unrelated_record.key,
                                                created_on=now,
                                                latest_facility="Washington")
    least_recent_snapshot_overall = FakeSnapshot(
        parent_key=unrelated_record.key, created_on=now,
        latest_facility="Greene")

    last_facility = identifier.last_facility(new_record, [
        most_recent_snapshot_overall, least_recent_snapshot_overall])

    assert last_facility is None


def test_last_facility_none_at_all():
    new_record = FakeRecord(key="parent-record")

    last_facility = identifier.last_facility(new_record, [])
    assert last_facility is None


class TestFindRecidivism(object):
    """Tests for the find_recidivism function."""

    def test_find_recidivism(self):
        """Tests the find_recidivism function path where the person did
        recidivate."""
        initial_incarceration = FakeRecord(
            key="initial", is_released=True, custody_date=date(2008, 11, 20),
            latest_release_date=date(2010, 12, 4))
        initial_incarceration_first_snapshot = FakeSnapshot(
            initial_incarceration.key, datetime(2009, 6, 17), "Sing Sing")
        initial_incarceration_second_snapshot = FakeSnapshot(
            initial_incarceration.key, datetime(2010, 10, 17), "Adirondack")

        first_reincarceration = FakeRecord(
            key="first_re", is_released=True, custody_date=date(2011, 4, 5),
            latest_release_date=date(2014, 4, 14))
        first_reincarceration_first_snapshot = FakeSnapshot(
            first_reincarceration.key, datetime(2012, 10, 15), "Adirondack")
        first_reincarceration_second_snapshot = FakeSnapshot(
            first_reincarceration.key, datetime(2013, 10, 15), "Upstate")

        subsequent_reincarceration = FakeRecord(
            key="subsequent_re", is_released=False,
            custody_date=date(2017, 1, 4))
        subsequent_reincarceration_snapshot = FakeSnapshot(
            subsequent_reincarceration.key, datetime(2017, 10, 15), "Downstate")

        recidivism_events_by_cohort = identifier.find_recidivism(
            records=[
                initial_incarceration,
                first_reincarceration,
                subsequent_reincarceration,
            ], snapshots=[
                subsequent_reincarceration_snapshot,
                first_reincarceration_second_snapshot,
                first_reincarceration_first_snapshot,
                initial_incarceration_second_snapshot,
                initial_incarceration_first_snapshot,
            ])

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
        recidivism_events_by_cohort = identifier.find_recidivism(
            records=[], snapshots=[])

        assert not recidivism_events_by_cohort

    def test_find_recidivism_no_recidivism_after_first(self):
        """Tests the find_recidivism function where the person did not
        recidivate."""
        initial_incarceration = FakeRecord(
            key="initial", is_released=True, custody_date=date(2008, 11, 20),
            latest_release_date=date(2010, 12, 4))
        initial_incarceration_first_snapshot = FakeSnapshot(
            initial_incarceration.key, datetime(2009, 6, 17), "Sing Sing")
        initial_incarceration_second_snapshot = FakeSnapshot(
            initial_incarceration.key, datetime(2010, 10, 17), "Adirondack")

        recidivism_events_by_cohort = identifier.find_recidivism(
            records=[initial_incarceration], snapshots=[
                initial_incarceration_second_snapshot,
                initial_incarceration_first_snapshot,
            ]
        )

        assert len(recidivism_events_by_cohort) == 1

        assert recidivism_events_by_cohort[2010] == recidivism_event.\
            RecidivismEvent.non_recidivism_event(
                initial_incarceration.custody_date,
                initial_incarceration.latest_release_date,
                initial_incarceration_second_snapshot.latest_facility)

    def test_find_recidivism_still_incarcerated_on_first(self):
        """Tests the find_recidivism function where the person is still
        incarcerated on their very first record."""
        initial_incarceration = FakeRecord(
            key="initial", is_released=False, custody_date=date(2008, 11, 20))
        initial_incarceration_snapshot = FakeSnapshot(
            initial_incarceration.key, datetime(2009, 6, 17), "Sing Sing")

        recidivism_events_by_cohort = identifier.find_recidivism(
            records=[initial_incarceration],
            snapshots=[initial_incarceration_snapshot])

        assert not recidivism_events_by_cohort

    def test_find_recidivism_invalid_no_entry_date(self):
        """Tests the find_recidivism function error handling when the record
        is invalid because it has no entry date."""
        initial_incarceration = FakeRecord(
            key="initial", is_released=True, custody_date=None,
            latest_release_date=date(2010, 12, 4))
        initial_incarceration_first_snapshot = FakeSnapshot(
            initial_incarceration.key, datetime(2009, 6, 17), "Sing Sing")
        initial_incarceration_second_snapshot = FakeSnapshot(
            initial_incarceration.key, datetime(2010, 10, 17), "Adirondack")

        first_reincarceration = FakeRecord(
            key="first_re", is_released=True, custody_date=date(2011, 4, 5),
            latest_release_date=date(2014, 4, 14))
        first_reincarceration_first_snapshot = FakeSnapshot(
            first_reincarceration.key, datetime(2012, 10, 15), "Adirondack")
        first_reincarceration_second_snapshot = FakeSnapshot(
            first_reincarceration.key, datetime(2013, 10, 15), "Upstate")

        subsequent_reincarceration = FakeRecord(
            key="subsequent_re", is_released=False,
            custody_date=date(2017, 1, 4))
        subsequent_reincarceration_snapshot = FakeSnapshot(
            subsequent_reincarceration.key, datetime(2017, 10, 15), "Downstate")

        recidivism_events_by_cohort = identifier.find_recidivism(
            records=[
                initial_incarceration,
                first_reincarceration,
                subsequent_reincarceration,
            ], snapshots=[
                subsequent_reincarceration_snapshot,
                first_reincarceration_second_snapshot,
                first_reincarceration_first_snapshot,
                initial_incarceration_second_snapshot,
                initial_incarceration_first_snapshot
            ])

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
        initial_incarceration = FakeRecord(
            key="initial", is_released=True, custody_date=date(2008, 11, 20),
            latest_release_date=None)
        initial_incarceration_first_snapshot = FakeSnapshot(
            initial_incarceration.key, datetime(2009, 6, 17), "Sing Sing")
        initial_incarceration_second_snapshot = FakeSnapshot(
            initial_incarceration.key, datetime(2010, 10, 17), "Adirondack")

        first_reincarceration = FakeRecord(
            key="first_re", is_released=True, custody_date=date(2011, 4, 5),
            latest_release_date=date(2014, 4, 14))
        first_reincaceration_first_snapshot = FakeSnapshot(
            first_reincarceration.key, datetime(2012, 10, 15), "Adirondack")
        first_reincarceration_second_snapshot = FakeSnapshot(
            first_reincarceration.key, datetime(2013, 10, 15), "Upstate")

        subsequent_reincarceration = FakeRecord(
            key="subsequent_re", is_released=False,
            custody_date=date(2017, 1, 4))
        subsequent_reincarceration_snapshot = FakeSnapshot(
            subsequent_reincarceration.key, datetime(2017, 10, 15), "Downstate")

        recidivism_events_by_cohort = identifier.find_recidivism(
            records=[
                initial_incarceration,
                first_reincarceration,
                subsequent_reincarceration,
            ], snapshots=[
                subsequent_reincarceration_snapshot,
                first_reincarceration_second_snapshot,
                first_reincaceration_first_snapshot,
                initial_incarceration_second_snapshot,
                initial_incarceration_first_snapshot
            ])

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
        initial_incarceration = FakeRecord(
            key="initial", is_released=True, custody_date=date(2008, 11, 20),
            latest_release_date=date(2010, 12, 4))
        initial_incarceration_first_snapshot = FakeSnapshot(
            initial_incarceration.key, datetime(2009, 6, 17), "Sing Sing")
        initial_incarceration_second_snapshot = FakeSnapshot(
            initial_incarceration.key, datetime(2010, 10, 17), "Adirondack")

        first_reincarceration = FakeRecord(
            key="first_re", is_released=True, custody_date=None,
            latest_release_date=date(2014, 4, 14))
        first_reincarceration_first_snapshot = FakeSnapshot(
            first_reincarceration.key, datetime(2012, 10, 15), "Adirondack")
        first_reincarceration_second_snapshot = FakeSnapshot(
            first_reincarceration.key, datetime(2013, 10, 15), "Upstate")

        subsequent_reincarceration = FakeRecord(
            key="subsequent_re", is_released=False,
            custody_date=date(2017, 1, 4))
        subsequent_reincarceration_snapshot = FakeSnapshot(
            subsequent_reincarceration.key, datetime(2017, 10, 15), "Downstate")

        recidivism_events_by_cohort = identifier.find_recidivism(
            records=[
                first_reincarceration, # first as it doesn't have a custody date
                initial_incarceration,
                subsequent_reincarceration,
            ], snapshots=[
                subsequent_reincarceration_snapshot,
                first_reincarceration_second_snapshot,
                first_reincarceration_first_snapshot,
                initial_incarceration_second_snapshot,
                initial_incarceration_first_snapshot,
            ])

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


class FakeRecord(object):
    def __init__(self, key=None, is_released=None, custody_date=None,
                 last_custody_date=None, latest_release_date=None):
        self.key = key
        self.is_released = is_released
        self.custody_date = custody_date
        self.last_custody_date = last_custody_date
        self.latest_release_date = latest_release_date


class FakeSnapshot(object):
    def __init__(self, parent_key=None, created_on=None, latest_facility=None):
        self.parent = parent_key
        self.created_on = created_on
        self.latest_facility = latest_facility
