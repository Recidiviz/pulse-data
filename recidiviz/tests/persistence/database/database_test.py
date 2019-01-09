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
"""Tests for database.py."""

import datetime
from copy import deepcopy
from unittest import TestCase

from recidiviz import Session
import recidiviz.common.constants.enum_canonical_strings as enum_strings
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.persistence import entities
from recidiviz.persistence.database import database, database_utils
from recidiviz.persistence.database.schema import Booking, Person
from recidiviz.tests.utils import fakes

_REGION = 'region'
_REGION_ANOTHER = 'wrong region'
_GIVEN_NAMES = 'given_names'
_SURNAME = 'surname'
_EXTERNAL_ID = 'external_id'
_BIRTHDATE = datetime.date(year=2012, month=1, day=2)


class TestDatabase(TestCase):
    """Test that the methods in database.py correctly read from the SQL
    database """

    def setup_method(self, _test_method):
        fakes.use_in_memory_sqlite_database()

    def test_readOpenBookingsBeforeDate(self):
        # Arrange
        person = Person(person_id=8, region=_REGION)
        person_wrong_region = Person(person_id=9, region=_REGION_ANOTHER)

        release_date = datetime.date(2018, 7, 20)
        most_recent_scrape_date = datetime.datetime(2018, 6, 20)
        date_in_past = most_recent_scrape_date - datetime.timedelta(days=1)

        # TODO(176): Replace enum_strings with schema enum values once we've
        # migrated to Python 3.

        # Bookings that should be returned
        open_booking_before_last_scrape = Booking(
            person_id=person.person_id,
            custody_status=enum_strings.custody_status_in_custody,
            last_seen_time=date_in_past)

        # Bookings that should not be returned
        open_booking_incorrect_region = Booking(
            person_id=person_wrong_region.person_id,
            custody_status=enum_strings.custody_status_in_custody,
            last_seen_time=date_in_past)
        open_booking_most_recent_scrape = Booking(
            person_id=person.person_id,
            custody_status=enum_strings.custody_status_in_custody,
            last_seen_time=most_recent_scrape_date)
        resolved_booking = Booking(
            person_id=person.person_id,
            custody_status=enum_strings.custody_status_in_custody,
            release_date=release_date,
            last_seen_time=date_in_past)

        session = Session()
        session.add(person)
        session.add(person_wrong_region)
        session.add(open_booking_before_last_scrape)
        session.add(open_booking_incorrect_region)
        session.add(open_booking_most_recent_scrape)
        session.add(resolved_booking)
        session.commit()

        # Act
        bookings = database.read_open_bookings_scraped_before_time(
            session, person.region, most_recent_scrape_date)

        # Assert
        self.assertEqual(bookings,
                         [database_utils.convert_booking(
                             open_booking_before_last_scrape)])

    def test_readPeopleByExternalId(self):
        admission_date = datetime.datetime(2018, 6, 20)
        release_date = datetime.date(2018, 7, 20)
        closed_booking = Booking(
            custody_status=enum_strings.custody_status_in_custody,
            admission_date=admission_date,
            release_date=release_date,
            last_seen_time=admission_date)

        person_no_match = Person(person_id=1, region=_REGION,
                                 bookings=[deepcopy(closed_booking)])
        person_match_external_id = Person(person_id=2, region=_REGION,
                                          bookings=[closed_booking],
                                          external_id=_EXTERNAL_ID)

        session = Session()
        session.add(person_no_match)
        session.add(person_match_external_id)
        session.commit()

        ingested_person = entities.Person.new_with_defaults(
            external_id=_EXTERNAL_ID)
        people = database.read_people_by_external_ids(session, _REGION,
                                                      [ingested_person])

        expected_people = [
            database_utils.convert_person(person_match_external_id)]
        self.assertCountEqual(people, expected_people)

    def test_readPeopleWithOpenBookings(self):
        admission_date = datetime.datetime(2018, 6, 20)
        release_date = datetime.date(2018, 7, 20)

        open_booking = Booking(
            custody_status=enum_strings.custody_status_in_custody,
            admission_date=admission_date,
            last_seen_time=admission_date)
        closed_booking = Booking(
            custody_status=enum_strings.custody_status_in_custody,
            admission_date=admission_date,
            release_date=release_date,
            last_seen_time=admission_date)

        person_no_match = Person(person_id=1, region=_REGION,
                                 bookings=[deepcopy(open_booking)])
        person_match_surname = Person(person_id=2, region=_REGION,
                                      bookings=[deepcopy(open_booking)],
                                      surname=_SURNAME)
        person_match_given_names = Person(person_id=3, region=_REGION,
                                          bookings=[deepcopy(open_booking)],
                                          given_names=_GIVEN_NAMES)
        person_match_birthdate = Person(person_id=5, region=_REGION,
                                        bookings=[deepcopy(open_booking)],
                                        birthdate=_BIRTHDATE)
        person_no_open_bookings = Person(person_id=6, region=_REGION,
                                         surname=_SURNAME,
                                         bookings=[closed_booking])

        session = Session()
        session.add(person_no_match)
        session.add(person_no_open_bookings)
        session.add(person_match_given_names)
        session.add(person_match_surname)
        session.add(person_match_birthdate)
        session.commit()

        info = IngestInfo()
        info.create_person(surname=_SURNAME, person_id=_EXTERNAL_ID)
        info.create_person(given_names=_GIVEN_NAMES, birthdate=_BIRTHDATE)
        people = database.read_people_with_open_bookings(session, _REGION,
                                                         info.person)

        expected_people = [database_utils.convert_person(p) for p in
                           [person_match_given_names, person_match_surname]]
        self.assertCountEqual(people, expected_people)
