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
from unittest import TestCase

from recidiviz import Session
import recidiviz.common.constants.enum_canonical_strings as enum_strings
from recidiviz.persistence.database import database, database_utils
from recidiviz.persistence.database.schema import Booking, Person
from recidiviz.tests.utils import fakes

_REGION = 'region'
_REGION_ANOTHER = 'wrong region'

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
        self.assertEqual(bookings, [open_booking_before_last_scrape])

    def test_readPeopleWithOpenBookings(self):
        admission_date = datetime.datetime(2018, 6, 20)
        release_date = datetime.date(2018, 7, 20)

        open_booking = Booking(
            custody_status=enum_strings.custody_status_in_custody,
            admission_date=admission_date,
            last_seen_time=admission_date)
        person = Person(person_id=8, region=_REGION, bookings=[open_booking])

        closed_booking = Booking(
            custody_status=enum_strings.custody_status_in_custody,
            admission_date=admission_date,
            release_date=release_date,
            last_seen_time=admission_date)
        person_no_open_bookings = Person(person_id=9, region=_REGION,
                                         bookings=[closed_booking])

        session = Session()
        session.add(person)
        session.add(person_no_open_bookings)
        session.commit()

        people = database.read_people_with_open_bookings(session, _REGION)

        self.assertEqual(people, [database_utils.convert_person(person)])
