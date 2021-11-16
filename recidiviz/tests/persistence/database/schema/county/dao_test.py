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

"""Tests for county/dao.py."""

import datetime
from copy import deepcopy
from unittest import TestCase

from recidiviz.common.constants.county.booking import CustodyStatus
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.persistence.database.schema.county import dao
from recidiviz.persistence.database.schema.county.schema import Booking, Person
from recidiviz.persistence.database.schema_entity_converter import (
    schema_entity_converter as converter,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.county import entities
from recidiviz.tests.persistence.database.database_test_utils import (
    FakeLegacyStateAndJailsIngestMetadata,
)
from recidiviz.tests.utils import fakes

_REGION = "region"
_REGION_ANOTHER = "wrong region"
_JURISDICTION_ID = "12345678"
_FULL_NAME = "full_name"
_EXTERNAL_ID = "external_id"
_BIRTHDATE = datetime.date(year=2012, month=1, day=2)
_FACILITY = "facility"
_DEFAULT_METADATA = FakeLegacyStateAndJailsIngestMetadata.for_county(
    region="default_region"
)

DATE_SCRAPED = datetime.date(year=2019, month=1, day=1)


class TestDao(TestCase):
    """Test that the methods in dao.py correctly read from the SQL database."""

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.JAILS)
        fakes.use_in_memory_sqlite_database(self.database_key)

    def tearDown(self) -> None:
        fakes.teardown_in_memory_sqlite_databases()

    def test_readPeopleWithOpenBookingsBeforeDate(self):
        # Arrange
        person = Person(person_id=8, region=_REGION, jurisdiction_id=_JURISDICTION_ID)
        person_resolved_booking = Person(
            person_id=9, region=_REGION, jurisdiction_id=_JURISDICTION_ID
        )
        person_most_recent_scrape = Person(
            person_id=10, region=_REGION, jurisdiction_id=_JURISDICTION_ID
        )
        person_wrong_region = Person(
            person_id=11, region=_REGION_ANOTHER, jurisdiction_id=_JURISDICTION_ID
        )

        release_date = datetime.date(2018, 7, 20)
        most_recent_scrape_date = datetime.datetime(2018, 6, 20)
        date_in_past = most_recent_scrape_date - datetime.timedelta(days=1)
        first_seen_time = most_recent_scrape_date - datetime.timedelta(days=3)

        # Bookings that should be returned
        open_booking_before_last_scrape = Booking(
            person_id=person.person_id,
            custody_status=CustodyStatus.IN_CUSTODY.value,
            first_seen_time=first_seen_time,
            last_seen_time=date_in_past,
        )

        # Bookings that should not be returned
        open_booking_incorrect_region = Booking(
            person_id=person_wrong_region.person_id,
            custody_status=CustodyStatus.IN_CUSTODY.value,
            first_seen_time=first_seen_time,
            last_seen_time=date_in_past,
        )
        open_booking_most_recent_scrape = Booking(
            person_id=person_most_recent_scrape.person_id,
            custody_status=CustodyStatus.IN_CUSTODY.value,
            first_seen_time=first_seen_time,
            last_seen_time=most_recent_scrape_date,
        )
        resolved_booking = Booking(
            person_id=person_resolved_booking.person_id,
            custody_status=CustodyStatus.RELEASED.value,
            release_date=release_date,
            first_seen_time=first_seen_time,
            last_seen_time=date_in_past,
        )

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(person)
            session.add(person_resolved_booking)
            session.add(person_most_recent_scrape)
            session.add(person_wrong_region)
            session.add(open_booking_before_last_scrape)
            session.add(open_booking_incorrect_region)
            session.add(open_booking_most_recent_scrape)
            session.add(resolved_booking)
            session.commit()

            # Act
            people = dao.read_people_with_open_bookings_scraped_before_time(
                session, person.region, most_recent_scrape_date
            )

            # Assert
            self.assertEqual(
                people, [converter.convert_schema_object_to_entity(person)]
            )

    def test_readPeopleByExternalId(self):
        admission_date = datetime.datetime(2018, 6, 20)
        release_date = datetime.date(2018, 7, 20)
        closed_booking = Booking(
            custody_status=CustodyStatus.IN_CUSTODY.value,
            admission_date=admission_date,
            release_date=release_date,
            first_seen_time=admission_date,
            last_seen_time=admission_date,
        )

        person_no_match = Person(
            person_id=1,
            region=_REGION,
            jurisdiction_id=_JURISDICTION_ID,
            bookings=[deepcopy(closed_booking)],
        )
        person_match_external_id = Person(
            person_id=2,
            region=_REGION,
            jurisdiction_id=_JURISDICTION_ID,
            bookings=[closed_booking],
            external_id=_EXTERNAL_ID,
        )

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(person_no_match)
            session.add(person_match_external_id)
            session.commit()

            ingested_person = entities.Person.new_with_defaults(
                external_id=_EXTERNAL_ID
            )
            people = dao.read_people_by_external_ids(
                session, _REGION, [ingested_person]
            )

            expected_people = [
                converter.convert_schema_object_to_entity(person_match_external_id)
            ]
            self.assertCountEqual(people, expected_people)

    def test_readPeopleWithOpenBookings(self):
        admission_date = datetime.datetime(2018, 6, 20)
        release_date = datetime.date(2018, 7, 20)

        open_booking = Booking(
            custody_status=CustodyStatus.IN_CUSTODY.value,
            admission_date=admission_date,
            first_seen_time=admission_date,
            last_seen_time=admission_date,
        )
        closed_booking = Booking(
            custody_status=CustodyStatus.RELEASED.value,
            admission_date=admission_date,
            release_date=release_date,
            first_seen_time=admission_date,
            last_seen_time=admission_date,
        )

        person_no_match = Person(
            person_id=1,
            region=_REGION,
            jurisdiction_id=_JURISDICTION_ID,
            bookings=[deepcopy(open_booking)],
        )
        person_match_full_name = Person(
            person_id=2,
            region=_REGION,
            jurisdiction_id=_JURISDICTION_ID,
            bookings=[deepcopy(open_booking)],
            full_name=_FULL_NAME,
        )
        person_no_open_bookings = Person(
            person_id=6,
            region=_REGION,
            jurisdiction_id=_JURISDICTION_ID,
            full_name=_FULL_NAME,
            bookings=[closed_booking],
        )

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            session.add(person_no_match)
            session.add(person_no_open_bookings)
            session.add(person_match_full_name)
            session.commit()

            info = IngestInfo()
            info.create_person(full_name=_FULL_NAME, person_id=_EXTERNAL_ID)
            people = dao.read_people_with_open_bookings(session, _REGION, info.people)

            expected_people = [
                converter.convert_schema_object_to_entity(p)
                for p in [person_match_full_name]
            ]
            self.assertCountEqual(people, expected_people)
