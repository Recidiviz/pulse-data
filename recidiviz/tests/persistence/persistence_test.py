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
"""Tests for persistence.py."""

from datetime import date, datetime, timedelta
from unittest import TestCase

from mock import patch, Mock
from recidiviz import Session
from recidiviz.common.constants.booking import ReleaseReason, CustodyStatus
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models.ingest_info_pb2 import IngestInfo, Charge, \
    Sentence
from recidiviz.persistence import persistence, entities
from recidiviz.persistence.database import database, schema
from recidiviz.tests.utils import fakes

BIRTHDATE_1 = '11/15/1993'
BIRTHDATE_1_DATE = date(year=1993, month=11, day=15)
BIRTHDATE_2 = '11-2-1996'
BOND_TYPE = 'Cash'
BOND_STATUS = 'Active'
BOOKING_CUSTODY_STATUS = 'In Custody'
BOOKING_ID = 19
CHARGE_NAME_1 = 'TEST_CHARGE_1'
CHARGE_NAME_2 = 'TEST_CHARGE_2'
CHARGE_STATUS = 'Pending'
EXTERNAL_PERSON_ID = 'EXTERNAL_PERSON_ID'
EXTERNAL_BOOKING_ID = 'EXTERNAL_BOOKING_ID'
FACILITY = 'TEST_FACILITY'
FINE_1 = '$1,500.25'
FINE_1_INT = 1500
FINE_2 = ' '
FINE_2_INT = 0
GIVEN_NAME = "TEST_GIVEN_NAME"
OFFICER_NAME = 'TEST_OFFICER_NAME'
PERSON_ID = 9
PLACE_1 = 'TEST_PLACE_1'
PLACE_2 = 'TEST_PLACE_2'
REGION_1 = 'REGION_1'
REGION_2 = 'REGION_2'
SCRAPER_START_DATETIME = datetime(year=2018, month=8, day=6)
SURNAME_1 = 'TEST_SURNAME_1'
SURNAME_2 = 'TEST_SURNAME_2'
DEFAULT_METADATA = IngestMetadata(
    "default_region", datetime(year=1000, month=1, day=1), {})


@patch('os.getenv', Mock(return_value='production'))
@patch.dict('os.environ', {'PERSIST_LOCALLY': 'false'})
class TestPersistence(TestCase):
    """Test that the persistence layer correctly writes to the SQL database."""

    def setup_method(self, _test_method):
        fakes.use_in_memory_sqlite_database()

    def test_localRun(self):
        with patch('os.getenv', Mock(return_value='Local')):
            # Arrange
            ingest_info = IngestInfo()
            ingest_info.people.add(surname=SURNAME_1)

            # Act
            persistence.write(ingest_info, DEFAULT_METADATA)
            result = database.read_people(Session())

            # Assert
            assert not result

    def test_persistLocally(self):
        # Arrange
        with patch('os.getenv', Mock(return_value='local')) \
             and patch.dict('os.environ', {'PERSIST_LOCALLY': 'true'}):

            ingest_info = IngestInfo()
            ingest_info.people.add(surname=SURNAME_1)

            # Act
            persistence.write(ingest_info, DEFAULT_METADATA)
            result = database.read_people(Session())

            # Assert
            assert len(result) == 1
            assert result[0].surname == SURNAME_1

    def test_twoDifferentPeople_persistsBoth(self):
        # Arrange
        ingest_info = IngestInfo()
        ingest_info.people.add(surname=SURNAME_1, given_names=GIVEN_NAME)
        ingest_info.people.add(surname=SURNAME_2, given_names=GIVEN_NAME)

        # Act
        persistence.write(ingest_info, DEFAULT_METADATA)
        result = database.read_people(Session())

        # Assert
        assert len(result) == 2
        assert result[0].surname == SURNAME_1
        assert result[1].surname == SURNAME_2

    # TODO: test entity matching end to end

    def test_readSinglePersonByName(self):
        # Arrange
        ingest_info = IngestInfo()
        ingest_info.people.add(surname=SURNAME_1, given_names=GIVEN_NAME,
                               birthdate=BIRTHDATE_1)
        ingest_info.people.add(surname=SURNAME_2, given_names=GIVEN_NAME,
                               birthdate=BIRTHDATE_2)

        # Act
        persistence.write(ingest_info, DEFAULT_METADATA)
        result = database.read_people(Session(), surname=SURNAME_1)

        # Assert
        assert len(result) == 1
        assert result[0].surname == SURNAME_1
        assert result[0].birthdate == BIRTHDATE_1_DATE

    # TODO: Rewrite this test to directly test __eq__ between the two People
    def test_readPersonAndAllRelationships(self):
        # Arrange
        metadata = IngestMetadata.new_with_none_defaults(
            region=REGION_1,
            last_seen_time=SCRAPER_START_DATETIME)

        ingest_info = IngestInfo()
        ingest_info.people.add(
            surname=SURNAME_1,
            given_names=GIVEN_NAME,
            booking_ids=['BOOKING_ID']
        )
        ingest_info.bookings.add(
            booking_id='BOOKING_ID',
            facility=FACILITY,
            custody_status=BOOKING_CUSTODY_STATUS,
            arrest_id='ARREST_ID',
            charge_ids=['CHARGE_ID_1', 'CHARGE_ID_2']
        )
        ingest_info.arrests.add(
            arrest_id='ARREST_ID',
            officer_name=OFFICER_NAME
        )

        ingest_info.bonds.add(
            bond_id='SHARED_BOND_ID',
            bond_type=BOND_TYPE,
            status=BOND_STATUS
        )

        ingest_info.charges.extend([
            Charge(
                charge_id='CHARGE_ID_1',
                name=CHARGE_NAME_1,
                status=CHARGE_STATUS,
                bond_id='SHARED_BOND_ID',
                sentence_id='SENTENCE_ID_1'

            ), Charge(
                charge_id='CHARGE_ID_2',
                name=CHARGE_NAME_2,
                status=CHARGE_STATUS,
                bond_id='SHARED_BOND_ID',
                sentence_id='SENTENCE_ID_2'
            )
        ])

        ingest_info.sentences.extend([
            Sentence(
                sentence_id='SENTENCE_ID_1',
                fine_dollars=FINE_1
            ), Sentence(
                sentence_id='SENTENCE_ID_2',
                fine_dollars=FINE_2
            )
        ])

        # Act
        persistence.write(ingest_info, metadata)
        result = database.read_people(Session())

        # Assert
        assert len(result) == 1
        result_person = result[0]
        assert result_person.surname == SURNAME_1

        assert len(result_person.bookings) == 1
        result_booking = result_person.bookings[0]
        assert result_booking.facility == FACILITY
        assert result_booking.last_seen_time == SCRAPER_START_DATETIME

        result_arrest = result_booking.arrest
        assert result_arrest.officer_name == OFFICER_NAME

        result_charges = result_booking.charges
        assert len(result_charges) == 2
        assert result_charges[0].name == CHARGE_NAME_1
        assert result_charges[1].name == CHARGE_NAME_2

        bond_1 = result_charges[0].bond
        bond_2 = result_charges[1].bond
        assert bond_1.bond_type == bond_2.bond_type

        sentence_1 = result_charges[0].sentence
        sentence_2 = result_charges[1].sentence
        assert sentence_1.fine_dollars == FINE_1_INT
        assert sentence_2.fine_dollars == FINE_2_INT

    def test_write_preexisting_person(self):
        # Arrange
        most_recent_scrape_time = (SCRAPER_START_DATETIME + timedelta(days=1))
        metadata = IngestMetadata.new_with_none_defaults(
            region=REGION_1,
            last_seen_time=most_recent_scrape_time)

        schema_booking = schema.Booking(
            booking_id=BOOKING_ID,
            external_id=EXTERNAL_BOOKING_ID,
            admission_date_inferred=True,
            custody_status=CustodyStatus.IN_CUSTODY.value,
            last_seen_time=SCRAPER_START_DATETIME)
        schema_person = schema.Person(
            person_id=PERSON_ID,
            external_id=EXTERNAL_PERSON_ID,
            given_names=GIVEN_NAME,
            region=REGION_1,
            bookings=[schema_booking])

        session = Session()
        session.add(schema_person)
        session.commit()

        ingest_info = IngestInfo()
        ingest_info.people.add(surname=SURNAME_1,
                               given_names=GIVEN_NAME,
                               person_id=EXTERNAL_PERSON_ID,
                               booking_ids=[EXTERNAL_BOOKING_ID])
        ingest_info.bookings.add(
            booking_id=EXTERNAL_BOOKING_ID,
            custody_status='IN CUSTODY',
        )

        # Act
        persistence.write(ingest_info, metadata)

        # Assert
        expected_booking = entities.Booking(
            booking_id=BOOKING_ID,
            external_id=EXTERNAL_BOOKING_ID,
            admission_date_inferred=True,
            custody_status=CustodyStatus.IN_CUSTODY,
            last_seen_time=most_recent_scrape_time)
        expected_person = entities.Person.new_with_none_defaults(
            person_id=PERSON_ID,
            external_id=EXTERNAL_PERSON_ID,
            given_names=GIVEN_NAME,
            region=REGION_1,
            surname=SURNAME_1,
            bookings=[expected_booking])
        self.assertEqual([expected_person], database.read_people(Session()))

    def test_inferReleaseDateOnOpenBookings(self):
        # Arrange
        metadata_1 = IngestMetadata.new_with_none_defaults(
            region=REGION_1,
            last_seen_time=SCRAPER_START_DATETIME)
        metadata_2 = IngestMetadata(region=REGION_2,
                                    last_seen_time=SCRAPER_START_DATETIME)

        most_recent_scrape_time = (SCRAPER_START_DATETIME + timedelta(days=1))

        ingest_info = IngestInfo()
        ingest_info.people.add(surname=SURNAME_1,
                               given_names=GIVEN_NAME,
                               booking_ids=['BOOKING_ID_1'])
        ingest_info.bookings.add(
            booking_id='BOOKING_ID_1',
            custody_status='IN CUSTODY',
            charge_ids=['CHARGE_ID_1']
        )
        ingest_info.charges.add(
            charge_id='CHARGE_ID_1',
            name=CHARGE_NAME_1,
            status='PENDING')

        ingest_info_other_region = IngestInfo()
        ingest_info_other_region.people.add(surname=SURNAME_2,
                                            given_names=GIVEN_NAME,
                                            booking_ids=['BOOKING_ID_2'])
        ingest_info_other_region.bookings.add(
            booking_id='BOOKING_ID_2',
            custody_status='IN CUSTODY',
            charge_ids=['CHARGE_ID_2']
        )
        ingest_info_other_region.charges.add(
            charge_id='CHARGE_ID_2',
            name=CHARGE_NAME_2,
            status='PENDING')

        # Act
        persistence.write(ingest_info, metadata_1)
        persistence.infer_release_on_open_bookings(
            REGION_1, most_recent_scrape_time)
        persistence.write(ingest_info_other_region, metadata_2)

        # Assert
        bookings = database.read_bookings(Session())
        assert bookings[0].last_seen_time == SCRAPER_START_DATETIME
        assert bookings[0].release_date == most_recent_scrape_time.date()
        assert bookings[0].release_reason == ReleaseReason.INFERRED_RELEASE
        assert bookings[0].charges[0].name == CHARGE_NAME_1

        assert bookings[1].last_seen_time == SCRAPER_START_DATETIME
        assert bookings[1].release_date is None
        assert bookings[1].charges[0].name == CHARGE_NAME_2
        # TODO: assert status when RESOLVED status is handled
