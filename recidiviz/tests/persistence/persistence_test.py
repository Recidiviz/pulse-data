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
from recidiviz.ingest.models.ingest_info import IngestInfo, _Bond
from recidiviz.persistence import persistence
from recidiviz.persistence.database import database
from recidiviz.persistence.database.schema import Charge
from recidiviz.tests.utils import fakes

BIRTHDATE_1 = '11/15/1993'
BIRTHDATE_1_DATE = date(year=1993, month=11, day=15)
BIRTHDATE_2 = '11-2-1996'
BOND_TYPE = 'Cash'
BOND_STATUS = 'Active'
BOOKING_CUSTODY_STATUS = 'In Custody'
CHARGE_NAME_1 = 'TEST_CHARGE_1'
CHARGE_NAME_2 = 'TEST_CHARGE_2'
CHARGE_STATUS = 'Pending'
FACILITY = 'TEST_FACILITY'
FINE_1 = '$1,500.25'
FINE_1_INT = 1500
FINE_2 = ' '
FINE_2_INT = 0
GIVEN_NAME = "TEST_GIVEN_NAME"
OFFICER_NAME = 'TEST_OFFICER_NAME'
PLACE_1 = 'TEST_PLACE_1'
PLACE_2 = 'TEST_PLACE_2'
REGION_1 = 'REGION_1'
REGION_2 = 'REGION_2'
SCRAPER_START_DATETIME = datetime(year=2018, month=8, day=06)
SURNAME_1 = 'TEST_SURNAME_1'
SURNAME_2 = 'TEST_SURNAME_2'


@patch('os.getenv', Mock(return_value='Google App Engine/'))
@patch.dict('os.environ', {'PERSIST_LOCALLY': 'false'})
class TestPersistence(TestCase):
    """Test that the persistence layer correctly writes to the SQL database."""

    def setup_method(self, _test_method):
        fakes.use_in_memory_sqlite_database()

    def test_localRun(self):
        with patch('os.getenv', Mock(return_value='Local')):
            # Arrange
            ingest_info = IngestInfo()
            ingest_info.create_person(surname=SURNAME_1)

            # Act
            persistence.write(ingest_info, SCRAPER_START_DATETIME)
            result = database.read_people(Session())

            # Assert
            assert not result

    def test_persistLocally(self):
        # Arrange
        with patch('os.getenv', Mock(return_value='Local'))\
                and patch.dict('os.environ', {'PERSIST_LOCALLY': 'true'}):
            ingest_info = IngestInfo()
            ingest_info.create_person(surname=SURNAME_1)

            # Act
            persistence.write(ingest_info, SCRAPER_START_DATETIME)
            result = database.read_people(Session())

            # Assert
            assert len(result) == 1
            assert result[0].surname == SURNAME_1

    def test_twoDifferentPeople_persistsBoth(self):
        # Arrange
        ingest_info = IngestInfo()
        ingest_info.create_person(surname=SURNAME_1, given_names=GIVEN_NAME)
        ingest_info.create_person(surname=SURNAME_2, given_names=GIVEN_NAME)

        # Act
        persistence.write(ingest_info, SCRAPER_START_DATETIME)
        result = database.read_people(Session())

        # Assert
        assert len(result) == 2
        assert result[0].surname == SURNAME_1
        assert result[1].surname == SURNAME_2

    def test_sameTwoPeople_persistsOne(self):
        # Arrange
        ingest_info = IngestInfo()
        ingest_info.create_person(surname=SURNAME_1, given_names=GIVEN_NAME)
        ingest_info.create_person(surname=SURNAME_1, given_names=GIVEN_NAME)

        # Act
        persistence.write(ingest_info, SCRAPER_START_DATETIME)
        result = database.read_people(Session())

        # Assert
        assert len(result) == 1
        assert result[0].surname == SURNAME_1

    # TODO: Consider not replace everything with the newly scraped data, instead
    # update only the newly scraped fields (keeping old data)
    def test_sameTwoPeople_matchesPeopleAndReplacesWithNewerData(self):
        # Arrange
        ingest_info = IngestInfo()
        ingest_info.create_person(surname=SURNAME_1,
                                  given_names=GIVEN_NAME,
                                  place_of_residence=PLACE_1,
                                  birthdate=BIRTHDATE_1)
        persistence.write(ingest_info, SCRAPER_START_DATETIME)

        ingest_info = IngestInfo()
        ingest_info.create_person(surname=SURNAME_1,
                                  given_names=GIVEN_NAME,
                                  place_of_residence=PLACE_2)
        # Act
        persistence.write(ingest_info, SCRAPER_START_DATETIME)
        result = database.read_people(Session())

        # Assert
        assert len(result) == 1
        assert result[0].surname == SURNAME_1
        assert result[0].place_of_residence == PLACE_2
        assert result[0].birthdate == BIRTHDATE_1_DATE

    def test_readSinglePersonByName(self):
        # Arrange
        ingest_info = IngestInfo()
        ingest_info.create_person(surname=SURNAME_1, given_names=GIVEN_NAME,
                                  birthdate=BIRTHDATE_1)
        ingest_info.create_person(surname=SURNAME_2, given_names=GIVEN_NAME,
                                  birthdate=BIRTHDATE_2)

        # Act
        persistence.write(ingest_info, SCRAPER_START_DATETIME)
        result = database.read_people(Session(), surname=SURNAME_1)

        # Assert
        assert len(result) == 1
        assert result[0].surname == SURNAME_1
        assert result[0].birthdate == BIRTHDATE_1_DATE

    # TODO: Rewrite this test to directly test __eq__ between the two People
    def test_readPersonAndAllRelationships(self):
        # Arrange
        ingest_info = IngestInfo()
        person = ingest_info.create_person(surname=SURNAME_1,
                                           given_names=GIVEN_NAME)
        booking = person.create_booking(facility=FACILITY,
                                        custody_status=BOOKING_CUSTODY_STATUS,
                                        region=REGION_1)
        booking.create_arrest(officer_name=OFFICER_NAME)

        shared_bond = _Bond(bond_type=BOND_TYPE, status=BOND_STATUS)

        charge_1 = booking.create_charge(
            name=CHARGE_NAME_1, status=CHARGE_STATUS)
        charge_1.bond = shared_bond
        charge_1.create_sentence(fine_dollars=FINE_1)

        charge_2 = booking.create_charge(
            name=CHARGE_NAME_2, status=CHARGE_STATUS)
        charge_2.bond = shared_bond
        charge_2.create_sentence(fine_dollars=FINE_2)

        # Act
        persistence.write(ingest_info, SCRAPER_START_DATETIME)
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

    def test_inferReleaseDateOnOpenBookings(self):
        # Arrange
        most_recent_scrape_date = \
            (SCRAPER_START_DATETIME + timedelta(days=1)).date()
        charge_1 = Charge(name=CHARGE_NAME_1, status='PENDING')
        charge_2 = Charge(name=CHARGE_NAME_2, status='PENDING')
        ingest_info = IngestInfo()
        person = ingest_info.create_person(surname=SURNAME_1,
                                           given_names=GIVEN_NAME)
        person.create_booking(
            region=REGION_1, charges=[charge_1], custody_status='IN CUSTODY')
        person.create_booking(
            region=REGION_2, charges=[charge_2], custody_status='IN CUSTODY')

        # Act
        persistence.write(ingest_info, SCRAPER_START_DATETIME)
        persistence.infer_release_on_open_bookings(
            REGION_1, most_recent_scrape_date)

        # Assert
        bookings = database.read_bookings(Session())
        assert bookings[0].region == REGION_1
        assert bookings[0].last_seen_time == SCRAPER_START_DATETIME
        assert bookings[0].release_date == most_recent_scrape_date
        assert bookings[0].release_date_inferred is True
        assert bookings[0].charges[0].name == CHARGE_NAME_1
        # TODO: assert status when RESOLVED status is handled

        assert bookings[1].region == REGION_2
        assert bookings[1].last_seen_time == SCRAPER_START_DATETIME
        assert bookings[1].release_date is None
        assert bookings[1].release_date_inferred is None
        assert bookings[1].charges[0].name == CHARGE_NAME_2
        # TODO: assert status when RESOLVED status is handled
