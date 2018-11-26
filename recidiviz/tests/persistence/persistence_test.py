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
"""Tests for persistence."""

from datetime import datetime

from recidiviz import Session
from recidiviz.ingest.models.ingest_info import IngestInfo, _Bond
from recidiviz.persistence import persistence
from recidiviz.persistence.database import database
from recidiviz.tests.utils import fakes

BIRTHDATE_1 = datetime(year=1993, month=11, day=15)
BIRTHDATE_2 = datetime(year=1996, month=2, day=11)
BOND_TYPE = 'TEST_BOND_TYPE'
CHARGE_NAME_1 = 'TEST_CHARGE_1'
CHARGE_NAME_2 = 'TEST_CHARGE_2'
FACILITY = 'TEST_FACILITY'
GIVEN_NAME = "TEST_GIVEN_NAME"
IS_LIFE_1 = False
IS_LIFE_2 = True
OFFICER_NAME = 'TEST_OFFICER_NAME'
PLACE_1 = 'TEST_PLACE_1'
PLACE_2 = 'TEST_PLACE_2'
SURNAME_1 = 'TEST_SURNAME_1'
SURNAME_2 = 'TEST_SURNAME_2'


class TestPersistence(object):
    """Test that the persistence layer correctly writes to the SQL database."""

    def setup_method(self, _test_method):
        fakes.use_in_memory_sqlite_database()

    def test_twoDifferentPeople_persistsBoth(self):
        # Arrange
        ingest_info = IngestInfo()
        ingest_info.create_person(surname=SURNAME_1)
        ingest_info.create_person(surname=SURNAME_2)

        # Act
        persistence.write(ingest_info)
        result = database.read_people(Session())

        # Assert
        assert len(result) == 2
        assert result[0].surname == SURNAME_1
        assert result[1].surname == SURNAME_2

    def test_sameTwoPeople_persistsOne(self):
        # Arrange
        ingest_info = IngestInfo()
        ingest_info.create_person(surname=SURNAME_1)
        ingest_info.create_person(surname=SURNAME_1)

        # Act
        persistence.write(ingest_info)
        result = database.read_people(Session())

        # Assert
        assert len(result) == 1
        assert result[0].surname == SURNAME_1

    # TODO: Consider not replace everything with the newly scraped data, instead
    # update only the newly scraped fields (keeping old data)
    def test_sameTwoPeople_matchesPeopleAndReplacesWithNewerData(self):
        # Arrange
        ingest_info = IngestInfo()
        ingest_info.create_person(surname=SURNAME_1, place_of_residence=PLACE_1,
                                  given_names=GIVEN_NAME)
        persistence.write(ingest_info)

        ingest_info = IngestInfo()
        ingest_info.create_person(surname=SURNAME_1, place_of_residence=PLACE_2)

        # Act
        persistence.write(ingest_info)
        result = database.read_people(Session())

        # Assert
        assert len(result) == 1
        assert result[0].surname == SURNAME_1
        assert result[0].place_of_residence == PLACE_2
        assert result[0].given_names is None

    def test_readSinglePersonByName(self):
        # Arrange
        ingest_info = IngestInfo()
        ingest_info.create_person(surname=SURNAME_1, birthdate=BIRTHDATE_1)
        ingest_info.create_person(surname=SURNAME_2, birthdate=BIRTHDATE_2)

        # Act
        persistence.write(ingest_info)
        result = database.read_people(Session(), surname=SURNAME_1)

        # Assert
        assert len(result) == 1
        assert result[0].surname == SURNAME_1
        assert result[0].birthdate == BIRTHDATE_1

    # TODO: Rewrite this test to directly test __eq__ between the two People
    def test_readPersonAndAllRelationships(self):
        # Arrange
        ingest_info = IngestInfo()
        person = ingest_info.create_person(surname=SURNAME_1)
        booking = person.create_booking(facility=FACILITY)
        booking.create_arrest(officer_name=OFFICER_NAME)

        shared_bond = _Bond(bond_type=BOND_TYPE)

        charge_1 = booking.create_charge(name=CHARGE_NAME_1)
        charge_1.bond = shared_bond
        charge_1.create_sentence(is_life=IS_LIFE_1)

        charge_2 = booking.create_charge(name=CHARGE_NAME_2)
        charge_2.bond = shared_bond
        charge_2.create_sentence(is_life=IS_LIFE_2)

        # Act
        persistence.write(ingest_info)
        result = database.read_people(Session())

        # Assert
        assert len(result) == 1
        result_person = result[0]
        assert result_person.surname == SURNAME_1

        assert len(result_person.bookings) == 1
        result_booking = result_person.bookings[0]
        assert result_booking.facility == FACILITY

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
        assert sentence_1.is_life == IS_LIFE_1
        assert sentence_2.is_life == IS_LIFE_2
