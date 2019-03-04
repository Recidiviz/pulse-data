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

import attr
import pytest

from mock import patch, Mock

import recidiviz.ingest.models.ingest_info as ii

from recidiviz import Session
from recidiviz.common.constants.bond import BondStatus
from recidiviz.common.constants.booking import CustodyStatus
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.constants.hold import HoldStatus
from recidiviz.common.constants.sentence import SentenceStatus
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models.ingest_info_pb2 import IngestInfo, Charge, \
    Sentence
from recidiviz.ingest.scrape.ingest_utils import convert_ingest_info_to_proto
from recidiviz.persistence import persistence, entities
from recidiviz.persistence.database import database, schema
from recidiviz.persistence.errors import PersistenceError
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
DATE_RAW = '1/1/2011'
DATE = date(year=2019, day=1, month=2)
DATE_2 = date(year=2020, day=1, month=2)
EXTERNAL_PERSON_ID = 'EXTERNAL_PERSON_ID'
EXTERNAL_BOOKING_ID = 'EXTERNAL_BOOKING_ID'
FACILITY = 'TEST_FACILITY'
FINE_1 = '$1,500.25'
FINE_1_INT = 1500
FINE_2 = ' '
FINE_2_INT = 0
OFFICER_NAME = 'TEST_OFFICER_NAME'
PERSON_ID = 9
PLACE_1 = 'TEST_PLACE_1'
PLACE_2 = 'TEST_PLACE_2'
REGION_1 = 'REGION_1'
REGION_2 = 'REGION_2'
SCRAPER_START_DATETIME = datetime(year=2018, month=8, day=6)
SENTENCE_STATUS = 'SERVING'
FULL_NAME_1 = 'TEST_FULL_NAME_1'
FULL_NAME_2 = 'TEST_FULL_NAME_2'
FULL_NAME_3 = 'TEST_FULL_NAME_3'
DEFAULT_METADATA = IngestMetadata("default_region",
                                  datetime(year=1000, month=1, day=1),
                                  EnumOverrides.empty())
ID = 1
ID_2 = 2
ID_3 = 3


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
            ingest_info.people.add(full_name=FULL_NAME_1)

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
            ingest_info.people.add(full_name=FULL_NAME_1)

            # Act
            persistence.write(ingest_info, DEFAULT_METADATA)
            result = database.read_people(Session())

            # Assert
            assert len(result) == 1
            assert result[0].full_name == _format_full_name(FULL_NAME_1)

    def test_multipleOpenBookings_raisesPersistenceError(self):
        ingest_info = ii.IngestInfo()
        person = ingest_info.create_person(full_name=FULL_NAME_1)
        person.create_booking(admission_date=DATE_RAW)
        person.create_booking(admission_date=DATE_RAW)

        with pytest.raises(PersistenceError):
            persistence.write(convert_ingest_info_to_proto(ingest_info),
                              DEFAULT_METADATA)

    def test_twoDifferentPeople_persistsBoth(self):
        # Arrange
        ingest_info = IngestInfo()
        ingest_info.people.add(full_name=FULL_NAME_1)
        ingest_info.people.add(full_name=FULL_NAME_2)

        # Act
        persistence.write(ingest_info, DEFAULT_METADATA)
        result = database.read_people(Session())

        # Assert
        assert len(result) == 2

        assert result[0].full_name == _format_full_name(FULL_NAME_2)
        assert result[1].full_name == _format_full_name(FULL_NAME_1)

    def test_twoDifferentPeople_persistsNoneProtectedError(self):
        # Arrange
        ingest_info = IngestInfo()
        ingest_info.people.add(full_name=FULL_NAME_1)
        ingest_info.people.add(full_name=FULL_NAME_2, gender='X')

        # Act
        with self.assertRaises(PersistenceError):
            persistence.write(ingest_info, DEFAULT_METADATA)
        result = database.read_people(Session())

        # Assert
        assert not result

    def test_twoDifferentPeople_persistsNoneErrorThreshold(self):
        # Arrange
        ingest_info = IngestInfo()
        ingest_info.people.add(full_name=FULL_NAME_2)
        ingest_info.people.add(full_name=FULL_NAME_1,
                               person_id=EXTERNAL_PERSON_ID,
                               booking_ids=[EXTERNAL_BOOKING_ID])
        ingest_info.bookings.add(
            booking_id=EXTERNAL_BOOKING_ID,
            custody_status='NO EXIST',
        )

        # Act
        with self.assertRaises(PersistenceError):
            persistence.write(ingest_info, DEFAULT_METADATA)
        result = database.read_people(Session())

        # Assert
        assert not result

    def test_threeDifferentPeople_persistsTwoBelowThreshold(self):
        # Arrange
        ingest_info = IngestInfo()
        ingest_info.people.add(full_name=FULL_NAME_2)
        ingest_info.people.add(full_name=FULL_NAME_3)
        ingest_info.people.add(full_name=FULL_NAME_1,
                               person_id=EXTERNAL_PERSON_ID,
                               booking_ids=[EXTERNAL_BOOKING_ID])
        ingest_info.bookings.add(
            booking_id=EXTERNAL_BOOKING_ID,
            custody_status='NO EXIST',
        )

        # Act
        persistence.write(ingest_info, DEFAULT_METADATA)
        result = database.read_people(Session())

        # Assert
        assert len(result) == 2
        assert result[0].full_name == _format_full_name(FULL_NAME_3)
        assert result[1].full_name == _format_full_name(FULL_NAME_2)

    # TODO: test entity matching end to end

    def test_readSinglePersonByName(self):
        # Arrange
        ingest_info = IngestInfo()
        ingest_info.people.add(full_name=FULL_NAME_1, birthdate=BIRTHDATE_1)
        ingest_info.people.add(full_name=FULL_NAME_2, birthdate=BIRTHDATE_2)

        # Act
        persistence.write(ingest_info, DEFAULT_METADATA)
        result = database.read_people(
            Session(), full_name=_format_full_name(FULL_NAME_1))

        # Assert
        assert len(result) == 1
        assert result[0].full_name == _format_full_name(FULL_NAME_1)
        assert result[0].birthdate == BIRTHDATE_1_DATE

    # TODO: Rewrite this test to directly test __eq__ between the two People
    def test_readPersonAndAllRelationships(self):
        # Arrange
        metadata = IngestMetadata.new_with_defaults(
            region=REGION_1,
            last_seen_time=SCRAPER_START_DATETIME)

        ingest_info = IngestInfo()
        ingest_info.people.add(
            full_name=FULL_NAME_1,
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
                fine_dollars=FINE_1,
                status=SENTENCE_STATUS
            ), Sentence(
                sentence_id='SENTENCE_ID_2',
                fine_dollars=FINE_2,
                status=SENTENCE_STATUS
            )
        ])

        # Act
        persistence.write(ingest_info, metadata)
        result = database.read_people(Session())

        # Assert
        assert len(result) == 1
        result_person = result[0]
        assert result_person.full_name == _format_full_name(FULL_NAME_1)

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
        metadata = IngestMetadata.new_with_defaults(
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
            region=REGION_1,
            bookings=[schema_booking])

        session = Session()
        session.add(schema_person)
        session.commit()

        ingest_info = IngestInfo()
        ingest_info.people.add(full_name=FULL_NAME_1,
                               person_id=EXTERNAL_PERSON_ID,
                               booking_ids=[EXTERNAL_BOOKING_ID])
        ingest_info.bookings.add(
            booking_id=EXTERNAL_BOOKING_ID,
            custody_status='IN CUSTODY',
        )

        # Act
        persistence.write(ingest_info, metadata)

        # Assert
        expected_booking = entities.Booking.new_with_defaults(
            booking_id=BOOKING_ID,
            external_id=EXTERNAL_BOOKING_ID,
            admission_date_inferred=True,
            custody_status=CustodyStatus.IN_CUSTODY,
            custody_status_raw_text=BOOKING_CUSTODY_STATUS.upper(),
            last_seen_time=most_recent_scrape_time)
        expected_person = entities.Person.new_with_defaults(
            person_id=PERSON_ID,
            external_id=EXTERNAL_PERSON_ID,
            region=REGION_1,
            full_name=_format_full_name(FULL_NAME_1),
            bookings=[expected_booking])
        self.assertEqual([expected_person], database.read_people(Session()))

    def test_inferReleaseDateOnOpenBookings(self):
        # Arrange
        hold = entities.Hold.new_with_defaults(
            hold_id=ID, status=HoldStatus.ACTIVE, status_raw_text='ACTIVE')
        sentence = entities.Sentence.new_with_defaults(
            sentence_id=ID, status=SentenceStatus.SERVING,
            status_raw_text='SERVING',
            booking_id=ID)
        bond = entities.Bond.new_with_defaults(
            bond_id=ID, status=BondStatus.NOT_REQUIRED,
            status_raw_text='NOT_REQUIRED',
            booking_id=ID)
        charge = entities.Charge.new_with_defaults(
            charge_id=ID, status=ChargeStatus.PENDING,
            status_raw_text='PENDING', sentence=sentence, bond=bond)
        booking_open = entities.Booking.new_with_defaults(
            booking_id=ID,
            custody_status=CustodyStatus.IN_CUSTODY,
            custody_status_raw_text='IN CUSTODY',
            admission_date=DATE,
            last_seen_time=SCRAPER_START_DATETIME - timedelta(days=1),
            charges=[charge],
            holds=[hold])
        booking_resolved = attr.evolve(
            booking_open, booking_id=ID_2,
            custody_status=CustodyStatus.RELEASED,
            custody_status_raw_text='RELEASED', release_date=DATE_2, charges=[],
            holds=[])
        booking_open_most_recent_scrape = attr.evolve(
            booking_open, booking_id=ID_3,
            last_seen_time=SCRAPER_START_DATETIME, charges=[], holds=[])

        person = entities.Person.new_with_defaults(
            person_id=ID, region=REGION_1,
            bookings=[booking_open, booking_resolved])
        person_unmatched = entities.Person.new_with_defaults(
            person_id=ID_2, region=REGION_1,
            bookings=[booking_open_most_recent_scrape])

        session = Session()
        database.write_person(session, person, DEFAULT_METADATA)
        database.write_person(session, person_unmatched, DEFAULT_METADATA)
        session.commit()
        session.close()

        expected_hold = attr.evolve(
            hold, status=HoldStatus.UNKNOWN_REMOVED_FROM_SOURCE,
            status_raw_text=None)
        expected_sentence = attr.evolve(
            sentence, status=SentenceStatus.UNKNOWN_REMOVED_FROM_SOURCE,
            status_raw_text=None)
        expected_bond = attr.evolve(
            bond, status=BondStatus.UNKNOWN_REMOVED_FROM_SOURCE,
            status_raw_text=None)
        expected_charge = attr.evolve(
            charge, status=ChargeStatus.UNKNOWN_REMOVED_FROM_SOURCE,
            status_raw_text=None, bond=expected_bond,
            sentence=expected_sentence)
        expected_resolved_booking = attr.evolve(
            booking_open, custody_status=CustodyStatus.INFERRED_RELEASE,
            custody_status_raw_text=None,
            release_date=SCRAPER_START_DATETIME.date(),
            release_date_inferred=True, charges=[expected_charge],
            holds=[expected_hold])
        expected_person = attr.evolve(
            person, bookings=[expected_resolved_booking, booking_resolved])

        # Act
        persistence.infer_release_on_open_bookings(
            REGION_1, SCRAPER_START_DATETIME, CustodyStatus.INFERRED_RELEASE)

        # Assert
        people = database.read_people(Session())
        self.assertCountEqual(people, [expected_person, person_unmatched])

def _format_full_name(full_name: str) -> str:
    return '{{"full_name": "{}"}}'.format(full_name)
