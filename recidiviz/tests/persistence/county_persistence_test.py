# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""County/jails tests for persistence.py"""

from datetime import date, datetime, timedelta
from typing import Dict
from unittest import TestCase

import attr
import mock
import psycopg2
import sqlalchemy
from mock import Mock, call, create_autospec, patch
from psycopg2.errorcodes import NOT_NULL_VIOLATION, SERIALIZATION_FAILURE

from recidiviz.common.constants.bond import BondStatus
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.county.booking import CustodyStatus
from recidiviz.common.constants.county.hold import HoldStatus
from recidiviz.common.constants.county.sentence import SentenceStatus
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.models.ingest_info_pb2 import Charge
from recidiviz.ingest.models.ingest_info_pb2 import IngestInfo as IngestInfoProto
from recidiviz.ingest.models.ingest_info_pb2 import Sentence
from recidiviz.persistence import persistence
from recidiviz.persistence.database import database
from recidiviz.persistence.database.schema.county import dao as county_dao
from recidiviz.persistence.database.schema.county import schema as county_schema
from recidiviz.persistence.database.schema_entity_converter import (
    schema_entity_converter as converter,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.county import entities as county_entities
from recidiviz.persistence.entity_matching.entity_matching_types import MatchedEntities
from recidiviz.persistence.ingest_info_converter.base_converter import (
    EntityDeserializationResult,
)
from recidiviz.persistence.persistence import (
    DATABASE_INVARIANT_THRESHOLD,
    ENTITY_MATCHING_THRESHOLD,
    ENUM_THRESHOLD,
    OVERALL_THRESHOLD,
)
from recidiviz.tests.persistence.database.database_test_utils import (
    FakeLegacyStateAndJailsIngestMetadata,
)
from recidiviz.tests.utils import fakes

ARREST_ID = "ARREST_ID_1"
BIRTHDATE_1 = "11/15/1993"
BIRTHDATE_1_DATE = date(year=1993, month=11, day=15)
BIRTHDATE_2 = "11-2-1996"
BOND_TYPE = "Cash"
BOND_STATUS = "Active"
BOOKING_CUSTODY_STATUS = "In Custody"
BOOKING_ID = 19
CHARGE_NAME_1 = "TEST_CHARGE_1"
CHARGE_NAME_2 = "TEST_CHARGE_2"
CHARGE_STATUS = "Pending"
DATE_RAW = "1/1/2011"
DATE = date(year=2019, day=1, month=2)
DATETIME = datetime(DATE.year, DATE.month, DATE.day)
DATE_2 = date(year=2020, day=1, month=2)
DATETIME_2 = datetime(DATE_2.year, DATE_2.month, DATE_2.day)
EXTERNAL_PERSON_ID = "EXTERNAL_PERSON_ID"
EXTERNAL_BOOKING_ID = "EXTERNAL_BOOKING_ID"
EXTERNAL_ID = "EXTERNAL_ID"
FACILITY = "TEST_FACILITY"
FINE_1 = "$1,500.25"
FINE_1_INT = 1500
FINE_2 = " "
FINE_2_INT = 0
OFFICER_NAME = "TEST_OFFICER_NAME"
OFFICER_NAME_ANOTHER = "SECOND_OFFICER_NAME"
PERSON_ID = 9
PLACE_1 = "TEST_PLACE_1"
PLACE_2 = "TEST_PLACE_2"
REGION_1 = "REGION_1"
REGION_2 = "REGION_2"
SCRAPER_START_DATETIME = datetime(year=2018, month=8, day=6)
SENTENCE_STATUS = "SERVING"
FULL_NAME_1 = "TEST_FULL_NAME_1"
FULL_NAME_2 = "TEST_FULL_NAME_2"
FULL_NAME_3 = "TEST_FULL_NAME_3"
JURISDICTION_ID = "12345678"
DEFAULT_METADATA = FakeLegacyStateAndJailsIngestMetadata.for_county(
    region="region_code",
    jurisdiction_id="12345678",
    ingest_time=datetime(year=1000, month=1, day=1),
)
ID = 1
ID_2 = 2
ID_3 = 3


ERROR_THRESHOLDS_WITH_FORTY_PERCENT_RATIOS = {
    SystemLevel.STATE: {
        OVERALL_THRESHOLD: 0.4,
        ENUM_THRESHOLD: 0.4,
        ENTITY_MATCHING_THRESHOLD: 0.4,
        DATABASE_INVARIANT_THRESHOLD: 0,
    },
    SystemLevel.COUNTY: {
        OVERALL_THRESHOLD: 0.4,
        ENUM_THRESHOLD: 0.4,
        ENTITY_MATCHING_THRESHOLD: 0.4,
        DATABASE_INVARIANT_THRESHOLD: 0,
    },
}

FAKE_PROJECT_ID = "test-project"
STATE_CODE_TO_ENTITY_MATCHING_THRESHOLD_OVERRIDE_FAKE_PROJECT: Dict[
    str, Dict[str, float]
] = {
    FAKE_PROJECT_ID: {
        "US_XY": 0.4,
    }
}


@patch("recidiviz.utils.metadata.project_id", Mock(return_value=FAKE_PROJECT_ID))
@patch("recidiviz.utils.environment.in_gcp", Mock(return_value=True))
@patch.dict("os.environ", {"PERSIST_LOCALLY": "false"})
@patch(
    "recidiviz.persistence.persistence.STATE_CODE_TO_ENTITY_MATCHING_THRESHOLD_OVERRIDE",
    STATE_CODE_TO_ENTITY_MATCHING_THRESHOLD_OVERRIDE_FAKE_PROJECT,
)
class TestPersistence(TestCase):
    """Test that the persistence layer correctly writes to the SQL database."""

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.JAILS)
        fakes.use_in_memory_sqlite_database(self.database_key)

    def tearDown(self) -> None:
        fakes.teardown_in_memory_sqlite_databases()

    def test_localRun(self):
        with patch("recidiviz.utils.environment.in_gcp", Mock(return_value=False)):
            # Arrange
            ingest_info = IngestInfoProto()
            ingest_info.people.add(full_name=FULL_NAME_1)

            # Act
            persistence.write_ingest_info(ingest_info, DEFAULT_METADATA)
            with SessionFactory.using_database(
                self.database_key, autocommit=False
            ) as session:
                result = county_dao.read_people(session)

            # Assert
            assert not result

    def test_persistLocally(self):
        # Arrange
        with patch(
            "recidiviz.utils.environment.in_gcp", Mock(return_value=False)
        ) and patch.dict("os.environ", {"PERSIST_LOCALLY": "true"}):
            ingest_info = IngestInfoProto()
            ingest_info.people.add(full_name=FULL_NAME_1)

            # Act
            persistence.write_ingest_info(ingest_info, DEFAULT_METADATA)
            with SessionFactory.using_database(
                self.database_key, autocommit=False
            ) as session:
                result = county_dao.read_people(session)

            # Assert
            assert len(result) == 1
            assert result[0].full_name == _format_full_name(FULL_NAME_1)

    @patch.object(sqlalchemy.orm.Session, "close")
    @patch.object(sqlalchemy.orm.Session, "commit")
    def test_retryableError_retries(self, mock_commit, mock_close):
        # Arrange
        ingest_info = IngestInfoProto()
        ingest_info.people.add(full_name=FULL_NAME_1)

        inner_error = create_autospec(psycopg2.OperationalError)
        # Serialization Failure is retryable
        inner_error.pgcode = SERIALIZATION_FAILURE
        error = sqlalchemy.exc.DatabaseError(
            statement=None, params=None, orig=inner_error
        )
        # 5 retries is allowed
        mock_commit.side_effect = [error] * 5 + [mock.DEFAULT]

        # Act
        persistence.write_ingest_info(ingest_info, DEFAULT_METADATA)

        # Assert
        assert mock_commit.call_args_list == [call()] * 6
        mock_close.assert_called_once()

    @patch.object(sqlalchemy.orm.Session, "close")
    @patch.object(sqlalchemy.orm.Session, "commit")
    def test_retryableError_exceedsMaxRetries(self, mock_commit, mock_close):
        # Arrange
        ingest_info = IngestInfoProto()
        ingest_info.people.add(full_name=FULL_NAME_1)

        inner_error = create_autospec(psycopg2.OperationalError)
        # Serialization Failure is retryable
        inner_error.pgcode = SERIALIZATION_FAILURE
        error = sqlalchemy.exc.DatabaseError(
            statement=None, params=None, orig=inner_error
        )
        # 6 retries is too many
        mock_commit.side_effect = [error] * 6 + [mock.DEFAULT]

        # Act / Assert
        with self.assertRaises(sqlalchemy.exc.DatabaseError):
            persistence.write_ingest_info(ingest_info, DEFAULT_METADATA)

        # Assert
        assert mock_commit.call_args_list == [call()] * 6
        mock_close.assert_called_once()

    @patch.object(sqlalchemy.orm.Session, "close")
    @patch.object(sqlalchemy.orm.Session, "commit")
    def test_nonRetryableError_failsImmediately(self, mock_commit, mock_close):
        # Arrange
        ingest_info = IngestInfoProto()
        ingest_info.people.add(full_name=FULL_NAME_1)

        inner_error = create_autospec(psycopg2.OperationalError)
        # Not Null Violation is not retryable
        inner_error.pgcode = NOT_NULL_VIOLATION
        error = sqlalchemy.exc.DatabaseError(
            statement=None, params=None, orig=inner_error
        )
        mock_commit.side_effect = [error, mock.DEFAULT]

        # Act / Assert
        with self.assertRaises(sqlalchemy.exc.DatabaseError):
            persistence.write_ingest_info(ingest_info, DEFAULT_METADATA)

        # Assert
        assert mock_commit.call_args_list == [call()]
        mock_close.assert_called_once()

    def test_twoDifferentPeople_persistsBoth(self):
        # Arrange
        ingest_info = IngestInfoProto()
        ingest_info.people.add(person_id="1_GENERATE", full_name=FULL_NAME_1)
        ingest_info.people.add(person_id="2_GENERATE", full_name=FULL_NAME_2)

        # Act
        persistence.write_ingest_info(ingest_info, DEFAULT_METADATA)
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            result = county_dao.read_people(session)

        # Assert
        assert len(result) == 2

        assert result[0].full_name == _format_full_name(FULL_NAME_2)
        assert result[1].full_name == _format_full_name(FULL_NAME_1)

    def test_twoDifferentPeople_persistsNone(self):
        # Arrange
        ingest_info = IngestInfoProto()
        ingest_info.people.add(person_id="1", full_name=FULL_NAME_1)
        ingest_info.people.add(person_id="2", full_name=FULL_NAME_2, gender="X")

        # Act
        self.assertFalse(persistence.write_ingest_info(ingest_info, DEFAULT_METADATA))
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            result = county_dao.read_people(session)

        # Assert
        assert not result

    @patch(
        "recidiviz.persistence.persistence.SYSTEM_TYPE_TO_ERROR_THRESHOLD",
        ERROR_THRESHOLDS_WITH_FORTY_PERCENT_RATIOS,
    )
    def test_twoDifferentPeopleWithBooking_persistsNone(self):
        # Arrange
        ingest_info = IngestInfoProto()
        ingest_info.people.add(full_name=FULL_NAME_2)
        ingest_info.people.add(
            full_name=FULL_NAME_1,
            person_id=EXTERNAL_PERSON_ID,
            booking_ids=[EXTERNAL_BOOKING_ID],
        )
        ingest_info.bookings.add(
            booking_id=EXTERNAL_BOOKING_ID,
            custody_status="NO EXIST",
        )

        # Act
        self.assertFalse(persistence.write_ingest_info(ingest_info, DEFAULT_METADATA))
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            result = county_dao.read_people(session)

        # Assert
        assert not result

    @patch(
        "recidiviz.persistence.ingest_info_converter.ingest_info_converter.convert_to_persistence_entities"
    )
    @patch("recidiviz.persistence.entity_validator.entity_validator.validate")
    @patch(
        "recidiviz.persistence.persistence.SYSTEM_TYPE_TO_ERROR_THRESHOLD",
        ERROR_THRESHOLDS_WITH_FORTY_PERCENT_RATIOS,
    )
    def test_enum_error_threshold_should_abort_persistsNone(
        self, validator, entity_converter
    ):
        # Arrange
        ingest_info = IngestInfoProto()
        ingest_info.people.add(full_name=FULL_NAME_2)
        ingest_info.people.add(full_name=FULL_NAME_1, person_id=EXTERNAL_PERSON_ID)

        # Mock out 1 enum error, and no others
        entity_converter.return_value = EntityDeserializationResult(
            enum_parsing_errors=1,
            general_parsing_errors=0,
            protected_class_errors=0,
            people=ingest_info.people,
        )
        validator.return_value = ingest_info.people, 0

        # Act
        self.assertFalse(persistence.write_ingest_info(ingest_info, DEFAULT_METADATA))
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            result = county_dao.read_people(session)

        # Assert
        assert not result

    @patch("recidiviz.persistence.persistence.database_invariant_validator")
    def test_abort_from_database_invariant_error_persistsNone(
        self, mock_database_invariant_validator
    ):
        # Arrange
        ingest_info = IngestInfoProto()
        ingest_info.people.add(full_name=FULL_NAME_2)
        ingest_info.people.add(full_name=FULL_NAME_1, person_id=EXTERNAL_PERSON_ID)

        mock_database_invariant_validator.validate_invariants.return_value = 1

        # Act
        self.assertFalse(persistence.write_ingest_info(ingest_info, DEFAULT_METADATA))
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            result = county_dao.read_people(session)

        # Assert
        assert not result

    @patch("recidiviz.persistence.entity_matching.entity_matching.match")
    @patch(
        "recidiviz.persistence.persistence.SYSTEM_TYPE_TO_ERROR_THRESHOLD",
        ERROR_THRESHOLDS_WITH_FORTY_PERCENT_RATIOS,
    )
    def test_entity_match_error_threshold_should_abort_persistsNone(
        self, mock_entity_match
    ):
        # Mock out 1 entity matching error, and no others
        mock_entity_match.return_value = MatchedEntities(
            people=[], orphaned_entities=[], error_count=1, total_root_entities=0
        )

        # Arrange
        ingest_info = IngestInfoProto()
        ingest_info.people.add(person_id="2_GENERATE", full_name=FULL_NAME_2)
        ingest_info.people.add(person_id="3_GENERATE", full_name=FULL_NAME_3)

        # Act
        self.assertFalse(persistence.write_ingest_info(ingest_info, DEFAULT_METADATA))
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            result = county_dao.read_people(session)

        # Assert
        assert not result

    @patch(
        "recidiviz.persistence.persistence.SYSTEM_TYPE_TO_ERROR_THRESHOLD",
        ERROR_THRESHOLDS_WITH_FORTY_PERCENT_RATIOS,
    )
    def test_threeDifferentPeople_persistsTwoBelowThreshold(self):
        # Arrange
        ingest_info = IngestInfoProto()
        ingest_info.people.add(person_id="1_GENERATE", full_name=FULL_NAME_2)
        ingest_info.people.add(person_id="2_GENERATE", full_name=FULL_NAME_3)
        ingest_info.people.add(
            person_id=EXTERNAL_PERSON_ID,
            full_name=FULL_NAME_1,
            booking_ids=[EXTERNAL_BOOKING_ID],
        )
        ingest_info.bookings.add(
            booking_id=EXTERNAL_BOOKING_ID,
            custody_status="NO EXIST",
        )

        # Act
        persistence.write_ingest_info(ingest_info, DEFAULT_METADATA)
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            result = county_dao.read_people(session)

        # Assert
        assert len(result) == 2
        assert result[0].full_name == _format_full_name(FULL_NAME_3)
        assert result[1].full_name == _format_full_name(FULL_NAME_2)

    # TODO(#4135): test entity matching end to end

    def test_readSinglePersonByName(self):
        # Arrange
        ingest_info = IngestInfoProto()
        ingest_info.people.add(
            person_id="1_GENERATE", full_name=FULL_NAME_1, birthdate=BIRTHDATE_1
        )
        ingest_info.people.add(
            person_id="2_GENERATE", full_name=FULL_NAME_2, birthdate=BIRTHDATE_2
        )

        # Act
        persistence.write_ingest_info(ingest_info, DEFAULT_METADATA)
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            result = county_dao.read_people(
                session, full_name=_format_full_name(FULL_NAME_1)
            )

        # Assert
        assert len(result) == 1
        assert result[0].full_name == _format_full_name(FULL_NAME_1)
        assert result[0].birthdate == BIRTHDATE_1_DATE

    # TODO(#4135): Rewrite this test to directly test __eq__ between the two People
    def test_readPersonAndAllRelationships(self):
        # Arrange
        metadata = FakeLegacyStateAndJailsIngestMetadata.for_county(
            region=REGION_1,
            jurisdiction_id=JURISDICTION_ID,
            ingest_time=SCRAPER_START_DATETIME,
        )

        ingest_info = IngestInfoProto()
        ingest_info.people.add(full_name=FULL_NAME_1, booking_ids=["BOOKING_ID"])
        ingest_info.bookings.add(
            booking_id="BOOKING_ID",
            facility=FACILITY,
            custody_status=BOOKING_CUSTODY_STATUS,
            arrest_id="ARREST_ID",
            charge_ids=["CHARGE_ID_1", "CHARGE_ID_2"],
        )
        ingest_info.arrests.add(arrest_id="ARREST_ID", officer_name=OFFICER_NAME)

        ingest_info.bonds.add(
            bond_id="SHARED_BOND_ID", bond_type=BOND_TYPE, status=BOND_STATUS
        )

        ingest_info.charges.extend(
            [
                Charge(
                    charge_id="CHARGE_ID_1",
                    name=CHARGE_NAME_1,
                    status=CHARGE_STATUS,
                    bond_id="SHARED_BOND_ID",
                    sentence_id="SENTENCE_ID_1",
                ),
                Charge(
                    charge_id="CHARGE_ID_2",
                    name=CHARGE_NAME_2,
                    status=CHARGE_STATUS,
                    bond_id="SHARED_BOND_ID",
                    sentence_id="SENTENCE_ID_2",
                ),
            ]
        )

        ingest_info.sentences.extend(
            [
                Sentence(
                    sentence_id="SENTENCE_ID_1",
                    fine_dollars=FINE_1,
                    status=SENTENCE_STATUS,
                ),
                Sentence(
                    sentence_id="SENTENCE_ID_2",
                    fine_dollars=FINE_2,
                    status=SENTENCE_STATUS,
                ),
            ]
        )

        # Act
        persistence.write_ingest_info(ingest_info, metadata)
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            result = county_dao.read_people(session)

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
        most_recent_scrape_time = SCRAPER_START_DATETIME + timedelta(days=1)
        metadata = FakeLegacyStateAndJailsIngestMetadata.for_county(
            region=REGION_1,
            jurisdiction_id=JURISDICTION_ID,
            ingest_time=most_recent_scrape_time,
        )

        schema_booking = county_schema.Booking(
            booking_id=BOOKING_ID,
            external_id=EXTERNAL_BOOKING_ID,
            admission_date_inferred=True,
            custody_status=CustodyStatus.IN_CUSTODY.value,
            last_seen_time=SCRAPER_START_DATETIME,
            first_seen_time=SCRAPER_START_DATETIME,
        )
        schema_person = county_schema.Person(
            person_id=PERSON_ID,
            jurisdiction_id=JURISDICTION_ID,
            external_id=EXTERNAL_PERSON_ID,
            region=REGION_1,
            bookings=[schema_booking],
        )

        with SessionFactory.using_database(self.database_key) as session:
            session.add(schema_person)

        ingest_info = IngestInfoProto()
        ingest_info.people.add(
            full_name=FULL_NAME_1,
            person_id=EXTERNAL_PERSON_ID,
            booking_ids=[EXTERNAL_BOOKING_ID],
        )
        ingest_info.bookings.add(
            booking_id=EXTERNAL_BOOKING_ID,
            custody_status="IN CUSTODY",
        )

        # Act
        persistence.write_ingest_info(ingest_info, metadata)

        # Assert
        expected_booking = county_entities.Booking.new_with_defaults(
            booking_id=BOOKING_ID,
            external_id=EXTERNAL_BOOKING_ID,
            admission_date_inferred=True,
            custody_status=CustodyStatus.IN_CUSTODY,
            custody_status_raw_text=BOOKING_CUSTODY_STATUS.upper(),
            last_seen_time=most_recent_scrape_time,
            first_seen_time=SCRAPER_START_DATETIME,
        )
        expected_person = county_entities.Person.new_with_defaults(
            person_id=PERSON_ID,
            external_id=EXTERNAL_PERSON_ID,
            region=REGION_1,
            jurisdiction_id=JURISDICTION_ID,
            bookings=[expected_booking],
        )
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            self.assertEqual(
                [expected_person],
                county_dao.read_people(session),
            )

    def test_write_preexisting_person_duplicate_charges(self):
        # Arrange
        most_recent_scrape_time = SCRAPER_START_DATETIME + timedelta(days=1)
        metadata = FakeLegacyStateAndJailsIngestMetadata.for_county(
            region=REGION_1,
            jurisdiction_id=JURISDICTION_ID,
            ingest_time=most_recent_scrape_time,
        )

        schema_charge = county_schema.Charge(
            charge_id=ID,
            external_id=EXTERNAL_ID + "_COUNT_1",
            status=ChargeStatus.PRESENT_WITHOUT_INFO.value,
        )
        schema_charge_another = county_schema.Charge(
            charge_id=ID_2,
            external_id=EXTERNAL_ID + "_COUNT_2",
            status=ChargeStatus.PRESENT_WITHOUT_INFO.value,
        )
        schema_booking = county_schema.Booking(
            booking_id=BOOKING_ID,
            external_id=EXTERNAL_BOOKING_ID,
            admission_date_inferred=True,
            custody_status=CustodyStatus.IN_CUSTODY.value,
            last_seen_time=SCRAPER_START_DATETIME,
            first_seen_time=SCRAPER_START_DATETIME,
            charges=[schema_charge, schema_charge_another],
        )
        schema_person = county_schema.Person(
            person_id=PERSON_ID,
            jurisdiction_id=JURISDICTION_ID,
            external_id=EXTERNAL_PERSON_ID,
            region=REGION_1,
            bookings=[schema_booking],
        )

        with SessionFactory.using_database(self.database_key) as session:
            session.add(schema_person)

        ingest_info = IngestInfoProto()
        ingest_info.people.add(
            full_name=FULL_NAME_1,
            person_id=EXTERNAL_PERSON_ID,
            booking_ids=[EXTERNAL_BOOKING_ID],
        )
        ingest_info.bookings.add(
            booking_id=EXTERNAL_BOOKING_ID,
            custody_status="IN CUSTODY",
            charge_ids=[EXTERNAL_ID],
        )
        ingest_info.charges.add(charge_id=EXTERNAL_ID, number_of_counts="2")

        # Act
        persistence.write_ingest_info(ingest_info, metadata)

        # Assert
        expected_charge = county_entities.Charge.new_with_defaults(
            charge_id=ID,
            external_id=EXTERNAL_ID + "_COUNT_1",
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
        )
        expected_charge_another = attr.evolve(
            expected_charge, charge_id=ID_2, external_id=EXTERNAL_ID + "_COUNT_2"
        )
        expected_booking = county_entities.Booking.new_with_defaults(
            booking_id=BOOKING_ID,
            external_id=EXTERNAL_BOOKING_ID,
            admission_date_inferred=True,
            custody_status=CustodyStatus.IN_CUSTODY,
            custody_status_raw_text=BOOKING_CUSTODY_STATUS.upper(),
            last_seen_time=most_recent_scrape_time,
            first_seen_time=SCRAPER_START_DATETIME,
            charges=[expected_charge, expected_charge_another],
        )
        expected_person = county_entities.Person.new_with_defaults(
            person_id=PERSON_ID,
            external_id=EXTERNAL_PERSON_ID,
            region=REGION_1,
            jurisdiction_id=JURISDICTION_ID,
            bookings=[expected_booking],
        )
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            self.assertEqual(
                [expected_person],
                county_dao.read_people(session),
            )

    def test_write_noPeople(self):
        # Arrange
        most_recent_scrape_time = SCRAPER_START_DATETIME + timedelta(days=1)
        metadata = FakeLegacyStateAndJailsIngestMetadata.for_county(
            region=REGION_1,
            jurisdiction_id=JURISDICTION_ID,
            ingest_time=most_recent_scrape_time,
        )

        ingest_info = IngestInfoProto()

        # Act
        persistence.write_ingest_info(ingest_info, metadata)

        # Assert
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            people = county_dao.read_people(session)
            self.assertFalse(people)

    def test_inferReleaseDateOnOpenBookings(self):
        # Arrange
        hold = county_entities.Hold.new_with_defaults(
            hold_id=ID, status=HoldStatus.ACTIVE, status_raw_text="ACTIVE"
        )
        sentence = county_entities.Sentence.new_with_defaults(
            sentence_id=ID,
            status=SentenceStatus.SERVING,
            status_raw_text="SERVING",
            booking_id=ID,
        )
        bond = county_entities.Bond.new_with_defaults(
            bond_id=ID,
            status=BondStatus.SET,
            status_raw_text="NOT_REQUIRED",
            booking_id=ID,
        )
        charge = county_entities.Charge.new_with_defaults(
            charge_id=ID,
            status=ChargeStatus.PENDING,
            status_raw_text="PENDING",
            sentence=sentence,
            bond=bond,
        )
        booking_open = county_entities.Booking.new_with_defaults(
            booking_id=ID,
            custody_status=CustodyStatus.IN_CUSTODY,
            custody_status_raw_text="IN CUSTODY",
            admission_date=DATE,
            last_seen_time=SCRAPER_START_DATETIME - timedelta(days=1),
            first_seen_time=SCRAPER_START_DATETIME - timedelta(days=1),
            charges=[charge],
            holds=[hold],
        )
        booking_resolved = attr.evolve(
            booking_open,
            booking_id=ID_2,
            custody_status=CustodyStatus.RELEASED,
            custody_status_raw_text="RELEASED",
            release_date=DATE_2,
            charges=[],
            holds=[],
        )
        booking_open_most_recent_scrape = attr.evolve(
            booking_open,
            booking_id=ID_3,
            last_seen_time=SCRAPER_START_DATETIME,
            charges=[],
            holds=[],
        )

        person = county_entities.Person.new_with_defaults(
            person_id=ID,
            region=REGION_1,
            jurisdiction_id=JURISDICTION_ID,
            bookings=[booking_open, booking_resolved],
        )
        person_unmatched = county_entities.Person.new_with_defaults(
            person_id=ID_2,
            region=REGION_1,
            jurisdiction_id=JURISDICTION_ID,
            bookings=[booking_open_most_recent_scrape],
        )

        with SessionFactory.using_database(self.database_key) as session:
            database.write_people(
                session,
                converter.convert_entity_people_to_schema_people(
                    [person, person_unmatched]
                ),
                DEFAULT_METADATA,
            )

        expected_hold = attr.evolve(
            hold, status=HoldStatus.REMOVED_WITHOUT_INFO, status_raw_text=None
        )
        expected_sentence = attr.evolve(
            sentence, status=SentenceStatus.REMOVED_WITHOUT_INFO, status_raw_text=None
        )
        expected_bond = attr.evolve(
            bond, status=BondStatus.REMOVED_WITHOUT_INFO, status_raw_text=None
        )
        expected_charge = attr.evolve(
            charge,
            status=ChargeStatus.REMOVED_WITHOUT_INFO,
            status_raw_text=None,
            bond=expected_bond,
            sentence=expected_sentence,
        )
        expected_resolved_booking = attr.evolve(
            booking_open,
            custody_status=CustodyStatus.INFERRED_RELEASE,
            custody_status_raw_text=None,
            release_date=SCRAPER_START_DATETIME.date(),
            release_date_inferred=True,
            charges=[expected_charge],
            holds=[expected_hold],
        )
        expected_person = attr.evolve(
            person, bookings=[expected_resolved_booking, booking_resolved]
        )

        # Act
        persistence.infer_release_on_open_bookings(
            REGION_1, SCRAPER_START_DATETIME, CustodyStatus.INFERRED_RELEASE
        )

        # Assert
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            people = county_dao.read_people(session)
        self.assertCountEqual(people, [expected_person, person_unmatched])

    def test_write_different_arrest(self):
        # Arrange
        most_recent_scrape_time = SCRAPER_START_DATETIME + timedelta(days=1)
        metadata = FakeLegacyStateAndJailsIngestMetadata.for_county(
            region=REGION_1,
            jurisdiction_id=JURISDICTION_ID,
            ingest_time=most_recent_scrape_time,
        )

        schema_arrest = county_schema.Arrest(
            external_id=ARREST_ID, officer_name=OFFICER_NAME
        )
        schema_booking = county_schema.Booking(
            booking_id=BOOKING_ID,
            external_id=EXTERNAL_BOOKING_ID,
            admission_date_inferred=True,
            custody_status=CustodyStatus.IN_CUSTODY.value,
            arrest=schema_arrest,
            last_seen_time=SCRAPER_START_DATETIME,
            first_seen_time=SCRAPER_START_DATETIME,
        )
        schema_person = county_schema.Person(
            person_id=PERSON_ID,
            jurisdiction_id=JURISDICTION_ID,
            external_id=EXTERNAL_PERSON_ID,
            region=REGION_1,
            bookings=[schema_booking],
        )

        with SessionFactory.using_database(self.database_key) as session:
            session.add(schema_person)

        ingest_info = IngestInfoProto()
        ingest_info.people.add(
            full_name=FULL_NAME_1,
            person_id=EXTERNAL_PERSON_ID,
            booking_ids=[EXTERNAL_BOOKING_ID],
        )
        ingest_info.bookings.add(
            booking_id=EXTERNAL_BOOKING_ID,
            custody_status="IN CUSTODY",
            arrest_id=ARREST_ID,
        )
        ingest_info.arrests.add(arrest_id=ARREST_ID, officer_name=OFFICER_NAME_ANOTHER)

        # Act
        persistence.write_ingest_info(ingest_info, metadata)

        # Assert
        expected_arrest = county_entities.Arrest.new_with_defaults(
            external_id=ARREST_ID, arrest_id=1, officer_name=OFFICER_NAME_ANOTHER
        )
        expected_booking = county_entities.Booking.new_with_defaults(
            booking_id=BOOKING_ID,
            external_id=EXTERNAL_BOOKING_ID,
            admission_date_inferred=True,
            custody_status=CustodyStatus.IN_CUSTODY,
            custody_status_raw_text=BOOKING_CUSTODY_STATUS.upper(),
            arrest=expected_arrest,
            last_seen_time=most_recent_scrape_time,
            first_seen_time=SCRAPER_START_DATETIME,
        )
        expected_person = county_entities.Person.new_with_defaults(
            person_id=PERSON_ID,
            external_id=EXTERNAL_PERSON_ID,
            region=REGION_1,
            jurisdiction_id=JURISDICTION_ID,
            bookings=[expected_booking],
        )

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            self.assertEqual([expected_person], county_dao.read_people(session))

    def test_write_new_empty_arrest(self):
        # Arrange
        most_recent_scrape_time = SCRAPER_START_DATETIME + timedelta(days=1)
        metadata = FakeLegacyStateAndJailsIngestMetadata.for_county(
            region=REGION_1,
            jurisdiction_id=JURISDICTION_ID,
            ingest_time=most_recent_scrape_time,
        )

        schema_arrest = county_schema.Arrest(
            external_id=ARREST_ID, officer_name=OFFICER_NAME
        )
        schema_booking = county_schema.Booking(
            booking_id=BOOKING_ID,
            external_id=EXTERNAL_BOOKING_ID,
            admission_date_inferred=True,
            custody_status=CustodyStatus.IN_CUSTODY.value,
            arrest=schema_arrest,
            last_seen_time=SCRAPER_START_DATETIME,
            first_seen_time=SCRAPER_START_DATETIME,
        )
        schema_person = county_schema.Person(
            person_id=PERSON_ID,
            jurisdiction_id=JURISDICTION_ID,
            external_id=EXTERNAL_PERSON_ID,
            region=REGION_1,
            bookings=[schema_booking],
        )

        with SessionFactory.using_database(self.database_key) as session:
            session.add(schema_person)

        ingest_info = IngestInfoProto()
        ingest_info.people.add(
            full_name=FULL_NAME_1,
            person_id=EXTERNAL_PERSON_ID,
            booking_ids=[EXTERNAL_BOOKING_ID],
        )
        ingest_info.bookings.add(
            booking_id=EXTERNAL_BOOKING_ID,
            custody_status="IN CUSTODY",
        )

        # Act
        persistence.write_ingest_info(ingest_info, metadata)

        # Assert
        expected_arrest = county_entities.Arrest.new_with_defaults(
            arrest_id=1,
        )
        expected_booking = county_entities.Booking.new_with_defaults(
            booking_id=BOOKING_ID,
            external_id=EXTERNAL_BOOKING_ID,
            admission_date_inferred=True,
            custody_status=CustodyStatus.IN_CUSTODY,
            custody_status_raw_text=BOOKING_CUSTODY_STATUS.upper(),
            arrest=expected_arrest,
            last_seen_time=most_recent_scrape_time,
            first_seen_time=SCRAPER_START_DATETIME,
        )
        expected_person = county_entities.Person.new_with_defaults(
            person_id=PERSON_ID,
            external_id=EXTERNAL_PERSON_ID,
            region=REGION_1,
            jurisdiction_id=JURISDICTION_ID,
            bookings=[expected_booking],
        )

        self.maxDiff = None
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            self.assertEqual([expected_person], county_dao.read_people(session))


def _format_full_name(full_name: str) -> str:
    return f'{{"full_name": "{full_name}"}}'
