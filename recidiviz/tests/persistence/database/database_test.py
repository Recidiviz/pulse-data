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
"""Tests for database.py."""

import datetime
from unittest import TestCase

from more_itertools import one
from sqlalchemy.sql import text

from recidiviz.common.constants.bond import BondStatus
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.county.booking import CustodyStatus
from recidiviz.common.constants.county.sentence import SentenceStatus
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.constants.person_characteristics import Race
from recidiviz.persistence.database import database
from recidiviz.persistence.database.schema.county import dao as county_dao
from recidiviz.persistence.database.schema.county import schema as county_schema
from recidiviz.persistence.database.schema.county.schema import (
    Bond,
    BondHistory,
    Booking,
    BookingHistory,
    Charge,
    ChargeHistory,
    Person,
    PersonHistory,
    Sentence,
    SentenceHistory,
)
from recidiviz.persistence.database.schema_entity_converter import (
    schema_entity_converter as converter,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.county import entities as county_entities
from recidiviz.tests.persistence.database.database_test_utils import FakeIngestMetadata
from recidiviz.tests.utils import fakes

_REGION = "region"
_REGION_ANOTHER = "wrong region"
_JURISDICTION_ID = "12345678"
_FULL_NAME = "full_name"
_EXTERNAL_ID = "external_id"
_BIRTHDATE = datetime.date(year=2012, month=1, day=2)
_INGEST_TIME = datetime.datetime(year=2020, month=7, day=4)
_FACILITY = "facility"
_DEFAULT_METADATA = FakeIngestMetadata.for_county(
    region="default_region",
    jurisdiction_id="jid",
    ingest_time=_INGEST_TIME,
    enum_overrides=EnumOverrides.empty(),
)

DATE_SCRAPED = datetime.date(year=2019, month=1, day=1)


class TestDatabase(TestCase):
    """Test that the methods in database.py correctly read from the SQL
    database"""

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.JAILS)
        fakes.use_in_memory_sqlite_database(self.database_key)

    def tearDown(self) -> None:
        fakes.teardown_in_memory_sqlite_databases()

    def testWritePerson_noExistingSnapshots_createsSnapshots(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as act_session:
            person = county_schema.Person(
                region=_REGION, race=Race.OTHER.value, jurisdiction_id=_JURISDICTION_ID
            )

            booking = county_schema.Booking(
                custody_status=CustodyStatus.IN_CUSTODY.value,
                first_seen_time=_INGEST_TIME,
                last_seen_time=_INGEST_TIME,
            )
            person.bookings = [booking]

            charge_1 = county_schema.Charge(status=ChargeStatus.PENDING.value)
            charge_2 = county_schema.Charge(status=ChargeStatus.PRETRIAL.value)
            booking.charges = [charge_1, charge_2]

            persisted_person = database.write_person(
                act_session, person, _DEFAULT_METADATA
            )
            act_session.commit()

            person_id = persisted_person.person_id
            booking_id = persisted_person.bookings[0].booking_id
            charge_1_id = persisted_person.bookings[0].charges[0].charge_id
            charge_2_id = persisted_person.bookings[0].charges[1].charge_id

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as assert_session:
            person_snapshots = (
                assert_session.query(PersonHistory)
                .filter(PersonHistory.person_id == person_id)
                .all()
            )
            booking_snapshots = (
                assert_session.query(BookingHistory)
                .filter(BookingHistory.booking_id == booking_id)
                .all()
            )
            charge_snapshots = (
                assert_session.query(ChargeHistory)
                .filter(ChargeHistory.charge_id.in_([charge_1_id, charge_2_id]))
                .all()
            )

            self.assertEqual(len(person_snapshots), 1)
            self.assertEqual(len(booking_snapshots), 1)
            self.assertEqual(len(charge_snapshots), 2)

            person_snapshot = person_snapshots[0]
            booking_snapshot = booking_snapshots[0]
            charge_snapshot_statuses = {
                charge_snapshot.status for charge_snapshot in charge_snapshots
            }
            snapshot_time = person_snapshot.valid_from

            self.assertEqual(person_snapshot.region, _REGION)
            self.assertEqual(person_snapshot.race, Race.OTHER.value)
            self.assertIsNone(person_snapshot.valid_to)

            self.assertEqual(
                booking_snapshot.custody_status, CustodyStatus.IN_CUSTODY.value
            )
            self.assertEqual(booking_snapshot.valid_from, snapshot_time)
            self.assertIsNone(booking_snapshot.valid_to)

            self.assertEqual(
                charge_snapshot_statuses,
                {ChargeStatus.PENDING.value, ChargeStatus.PRETRIAL.value},
            )
            self.assertEqual(charge_snapshots[0].valid_from, snapshot_time)
            self.assertEqual(charge_snapshots[1].valid_from, snapshot_time)
            self.assertIsNone(charge_snapshots[0].valid_to)
            self.assertIsNone(charge_snapshots[1].valid_to)

    def testWritePerson_allExistingSnapshots_onlyUpdatesOnChange(self) -> None:
        charge_name_1 = "charge_name_1"
        charge_name_2 = "charge_name_2"
        updated_last_seen_time = datetime.datetime(year=2020, month=7, day=6)
        # Pick a date in the past so the assigned snapshot date will always be
        # later
        valid_from = datetime.datetime(year=2015, month=1, day=1)
        existing_person_id = 1
        existing_booking_id = 14
        existing_charge_id = 47

        with SessionFactory.using_database(self.database_key) as arrange_session:
            existing_person = Person(
                person_id=existing_person_id,
                region=_REGION,
                jurisdiction_id=_JURISDICTION_ID,
                race=Race.OTHER.value,
            )
            existing_person_snapshot = PersonHistory(
                person_history_id=1000,
                person_id=existing_person_id,
                region=_REGION,
                jurisdiction_id=_JURISDICTION_ID,
                race=Race.OTHER.value,
                valid_from=valid_from,
            )

            existing_booking = Booking(
                booking_id=existing_booking_id,
                custody_status=CustodyStatus.IN_CUSTODY.value,
                facility=_FACILITY,
                first_seen_time=_INGEST_TIME,
                last_seen_time=_INGEST_TIME,
            )
            existing_booking_snapshot = BookingHistory(
                booking_history_id=1000,
                person_id=existing_person_id,
                booking_id=existing_booking_id,
                custody_status=CustodyStatus.IN_CUSTODY.value,
                facility=_FACILITY,
                valid_from=valid_from,
            )
            existing_person.bookings = [existing_booking]

            existing_charge = Charge(
                charge_id=existing_charge_id,
                status=ChargeStatus.PENDING.value,
                name=charge_name_1,
            )
            existing_charge_snapshot = ChargeHistory(
                charge_history_id=1000,
                charge_id=existing_charge_id,
                booking_id=existing_booking_id,
                status=ChargeStatus.PENDING.value,
                name=charge_name_1,
                valid_from=valid_from,
            )
            existing_booking.charges = [existing_charge]

            arrange_session.add(existing_person)
            arrange_session.flush()
            # Snapshots must be added separately, as they are not included in ORM
            # relationships
            arrange_session.add(existing_person_snapshot)
            arrange_session.add(existing_booking_snapshot)
            arrange_session.add(existing_charge_snapshot)

        with SessionFactory.using_database(self.database_key) as act_session:
            # Ingested record tree has updates to person and charge but not booking
            ingested_person = county_schema.Person(
                person_id=existing_person_id,
                region=_REGION,
                jurisdiction_id=_JURISDICTION_ID,
                race=Race.EXTERNAL_UNKNOWN.value,
            )

            # Ingested booking has new last_seen_time but this is ignored, as it
            # is not included on the historical table.
            ingested_booking = county_schema.Booking(
                booking_id=existing_booking_id,
                custody_status=CustodyStatus.IN_CUSTODY.value,
                facility=_FACILITY,
                first_seen_time=updated_last_seen_time,
                last_seen_time=updated_last_seen_time,
            )
            ingested_person.bookings = [ingested_booking]

            ingested_charge = county_schema.Charge(
                charge_id=existing_charge_id,
                status=ChargeStatus.PENDING.value,
                name=charge_name_2,
            )
            ingested_booking.charges = [ingested_charge]

            database.write_person(act_session, ingested_person, _DEFAULT_METADATA)

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as assert_session:
            person_snapshots = (
                assert_session.query(PersonHistory)
                .filter(PersonHistory.person_id == existing_person_id)
                .order_by(PersonHistory.valid_from.asc())
                .all()
            )
            booking_snapshots = (
                assert_session.query(BookingHistory)
                .filter(BookingHistory.booking_id == existing_booking_id)
                .all()
            )
            charge_snapshots = (
                assert_session.query(ChargeHistory)
                .filter(ChargeHistory.charge_id == existing_charge_id)
                .order_by(ChargeHistory.valid_from.asc())
                .all()
            )

            self.assertEqual(len(person_snapshots), 2)
            self.assertEqual(len(booking_snapshots), 1)
            self.assertEqual(len(charge_snapshots), 2)

            old_person_snapshot = person_snapshots[0]
            new_person_snapshot = person_snapshots[1]
            booking_snapshot = booking_snapshots[0]
            old_charge_snapshot = charge_snapshots[0]
            new_charge_snapshot = charge_snapshots[1]
            snapshot_time = old_person_snapshot.valid_to

            self.assertEqual(old_person_snapshot.race, Race.OTHER.value)
            self.assertEqual(new_person_snapshot.race, Race.EXTERNAL_UNKNOWN.value)

            self.assertEqual(old_charge_snapshot.name, charge_name_1)
            self.assertEqual(new_charge_snapshot.name, charge_name_2)

            self.assertEqual(old_person_snapshot.valid_from, valid_from)
            self.assertEqual(new_person_snapshot.valid_from, snapshot_time)
            self.assertEqual(booking_snapshot.valid_from, valid_from)
            self.assertEqual(old_charge_snapshot.valid_from, valid_from)
            self.assertEqual(new_charge_snapshot.valid_from, snapshot_time)

            self.assertEqual(old_person_snapshot.valid_to, snapshot_time)
            self.assertIsNone(new_person_snapshot.valid_to)
            self.assertIsNone(booking_snapshot.valid_to)
            self.assertEqual(old_charge_snapshot.valid_to, snapshot_time)
            self.assertIsNone(new_charge_snapshot.valid_to)

    def testWritePerson_someExistingSnapshots_createsAndExtends(self) -> None:
        charge_name = "charge_name"
        updated_last_seen_time = datetime.datetime(year=2020, month=7, day=6)
        # Pick a date in the past so the assigned snapshot date will always be
        # later
        valid_from = datetime.datetime(year=2015, month=1, day=1)
        existing_person_id = 1
        existing_booking_id = 14

        with SessionFactory.using_database(self.database_key) as arrange_session:
            # Person and booking already exist, while charge is new.
            existing_person = Person(
                person_id=existing_person_id,
                region=_REGION,
                jurisdiction_id=_JURISDICTION_ID,
                race=Race.OTHER.value,
            )
            existing_person_snapshot = PersonHistory(
                person_history_id=1000,
                person_id=existing_person_id,
                region=_REGION,
                jurisdiction_id=_JURISDICTION_ID,
                race=Race.OTHER.value,
                valid_from=valid_from,
            )

            existing_booking = Booking(
                booking_id=existing_booking_id,
                custody_status=CustodyStatus.IN_CUSTODY.value,
                facility=_FACILITY,
                first_seen_time=_INGEST_TIME,
                last_seen_time=_INGEST_TIME,
            )
            existing_booking_snapshot = BookingHistory(
                booking_history_id=1000,
                person_id=existing_person_id,
                booking_id=existing_booking_id,
                custody_status=CustodyStatus.IN_CUSTODY.value,
                facility=_FACILITY,
                valid_from=valid_from,
            )
            existing_person.bookings = [existing_booking]

            arrange_session.add(existing_person)
            arrange_session.flush()
            # Snapshots must be added separately, as they are not included in ORM
            # relationships
            arrange_session.add(existing_person_snapshot)
            arrange_session.add(existing_booking_snapshot)

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as act_session:
            # Ingested record tree has update to person, no updates to booking, and
            # a new charge
            ingested_person = county_schema.Person(
                person_id=existing_person_id,
                region=_REGION,
                jurisdiction_id=_JURISDICTION_ID,
                race=Race.EXTERNAL_UNKNOWN.value,
            )

            # Ingested booking has new last_seen_time but this is ignored, as it
            # is not included on the historical table.
            ingested_booking = county_schema.Booking(
                booking_id=existing_booking_id,
                custody_status=CustodyStatus.IN_CUSTODY.value,
                facility=_FACILITY,
                first_seen_time=_INGEST_TIME,
                last_seen_time=updated_last_seen_time,
            )
            ingested_person.bookings = [ingested_booking]

            ingested_charge = county_schema.Charge(
                status=ChargeStatus.PENDING.value, name=charge_name
            )
            ingested_booking.charges = [ingested_charge]

            persisted_person = database.write_person(
                act_session, ingested_person, _DEFAULT_METADATA
            )
            act_session.commit()

            charge_id = persisted_person.bookings[0].charges[0].charge_id

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as assert_session:
            person_snapshots = (
                assert_session.query(PersonHistory)
                .filter(PersonHistory.person_id == existing_person_id)
                .order_by(PersonHistory.valid_from.asc())
                .all()
            )
            booking_snapshots = (
                assert_session.query(BookingHistory)
                .filter(BookingHistory.booking_id == existing_booking_id)
                .all()
            )
            charge_snapshots = (
                assert_session.query(ChargeHistory)
                .filter(ChargeHistory.charge_id == charge_id)
                .all()
            )

            self.assertEqual(len(person_snapshots), 2)
            self.assertEqual(len(booking_snapshots), 1)
            self.assertEqual(len(charge_snapshots), 1)

            old_person_snapshot = person_snapshots[0]
            new_person_snapshot = person_snapshots[1]
            booking_snapshot = booking_snapshots[0]
            charge_snapshot = charge_snapshots[0]
            snapshot_time = old_person_snapshot.valid_to

            self.assertEqual(old_person_snapshot.race, Race.OTHER.value)
            self.assertEqual(new_person_snapshot.race, Race.EXTERNAL_UNKNOWN.value)

            self.assertEqual(charge_snapshot.name, charge_name)

            self.assertEqual(old_person_snapshot.valid_from, valid_from)
            self.assertEqual(new_person_snapshot.valid_from, snapshot_time)
            self.assertEqual(booking_snapshot.valid_from, valid_from)
            self.assertEqual(charge_snapshot.valid_from, snapshot_time)

            self.assertEqual(old_person_snapshot.valid_to, snapshot_time)
            self.assertIsNone(new_person_snapshot.valid_to)
            self.assertIsNone(booking_snapshot.valid_to)
            self.assertIsNone(charge_snapshot.valid_to)

    def testWritePerson_overlappingSnapshots_doesNotRaiseError(self) -> None:
        name = "Steve Fakename"
        birthdate = datetime.date(year=1980, month=1, day=1)

        with SessionFactory.using_database(self.database_key) as arrange_session:
            person = Person(
                person_id=1,
                full_name=name,
                birthdate=birthdate,
                region=_REGION,
                jurisdiction_id=_JURISDICTION_ID,
                race=Race.OTHER.value,
            )
            booking = Booking(
                booking_id=2,
                custody_status=CustodyStatus.IN_CUSTODY.value,
                first_seen_time=datetime.datetime(year=2020, month=7, day=4),
                last_seen_time=datetime.datetime(year=2020, month=7, day=6),
            )
            person.bookings.append(booking)
            charge = Charge(charge_id=3, status=ChargeStatus.PENDING.value)
            booking.charges.append(charge)
            bond = Bond(
                bond_id=4, status=BondStatus.PRESENT_WITHOUT_INFO.value, booking_id=2
            )
            charge.bond = bond

            arrange_session.add(person)
            arrange_session.flush()

            snapshot_time = "2020-07-06 00:00:00"

            arrange_session.execute(
                text(
                    "INSERT INTO person_history (person_id, region, jurisdiction_id, race, valid_from) "
                    f"VALUES ({1}, '{_REGION}', '{_JURISDICTION_ID}', '{Race.OTHER.value}', '{snapshot_time}');"
                )
            )
            arrange_session.execute(
                text(
                    "INSERT INTO booking_history (booking_id, person_id, custody_status, valid_from) "
                    f"VALUES ({2}, {1}, '{CustodyStatus.IN_CUSTODY.value}', '{snapshot_time}');"
                )
            )
            arrange_session.execute(
                text(
                    "INSERT INTO bond_history (bond_id, booking_id, status, valid_from) "
                    f"VALUES ({4}, {2}, '{BondStatus.PRESENT_WITHOUT_INFO.value}', '{snapshot_time}');"
                )
            )

            # Create instantaneous closed charge snapshot and open charge snapshot
            arrange_session.execute(
                text(
                    "INSERT INTO charge_history "
                    "(charge_id, booking_id, bond_id, status, valid_from, valid_to) "
                    "VALUES "
                    f"({3}, {2}, {4}, '{ChargeStatus.PENDING.value}', '{snapshot_time}', '{snapshot_time}');"
                )
            )
            arrange_session.execute(
                text(
                    "INSERT INTO charge_history "
                    "(charge_id, booking_id, bond_id, status, valid_from) "
                    f"VALUES ({3}, {2}, {4}, '{ChargeStatus.PENDING.value}', '{snapshot_time}');"
                )
            )

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as assert_session:
            ingest_person = county_dao.read_people(
                assert_session, full_name=name, birthdate=birthdate
            )[0]
            ingest_person.bookings[0].custody_status = CustodyStatus.RELEASED

            try:
                database.write_people(
                    assert_session,
                    converter.convert_entity_people_to_schema_people([ingest_person]),
                    FakeIngestMetadata.for_county(
                        region=_REGION,
                        jurisdiction_id=_JURISDICTION_ID,
                        ingest_time=datetime.datetime(year=2020, month=7, day=8),
                    ),
                )
            except Exception as e:
                self.fail(f"Writing person failed with error: {e}")

    def testWritePeople_duplicatePeople_raisesError(self) -> None:
        shared_id = 48
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            person_1 = county_schema.Person(
                region=_REGION,
                race=Race.OTHER.value,
                person_id=shared_id,
                jurisdiction_id=_JURISDICTION_ID,
            )
            person_2 = county_schema.Person(
                region=_REGION,
                race=Race.EXTERNAL_UNKNOWN.value,
                person_id=shared_id,
                jurisdiction_id=_JURISDICTION_ID,
            )

            self.assertRaises(
                AssertionError,
                database.write_people,
                session,
                [person_1, person_2],
                _DEFAULT_METADATA,
            )

    def testWritePerson_backdatedBooking_backdatesSnapshot(self) -> None:
        person_scrape_time = datetime.datetime(year=2020, month=6, day=1)
        booking_scrape_time = datetime.datetime(year=2020, month=7, day=7)
        booking_admission_date = datetime.datetime(year=2020, month=7, day=1)

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as arrange_session:
            person = county_schema.Person(
                full_name=_FULL_NAME,
                birthdate=_BIRTHDATE,
                region=_REGION,
                jurisdiction_id=_JURISDICTION_ID,
            )
            persisted_person = database.write_person(
                arrange_session,
                person,
                FakeIngestMetadata.for_county(
                    region=_REGION,
                    jurisdiction_id=_JURISDICTION_ID,
                    ingest_time=person_scrape_time,
                ),
            )
            arrange_session.commit()
            person_id = persisted_person.person_id

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as act_session:
            queried_person = one(
                county_dao.read_people(
                    session=act_session, full_name=_FULL_NAME, birthdate=_BIRTHDATE
                )
            )
            booking = county_entities.Booking.new_with_defaults(
                custody_status=CustodyStatus.IN_CUSTODY,
                admission_date=booking_admission_date,
                admission_date_inferred=False,
                first_seen_time=booking_scrape_time,
                last_seen_time=booking_scrape_time,
            )
            queried_person.bookings = [booking]

            updated_person = database.write_person(
                act_session,
                converter.convert_entity_people_to_schema_people([queried_person])[0],
                FakeIngestMetadata.for_county(
                    region=_REGION,
                    jurisdiction_id=_JURISDICTION_ID,
                    ingest_time=booking_scrape_time,
                ),
            )
            act_session.commit()
            booking_id = updated_person.bookings[0].booking_id

        with SessionFactory.using_database(self.database_key) as assert_session:
            person_snapshot = one(
                assert_session.query(PersonHistory)
                .filter(PersonHistory.person_id == person_id)
                .all()
            )
            booking_snapshot = one(
                assert_session.query(BookingHistory)
                .filter(BookingHistory.booking_id == booking_id)
                .all()
            )

            self.assertEqual(person_snapshot.valid_from, person_scrape_time)
            self.assertEqual(booking_snapshot.valid_from, booking_admission_date)

    def testWritePerson_backdatedRelease_backdatesRecordTreeToRelease(self) -> None:
        scrape_time = datetime.datetime(year=2020, month=7, day=7)
        booking_release_date = datetime.datetime(year=2020, month=7, day=1)

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as act_session:
            person = county_schema.Person(
                full_name=_FULL_NAME,
                birthdate=_BIRTHDATE,
                region=_REGION,
                jurisdiction_id=_JURISDICTION_ID,
            )
            booking = county_schema.Booking(
                custody_status=CustodyStatus.RELEASED.value,
                release_date=booking_release_date,
                release_date_inferred=False,
                first_seen_time=scrape_time,
                last_seen_time=scrape_time,
            )
            person.bookings = [booking]
            charge = county_schema.Charge(status=ChargeStatus.PENDING.value)
            booking.charges = [charge]
            persisted_person = database.write_person(
                act_session,
                person,
                FakeIngestMetadata.for_county(
                    region=_REGION,
                    jurisdiction_id=_JURISDICTION_ID,
                    ingest_time=scrape_time,
                ),
            )
            act_session.commit()
            person_id = persisted_person.person_id
            booking_id = persisted_person.bookings[0].booking_id
            charge_id = persisted_person.bookings[0].charges[0].charge_id

        with SessionFactory.using_database(self.database_key) as assert_session:
            person_snapshot = one(
                assert_session.query(PersonHistory)
                .filter(PersonHistory.person_id == person_id)
                .all()
            )
            booking_snapshot = one(
                assert_session.query(BookingHistory)
                .filter(BookingHistory.booking_id == booking_id)
                .all()
            )
            charge_snapshot = one(
                assert_session.query(ChargeHistory)
                .filter(ChargeHistory.charge_id == charge_id)
                .all()
            )

            self.assertEqual(person_snapshot.valid_from, booking_release_date)
            self.assertEqual(booking_snapshot.valid_from, booking_release_date)
            self.assertEqual(charge_snapshot.valid_from, booking_release_date)

    def testWritePerson_releaseAfterScrapeTime_usesScrapeTime(self) -> None:
        scrape_time = datetime.datetime(year=2020, month=7, day=7)
        booking_release_date = datetime.datetime(year=2020, month=7, day=15)

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as act_session:
            person = county_schema.Person(
                full_name=_FULL_NAME,
                birthdate=_BIRTHDATE,
                region=_REGION,
                jurisdiction_id=_JURISDICTION_ID,
            )
            booking = county_schema.Booking(
                custody_status=CustodyStatus.RELEASED.value,
                release_date=booking_release_date,
                release_date_inferred=False,
                first_seen_time=scrape_time,
                last_seen_time=scrape_time,
            )
            person.bookings = [booking]
            charge = county_schema.Charge(status=ChargeStatus.PENDING.value)
            booking.charges = [charge]
            persisted_person = database.write_person(
                act_session,
                person,
                FakeIngestMetadata.for_county(
                    region=_REGION,
                    jurisdiction_id=_JURISDICTION_ID,
                    ingest_time=scrape_time,
                ),
            )
            act_session.commit()
            person_id = persisted_person.person_id
            booking_id = persisted_person.bookings[0].booking_id
            charge_id = persisted_person.bookings[0].charges[0].charge_id

        with SessionFactory.using_database(self.database_key) as assert_session:
            person_snapshot = one(
                assert_session.query(PersonHistory)
                .filter(PersonHistory.person_id == person_id)
                .all()
            )
            booking_snapshot = one(
                assert_session.query(BookingHistory)
                .filter(BookingHistory.booking_id == booking_id)
                .all()
            )
            charge_snapshot = one(
                assert_session.query(ChargeHistory)
                .filter(ChargeHistory.charge_id == charge_id)
                .all()
            )

            self.assertEqual(person_snapshot.valid_from, scrape_time)
            self.assertEqual(booking_snapshot.valid_from, scrape_time)
            self.assertEqual(charge_snapshot.valid_from, scrape_time)

    def testWritePerson_admissionAndReleaseDate_createsTwoSnapshots(self) -> None:
        scrape_time = datetime.datetime(year=2020, month=7, day=7)
        admission_date = datetime.datetime(year=2020, month=6, day=1)
        release_date = datetime.datetime(year=2020, month=7, day=1)

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as act_session:
            person = county_schema.Person(
                full_name=_FULL_NAME,
                birthdate=_BIRTHDATE,
                region=_REGION,
                jurisdiction_id=_JURISDICTION_ID,
            )
            booking = county_schema.Booking(
                custody_status=CustodyStatus.RELEASED.value,
                admission_date=admission_date,
                admission_date_inferred=False,
                release_date=release_date,
                release_date_inferred=False,
                first_seen_time=scrape_time,
                last_seen_time=scrape_time,
            )
            person.bookings = [booking]
            charge = county_schema.Charge(status=ChargeStatus.PENDING.value)
            booking.charges = [charge]
            persisted_person = database.write_person(
                act_session,
                person,
                FakeIngestMetadata.for_county(
                    region=_REGION,
                    jurisdiction_id=_JURISDICTION_ID,
                    ingest_time=scrape_time,
                ),
            )
            act_session.commit()
            person_id = persisted_person.person_id
            booking_id = persisted_person.bookings[0].booking_id
            charge_id = persisted_person.bookings[0].charges[0].charge_id

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as assert_session:
            person_snapshot = one(
                assert_session.query(PersonHistory)
                .filter(PersonHistory.person_id == person_id)
                .all()
            )
            charge_snapshot = one(
                assert_session.query(ChargeHistory)
                .filter(ChargeHistory.charge_id == charge_id)
                .all()
            )

            self.assertEqual(person_snapshot.valid_from, admission_date)
            self.assertEqual(charge_snapshot.valid_from, admission_date)

            booking_snapshots = (
                assert_session.query(BookingHistory)
                .filter(BookingHistory.booking_id == booking_id)
                .order_by(BookingHistory.valid_from.asc())
                .all()
            )

            self.assertEqual(len(booking_snapshots), 2)
            self.assertEqual(booking_snapshots[0].valid_from, admission_date)
            self.assertEqual(booking_snapshots[0].valid_to, release_date)
            self.assertEqual(
                booking_snapshots[0].custody_status, CustodyStatus.IN_CUSTODY.value
            )
            self.assertEqual(booking_snapshots[1].valid_from, release_date)
            self.assertEqual(booking_snapshots[1].valid_to, None)
            self.assertEqual(
                booking_snapshots[1].custody_status, CustodyStatus.RELEASED.value
            )

    def testWritePerson_imposedAndCompletionDate_createsTwoSnapshots(self) -> None:
        scrape_time = datetime.datetime(year=2020, month=7, day=7)
        date_imposed = datetime.datetime(year=2020, month=6, day=1)
        completion_date = datetime.datetime(year=2020, month=7, day=1)

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as act_session:
            person = county_schema.Person(
                full_name=_FULL_NAME,
                birthdate=_BIRTHDATE,
                region=_REGION,
                jurisdiction_id=_JURISDICTION_ID,
            )
            booking = county_schema.Booking(
                custody_status=CustodyStatus.RELEASED.value,
                first_seen_time=scrape_time,
                last_seen_time=scrape_time,
            )
            person.bookings = [booking]
            charge = county_schema.Charge(status=ChargeStatus.PENDING.value)
            booking.charges = [charge]
            sentence = county_schema.Sentence(
                status=SentenceStatus.COMPLETED.value,
                completion_date=completion_date,
                date_imposed=date_imposed,
            )
            charge.sentence = sentence
            persisted_person = database.write_person(
                act_session,
                person,
                FakeIngestMetadata.for_county(
                    region=_REGION,
                    jurisdiction_id=_JURISDICTION_ID,
                    ingest_time=scrape_time,
                ),
            )
            act_session.commit()
            person_id = persisted_person.person_id
            booking_id = persisted_person.bookings[0].booking_id
            charge_id = persisted_person.bookings[0].charges[0].charge_id
            sentence_id = persisted_person.bookings[0].charges[0].sentence.sentence_id

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as assert_session:
            person_snapshot = one(
                assert_session.query(PersonHistory)
                .filter(PersonHistory.person_id == person_id)
                .all()
            )
            booking_snapshot = one(
                assert_session.query(BookingHistory)
                .filter(BookingHistory.booking_id == booking_id)
                .all()
            )
            charge_snapshot = one(
                assert_session.query(ChargeHistory)
                .filter(ChargeHistory.charge_id == charge_id)
                .all()
            )

            self.assertEqual(person_snapshot.valid_from, scrape_time)
            self.assertEqual(booking_snapshot.valid_from, scrape_time)
            self.assertEqual(charge_snapshot.valid_from, scrape_time)

            sentence_snapshots = (
                assert_session.query(SentenceHistory)
                .filter(SentenceHistory.sentence_id == sentence_id)
                .order_by(SentenceHistory.valid_from.asc())
                .all()
            )

            self.assertEqual(len(sentence_snapshots), 2)
            self.assertEqual(sentence_snapshots[0].valid_from, date_imposed)
            self.assertEqual(sentence_snapshots[0].valid_to, completion_date)
            self.assertEqual(
                sentence_snapshots[0].status, SentenceStatus.PRESENT_WITHOUT_INFO.value
            )
            self.assertEqual(sentence_snapshots[1].valid_from, completion_date)
            self.assertEqual(sentence_snapshots[1].valid_to, None)
            self.assertEqual(
                sentence_snapshots[1].status, SentenceStatus.COMPLETED.value
            )

    def testWritePerson_startAfterEnd_usesEndToBackdate(self) -> None:
        scrape_time = datetime.datetime(year=2020, month=7, day=7)
        date_imposed = datetime.datetime(year=2020, month=7, day=1)
        completion_date = datetime.datetime(year=2020, month=6, day=1)

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as act_session:
            person = county_schema.Person(
                full_name=_FULL_NAME,
                birthdate=_BIRTHDATE,
                region=_REGION,
                jurisdiction_id=_JURISDICTION_ID,
            )
            booking = county_schema.Booking(
                custody_status=CustodyStatus.IN_CUSTODY.value,
                first_seen_time=scrape_time,
                last_seen_time=scrape_time,
            )
            person.bookings = [booking]
            charge = county_schema.Charge(status=ChargeStatus.PENDING.value)
            booking.charges = [charge]
            sentence = county_schema.Sentence(
                status=SentenceStatus.PRESENT_WITHOUT_INFO.value,
                completion_date=completion_date,
                date_imposed=date_imposed,
            )
            charge.sentence = sentence
            persisted_person = database.write_person(
                act_session,
                person,
                FakeIngestMetadata.for_county(
                    region=_REGION,
                    jurisdiction_id=_JURISDICTION_ID,
                    ingest_time=scrape_time,
                ),
            )
            act_session.commit()
            person_id = persisted_person.person_id
            booking_id = persisted_person.bookings[0].booking_id
            charge_id = persisted_person.bookings[0].charges[0].charge_id
            sentence_id = persisted_person.bookings[0].charges[0].sentence.sentence_id

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as assert_session:
            person_snapshot = one(
                assert_session.query(PersonHistory)
                .filter(PersonHistory.person_id == person_id)
                .all()
            )
            booking_snapshot = one(
                assert_session.query(BookingHistory)
                .filter(BookingHistory.booking_id == booking_id)
                .all()
            )
            charge_snapshot = one(
                assert_session.query(ChargeHistory)
                .filter(ChargeHistory.charge_id == charge_id)
                .all()
            )
            sentence_snapshot = one(
                assert_session.query(SentenceHistory)
                .filter(SentenceHistory.sentence_id == sentence_id)
                .all()
            )

            self.assertEqual(person_snapshot.valid_from, scrape_time)
            self.assertEqual(booking_snapshot.valid_from, scrape_time)
            self.assertEqual(charge_snapshot.valid_from, scrape_time)
            self.assertEqual(sentence_snapshot.valid_from, completion_date)

    def testWritePerson_admissionDateChanges_doesNotBackdateSnapshot(self) -> None:
        initial_scrape_time = datetime.datetime(year=2020, month=6, day=1)
        update_scrape_time = datetime.datetime(year=2020, month=7, day=7)
        booking_admission_date = datetime.datetime(year=2020, month=7, day=1)

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as arrange_session:
            person = county_schema.Person(
                full_name=_FULL_NAME,
                birthdate=_BIRTHDATE,
                region=_REGION,
                jurisdiction_id=_JURISDICTION_ID,
            )
            booking = county_schema.Booking(
                custody_status=CustodyStatus.IN_CUSTODY.value,
                first_seen_time=initial_scrape_time,
                last_seen_time=initial_scrape_time,
            )
            person.bookings = [booking]
            persisted_person = database.write_person(
                arrange_session,
                person,
                FakeIngestMetadata.for_county(
                    region=_REGION,
                    jurisdiction_id=_JURISDICTION_ID,
                    ingest_time=initial_scrape_time,
                ),
            )
            arrange_session.commit()
            booking_id = persisted_person.bookings[0].booking_id

        with SessionFactory.using_database(self.database_key) as act_session:
            queried_person = one(
                county_dao.read_people(
                    session=act_session, full_name=_FULL_NAME, birthdate=_BIRTHDATE
                )
            )
            queried_person.bookings[0].admission_date = booking_admission_date
            queried_person.bookings[0].admission_date_inferred = False
            queried_person.bookings[0].last_seen_time = update_scrape_time

            database.write_people(
                act_session,
                converter.convert_entity_people_to_schema_people([queried_person]),
                FakeIngestMetadata.for_county(
                    region=_REGION,
                    jurisdiction_id=_JURISDICTION_ID,
                    ingest_time=update_scrape_time,
                ),
            )

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as assert_session:
            booking_snapshots = (
                assert_session.query(BookingHistory)
                .filter(BookingHistory.booking_id == booking_id)
                .all()
            )
            self.assertEqual(len(booking_snapshots), 2)
            self.assertEqual(booking_snapshots[0].valid_from, initial_scrape_time)
            self.assertEqual(booking_snapshots[1].valid_from, update_scrape_time)

    def testWritePerson_newPersonWithBackdatedBooking_backdatesPerson(self) -> None:
        scrape_time = datetime.datetime(year=2020, month=7, day=7)
        booking_admission_date = datetime.datetime(year=2020, month=7, day=1)

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as act_session:
            person = county_schema.Person(
                full_name=_FULL_NAME,
                birthdate=_BIRTHDATE,
                region=_REGION,
                jurisdiction_id=_JURISDICTION_ID,
            )
            booking = county_schema.Booking(
                custody_status=CustodyStatus.IN_CUSTODY.value,
                admission_date=booking_admission_date,
                admission_date_inferred=False,
                first_seen_time=scrape_time,
                last_seen_time=scrape_time,
            )
            person.bookings = [booking]
            persisted_person = database.write_person(
                act_session,
                person,
                FakeIngestMetadata.for_county(
                    region=_REGION,
                    jurisdiction_id=_JURISDICTION_ID,
                    ingest_time=scrape_time,
                ),
            )
            act_session.commit()
            person_id = persisted_person.person_id
            booking_id = persisted_person.bookings[0].booking_id

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as assert_session:
            person_snapshot = one(
                assert_session.query(PersonHistory)
                .filter(PersonHistory.person_id == person_id)
                .all()
            )
            booking_snapshot = one(
                assert_session.query(BookingHistory)
                .filter(BookingHistory.booking_id == booking_id)
                .all()
            )

            self.assertEqual(person_snapshot.valid_from, booking_admission_date)
            self.assertEqual(booking_snapshot.valid_from, booking_admission_date)

    def testWritePerson_backdatedBooking_backdatesChildEntities(self) -> None:
        scrape_time = datetime.datetime(year=2020, month=7, day=7)
        booking_admission_date = datetime.datetime(year=2020, month=7, day=1)

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as act_session:
            person = county_schema.Person(
                full_name=_FULL_NAME,
                birthdate=_BIRTHDATE,
                region=_REGION,
                jurisdiction_id=_JURISDICTION_ID,
            )
            booking = county_schema.Booking(
                custody_status=CustodyStatus.IN_CUSTODY.value,
                admission_date=booking_admission_date,
                admission_date_inferred=False,
                first_seen_time=scrape_time,
                last_seen_time=scrape_time,
            )
            person.bookings = [booking]
            charge = county_schema.Charge(status=ChargeStatus.PENDING.value)
            booking.charges = [charge]
            bond = county_schema.Bond(status=BondStatus.PRESENT_WITHOUT_INFO.value)
            charge.bond = bond
            persisted_person = database.write_person(
                act_session,
                person,
                FakeIngestMetadata.for_county(
                    region=_REGION,
                    jurisdiction_id=_JURISDICTION_ID,
                    ingest_time=scrape_time,
                ),
            )
            act_session.commit()
            charge_id = persisted_person.bookings[0].charges[0].charge_id
            bond_id = persisted_person.bookings[0].charges[0].bond.bond_id

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as assert_session:
            charge_snapshot = one(
                assert_session.query(ChargeHistory)
                .filter(ChargeHistory.charge_id == charge_id)
                .all()
            )
            bond_snapshot = one(
                assert_session.query(BondHistory)
                .filter(BondHistory.bond_id == bond_id)
                .all()
            )

            self.assertEqual(charge_snapshot.valid_from, booking_admission_date)
            self.assertEqual(bond_snapshot.valid_from, booking_admission_date)

    def testWritePerson_backdatedExistingBooking_doesNotBackdateChildren(self) -> None:
        initial_scrape_time = datetime.datetime(year=2020, month=7, day=1)
        update_scrape_time = datetime.datetime(year=2020, month=7, day=7)
        booking_admission_date = datetime.datetime(year=2020, month=6, day=1)

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as arrange_session:
            person = county_schema.Person(
                full_name=_FULL_NAME,
                birthdate=_BIRTHDATE,
                region=_REGION,
                jurisdiction_id=_JURISDICTION_ID,
            )
            booking = county_schema.Booking(
                custody_status=CustodyStatus.IN_CUSTODY.value,
                admission_date=booking_admission_date,
                admission_date_inferred=False,
                first_seen_time=initial_scrape_time,
                last_seen_time=initial_scrape_time,
            )
            person.bookings = [booking]
            persisted_person = database.write_person(
                arrange_session,
                person,
                FakeIngestMetadata.for_county(
                    region=_REGION,
                    jurisdiction_id=_JURISDICTION_ID,
                    ingest_time=initial_scrape_time,
                ),
            )
            arrange_session.commit()
            person_id = persisted_person.person_id
            booking_id = persisted_person.bookings[0].booking_id

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as act_session:
            queried_person = one(
                county_dao.read_people(
                    session=act_session, full_name=_FULL_NAME, birthdate=_BIRTHDATE
                )
            )
            charge = county_entities.Charge.new_with_defaults(
                status=ChargeStatus.PENDING
            )
            queried_person.bookings[0].charges = [charge]
            bond = county_entities.Bond.new_with_defaults(
                status=BondStatus.PRESENT_WITHOUT_INFO
            )
            charge.bond = bond
            updated_person = database.write_person(
                act_session,
                converter.convert_entity_people_to_schema_people([queried_person])[0],
                FakeIngestMetadata.for_county(
                    region=_REGION,
                    jurisdiction_id=_JURISDICTION_ID,
                    ingest_time=update_scrape_time,
                ),
            )
            act_session.commit()
            charge_id = updated_person.bookings[0].charges[0].charge_id
            bond_id = updated_person.bookings[0].charges[0].bond.bond_id

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as assert_session:
            person_snapshot = one(
                assert_session.query(PersonHistory)
                .filter(PersonHistory.person_id == person_id)
                .all()
            )
            booking_snapshot = one(
                assert_session.query(BookingHistory)
                .filter(BookingHistory.booking_id == booking_id)
                .all()
            )
            charge_snapshot = one(
                assert_session.query(ChargeHistory)
                .filter(ChargeHistory.charge_id == charge_id)
                .all()
            )
            bond_snapshot = one(
                assert_session.query(BondHistory)
                .filter(BondHistory.bond_id == bond_id)
                .all()
            )

            self.assertEqual(person_snapshot.valid_from, booking_admission_date)
            self.assertEqual(booking_snapshot.valid_from, booking_admission_date)
            # Uses scrape time rather than backdating to admission date
            self.assertEqual(charge_snapshot.valid_from, update_scrape_time)
            self.assertEqual(bond_snapshot.valid_from, update_scrape_time)

    def testWritePerson_backdatedBookingDescendant_usesProvidedStartTime(self) -> None:
        scrape_time = datetime.datetime(year=2020, month=7, day=7)
        booking_admission_date = datetime.datetime(year=2020, month=7, day=1)
        sentence_date_imposed = datetime.datetime(year=2020, month=7, day=3)

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as act_session:
            person = county_schema.Person(
                full_name=_FULL_NAME,
                birthdate=_BIRTHDATE,
                region=_REGION,
                jurisdiction_id=_JURISDICTION_ID,
            )
            booking = county_schema.Booking(
                custody_status=CustodyStatus.IN_CUSTODY.value,
                admission_date=booking_admission_date,
                admission_date_inferred=False,
                first_seen_time=scrape_time,
                last_seen_time=scrape_time,
            )
            person.bookings = [booking]
            charge = county_schema.Charge(status=ChargeStatus.PENDING.value)
            booking.charges = [charge]
            sentence = county_schema.Sentence(
                status=SentenceStatus.PRESENT_WITHOUT_INFO.value,
                date_imposed=sentence_date_imposed,
            )
            charge.sentence = sentence
            persisted_person = database.write_person(
                act_session,
                person,
                FakeIngestMetadata.for_county(
                    region=_REGION,
                    jurisdiction_id=_JURISDICTION_ID,
                    ingest_time=scrape_time,
                ),
            )
            act_session.commit()
            booking_id = persisted_person.bookings[0].booking_id
            sentence_id = persisted_person.bookings[0].charges[0].sentence.sentence_id

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as assert_session:
            booking_snapshot = one(
                assert_session.query(BookingHistory)
                .filter(BookingHistory.booking_id == booking_id)
                .all()
            )
            sentence_snapshot = one(
                assert_session.query(SentenceHistory)
                .filter(SentenceHistory.sentence_id == sentence_id)
                .all()
            )

            self.assertEqual(booking_snapshot.valid_from, booking_admission_date)
            self.assertEqual(sentence_snapshot.valid_from, sentence_date_imposed)

    def test_removeBondFromCharge_shouldNotOrphanOldBond(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as arrange_session:
            person = county_schema.Person(
                region=_REGION, race=Race.OTHER.value, jurisdiction_id=_JURISDICTION_ID
            )
            booking = county_schema.Booking(
                custody_status=CustodyStatus.IN_CUSTODY.value,
                first_seen_time=_INGEST_TIME,
                last_seen_time=_INGEST_TIME,
            )
            person.bookings = [booking]
            charge = county_schema.Charge(status=ChargeStatus.PENDING.value)
            booking.charges = [charge]
            bond = county_schema.Bond(status=BondStatus.PRESENT_WITHOUT_INFO.value)
            charge.bond = bond

            persisted_person = database.write_person(
                arrange_session,
                person,
                FakeIngestMetadata.for_county(
                    region="default_region",
                    ingest_time=datetime.datetime(year=2020, month=7, day=6),
                ),
            )
            arrange_session.commit()
            persisted_person_id = persisted_person.person_id
            persisted_booking_id = persisted_person.bookings[0].booking_id

        with SessionFactory.using_database(self.database_key) as act_session:
            person_query = act_session.query(Person).filter(
                Person.person_id == persisted_person_id
            )
            fetched_person = person_query.first()
            # Remove bond from charge so bond is no longer directly associated
            # with ORM copy of the record tree
            fetched_charge = fetched_person.bookings[0].charges[0]
            fetched_charge.bond = None
            database.write_person(
                act_session,
                fetched_person,
                FakeIngestMetadata.for_county(
                    region="default_region",
                    ingest_time=datetime.datetime(year=2020, month=7, day=7),
                ),
            )

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as assert_session:
            # Bond should still be associated with booking, even though it is no
            # longer associated with the charge
            # pylint: disable=W0143
            bonds = (
                assert_session.query(Bond)
                .filter(Bond.booking_id == persisted_booking_id)
                .all()
            )
            self.assertEqual(len(bonds), 1)

    def test_removeSentenceFromCharge_shouldNotOrphanOldSentence(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as arrange_session:
            person = county_schema.Person(
                region=_REGION, race=Race.OTHER.value, jurisdiction_id=_JURISDICTION_ID
            )
            booking = county_schema.Booking(
                custody_status=CustodyStatus.IN_CUSTODY.value,
                first_seen_time=_INGEST_TIME,
                last_seen_time=_INGEST_TIME,
            )
            person.bookings = [booking]
            charge = county_schema.Charge(status=ChargeStatus.PENDING.value)
            booking.charges = [charge]
            sentence = county_schema.Sentence(
                status=SentenceStatus.PRESENT_WITHOUT_INFO.value
            )
            charge.sentence = sentence

            persisted_person = database.write_person(
                arrange_session,
                person,
                FakeIngestMetadata.for_county(
                    region="default_region",
                    ingest_time=datetime.datetime(year=2020, month=7, day=6),
                ),
            )
            arrange_session.commit()
            persisted_person_id = persisted_person.person_id
            persisted_booking_id = persisted_person.bookings[0].booking_id

        with SessionFactory.using_database(self.database_key) as act_session:
            person_query = act_session.query(Person).filter(
                Person.person_id == persisted_person_id
            )
            fetched_person = person_query.first()
            # Remove sentence from charge so sentence is no longer directly
            # associated with ORM copy of the record tree
            fetched_charge = fetched_person.bookings[0].charges[0]
            fetched_charge.sentence = None
            database.write_person(
                act_session,
                fetched_person,
                FakeIngestMetadata.for_county(
                    region="default_region",
                    ingest_time=datetime.datetime(year=2020, month=7, day=7),
                ),
            )

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as assert_session:
            # Sentence should still be associated with booking, even though it is no
            # longer associated with the charge
            # pylint: disable=W0143
            sentences = (
                assert_session.query(Sentence)
                .filter(Sentence.booking_id == persisted_booking_id)
                .all()
            )
            self.assertEqual(len(sentences), 1)

    def test_orphanedEntities_shouldStillWriteSnapshots(self) -> None:
        orphan_scrape_time = datetime.datetime(year=2020, month=7, day=8)

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as arrange_session:
            person = county_schema.Person(
                region=_REGION, jurisdiction_id=_JURISDICTION_ID
            )
            booking = county_schema.Booking(
                custody_status=CustodyStatus.IN_CUSTODY.value,
                first_seen_time=_INGEST_TIME,
                last_seen_time=_INGEST_TIME,
            )
            person.bookings = [booking]
            charge = county_schema.Charge(status=ChargeStatus.PENDING.value)
            booking.charges = [charge]
            sentence = county_schema.Sentence(
                status=SentenceStatus.PRESENT_WITHOUT_INFO.value
            )
            charge.sentence = sentence

            persisted_person = database.write_person(
                arrange_session,
                person,
                FakeIngestMetadata.for_county(
                    region="default_region",
                    ingest_time=datetime.datetime(year=2020, month=7, day=6),
                ),
            )
            arrange_session.commit()
            persisted_person_id = persisted_person.person_id

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as act_session:
            person_query = act_session.query(Person).filter(
                Person.person_id == persisted_person_id
            )
            fetched_person = person_query.first()
            # Remove sentence from charge so sentence is no longer directly
            # associated with ORM copy of the record tree
            fetched_charge = fetched_person.bookings[0].charges[0]
            fetched_sentence = fetched_charge.sentence
            fetched_charge.sentence = None
            # Update sentence status so new snapshot will be required
            fetched_sentence.status = SentenceStatus.REMOVED_WITHOUT_INFO.value
            database.write_person(
                act_session,
                fetched_person,
                FakeIngestMetadata.for_county(
                    region="default_region", ingest_time=orphan_scrape_time
                ),
                orphaned_entities=[fetched_sentence],
            )
            act_session.commit()
            fetched_sentence_id = fetched_sentence.sentence_id

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as assert_session:
            sentence_snapshot = (
                assert_session.query(SentenceHistory)
                .filter(SentenceHistory.sentence_id == fetched_sentence_id)
                .order_by(SentenceHistory.valid_from.desc())
                .first()
            )

            self.assertEqual(sentence_snapshot.valid_from, orphan_scrape_time)
            self.assertEqual(
                sentence_snapshot.status, SentenceStatus.REMOVED_WITHOUT_INFO.value
            )

    def test_addBondToExistingBooking_shouldSetBookingIdOnBond(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as arrange_session:
            person = county_schema.Person(
                region=_REGION, race=Race.OTHER.value, jurisdiction_id=_JURISDICTION_ID
            )
            booking = county_schema.Booking(
                custody_status=CustodyStatus.IN_CUSTODY.value,
                first_seen_time=_INGEST_TIME,
                last_seen_time=_INGEST_TIME,
            )
            person.bookings = [booking]
            charge = county_schema.Charge(status=ChargeStatus.PENDING.value)
            booking.charges = [charge]

            persisted_person = database.write_person(
                arrange_session,
                person,
                FakeIngestMetadata.for_county(
                    region="default_region",
                    ingest_time=datetime.datetime(year=2020, month=7, day=6),
                ),
            )
            arrange_session.commit()
            persisted_person_id = persisted_person.person_id
            persisted_booking_id = persisted_person.bookings[0].booking_id

        with SessionFactory.using_database(self.database_key) as act_session:
            person_query = act_session.query(Person).filter(
                Person.person_id == persisted_person_id
            )
            fetched_person = person_query.first()

            new_bond = county_schema.Bond(status=BondStatus.PRESENT_WITHOUT_INFO.value)
            fetched_person.bookings[0].charges[0].bond = new_bond
            database.write_person(
                act_session,
                fetched_person,
                FakeIngestMetadata.for_county(
                    region="default_region",
                    ingest_time=datetime.datetime(year=2020, month=7, day=7),
                ),
            )

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as assert_session:
            assert_person_query = assert_session.query(Person).filter(
                Person.person_id == persisted_person_id
            )
            final_fetched_person = converter.convert_schema_object_to_entity(
                assert_person_query.first()
            )
            if not isinstance(final_fetched_person, county_entities.Person):
                raise ValueError(f"Unexpected person type [{final_fetched_person}]")

            final_charge = final_fetched_person.bookings[0].charges[0]
            self.assertIsNotNone(final_charge)
            final_bond = final_charge.bond
            self.assertIsNotNone(final_bond)
            if final_bond is None:
                raise ValueError("Unexpected None bond.")
            self.assertEqual(final_bond.booking_id, persisted_booking_id)
