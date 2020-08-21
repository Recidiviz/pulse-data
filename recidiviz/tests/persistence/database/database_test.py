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
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_incarceration_period import StateIncarcerationPeriodStatus
from recidiviz.common.constants.state.state_parole_decision import StateParoleDecisionOutcome
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodStatus
from recidiviz.common.constants.state.state_supervision_violation import StateSupervisionViolationType
from recidiviz.common.constants.state.state_supervision_violation_response import \
    StateSupervisionViolationResponseDecision, StateSupervisionViolationResponseRevocationType
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.persistence.database import database
from recidiviz.persistence.database.base_schema import \
    JailsBase, StateBase
from recidiviz.persistence.database.database import ASSOCIATION_TABLE_NAME_SUFFIX, \
    _hydrate_state_codes_in_association_tables
from recidiviz.persistence.database.schema.county import dao as county_dao
from recidiviz.persistence.database.schema.county import schema as county_schema
from recidiviz.persistence.database.schema.county.schema import Bond, Booking, \
    Person, Sentence
from recidiviz.persistence.database.schema.county.schema import (
    BondHistory,
    BookingHistory,
    Charge, ChargeHistory,
    PersonHistory,
    SentenceHistory)
from recidiviz.persistence.database.schema.state import schema as state_schema
from recidiviz.persistence.database.schema_entity_converter import (
    schema_entity_converter as converter,
)
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.entity.county import entities as county_entities
from recidiviz.tests.persistence.database.schema.state.schema_test_utils import generate_person, \
    generate_incarceration_period, generate_incarceration_sentence, generate_supervision_sentence, \
    generate_sentence_group, generate_external_id, generate_bond, generate_court_case, generate_charge, generate_fine, \
    generate_assessment, generate_agent, generate_incarceration_incident, generate_parole_decision, \
    generate_supervision_violation_response_decision_entry, generate_supervision_violation_response, \
    generate_supervision_violation_type_entry, generate_supervision_violated_condition_entry, \
    generate_supervision_violation, generate_supervision_case_type_entry, generate_supervision_period
from recidiviz.tests.utils import fakes

_REGION = 'region'
_REGION_ANOTHER = 'wrong region'
_JURISDICTION_ID = '12345678'
_FULL_NAME = 'full_name'
_EXTERNAL_ID = 'external_id'
_BIRTHDATE = datetime.date(year=2012, month=1, day=2)
_INGEST_TIME = datetime.datetime(year=2020, month=7, day=4)
_FACILITY = 'facility'
_DEFAULT_METADATA = IngestMetadata(
    region='default_region', jurisdiction_id='jid',
    ingest_time=_INGEST_TIME, enum_overrides=EnumOverrides.empty())

DATE_SCRAPED = datetime.date(year=2019, month=1, day=1)

_EXTERNAL_ID_1 = 'EXTERNAL_ID-1'
_EXTERNAL_ID_2 = 'EXTERNAL_ID-2'
_EXTERNAL_ID_3 = 'EXTERNAL_ID-3'
_EXTERNAL_ID_4 = 'EXTERNAL_ID-4'
_EXTERNAL_ID_5 = 'EXTERNAL_ID-5'
_EXTERNAL_ID_6 = 'EXTERNAL_ID-6'
_ID = 1
_ID_2 = 2
_ID_3 = 3
_ID_4 = 4
_ID_5 = 5
_ID_6 = 6
_ID_TYPE = 'ID_TYPE'
_ID_TYPE_ANOTHER = 'ID_TYPE_ANOTHER'
_FULL_NAME = 'FULL_NAME'
_FULL_NAME_ANOTHER = 'FULL_NAME_ANOTHER'
_COUNTY_CODE = 'Iredell'
_STATE_CODE = 'NC'
_DATE_1 = datetime.date(year=2019, month=1, day=1)
_DATE_2 = datetime.date(year=2019, month=2, day=1)


class TestDatabase(TestCase):
    """Test that the methods in database.py correctly read from the SQL
    database """

    def setUp(self):
        fakes.use_in_memory_sqlite_database(JailsBase)

    def tearDown(self) -> None:
        fakes.teardown_in_memory_sqlite_databases()

    def testWritePerson_noExistingSnapshots_createsSnapshots(self):
        act_session = SessionFactory.for_schema_base(JailsBase)

        person = county_schema.Person(
            region=_REGION, race=Race.OTHER.value,
            jurisdiction_id=_JURISDICTION_ID)

        booking = county_schema.Booking(
            custody_status=CustodyStatus.IN_CUSTODY.value,
            first_seen_time=_INGEST_TIME,
            last_seen_time=_INGEST_TIME)
        person.bookings = [booking]

        charge_1 = county_schema.Charge(
            status=ChargeStatus.PENDING.value)
        charge_2 = county_schema.Charge(
            status=ChargeStatus.PRETRIAL.value)
        booking.charges = [charge_1, charge_2]

        persisted_person = database.write_person(
            act_session, person, _DEFAULT_METADATA)
        act_session.commit()

        person_id = persisted_person.person_id
        booking_id = persisted_person.bookings[0].booking_id
        charge_1_id = persisted_person.bookings[0].charges[0].charge_id
        charge_2_id = persisted_person.bookings[0].charges[1].charge_id

        act_session.close()

        assert_session = SessionFactory.for_schema_base(JailsBase)

        person_snapshots = assert_session.query(PersonHistory).filter(
            PersonHistory.person_id == person_id).all()
        booking_snapshots = assert_session.query(BookingHistory).filter(
            BookingHistory.booking_id == booking_id).all()
        charge_snapshots = assert_session.query(ChargeHistory).filter(
            ChargeHistory.charge_id.in_([charge_1_id, charge_2_id])).all()

        self.assertEqual(len(person_snapshots), 1)
        self.assertEqual(len(booking_snapshots), 1)
        self.assertEqual(len(charge_snapshots), 2)

        person_snapshot = person_snapshots[0]
        booking_snapshot = booking_snapshots[0]
        charge_snapshot_statuses = {charge_snapshot.status for charge_snapshot
                                    in charge_snapshots}
        snapshot_time = person_snapshot.valid_from

        self.assertEqual(person_snapshot.region, _REGION)
        self.assertEqual(person_snapshot.race, Race.OTHER.value)
        self.assertIsNone(person_snapshot.valid_to)

        self.assertEqual(
            booking_snapshot.custody_status, CustodyStatus.IN_CUSTODY.value)
        self.assertEqual(booking_snapshot.valid_from, snapshot_time)
        self.assertIsNone(booking_snapshot.valid_to)

        self.assertEqual(
            charge_snapshot_statuses,
            {ChargeStatus.PENDING.value, ChargeStatus.PRETRIAL.value})
        self.assertEqual(charge_snapshots[0].valid_from, snapshot_time)
        self.assertEqual(charge_snapshots[1].valid_from, snapshot_time)
        self.assertIsNone(charge_snapshots[0].valid_to)
        self.assertIsNone(charge_snapshots[1].valid_to)

        assert_session.close()

    def testWritePerson_allExistingSnapshots_onlyUpdatesOnChange(self):
        charge_name_1 = 'charge_name_1'
        charge_name_2 = 'charge_name_2'
        updated_last_seen_time = datetime.datetime(year=2020, month=7, day=6)
        # Pick a date in the past so the assigned snapshot date will always be
        # later
        valid_from = datetime.datetime(year=2015, month=1, day=1)
        existing_person_id = 1
        existing_booking_id = 14
        existing_charge_id = 47

        arrange_session = SessionFactory.for_schema_base(JailsBase)

        existing_person = Person(
            person_id=existing_person_id,
            region=_REGION,
            jurisdiction_id=_JURISDICTION_ID,
            race=Race.OTHER.value)
        existing_person_snapshot = PersonHistory(
            person_history_id=1000,
            person_id=existing_person_id,
            region=_REGION,
            jurisdiction_id=_JURISDICTION_ID,
            race=Race.OTHER.value,
            valid_from=valid_from)

        existing_booking = Booking(
            booking_id=existing_booking_id,
            custody_status=CustodyStatus.IN_CUSTODY.value,
            facility=_FACILITY,
            first_seen_time=_INGEST_TIME,
            last_seen_time=_INGEST_TIME)
        existing_booking_snapshot = BookingHistory(
            booking_history_id=1000,
            person_id=existing_person_id,
            booking_id=existing_booking_id,
            custody_status=CustodyStatus.IN_CUSTODY.value,
            facility=_FACILITY,
            valid_from=valid_from)
        existing_person.bookings = [existing_booking]

        existing_charge = Charge(
            charge_id=existing_charge_id,
            status=ChargeStatus.PENDING.value,
            name=charge_name_1)
        existing_charge_snapshot = ChargeHistory(
            charge_history_id=1000,
            charge_id=existing_charge_id,
            booking_id=existing_booking_id,
            status=ChargeStatus.PENDING.value,
            name=charge_name_1,
            valid_from=valid_from)
        existing_booking.charges = [existing_charge]

        arrange_session.add(existing_person)
        arrange_session.flush()
        # Snapshots must be added separately, as they are not included in ORM
        # relationships
        arrange_session.add(existing_person_snapshot)
        arrange_session.add(existing_booking_snapshot)
        arrange_session.add(existing_charge_snapshot)
        arrange_session.commit()
        arrange_session.close()

        act_session = SessionFactory.for_schema_base(JailsBase)

        # Ingested record tree has updates to person and charge but not booking
        ingested_person = county_schema.Person(
            person_id=existing_person_id,
            region=_REGION,
            jurisdiction_id=_JURISDICTION_ID,
            race=Race.EXTERNAL_UNKNOWN.value)

        # Ingested booking has new last_seen_time but this is ignored, as it
        # is not included on the historical table.
        ingested_booking = county_schema.Booking(
            booking_id=existing_booking_id,
            custody_status=CustodyStatus.IN_CUSTODY.value,
            facility=_FACILITY,
            first_seen_time=updated_last_seen_time,
            last_seen_time=updated_last_seen_time)
        ingested_person.bookings = [ingested_booking]

        ingested_charge = county_schema.Charge(
            charge_id=existing_charge_id,
            status=ChargeStatus.PENDING.value,
            name=charge_name_2)
        ingested_booking.charges = [ingested_charge]

        database.write_person(
            act_session, ingested_person, _DEFAULT_METADATA)
        act_session.commit()
        act_session.close()

        assert_session = SessionFactory.for_schema_base(JailsBase)

        person_snapshots = assert_session.query(PersonHistory) \
            .filter(PersonHistory.person_id == existing_person_id) \
            .order_by(PersonHistory.valid_from.asc()) \
            .all()
        booking_snapshots = assert_session.query(BookingHistory).filter(
            BookingHistory.booking_id == existing_booking_id).all()
        charge_snapshots = assert_session.query(ChargeHistory) \
            .filter(ChargeHistory.charge_id == existing_charge_id) \
            .order_by(ChargeHistory.valid_from.asc()) \
            .all()

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
        self.assertEqual(
            new_person_snapshot.race, Race.EXTERNAL_UNKNOWN.value)

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

        assert_session.close()

    def testWritePerson_someExistingSnapshots_createsAndExtends(self):
        charge_name = 'charge_name'
        updated_last_seen_time = datetime.datetime(year=2020, month=7, day=6)
        # Pick a date in the past so the assigned snapshot date will always be
        # later
        valid_from = datetime.datetime(year=2015, month=1, day=1)
        existing_person_id = 1
        existing_booking_id = 14

        arrange_session = SessionFactory.for_schema_base(JailsBase)

        # Person and booking already exist, while charge is new.
        existing_person = Person(
            person_id=existing_person_id,
            region=_REGION,
            jurisdiction_id=_JURISDICTION_ID,
            race=Race.OTHER.value)
        existing_person_snapshot = PersonHistory(
            person_history_id=1000,
            person_id=existing_person_id,
            region=_REGION,
            jurisdiction_id=_JURISDICTION_ID,
            race=Race.OTHER.value,
            valid_from=valid_from)

        existing_booking = Booking(
            booking_id=existing_booking_id,
            custody_status=CustodyStatus.IN_CUSTODY.value,
            facility=_FACILITY,
            first_seen_time=_INGEST_TIME,
            last_seen_time=_INGEST_TIME)
        existing_booking_snapshot = BookingHistory(
            booking_history_id=1000,
            person_id=existing_person_id,
            booking_id=existing_booking_id,
            custody_status=CustodyStatus.IN_CUSTODY.value,
            facility=_FACILITY,
            valid_from=valid_from)
        existing_person.bookings = [existing_booking]

        arrange_session.add(existing_person)
        arrange_session.flush()
        # Snapshots must be added separately, as they are not included in ORM
        # relationships
        arrange_session.add(existing_person_snapshot)
        arrange_session.add(existing_booking_snapshot)
        arrange_session.commit()
        arrange_session.close()

        act_session = SessionFactory.for_schema_base(JailsBase)

        # Ingested record tree has update to person, no updates to booking, and
        # a new charge
        ingested_person = county_schema.Person(
            person_id=existing_person_id,
            region=_REGION,
            jurisdiction_id=_JURISDICTION_ID,
            race=Race.EXTERNAL_UNKNOWN.value)

        # Ingested booking has new last_seen_time but this is ignored, as it
        # is not included on the historical table.
        ingested_booking = county_schema.Booking(
            booking_id=existing_booking_id,
            custody_status=CustodyStatus.IN_CUSTODY.value,
            facility=_FACILITY,
            first_seen_time=_INGEST_TIME,
            last_seen_time=updated_last_seen_time)
        ingested_person.bookings = [ingested_booking]

        ingested_charge = county_schema.Charge(
            status=ChargeStatus.PENDING.value,
            name=charge_name)
        ingested_booking.charges = [ingested_charge]

        persisted_person = database.write_person(
            act_session, ingested_person, _DEFAULT_METADATA)
        act_session.commit()

        charge_id = persisted_person.bookings[0].charges[0].charge_id

        act_session.close()

        assert_session = SessionFactory.for_schema_base(JailsBase)

        person_snapshots = assert_session.query(PersonHistory) \
            .filter(PersonHistory.person_id == existing_person_id) \
            .order_by(PersonHistory.valid_from.asc()) \
            .all()
        booking_snapshots = assert_session.query(BookingHistory).filter(
            BookingHistory.booking_id == existing_booking_id).all()
        charge_snapshots = assert_session.query(ChargeHistory).filter(
            ChargeHistory.charge_id == charge_id).all()

        self.assertEqual(len(person_snapshots), 2)
        self.assertEqual(len(booking_snapshots), 1)
        self.assertEqual(len(charge_snapshots), 1)

        old_person_snapshot = person_snapshots[0]
        new_person_snapshot = person_snapshots[1]
        booking_snapshot = booking_snapshots[0]
        charge_snapshot = charge_snapshots[0]
        snapshot_time = old_person_snapshot.valid_to

        self.assertEqual(old_person_snapshot.race, Race.OTHER.value)
        self.assertEqual(
            new_person_snapshot.race, Race.EXTERNAL_UNKNOWN.value)

        self.assertEqual(charge_snapshot.name, charge_name)

        self.assertEqual(old_person_snapshot.valid_from, valid_from)
        self.assertEqual(new_person_snapshot.valid_from, snapshot_time)
        self.assertEqual(booking_snapshot.valid_from, valid_from)
        self.assertEqual(charge_snapshot.valid_from, snapshot_time)

        self.assertEqual(old_person_snapshot.valid_to, snapshot_time)
        self.assertIsNone(new_person_snapshot.valid_to)
        self.assertIsNone(booking_snapshot.valid_to)
        self.assertIsNone(charge_snapshot.valid_to)

        assert_session.close()

    def testWritePerson_overlappingSnapshots_doesNotRaiseError(self):
        name = 'Steve Fakename'
        birthdate = datetime.date(year=1980, month=1, day=1)

        arrange_session = SessionFactory.for_schema_base(JailsBase)

        person = Person(
            person_id=1, full_name=name, birthdate=birthdate,
            region=_REGION, jurisdiction_id=_JURISDICTION_ID,
            race=Race.OTHER.value)
        booking = Booking(
            booking_id=2, custody_status=CustodyStatus.IN_CUSTODY.value,
            first_seen_time=datetime.datetime(year=2020, month=7, day=4),
            last_seen_time=datetime.datetime(year=2020, month=7, day=6))
        person.bookings.append(booking)
        charge = Charge(
            charge_id=3, status=ChargeStatus.PENDING.value)
        booking.charges.append(charge)
        bond = Bond(
            bond_id=4, status=BondStatus.PRESENT_WITHOUT_INFO.value,
            booking_id=2)
        charge.bond = bond

        arrange_session.add(person)
        arrange_session.flush()

        snapshot_time = '2020-07-06 00:00:00'

        arrange_session.execute(text(
            'INSERT INTO person_history (person_id, region, jurisdiction_id, '
            'race, valid_from) '
            'VALUES ({}, \'{}\', \'{}\', \'{}\', \'{}\');'.format(
                1, _REGION, _JURISDICTION_ID, Race.OTHER.value, snapshot_time)))
        arrange_session.execute(text(
            'INSERT INTO booking_history '
            '(booking_id, person_id, custody_status, valid_from) '
            'VALUES ({}, {}, \'{}\', \'{}\');'.format(
                2, 1, CustodyStatus.IN_CUSTODY.value, snapshot_time)))
        arrange_session.execute(text(
            'INSERT INTO bond_history '
            '(bond_id, booking_id, status, valid_from) '
            'VALUES ({}, {}, \'{}\', \'{}\');'.format(
                4, 2, BondStatus.PRESENT_WITHOUT_INFO.value, snapshot_time)))

        # Create instantaneous closed charge snapshot and open charge snapshot
        arrange_session.execute(text(
            'INSERT INTO charge_history '
            '(charge_id, booking_id, bond_id, status, valid_from, valid_to) '
            'VALUES '
            '({}, {}, {}, \'{}\', \'{valid_from}\', \'{valid_to}\');'.format(
                3, 2, 4, ChargeStatus.PENDING.value, valid_from=snapshot_time,
                valid_to=snapshot_time)))
        arrange_session.execute(text(
            'INSERT INTO charge_history '
            '(charge_id, booking_id, bond_id, status, valid_from) '
            'VALUES ({}, {}, {}, \'{}\', \'{}\');'.format(
                3, 2, 4, ChargeStatus.PENDING.value, snapshot_time)))

        arrange_session.commit()
        arrange_session.close()

        assert_session = SessionFactory.for_schema_base(JailsBase)

        ingest_person = county_dao.read_people(
            assert_session, full_name=name, birthdate=birthdate)[0]
        ingest_person.bookings[0].custody_status = CustodyStatus.RELEASED

        try:
            database.write_people(
                assert_session,
                converter.convert_entity_people_to_schema_people(
                    [ingest_person]),
                IngestMetadata(
                    _REGION, _JURISDICTION_ID,
                    datetime.datetime(year=2020, month=7, day=8), {}))
        except Exception as e:
            self.fail('Writing person failed with error: {}'.format(e))

    def testWritePeople_duplicatePeople_raisesError(self):
        shared_id = 48
        session = SessionFactory.for_schema_base(JailsBase)

        person_1 = county_schema.Person(
            region=_REGION, race=Race.OTHER.value, person_id=shared_id,
            jurisdiction_id=_JURISDICTION_ID)
        person_2 = county_schema.Person(
            region=_REGION,
            race=Race.EXTERNAL_UNKNOWN.value,
            person_id=shared_id,
            jurisdiction_id=_JURISDICTION_ID)

        self.assertRaises(
            AssertionError,
            database.write_people,
            session,
            [person_1, person_2],
            _DEFAULT_METADATA)

        session.close()

    def testWritePerson_backdatedBooking_backdatesSnapshot(self):
        person_scrape_time = datetime.datetime(year=2020, month=6, day=1)
        booking_scrape_time = datetime.datetime(year=2020, month=7, day=7)
        booking_admission_date = datetime.datetime(year=2020, month=7, day=1)

        arrange_session = SessionFactory.for_schema_base(JailsBase)
        person = county_schema.Person(
            full_name=_FULL_NAME, birthdate=_BIRTHDATE, region=_REGION,
            jurisdiction_id=_JURISDICTION_ID)
        persisted_person = database.write_person(
            arrange_session, person, IngestMetadata(
                _REGION, _JURISDICTION_ID, person_scrape_time, {}))
        arrange_session.commit()
        person_id = persisted_person.person_id
        arrange_session.close()

        act_session = SessionFactory.for_schema_base(JailsBase)
        queried_person = one(county_dao.read_people(
            session=act_session, full_name=_FULL_NAME, birthdate=_BIRTHDATE))
        booking = county_entities.Booking.new_with_defaults(
            custody_status=CustodyStatus.IN_CUSTODY,
            admission_date=booking_admission_date,
            admission_date_inferred=False,
            first_seen_time=booking_scrape_time,
            last_seen_time=booking_scrape_time)
        queried_person.bookings = [booking]

        updated_person = database.write_person(
            act_session,
            converter.convert_entity_people_to_schema_people(
                [queried_person])[0],
            IngestMetadata(
                _REGION, _JURISDICTION_ID, booking_scrape_time, {}))
        act_session.commit()
        booking_id = updated_person.bookings[0].booking_id
        act_session.close()

        assert_session = SessionFactory.for_schema_base(JailsBase)

        person_snapshot = one(assert_session.query(PersonHistory).filter(
            PersonHistory.person_id == person_id).all())
        booking_snapshot = one(assert_session.query(BookingHistory).filter(
            BookingHistory.booking_id == booking_id).all())

        self.assertEqual(person_snapshot.valid_from, person_scrape_time)
        self.assertEqual(booking_snapshot.valid_from, booking_admission_date)

        assert_session.commit()
        assert_session.close()

    def testWritePerson_backdatedRelease_backdatesRecordTreeToRelease(self):
        scrape_time = datetime.datetime(year=2020, month=7, day=7)
        booking_release_date = datetime.datetime(year=2020, month=7, day=1)

        act_session = SessionFactory.for_schema_base(JailsBase)
        person = county_schema.Person(
            full_name=_FULL_NAME, birthdate=_BIRTHDATE, region=_REGION,
            jurisdiction_id=_JURISDICTION_ID)
        booking = county_schema.Booking(
            custody_status=CustodyStatus.RELEASED.value,
            release_date=booking_release_date,
            release_date_inferred=False,
            first_seen_time=scrape_time,
            last_seen_time=scrape_time)
        person.bookings = [booking]
        charge = county_schema.Charge(
            status=ChargeStatus.PENDING.value)
        booking.charges = [charge]
        persisted_person = database.write_person(
            act_session, person, IngestMetadata(
                _REGION, _JURISDICTION_ID, scrape_time, {}))
        act_session.commit()
        person_id = persisted_person.person_id
        booking_id = persisted_person.bookings[0].booking_id
        charge_id = persisted_person.bookings[0].charges[0].charge_id
        act_session.close()

        assert_session = SessionFactory.for_schema_base(JailsBase)

        person_snapshot = one(assert_session.query(PersonHistory).filter(
            PersonHistory.person_id == person_id).all())
        booking_snapshot = one(assert_session.query(BookingHistory).filter(
            BookingHistory.booking_id == booking_id).all())
        charge_snapshot = one(assert_session.query(ChargeHistory).filter(
            ChargeHistory.charge_id == charge_id).all())

        self.assertEqual(person_snapshot.valid_from, booking_release_date)
        self.assertEqual(booking_snapshot.valid_from, booking_release_date)
        self.assertEqual(charge_snapshot.valid_from, booking_release_date)

        assert_session.commit()
        assert_session.close()

    def testWritePerson_releaseAfterScrapeTime_usesScrapeTime(self):
        scrape_time = datetime.datetime(year=2020, month=7, day=7)
        booking_release_date = datetime.datetime(year=2020, month=7, day=15)

        act_session = SessionFactory.for_schema_base(JailsBase)
        person = county_schema.Person(
            full_name=_FULL_NAME, birthdate=_BIRTHDATE, region=_REGION,
            jurisdiction_id=_JURISDICTION_ID)
        booking = county_schema.Booking(
            custody_status=CustodyStatus.RELEASED.value,
            release_date=booking_release_date,
            release_date_inferred=False,
            first_seen_time=scrape_time,
            last_seen_time=scrape_time)
        person.bookings = [booking]
        charge = county_schema.Charge(
            status=ChargeStatus.PENDING.value)
        booking.charges = [charge]
        persisted_person = database.write_person(
            act_session, person, IngestMetadata(
                _REGION, _JURISDICTION_ID, scrape_time, {}))
        act_session.commit()
        person_id = persisted_person.person_id
        booking_id = persisted_person.bookings[0].booking_id
        charge_id = persisted_person.bookings[0].charges[0].charge_id
        act_session.close()

        assert_session = SessionFactory.for_schema_base(JailsBase)

        person_snapshot = one(assert_session.query(PersonHistory).filter(
            PersonHistory.person_id == person_id).all())
        booking_snapshot = one(assert_session.query(BookingHistory).filter(
            BookingHistory.booking_id == booking_id).all())
        charge_snapshot = one(assert_session.query(ChargeHistory).filter(
            ChargeHistory.charge_id == charge_id).all())

        self.assertEqual(person_snapshot.valid_from, scrape_time)
        self.assertEqual(booking_snapshot.valid_from, scrape_time)
        self.assertEqual(charge_snapshot.valid_from, scrape_time)

        assert_session.commit()
        assert_session.close()

    def testWritePerson_admissionAndReleaseDate_createsTwoSnapshots(self):
        scrape_time = datetime.datetime(year=2020, month=7, day=7)
        admission_date = datetime.datetime(year=2020, month=6, day=1)
        release_date = datetime.datetime(year=2020, month=7, day=1)

        act_session = SessionFactory.for_schema_base(JailsBase)
        person = county_schema.Person(
            full_name=_FULL_NAME, birthdate=_BIRTHDATE, region=_REGION,
            jurisdiction_id=_JURISDICTION_ID)
        booking = county_schema.Booking(
            custody_status=CustodyStatus.RELEASED.value,
            admission_date=admission_date,
            admission_date_inferred=False,
            release_date=release_date,
            release_date_inferred=False,
            first_seen_time=scrape_time,
            last_seen_time=scrape_time)
        person.bookings = [booking]
        charge = county_schema.Charge(
            status=ChargeStatus.PENDING.value)
        booking.charges = [charge]
        persisted_person = database.write_person(
            act_session, person, IngestMetadata(
                _REGION, _JURISDICTION_ID, scrape_time, {}))
        act_session.commit()
        person_id = persisted_person.person_id
        booking_id = persisted_person.bookings[0].booking_id
        charge_id = persisted_person.bookings[0].charges[0].charge_id
        act_session.close()

        assert_session = SessionFactory.for_schema_base(JailsBase)

        person_snapshot = one(assert_session.query(PersonHistory).filter(
            PersonHistory.person_id == person_id).all())
        charge_snapshot = one(assert_session.query(ChargeHistory).filter(
            ChargeHistory.charge_id == charge_id).all())

        self.assertEqual(person_snapshot.valid_from, admission_date)
        self.assertEqual(charge_snapshot.valid_from, admission_date)

        booking_snapshots = assert_session.query(BookingHistory) \
            .filter(BookingHistory.booking_id == booking_id) \
            .order_by(BookingHistory.valid_from.asc()) \
            .all()

        self.assertEqual(len(booking_snapshots), 2)
        self.assertEqual(booking_snapshots[0].valid_from, admission_date)
        self.assertEqual(booking_snapshots[0].valid_to, release_date)
        self.assertEqual(booking_snapshots[0].custody_status, CustodyStatus.IN_CUSTODY.value)
        self.assertEqual(booking_snapshots[1].valid_from, release_date)
        self.assertEqual(booking_snapshots[1].valid_to, None)
        self.assertEqual(booking_snapshots[1].custody_status, CustodyStatus.RELEASED.value)

        assert_session.commit()
        assert_session.close()

    def testWritePerson_imposedAndCompletionDate_createsTwoSnapshots(self):
        scrape_time = datetime.datetime(year=2020, month=7, day=7)
        date_imposed = datetime.datetime(year=2020, month=6, day=1)
        completion_date = datetime.datetime(year=2020, month=7, day=1)

        act_session = SessionFactory.for_schema_base(JailsBase)
        person = county_schema.Person(
            full_name=_FULL_NAME, birthdate=_BIRTHDATE, region=_REGION,
            jurisdiction_id=_JURISDICTION_ID)
        booking = county_schema.Booking(
            custody_status=CustodyStatus.RELEASED.value,
            first_seen_time=scrape_time,
            last_seen_time=scrape_time)
        person.bookings = [booking]
        charge = county_schema.Charge(
            status=ChargeStatus.PENDING.value)
        booking.charges = [charge]
        sentence = county_schema.Sentence(
            status=SentenceStatus.COMPLETED.value,
            completion_date=completion_date,
            date_imposed=date_imposed)
        charge.sentence = sentence
        persisted_person = database.write_person(
            act_session, person, IngestMetadata(
                _REGION, _JURISDICTION_ID, scrape_time, {}))
        act_session.commit()
        person_id = persisted_person.person_id
        booking_id = persisted_person.bookings[0].booking_id
        charge_id = persisted_person.bookings[0].charges[0].charge_id
        sentence_id = \
            persisted_person.bookings[0].charges[0].sentence.sentence_id
        act_session.close()

        assert_session = SessionFactory.for_schema_base(JailsBase)

        person_snapshot = one(assert_session.query(PersonHistory).filter(
            PersonHistory.person_id == person_id).all())
        booking_snapshot = one(assert_session.query(BookingHistory).filter(
            BookingHistory.booking_id == booking_id).all())
        charge_snapshot = one(assert_session.query(ChargeHistory).filter(
            ChargeHistory.charge_id == charge_id).all())

        self.assertEqual(person_snapshot.valid_from, scrape_time)
        self.assertEqual(booking_snapshot.valid_from, scrape_time)
        self.assertEqual(charge_snapshot.valid_from, scrape_time)

        sentence_snapshots = assert_session.query(SentenceHistory) \
            .filter(SentenceHistory.sentence_id == sentence_id) \
            .order_by(SentenceHistory.valid_from.asc()) \
            .all()

        self.assertEqual(len(sentence_snapshots), 2)
        self.assertEqual(sentence_snapshots[0].valid_from, date_imposed)
        self.assertEqual(sentence_snapshots[0].valid_to, completion_date)
        self.assertEqual(sentence_snapshots[0].status,
                         SentenceStatus.PRESENT_WITHOUT_INFO.value)
        self.assertEqual(sentence_snapshots[1].valid_from, completion_date)
        self.assertEqual(sentence_snapshots[1].valid_to, None)
        self.assertEqual(sentence_snapshots[1].status,
                         SentenceStatus.COMPLETED.value)

        assert_session.commit()
        assert_session.close()

    def testWritePerson_startAfterEnd_usesEndToBackdate(self):
        scrape_time = datetime.datetime(year=2020, month=7, day=7)
        date_imposed = datetime.datetime(year=2020, month=7, day=1)
        completion_date = datetime.datetime(year=2020, month=6, day=1)

        act_session = SessionFactory.for_schema_base(JailsBase)
        person = county_schema.Person(
            full_name=_FULL_NAME, birthdate=_BIRTHDATE, region=_REGION,
            jurisdiction_id=_JURISDICTION_ID)
        booking = county_schema.Booking(
            custody_status=CustodyStatus.IN_CUSTODY.value,
            first_seen_time=scrape_time,
            last_seen_time=scrape_time)
        person.bookings = [booking]
        charge = county_schema.Charge(
            status=ChargeStatus.PENDING.value)
        booking.charges = [charge]
        sentence = county_schema.Sentence(
            status=SentenceStatus.PRESENT_WITHOUT_INFO.value,
            completion_date=completion_date,
            date_imposed=date_imposed)
        charge.sentence = sentence
        persisted_person = database.write_person(
            act_session, person, IngestMetadata(
                _REGION, _JURISDICTION_ID, scrape_time, {}))
        act_session.commit()
        person_id = persisted_person.person_id
        booking_id = persisted_person.bookings[0].booking_id
        charge_id = persisted_person.bookings[0].charges[0].charge_id
        sentence_id = \
            persisted_person.bookings[0].charges[0].sentence.sentence_id
        act_session.close()

        assert_session = SessionFactory.for_schema_base(JailsBase)

        person_snapshot = one(assert_session.query(PersonHistory).filter(
            PersonHistory.person_id == person_id).all())
        booking_snapshot = one(assert_session.query(BookingHistory).filter(
            BookingHistory.booking_id == booking_id).all())
        charge_snapshot = one(assert_session.query(ChargeHistory).filter(
            ChargeHistory.charge_id == charge_id).all())
        sentence_snapshot = one(assert_session.query(SentenceHistory).filter(
            SentenceHistory.sentence_id == sentence_id).all())

        self.assertEqual(person_snapshot.valid_from, scrape_time)
        self.assertEqual(booking_snapshot.valid_from, scrape_time)
        self.assertEqual(charge_snapshot.valid_from, scrape_time)
        self.assertEqual(sentence_snapshot.valid_from, completion_date)

        assert_session.commit()
        assert_session.close()

    def testWritePerson_admissionDateChanges_doesNotBackdateSnapshot(self):
        initial_scrape_time = datetime.datetime(year=2020, month=6, day=1)
        update_scrape_time = datetime.datetime(year=2020, month=7, day=7)
        booking_admission_date = datetime.datetime(year=2020, month=7, day=1)

        arrange_session = SessionFactory.for_schema_base(JailsBase)
        person = county_schema.Person(
            full_name=_FULL_NAME, birthdate=_BIRTHDATE, region=_REGION,
            jurisdiction_id=_JURISDICTION_ID)
        booking = county_schema.Booking(
            custody_status=CustodyStatus.IN_CUSTODY.value,
            first_seen_time=initial_scrape_time,
            last_seen_time=initial_scrape_time)
        person.bookings = [booking]
        persisted_person = database.write_person(
            arrange_session, person, IngestMetadata(
                _REGION, _JURISDICTION_ID, initial_scrape_time, {}))
        arrange_session.commit()
        booking_id = persisted_person.bookings[0].booking_id
        arrange_session.close()

        act_session = SessionFactory.for_schema_base(JailsBase)
        queried_person = one(county_dao.read_people(
            session=act_session, full_name=_FULL_NAME, birthdate=_BIRTHDATE))
        queried_person.bookings[0].admission_date = booking_admission_date
        queried_person.bookings[0].admission_date_inferred = False
        queried_person.bookings[0].last_seen_time = update_scrape_time

        database.write_people(
            act_session,
            converter.convert_entity_people_to_schema_people([queried_person]),
            IngestMetadata(
                _REGION, _JURISDICTION_ID, update_scrape_time, {}))
        act_session.commit()
        act_session.close()

        assert_session = SessionFactory.for_schema_base(JailsBase)

        booking_snapshots = assert_session.query(BookingHistory).filter(
            BookingHistory.booking_id == booking_id).all()
        self.assertEqual(len(booking_snapshots), 2)
        self.assertEqual(
            booking_snapshots[0].valid_from, initial_scrape_time)
        self.assertEqual(
            booking_snapshots[1].valid_from, update_scrape_time)

        assert_session.commit()
        assert_session.close()

    def testWritePerson_newPersonWithBackdatedBooking_backdatesPerson(self):
        scrape_time = datetime.datetime(year=2020, month=7, day=7)
        booking_admission_date = datetime.datetime(year=2020, month=7, day=1)

        act_session = SessionFactory.for_schema_base(JailsBase)
        person = county_schema.Person(
            full_name=_FULL_NAME, birthdate=_BIRTHDATE, region=_REGION,
            jurisdiction_id=_JURISDICTION_ID)
        booking = county_schema.Booking(
            custody_status=CustodyStatus.IN_CUSTODY.value,
            admission_date=booking_admission_date,
            admission_date_inferred=False,
            first_seen_time=scrape_time,
            last_seen_time=scrape_time)
        person.bookings = [booking]
        persisted_person = database.write_person(
            act_session, person, IngestMetadata(
                _REGION, _JURISDICTION_ID, scrape_time, {}))
        act_session.commit()
        person_id = persisted_person.person_id
        booking_id = persisted_person.bookings[0].booking_id
        act_session.close()

        assert_session = SessionFactory.for_schema_base(JailsBase)

        person_snapshot = one(assert_session.query(PersonHistory).filter(
            PersonHistory.person_id == person_id).all())
        booking_snapshot = one(assert_session.query(BookingHistory).filter(
            BookingHistory.booking_id == booking_id).all())

        self.assertEqual(person_snapshot.valid_from, booking_admission_date)
        self.assertEqual(booking_snapshot.valid_from, booking_admission_date)

        assert_session.commit()
        assert_session.close()

    def testWritePerson_backdatedBooking_backdatesChildEntities(self):
        scrape_time = datetime.datetime(year=2020, month=7, day=7)
        booking_admission_date = datetime.datetime(year=2020, month=7, day=1)

        act_session = SessionFactory.for_schema_base(JailsBase)
        person = county_schema.Person(
            full_name=_FULL_NAME, birthdate=_BIRTHDATE, region=_REGION,
            jurisdiction_id=_JURISDICTION_ID)
        booking = county_schema.Booking(
            custody_status=CustodyStatus.IN_CUSTODY.value,
            admission_date=booking_admission_date,
            admission_date_inferred=False,
            first_seen_time=scrape_time,
            last_seen_time=scrape_time)
        person.bookings = [booking]
        charge = county_schema.Charge(
            status=ChargeStatus.PENDING.value)
        booking.charges = [charge]
        bond = county_schema.Bond(
            status=BondStatus.PRESENT_WITHOUT_INFO.value)
        charge.bond = bond
        persisted_person = database.write_person(
            act_session, person, IngestMetadata(
                _REGION, _JURISDICTION_ID, scrape_time, {}))
        act_session.commit()
        charge_id = persisted_person.bookings[0].charges[0].charge_id
        bond_id = persisted_person.bookings[0].charges[0].bond.bond_id
        act_session.close()

        assert_session = SessionFactory.for_schema_base(JailsBase)

        charge_snapshot = one(assert_session.query(ChargeHistory).filter(
            ChargeHistory.charge_id == charge_id).all())
        bond_snapshot = one(assert_session.query(BondHistory).filter(
            BondHistory.bond_id == bond_id).all())

        self.assertEqual(charge_snapshot.valid_from, booking_admission_date)
        self.assertEqual(bond_snapshot.valid_from, booking_admission_date)

        assert_session.commit()
        assert_session.close()

    def testWritePerson_backdatedExistingBooking_doesNotBackdateChildren(self):
        initial_scrape_time = datetime.datetime(year=2020, month=7, day=1)
        update_scrape_time = datetime.datetime(year=2020, month=7, day=7)
        booking_admission_date = datetime.datetime(year=2020, month=6, day=1)

        arrange_session = SessionFactory.for_schema_base(JailsBase)
        person = county_schema.Person(
            full_name=_FULL_NAME, birthdate=_BIRTHDATE, region=_REGION,
            jurisdiction_id=_JURISDICTION_ID)
        booking = county_schema.Booking(
            custody_status=CustodyStatus.IN_CUSTODY.value,
            admission_date=booking_admission_date,
            admission_date_inferred=False,
            first_seen_time=initial_scrape_time,
            last_seen_time=initial_scrape_time)
        person.bookings = [booking]
        persisted_person = database.write_person(
            arrange_session, person, IngestMetadata(
                _REGION, _JURISDICTION_ID, initial_scrape_time, {}))
        arrange_session.commit()
        person_id = persisted_person.person_id
        booking_id = persisted_person.bookings[0].booking_id
        arrange_session.close()

        act_session = SessionFactory.for_schema_base(JailsBase)
        queried_person = one(county_dao.read_people(
            session=act_session, full_name=_FULL_NAME, birthdate=_BIRTHDATE))
        charge = county_entities.Charge.new_with_defaults(
            status=ChargeStatus.PENDING)
        queried_person.bookings[0].charges = [charge]
        bond = county_entities.Bond.new_with_defaults(
            status=BondStatus.PRESENT_WITHOUT_INFO)
        charge.bond = bond
        updated_person = database.write_person(
            act_session,
            converter.convert_entity_people_to_schema_people(
                [queried_person])[0],
            IngestMetadata(
                _REGION, _JURISDICTION_ID, update_scrape_time, {}))
        act_session.commit()
        charge_id = updated_person.bookings[0].charges[0].charge_id
        bond_id = updated_person.bookings[0].charges[0].bond.bond_id
        act_session.close()

        assert_session = SessionFactory.for_schema_base(JailsBase)

        person_snapshot = one(assert_session.query(PersonHistory).filter(
            PersonHistory.person_id == person_id).all())
        booking_snapshot = one(assert_session.query(BookingHistory).filter(
            BookingHistory.booking_id == booking_id).all())
        charge_snapshot = one(assert_session.query(ChargeHistory).filter(
            ChargeHistory.charge_id == charge_id).all())
        bond_snapshot = one(assert_session.query(BondHistory).filter(
            BondHistory.bond_id == bond_id).all())

        self.assertEqual(person_snapshot.valid_from, booking_admission_date)
        self.assertEqual(booking_snapshot.valid_from, booking_admission_date)
        # Uses scrape time rather than backdating to admission date
        self.assertEqual(charge_snapshot.valid_from, update_scrape_time)
        self.assertEqual(bond_snapshot.valid_from, update_scrape_time)

        assert_session.commit()
        assert_session.close()

    def testWritePerson_backdatedBookingDescendant_usesProvidedStartTime(self):
        scrape_time = datetime.datetime(year=2020, month=7, day=7)
        booking_admission_date = datetime.datetime(year=2020, month=7, day=1)
        sentence_date_imposed = datetime.datetime(year=2020, month=7, day=3)

        act_session = SessionFactory.for_schema_base(JailsBase)
        person = county_schema.Person(
            full_name=_FULL_NAME, birthdate=_BIRTHDATE, region=_REGION,
            jurisdiction_id=_JURISDICTION_ID)
        booking = county_schema.Booking(
            custody_status=CustodyStatus.IN_CUSTODY.value,
            admission_date=booking_admission_date,
            admission_date_inferred=False,
            first_seen_time=scrape_time,
            last_seen_time=scrape_time)
        person.bookings = [booking]
        charge = county_schema.Charge(
            status=ChargeStatus.PENDING.value)
        booking.charges = [charge]
        sentence = county_schema.Sentence(
            status=SentenceStatus.PRESENT_WITHOUT_INFO.value,
            date_imposed=sentence_date_imposed)
        charge.sentence = sentence
        persisted_person = database.write_person(
            act_session, person, IngestMetadata(
                _REGION, _JURISDICTION_ID, scrape_time, {}))
        act_session.commit()
        booking_id = persisted_person.bookings[0].booking_id
        sentence_id = \
            persisted_person.bookings[0].charges[0].sentence.sentence_id
        act_session.close()

        assert_session = SessionFactory.for_schema_base(JailsBase)

        booking_snapshot = one(assert_session.query(BookingHistory).filter(
            BookingHistory.booking_id == booking_id).all())
        sentence_snapshot = one(assert_session.query(SentenceHistory).filter(
            SentenceHistory.sentence_id == sentence_id).all())

        self.assertEqual(booking_snapshot.valid_from, booking_admission_date)
        self.assertEqual(sentence_snapshot.valid_from, sentence_date_imposed)

        assert_session.commit()
        assert_session.close()

    def test_removeBondFromCharge_shouldNotOrphanOldBond(self):
        arrange_session = SessionFactory.for_schema_base(JailsBase)

        person = county_schema.Person(
            region=_REGION, race=Race.OTHER.value,
            jurisdiction_id=_JURISDICTION_ID)
        booking = county_schema.Booking(
            custody_status=CustodyStatus.IN_CUSTODY.value,
            first_seen_time=_INGEST_TIME,
            last_seen_time=_INGEST_TIME)
        person.bookings = [booking]
        charge = county_schema.Charge(
            status=ChargeStatus.PENDING.value)
        booking.charges = [charge]
        bond = county_schema.Bond(
            status=BondStatus.PRESENT_WITHOUT_INFO.value)
        charge.bond = bond

        persisted_person = database.write_person(
            arrange_session, person,
            IngestMetadata.new_with_defaults(
                region='default_region',
                ingest_time=datetime.datetime(year=2020, month=7, day=6)))
        arrange_session.commit()
        persisted_person_id = persisted_person.person_id
        persisted_booking_id = persisted_person.bookings[0].booking_id
        arrange_session.close()

        act_session = SessionFactory.for_schema_base(JailsBase)
        person_query = act_session.query(Person) \
            .filter(Person.person_id == persisted_person_id)
        fetched_person = person_query.first()
        # Remove bond from charge so bond is no longer directly associated
        # with ORM copy of the record tree
        fetched_charge = fetched_person.bookings[0].charges[0]
        fetched_charge.bond = None
        database.write_person(
            act_session, fetched_person, IngestMetadata.new_with_defaults(
                region='default_region',
                ingest_time=datetime.datetime(
                    year=2020, month=7, day=7)))
        act_session.commit()
        act_session.close()

        assert_session = SessionFactory.for_schema_base(JailsBase)

        # Bond should still be associated with booking, even though it is no
        # longer associated with the charge
        # pylint: disable=W0143
        bonds = assert_session.query(Bond) \
            .filter(Bond.booking_id == persisted_booking_id) \
            .all()
        self.assertEqual(len(bonds), 1)

        assert_session.commit()
        assert_session.close()

    def test_removeSentenceFromCharge_shouldNotOrphanOldSentence(self):
        arrange_session = SessionFactory.for_schema_base(JailsBase)

        person = county_schema.Person(
            region=_REGION, race=Race.OTHER.value,
            jurisdiction_id=_JURISDICTION_ID)
        booking = county_schema.Booking(
            custody_status=CustodyStatus.IN_CUSTODY.value,
            first_seen_time=_INGEST_TIME,
            last_seen_time=_INGEST_TIME)
        person.bookings = [booking]
        charge = county_schema.Charge(
            status=ChargeStatus.PENDING.value)
        booking.charges = [charge]
        sentence = county_schema.Sentence(
            status=SentenceStatus.PRESENT_WITHOUT_INFO.value)
        charge.sentence = sentence

        persisted_person = database.write_person(
            arrange_session, person, IngestMetadata.new_with_defaults(
                region='default_region',
                ingest_time=datetime.datetime(year=2020, month=7, day=6)))
        arrange_session.commit()
        persisted_person_id = persisted_person.person_id
        persisted_booking_id = persisted_person.bookings[0].booking_id
        arrange_session.close()

        act_session = SessionFactory.for_schema_base(JailsBase)
        person_query = act_session.query(Person) \
            .filter(Person.person_id == persisted_person_id)
        fetched_person = person_query.first()
        # Remove sentence from charge so sentence is no longer directly
        # associated with ORM copy of the record tree
        fetched_charge = fetched_person.bookings[0].charges[0]
        fetched_charge.sentence = None
        database.write_person(
            act_session, fetched_person, IngestMetadata.new_with_defaults(
                region='default_region',
                ingest_time=datetime.datetime(year=2020, month=7, day=7)))
        act_session.commit()
        act_session.close()

        assert_session = SessionFactory.for_schema_base(JailsBase)

        # Sentence should still be associated with booking, even though it is no
        # longer associated with the charge
        # pylint: disable=W0143
        sentences = assert_session.query(Sentence) \
            .filter(Sentence.booking_id == persisted_booking_id) \
            .all()
        self.assertEqual(len(sentences), 1)

        assert_session.commit()
        assert_session.close()

    def test_orphanedEntities_shouldStillWriteSnapshots(self):
        orphan_scrape_time = datetime.datetime(year=2020, month=7, day=8)

        arrange_session = SessionFactory.for_schema_base(JailsBase)

        person = county_schema.Person(
            region=_REGION, jurisdiction_id=_JURISDICTION_ID)
        booking = county_schema.Booking(
            custody_status=CustodyStatus.IN_CUSTODY.value,
            first_seen_time=_INGEST_TIME,
            last_seen_time=_INGEST_TIME)
        person.bookings = [booking]
        charge = county_schema.Charge(
            status=ChargeStatus.PENDING.value)
        booking.charges = [charge]
        sentence = county_schema.Sentence(
            status=SentenceStatus.PRESENT_WITHOUT_INFO.value)
        charge.sentence = sentence

        persisted_person = database.write_person(
            arrange_session, person, IngestMetadata.new_with_defaults(
                region='default_region',
                ingest_time=datetime.datetime(year=2020, month=7, day=6)))
        arrange_session.commit()
        persisted_person_id = persisted_person.person_id
        arrange_session.close()

        act_session = SessionFactory.for_schema_base(JailsBase)
        person_query = act_session.query(Person) \
            .filter(Person.person_id == persisted_person_id)
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
            IngestMetadata.new_with_defaults(
                region='default_region',
                ingest_time=orphan_scrape_time),
            orphaned_entities=[fetched_sentence])
        act_session.commit()
        fetched_sentence_id = fetched_sentence.sentence_id
        act_session.close()

        assert_session = SessionFactory.for_schema_base(JailsBase)

        sentence_snapshot = assert_session.query(SentenceHistory) \
            .filter(SentenceHistory.sentence_id == fetched_sentence_id) \
            .order_by(SentenceHistory.valid_from.desc()) \
            .first()

        self.assertEqual(sentence_snapshot.valid_from, orphan_scrape_time)
        self.assertEqual(
            sentence_snapshot.status,
            SentenceStatus.REMOVED_WITHOUT_INFO.value)

        assert_session.commit()
        assert_session.close()

    def test_addBondToExistingBooking_shouldSetBookingIdOnBond(self):
        arrange_session = SessionFactory.for_schema_base(JailsBase)

        person = county_schema.Person(
            region=_REGION, race=Race.OTHER.value,
            jurisdiction_id=_JURISDICTION_ID)
        booking = county_schema.Booking(
            custody_status=CustodyStatus.IN_CUSTODY.value,
            first_seen_time=_INGEST_TIME,
            last_seen_time=_INGEST_TIME)
        person.bookings = [booking]
        charge = county_schema.Charge(
            status=ChargeStatus.PENDING.value)
        booking.charges = [charge]

        persisted_person = database.write_person(
            arrange_session, person, IngestMetadata.new_with_defaults(
                region='default_region',
                ingest_time=datetime.datetime(year=2020, month=7, day=6)))
        arrange_session.commit()
        persisted_person_id = persisted_person.person_id
        persisted_booking_id = persisted_person.bookings[0].booking_id
        arrange_session.close()

        act_session = SessionFactory.for_schema_base(JailsBase)
        person_query = act_session.query(Person) \
            .filter(Person.person_id == persisted_person_id)
        fetched_person = person_query.first()

        new_bond = county_schema.Bond(
            status=BondStatus.PRESENT_WITHOUT_INFO.value)
        fetched_person.bookings[0].charges[0].bond = new_bond
        database.write_person(
            act_session, fetched_person, IngestMetadata.new_with_defaults(
                region='default_region',
                ingest_time=datetime.datetime(year=2020, month=7, day=7)))
        act_session.commit()
        act_session.close()

        assert_session = SessionFactory.for_schema_base(JailsBase)

        assert_person_query = assert_session.query(Person) \
            .filter(Person.person_id == persisted_person_id)
        final_fetched_person = converter.convert_schema_object_to_entity(
            assert_person_query.first())
        self.assertEqual(
            final_fetched_person.bookings[0].charges[0].bond.booking_id,
            persisted_booking_id)

        assert_session.commit()
        assert_session.close()


class TestDatabaseAddStateCode(TestCase):
    """Test that state_code has successfully been updated in association tables in state schema"""

    @classmethod
    def setUpClass(cls) -> None:
        fakes.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        fakes.use_on_disk_postgresql_database(StateBase)

    def tearDown(self) -> None:
        fakes.teardown_on_disk_postgresql_database(StateBase)

    @classmethod
    def tearDownClass(cls) -> None:
        fakes.stop_and_clear_on_disk_postgresql_database()

    def testAddStateCodeAllAssociationTables(self):
        # Arrange 1 - Match
        db_person = generate_person(person_id=_ID, full_name=_FULL_NAME, state_code=_STATE_CODE)
        db_bond = generate_bond(
            person=db_person,
            bond_id=_ID, state_code=_STATE_CODE, external_id=_EXTERNAL_ID_1,
            status=BondStatus.PRESENT_WITHOUT_INFO.value, bond_agent='agent')
        db_court_case = generate_court_case(
            person=db_person,
            court_case_id=_ID, external_id=_EXTERNAL_ID_1, state_code=_STATE_CODE,
            county_code='county_code')
        db_charge_1 = generate_charge(
            person=db_person,
            charge_id=_ID, state_code=_STATE_CODE, external_id=_EXTERNAL_ID_1,
            description='charge_1',
            status=ChargeStatus.PRESENT_WITHOUT_INFO.value,
            bond=db_bond, court_case=db_court_case)
        db_charge_2 = generate_charge(
            person=db_person,
            charge_id=_ID_2, state_code=_STATE_CODE, external_id=_EXTERNAL_ID_2,
            description='charge_2',
            status=ChargeStatus.PRESENT_WITHOUT_INFO.value,
            bond=db_bond, court_case=db_court_case)
        db_charge_3 = generate_charge(
            person=db_person,
            charge_id=_ID_3, state_code=_STATE_CODE, external_id=_EXTERNAL_ID_3,
            description='charge_3',
            status=ChargeStatus.PRESENT_WITHOUT_INFO.value)
        db_fine = generate_fine(
            person=db_person,
            fine_id=_ID, state_code=_STATE_CODE, external_id=_EXTERNAL_ID_1,
            county_code='county_code', charges=[db_charge_1, db_charge_2])
        db_assessment = generate_assessment(
            person=db_person,
            assessment_id=_ID, state_code=_STATE_CODE, external_id=_EXTERNAL_ID_1,
            assessment_metadata='metadata')
        db_assessment_2 = generate_assessment(
            person=db_person,
            assessment_id=_ID_2, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2, assessment_metadata='metadata_2')
        db_agent = generate_agent(
            agent_id=_ID, state_code=_STATE_CODE, external_id=_EXTERNAL_ID_1,
            full_name='full_name')
        db_agent_2 = generate_agent(
            agent_id=_ID_2, state_code=_STATE_CODE, external_id=_EXTERNAL_ID_2,
            full_name='full_name_2')
        db_agent_po = generate_agent(
            agent_id=_ID_5, state_code=_STATE_CODE, external_id=_EXTERNAL_ID_5,
            full_name='full_name_po')
        db_agent_term = generate_agent(
            agent_id=_ID_6, state_code=_STATE_CODE, external_id=_EXTERNAL_ID_6,
            full_name='full_name_term')
        db_program_assignment = state_schema.StateProgramAssignment(program_assignment_id=1,
                                                                    referring_agent=db_agent,
                                                                    state_code=_STATE_CODE,
                                                                    external_id=_EXTERNAL_ID_1)
        db_incarceration_incident = \
            generate_incarceration_incident(
                person=db_person,
                incarceration_incident_id=_ID, state_code=_STATE_CODE,
                external_id=_EXTERNAL_ID_1, incident_details='details',
                responding_officer=db_agent)
        db_parole_decision = generate_parole_decision(
            person=db_person,
            parole_decision_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_1,
            decision_outcome=StateParoleDecisionOutcome.EXTERNAL_UNKNOWN.value,
            decision_agents=[db_agent_2])
        db_parole_decision_2 = generate_parole_decision(
            person=db_person,
            parole_decision_id=_ID_2, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_2,
            decision_outcome=StateParoleDecisionOutcome.EXTERNAL_UNKNOWN.value,
            decision_agents=[db_agent_2])
        db_incarceration_period = generate_incarceration_period(
            person=db_person,
            incarceration_period_id=_ID, external_id=_EXTERNAL_ID_1,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY.value,
            state_code=_STATE_CODE, facility='facility',
            incarceration_incidents=[db_incarceration_incident],
            parole_decisions=[db_parole_decision, db_parole_decision_2],
            assessments=[db_assessment],
            program_assignments=[db_program_assignment])
        db_supervision_violation_response_decision = \
            generate_supervision_violation_response_decision_entry(
                person=db_person,
                supervision_violation_response_decision_entry_id=_ID,
                decision=StateSupervisionViolationResponseDecision.REVOCATION.value,
                revocation_type=
                StateSupervisionViolationResponseRevocationType.TREATMENT_IN_PRISON.value
            )
        db_supervision_violation_response = \
            generate_supervision_violation_response(
                person=db_person,
                supervision_violation_response_id=_ID, state_code=_STATE_CODE,
                external_id=_EXTERNAL_ID_1,
                decision=
                StateSupervisionViolationResponseDecision.CONTINUANCE.value,
                supervision_violation_response_decisions=[
                    db_supervision_violation_response_decision],
                decision_agents=[db_agent_term])
        db_supervision_violation_type = \
            generate_supervision_violation_type_entry(
                person=db_person,
                supervision_violation_type_entry_id=_ID,
                violation_type=StateSupervisionViolationType.ABSCONDED.value,
            )
        db_supervision_violated_condition = \
            generate_supervision_violated_condition_entry(
                person=db_person,
                supervision_violated_condition_entry_id=_ID,
                condition='COND'
            )
        db_supervision_violation = generate_supervision_violation(
            person=db_person,
            supervision_violation_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_1, is_violent=True,
            supervision_violation_types=[db_supervision_violation_type],
            supervision_violated_conditions=[
                db_supervision_violated_condition],
            supervision_violation_responses=[db_supervision_violation_response])
        db_case_type_dv = generate_supervision_case_type_entry(
            person=db_person,
            supervision_case_type_entry_id=_ID,
            state_code=_STATE_CODE,
            case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE.value,
            case_type_raw_text='DV')
        db_supervision_contact = state_schema.StateSupervisionContact(supervision_contact_id=1,
                                                                      person=db_person, contacted_agent=db_agent,
                                                                      state_code=_STATE_CODE)
        db_supervision_period = generate_supervision_period(
            person=db_person,
            supervision_period_id=_ID, external_id=_EXTERNAL_ID_1,
            status=StateSupervisionPeriodStatus.EXTERNAL_UNKNOWN.value,
            state_code=_STATE_CODE, county_code='county_code',
            assessments=[db_assessment_2],
            supervision_violation_entries=[db_supervision_violation],
            case_type_entries=[db_case_type_dv],
            supervising_officer=db_agent_po,
            program_assignments=[db_program_assignment],
            supervision_contacts=[db_supervision_contact])
        db_supervision_sentence = generate_supervision_sentence(
            person=db_person,
            supervision_sentence_id=_ID, status=SentenceStatus.SERVING.value,
            external_id=_EXTERNAL_ID_1,
            state_code=_STATE_CODE,
            min_length_days=0,
            supervision_periods=[db_supervision_period],
            incarceration_periods=[db_incarceration_period],
            charges=[db_charge_2, db_charge_3])
        db_incarceration_sentence = \
            generate_incarceration_sentence(
                person=db_person,
                incarceration_sentence_id=_ID,
                status=StateSentenceStatus.SERVING.value,
                external_id=_EXTERNAL_ID_1, state_code=_STATE_CODE,
                county_code='county_code', charges=[db_charge_2, db_charge_3],
                incarceration_periods=[db_incarceration_period],
                supervision_periods=[db_supervision_period])
        db_sentence_group = generate_sentence_group(
            sentence_group_id=_ID,
            status=StateSentenceStatus.EXTERNAL_UNKNOWN.value,
            external_id=_EXTERNAL_ID_1, state_code=_STATE_CODE,
            county_code='county_code',
            supervision_sentences=[db_supervision_sentence],
            incarceration_sentences=[db_incarceration_sentence],
            fines=[db_fine])
        db_external_id = generate_external_id(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            external_id=_EXTERNAL_ID_1)
        db_person.program_assignments = [db_program_assignment]
        db_person.external_ids = [db_external_id]
        db_person.sentence_groups = [db_sentence_group]

        session = SessionFactory.for_schema_base(StateBase)
        session.add(db_person)
        session.commit()

        association_tables = [table for table in reversed(StateBase.metadata.sorted_tables) if
                              table.name.endswith(ASSOCIATION_TABLE_NAME_SUFFIX)]
        for table in association_tables:
            associations = session.query(table).all()
            for association in associations:
                self.assertIsNone(association.state_code)

        _hydrate_state_codes_in_association_tables(session)

        for table in association_tables:
            associations = session.query(table).all()
            for association in associations:
                self.assertEqual(association.state_code, _STATE_CODE)

        session.close()
