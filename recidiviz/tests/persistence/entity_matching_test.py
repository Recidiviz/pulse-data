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
"""Tests for entity_matching.py."""
from datetime import datetime
from unittest import TestCase

import attr

from recidiviz import Session
from recidiviz.common.constants.bond import BondStatus
from recidiviz.common.constants.booking import CustodyStatus
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.hold import HoldStatus
from recidiviz.persistence.database import schema, database_utils
from recidiviz.persistence import entities, entity_matching
from recidiviz.persistence.errors import EntityMatchingError
from recidiviz.tests.utils import fakes

_ID = 1
_ID_ANOTHER = 2
_PERSON_ID = 2
_PERSON_ID_ANOTHER = 3
_BOOKING_ID = 12
_BOOKING_ID_ANOTHER = 13
_CHARGE_ID = 112
_CHARGE_ID_ANOTHER = 113
_BOND_ID = 212
_BOND_ID_ANOTHER = 213
_SENTENCE_ID = 312
_SENTENCE_ID_ANOTHER = 313
_REGION = 'region'
_EXTERNAL_ID = 'external_id'
_EXTERNAL_ID_ANOTHER = 'external_id_another'
_FACILITY = 'facility'
_FULL_NAME = 'full_name'
_PLACE_1 = 'place'
_PLACE_2 = 'another'
_DATE = datetime(2018, 12, 13)
_DATE_2 = datetime(2019, 12, 13)
_DATE_3 = datetime(2020, 12, 13)
_NAME = 'name_1'
_NAME_2 = 'name_2'
_NAME_3 = 'name_3'
_JURISDICTION_ID = 'jurisdiction_id'


class TestEntityMatching(TestCase):
    """Tests for entity matching logic"""

    def setup_method(self, _test_method):
        fakes.use_in_memory_sqlite_database()

    def test_matchPeople_errorCount(self):
        # Arrange
        schema_booking = schema.Booking(
            external_id=_EXTERNAL_ID, admission_date=_DATE_2,
            booking_id=_BOOKING_ID,
            custody_status=CustodyStatus.IN_CUSTODY.value, last_seen_time=_DATE)

        schema_person = schema.Person(
            person_id=_PERSON_ID, external_id=_EXTERNAL_ID,
            jurisdiction_id=_JURISDICTION_ID,
            full_name=_FULL_NAME, birthdate=_DATE,
            region=_REGION, bookings=[schema_booking])

        session = Session()
        session.add(schema_person)
        session.commit()

        ingested_booking = attr.evolve(
            database_utils.convert(schema_booking), booking_id=None,
            custody_status=CustodyStatus.RELEASED)

        ingested_person = attr.evolve(
            database_utils.convert(schema_person), person_id=None,
            bookings=[ingested_booking])
        ingested_person_another = attr.evolve(ingested_person)

        # Act
        error_count, orphaned_entities = entity_matching.match(
            session, _REGION, [ingested_person, ingested_person_another])

        # Assert
        # TODO(1282): Update test here to check that ingested_person is
        #  updated and ingested_person_another is not.
        self.assertEqual(error_count, 1)
        self.assertEqual(len(orphaned_entities), 0)

    def test_matchPerson_updateStatusOnOrphanedEntities(self):
        # Arrange
        schema_bond = schema.Bond(
            bond_id=_BOND_ID, status=BondStatus.PENDING.value,
            booking_id=_BOOKING_ID)
        schema_charge = schema.Charge(
            charge_id=_CHARGE_ID, status=ChargeStatus.PENDING.value,
            bond=schema_bond)
        schema_booking = schema.Booking(
            admission_date=_DATE_2, booking_id=_BOOKING_ID,
            custody_status=CustodyStatus.IN_CUSTODY.value, last_seen_time=_DATE,
            charges=[schema_charge])

        schema_person = schema.Person(
            person_id=_PERSON_ID, full_name=_FULL_NAME, birthdate=_DATE,
            jurisdiction_id=_JURISDICTION_ID, region=_REGION,
            bookings=[schema_booking])

        session = Session()
        session.add(schema_person)
        session.commit()

        ingested_charge_no_bond = attr.evolve(
            database_utils.convert(schema_charge), charge_id=None,
            bond=None)
        ingested_booking = attr.evolve(
            database_utils.convert(schema_booking), booking_id=None,
            custody_status=CustodyStatus.RELEASED,
            charges=[ingested_charge_no_bond])
        ingested_person = attr.evolve(
            database_utils.convert(schema_person), person_id=None,
            bookings=[ingested_booking])

        # Act
        expected_orphaned_bond = attr.evolve(
            database_utils.convert(schema_bond),
            status=BondStatus.REMOVED_WITHOUT_INFO)
        expected_charge = attr.evolve(
            ingested_charge_no_bond, charge_id=schema_charge.charge_id)
        expected_booking = attr.evolve(
            ingested_booking, booking_id=schema_booking.booking_id,
            charges=[expected_charge])
        expected_person = attr.evolve(
            ingested_person, person_id=schema_person.person_id,
            bookings=[expected_booking])

        error_count, orphaned_entities = \
            entity_matching.match(session, _REGION, [ingested_person])

        # Assert
        self.assertEqual(error_count, 0)
        self.assertEqual(ingested_person, expected_person)
        self.assertCountEqual(orphaned_entities, [expected_orphaned_bond])

    def test_matchPeople_differentBookingIds(self):
        schema_booking = schema.Booking(
            external_id=_EXTERNAL_ID, admission_date=_DATE,
            booking_id=_BOOKING_ID,
            custody_status=CustodyStatus.IN_CUSTODY.value, last_seen_time=_DATE)

        schema_person = schema.Person(person_id=_PERSON_ID,
                                      full_name=_FULL_NAME,
                                      jurisdiction_id=_JURISDICTION_ID,
                                      region=_REGION, bookings=[schema_booking])

        schema_booking_another = schema.Booking(
            external_id=_EXTERNAL_ID_ANOTHER, admission_date=_DATE,
            booking_id=_BOOKING_ID_ANOTHER,
            custody_status=CustodyStatus.IN_CUSTODY.value, last_seen_time=_DATE)

        schema_person_another = schema.Person(person_id=_PERSON_ID_ANOTHER,
                                              full_name=_FULL_NAME,
                                              jurisdiction_id=_JURISDICTION_ID,
                                              region=_REGION,
                                              bookings=[schema_booking_another])

        session = Session()
        session.add(schema_person)
        session.add(schema_person_another)
        session.commit()

        ingested_booking = attr.evolve(
            database_utils.convert(schema_booking), booking_id=None)

        ingested_person = attr.evolve(
            database_utils.convert(schema_person), person_id=None,
            bookings=[ingested_booking])

        ingested_booking_another = attr.evolve(
            database_utils.convert(schema_booking_another),
            booking_id=None)
        ingested_person_another = attr.evolve(
            database_utils.convert(schema_person_another),
            person_id=None,
            bookings=[ingested_booking_another])

        expected_booking = attr.evolve(ingested_booking, booking_id=_BOOKING_ID)
        expected_person = attr.evolve(ingested_person, person_id=_PERSON_ID,
                                      bookings=[expected_booking])
        expected_booking_another = attr.evolve(ingested_booking_another,
                                               booking_id=_BOOKING_ID_ANOTHER)
        expected_person_another = attr.evolve(ingested_person_another,
                                              person_id=_PERSON_ID_ANOTHER,
                                              bookings=[
                                                  expected_booking_another])

        ingested_people = [ingested_person, ingested_person_another]
        error_count, orphaned_entities = entity_matching.match(
            session, _REGION, ingested_people)
        self.assertFalse(error_count)
        self.assertCountEqual(ingested_people,
                              [expected_person, expected_person_another])
        self.assertEqual(len(orphaned_entities), 0)

    def test_matchPeople(self):
        schema_booking = schema.Booking(
            admission_date=_DATE_2, booking_id=_BOOKING_ID,
            custody_status=CustodyStatus.IN_CUSTODY.value, last_seen_time=_DATE)

        schema_person = schema.Person(
            person_id=_PERSON_ID, full_name=_FULL_NAME, birthdate=_DATE,
            jurisdiction_id=_JURISDICTION_ID, region=_REGION,
            bookings=[schema_booking])

        schema_booking_external_id = schema.Booking(
            admission_date=_DATE_2, booking_id=_BOOKING_ID_ANOTHER,
            release_date=_DATE, custody_status=CustodyStatus.RELEASED.value,
            last_seen_time=_DATE)

        schema_person_external_id = schema.Person(
            person_id=_PERSON_ID_ANOTHER, external_id=_EXTERNAL_ID,
            full_name=_FULL_NAME, birthdate=_DATE,
            jurisdiction_id=_JURISDICTION_ID, region=_REGION,
            bookings=[schema_booking_external_id])

        session = Session()
        session.add(schema_person)
        session.add(schema_person_external_id)
        session.commit()

        ingested_booking = attr.evolve(
            database_utils.convert(schema_booking), booking_id=None,
            custody_status=CustodyStatus.RELEASED)
        ingested_person = attr.evolve(
            database_utils.convert(schema_person), person_id=None,
            bookings=[ingested_booking])

        ingested_booking_external_id = attr.evolve(
            database_utils.convert(schema_booking_external_id),
            booking_id=None, facility=_FACILITY)
        ingested_person_external_id = attr.evolve(
            database_utils.convert(schema_person_external_id),
            person_id=None, bookings=[ingested_booking_external_id])

        expected_booking = attr.evolve(ingested_booking, booking_id=_BOOKING_ID)
        expected_person = attr.evolve(
            ingested_person, person_id=_PERSON_ID, bookings=[expected_booking])

        expected_booking_external_id = attr.evolve(
            ingested_booking_external_id, booking_id=_BOOKING_ID_ANOTHER)
        expected_person_external_id = attr.evolve(
            ingested_person_external_id,
            person_id=_PERSON_ID_ANOTHER,
            bookings=[expected_booking_external_id])

        ingested_people = [ingested_person, ingested_person_external_id]
        error_count, orphaned_entities = entity_matching.match(
            session, _REGION, ingested_people)
        self.assertFalse(error_count)
        self.assertCountEqual(ingested_people,
                              [expected_person, expected_person_external_id])
        self.assertEqual(len(orphaned_entities), 0)

    def test_matchBooking_duplicateMatch_throws(self):
        db_booking = entities.Booking.new_with_defaults(
            booking_id=_ID, admission_date=_DATE, admission_date_inferred=True,
            custody_status=CustodyStatus.IN_CUSTODY)
        ingested_booking_open = entities.Booking.new_with_defaults(
            admission_date=_DATE, admission_date_inferred=True,
            custody_status=CustodyStatus.HELD_ELSEWHERE)
        ingested_booking_open_another = entities.Booking.new_with_defaults(
            admission_date=_DATE_2, admission_date_inferred=True,
            custody_status=CustodyStatus.HELD_ELSEWHERE)

        db_person = entities.Person.new_with_defaults(bookings=[db_booking])
        orphaned_entities = []
        ingested_person = entities.Person.new_with_defaults(
            bookings=[ingested_booking_open, ingested_booking_open_another])

        with self.assertRaises(EntityMatchingError):
            entity_matching.match_bookings(db_person=db_person,
                                           ingested_person=ingested_person,
                                           orphaned_entities=orphaned_entities)

    def test_matchBooking_withInferredDate(self):
        db_booking = entities.Booking.new_with_defaults(
            booking_id=_ID, admission_date=_DATE, admission_date_inferred=True,
            custody_status=CustodyStatus.IN_CUSTODY)
        ingested_booking = entities.Booking.new_with_defaults(
            admission_date=_DATE_2, admission_date_inferred=True,
            custody_status=CustodyStatus.HELD_ELSEWHERE)

        expected_booking = attr.evolve(
            ingested_booking, booking_id=db_booking.booking_id,
            admission_date=_DATE)

        db_person = entities.Person.new_with_defaults(bookings=[db_booking])
        ingested_person = entities.Person.new_with_defaults(
            bookings=[ingested_booking])
        orphaned_entities = []
        entity_matching.match_bookings(
            db_person=db_person, ingested_person=ingested_person,
            orphaned_entities=orphaned_entities)

        self.assertCountEqual(ingested_person.bookings, [expected_booking])
        self.assertEqual(len(orphaned_entities), 0)

    def test_matchBooking(self):
        db_booking = entities.Booking.new_with_defaults(
            booking_id=_ID, admission_date=_DATE_2,
            custody_status=CustodyStatus.IN_CUSTODY)
        db_booking_closed = entities.Booking.new_with_defaults(
            booking_id=_ID_ANOTHER, admission_date=_DATE,
            custody_status=CustodyStatus.RELEASED, release_date=_DATE_2)
        ingested_booking_closed = entities.Booking.new_with_defaults(
            admission_date=_DATE_2, release_date=_DATE_3,
            custody_status=CustodyStatus.RELEASED)
        ingested_booking_open = entities.Booking.new_with_defaults(
            admission_date=_DATE_3, custody_status=CustodyStatus.IN_CUSTODY)

        expected_unchanged_closed_booking = attr.evolve(db_booking_closed)
        expected_new_closed_booking = attr.evolve(
            ingested_booking_closed,
            booking_id=db_booking.booking_id)
        expected_new_open_booking = attr.evolve(ingested_booking_open)

        db_person = entities.Person.new_with_defaults(
            bookings=[db_booking, db_booking_closed])
        ingested_person = entities.Person.new_with_defaults(
            bookings=[ingested_booking_closed, ingested_booking_open])

        orphaned_entities = []
        entity_matching.match_bookings(
            db_person=db_person, ingested_person=ingested_person,
            orphaned_entities=orphaned_entities)

        self.assertCountEqual(ingested_person.bookings,
                              [expected_unchanged_closed_booking,
                               expected_new_closed_booking,
                               expected_new_open_booking])
        self.assertEqual(len(orphaned_entities), 0)

    def test_matchBookingWithChildren(self):
        db_arrest = entities.Arrest.new_with_defaults(
            arrest_id=_ID, external_id=_EXTERNAL_ID, agency=_NAME)
        db_hold = entities.Hold.new_with_defaults(
            hold_id=_ID, external_id=_EXTERNAL_ID, jurisdiction_name=_NAME)
        db_sentence = entities.Sentence.new_with_defaults(
            sentence_id=_ID, external_id=_EXTERNAL_ID, sentencing_region=_NAME)
        db_bond = entities.Bond.new_with_defaults(
            bond_id=_ID, external_id=_EXTERNAL_ID, status=BondStatus.SET)
        db_charge = entities.Charge.new_with_defaults(
            charge_id=_ID, external_id=_EXTERNAL_ID, name=_NAME,
            sentence=db_sentence, bond=db_bond)
        db_booking = entities.Booking.new_with_defaults(
            booking_id=_ID, external_id=_EXTERNAL_ID, admission_date=_DATE,
            custody_status=CustodyStatus.IN_CUSTODY, arrest=db_arrest,
            holds=[db_hold], charges=[db_charge])
        db_person = entities.Person.new_with_defaults(
            person_id=_ID, external_id=_EXTERNAL_ID, bookings=[db_booking])

        ingested_arrest = entities.Arrest.new_with_defaults(
            external_id=_EXTERNAL_ID, agency=_NAME_2)
        ingested_hold = entities.Hold.new_with_defaults(
            external_id=_EXTERNAL_ID, jurisdiction_name=_NAME_2)
        ingested_sentence = entities.Sentence.new_with_defaults(
            external_id=_EXTERNAL_ID, sentencing_region=_NAME_2)
        ingested_bond = entities.Bond.new_with_defaults(
            external_id=_EXTERNAL_ID, status=BondStatus.POSTED)
        ingested_charge = entities.Charge.new_with_defaults(
            external_id=_EXTERNAL_ID, name=_NAME, sentence=ingested_sentence,
            bond=ingested_bond)
        ingested_booking = entities.Booking.new_with_defaults(
            external_id=_EXTERNAL_ID, admission_date=_DATE,
            custody_status=CustodyStatus.IN_CUSTODY, arrest=ingested_arrest,
            holds=[ingested_hold], charges=[ingested_charge])
        ingested_person = entities.Person.new_with_defaults(
            external_id=_EXTERNAL_ID, bookings=[ingested_booking])

        expected_arrest = attr.evolve(ingested_arrest, arrest_id=_ID)
        expected_hold = attr.evolve(ingested_hold, hold_id=_ID)
        expected_sentence = attr.evolve(ingested_sentence, sentence_id=_ID)
        expected_bond = attr.evolve(ingested_bond, bond_id=_ID)
        expected_charge = attr.evolve(
            ingested_charge, charge_id=_ID, bond=expected_bond,
            sentence=expected_sentence)
        expected_booking = attr.evolve(
            ingested_booking, booking_id=_ID, arrest=expected_arrest,
            holds=[expected_hold], charges=[expected_charge])

        orphaned_entities = []
        entity_matching.match_bookings(
            db_person=db_person, ingested_person=ingested_person,
            orphaned_entities=orphaned_entities)

        self.assertCountEqual(ingested_person.bookings, [expected_booking])
        self.assertEqual(len(orphaned_entities), 0)

    def test_matchHolds_duplicateMatch_throws(self):
        db_hold = entities.Hold.new_with_defaults(
            hold_id=_ID, jurisdiction_name=_NAME)
        ingested_hold = entities.Hold.new_with_defaults(jurisdiction_name=_NAME)
        ingested_hold_another = attr.evolve(ingested_hold)

        db_booking = entities.Booking.new_with_defaults(holds=[db_hold])
        ingested_booking = entities.Booking.new_with_defaults(
            holds=[ingested_hold, ingested_hold_another])

        with self.assertRaises(EntityMatchingError):
            entity_matching.match_holds(
                db_booking=db_booking, ingested_booking=ingested_booking)

    def test_matchHolds(self):
        db_hold = entities.Hold.new_with_defaults(
            hold_id=_ID, jurisdiction_name=_NAME)
        db_hold_to_drop = entities.Hold.new_with_defaults(
            hold_id=_ID_ANOTHER, jurisdiction_name=_NAME_2)
        ingested_hold = entities.Hold.new_with_defaults(jurisdiction_name=_NAME)
        ingested_hold_new = entities.Hold.new_with_defaults(
            jurisdiction_name=_NAME_3)

        db_booking = entities.Booking.new_with_defaults(
            holds=[db_hold, db_hold_to_drop])
        ingested_booking = entities.Booking.new_with_defaults(
            holds=[ingested_hold, ingested_hold_new])

        expected_matched_hold = attr.evolve(
            ingested_hold, hold_id=db_hold.hold_id)
        expected_new_hold = attr.evolve(ingested_hold_new)
        expected_dropped_hold = attr.evolve(
            db_hold_to_drop, status=HoldStatus.INFERRED_DROPPED)

        entity_matching.match_holds(
            db_booking=db_booking, ingested_booking=ingested_booking)

        self.assertCountEqual(
            ingested_booking.holds,
            [expected_matched_hold, expected_dropped_hold, expected_new_hold])

    def test_matchArrests(self):
        db_arrest = entities.Arrest.new_with_defaults(arrest_id=_ID)
        ingested_arrest = entities.Arrest.new_with_defaults()

        expected_arrest = attr.evolve(ingested_arrest,
                                      arrest_id=db_arrest.arrest_id)

        entity_matching.match_arrest(
            db_booking=entities.Booking.new_with_defaults(arrest=db_arrest),
            ingested_booking=entities.Booking.new_with_defaults(
                arrest=ingested_arrest))

        self.assertEqual(ingested_arrest, expected_arrest)

    def test_matchCharges(self):
        db_charge = entities.Charge.new_with_defaults(
            charge_id=_ID, name=_NAME)
        db_identical_charge = entities.Charge.new_with_defaults(
            charge_id=_ID_ANOTHER, name=_NAME)

        ingested_charge = entities.Charge.new_with_defaults(
            name=_NAME, judge_name=_NAME_2)
        ingested_charge_new = entities.Charge.new_with_defaults(name=_NAME_2)

        expected_matched_charge = attr.evolve(
            ingested_charge, charge_id=db_charge.charge_id)
        expected_new_charge = attr.evolve(ingested_charge_new)
        expected_dropped_charge = attr.evolve(
            db_identical_charge, status=ChargeStatus.INFERRED_DROPPED)

        db_booking = entities.Booking.new_with_defaults(
            charges=[db_charge, db_identical_charge])
        ingested_booking = entities.Booking.new_with_defaults(
            charges=[ingested_charge, ingested_charge_new])

        entity_matching.match_charges(
            db_booking=db_booking, ingested_booking=ingested_booking)

        self.assertCountEqual(
            ingested_booking.charges,
            [expected_matched_charge, expected_new_charge,
             expected_dropped_charge])

    def test_matchCharges_bondRemoved(self):
        db_bond = entities.Bond.new_with_defaults(bond_id=_ID,
                                                  status=BondStatus.PENDING)
        db_charge = entities.Charge.new_with_defaults(charge_id=_ID, name=_NAME,
                                                      bond=db_bond)

        ingested_charge = attr.evolve(db_charge, charge_id=None, bond=None)

        expected_matched_charge = attr.evolve(
            ingested_charge, charge_id=db_charge.charge_id)

        db_booking = entities.Booking.new_with_defaults(charges=[db_charge])
        ingested_booking = entities.Booking.new_with_defaults(
            charges=[ingested_charge])

        entity_matching.match_charges(
            db_booking=db_booking, ingested_booking=ingested_booking)
        self.assertCountEqual(ingested_booking.charges,
                              [expected_matched_charge])

    def test_matchCharges_disambiguateByChildren(self):
        db_bond = entities.Bond.new_with_defaults(
            bond_id=_ID, external_id=_EXTERNAL_ID_ANOTHER,
            status=BondStatus.PENDING)
        db_bond_another = entities.Bond.new_with_defaults(
            bond_id=_ID_ANOTHER, external_id=_EXTERNAL_ID,
            status=BondStatus.PENDING)

        db_charge = entities.Charge.new_with_defaults(
            charge_id=_ID, name=_NAME, bond=db_bond)
        db_charge_another = entities.Charge.new_with_defaults(
            charge_id=_ID_ANOTHER, name=_NAME, bond=db_bond_another)

        ingested_charge = attr.evolve(
            db_charge, judge_name=_NAME, charge_id=None,
            bond=attr.evolve(db_bond, bond_id=None))
        ingested_charge_another = attr.evolve(
            db_charge_another, judge_name=_NAME_2, charge_id=None,
            bond=attr.evolve(db_bond_another, bond_id=None))

        expected_charge = attr.evolve(
            ingested_charge, charge_id=db_charge.charge_id)
        expected_charge_another = attr.evolve(
            ingested_charge_another, charge_id=db_charge_another.charge_id)

        db_booking = entities.Booking.new_with_defaults(
            charges=[db_charge, db_charge_another])
        ingested_booking = entities.Booking.new_with_defaults(
            charges=[ingested_charge_another, ingested_charge])

        entity_matching.match_charges(
            db_booking=db_booking, ingested_booking=ingested_booking)
        self.assertCountEqual(
            ingested_booking.charges,
            [expected_charge, expected_charge_another])

    def test_matchCharges_matchChargesWithChildrenFirst(self):
        db_bond = entities.Bond.new_with_defaults(bond_id=_ID,
                                                  status=BondStatus.PENDING)
        db_charge = entities.Charge.new_with_defaults(
            charge_id=_ID_ANOTHER, name=_NAME)
        db_charge_with_bond = entities.Charge.new_with_defaults(
            charge_id=_ID, name=_NAME, bond=db_bond)

        ingested_charge = entities.Charge.new_with_defaults(name=_NAME,
                                                            judge_name=_NAME)
        ingested_charge_with_bond = entities.Charge.new_with_defaults(
            name=_NAME, bond=attr.evolve(db_bond, bond_id=None),
            judge_name=_NAME_2)

        expected_matched_charge = attr.evolve(
            ingested_charge, charge_id=db_charge.charge_id)
        expected_charge_with_bond = attr.evolve(
            ingested_charge_with_bond, charge_id=db_charge_with_bond.charge_id)

        db_booking = entities.Booking.new_with_defaults(
            charges=[db_charge, db_charge_with_bond])
        ingested_booking = entities.Booking.new_with_defaults(
            charges=[ingested_charge_with_bond, ingested_charge])

        entity_matching.match_charges(
            db_booking=db_booking, ingested_booking=ingested_booking)
        self.assertCountEqual(
            ingested_booking.charges,
            [expected_matched_charge, expected_charge_with_bond])

    def test_matchBonds(self):
        db_bond_shared = entities.Bond.new_with_defaults(
            bond_id=_BOND_ID, amount_dollars=12)
        db_bond_third = entities.Bond.new_with_defaults(
            bond_id=_BOND_ID_ANOTHER, amount_dollars=3)

        db_charge_1 = entities.Charge.new_with_defaults(
            charge_id=_CHARGE_ID, bond=db_bond_shared)
        db_charge_2 = entities.Charge.new_with_defaults(
            charge_id=_CHARGE_ID_ANOTHER, bond=db_bond_shared)
        db_charge_3 = entities.Charge.new_with_defaults(
            charge_id=_ID, bond=db_bond_third)
        db_charge_4 = entities.Charge.new_with_defaults(charge_id=_ID_ANOTHER)

        db_booking = entities.Booking.new_with_defaults(
            charges=[db_charge_1, db_charge_2, db_charge_3, db_charge_4])

        ingested_bond_shared = entities.Bond.new_with_defaults(
            amount_dollars=12)
        ingested_bond_newly_shared = entities.Bond.new_with_defaults(
            amount_dollars=3)

        # match_bonds is called after match_charges, so ingested charges have
        # charge IDs at this point.
        ingested_charge_1 = entities.Charge.new_with_defaults(
            charge_id=_CHARGE_ID, bond=ingested_bond_shared)
        ingested_charge_2 = entities.Charge.new_with_defaults(
            charge_id=_CHARGE_ID_ANOTHER, bond=ingested_bond_shared)
        ingested_charge_3 = entities.Charge.new_with_defaults(
            charge_id=_ID, bond=ingested_bond_newly_shared)
        ingested_charge_4 = entities.Charge.new_with_defaults(
            charge_id=_ID_ANOTHER, bond=ingested_bond_newly_shared)

        ingested_booking = entities.Booking.new_with_defaults(
            charges=[ingested_charge_4, ingested_charge_2, ingested_charge_1,
                     ingested_charge_3])

        expected_matched_bond = attr.evolve(ingested_bond_shared,
                                            bond_id=_BOND_ID)
        expected_unmatched_bond = attr.evolve(ingested_bond_newly_shared,
                                              bond_id=_BOND_ID_ANOTHER)

        expected_charge_1 = entities.Charge.new_with_defaults(
            charge_id=_CHARGE_ID, bond=expected_matched_bond)
        expected_charge_2 = entities.Charge.new_with_defaults(
            charge_id=_CHARGE_ID_ANOTHER, bond=expected_matched_bond)
        expected_charge_3 = entities.Charge.new_with_defaults(
            charge_id=_ID, bond=expected_unmatched_bond)
        expected_charge_4 = entities.Charge.new_with_defaults(
            charge_id=_ID_ANOTHER, bond=expected_unmatched_bond)

        orphaned_entities = []
        entity_matching.match_bonds(db_booking=db_booking,
                                    ingested_booking=ingested_booking,
                                    orphaned_entities=orphaned_entities)

        self.assertCountEqual(ingested_booking.charges,
                              [expected_charge_1, expected_charge_2,
                               expected_charge_3, expected_charge_4])
        self.assertEqual(len(orphaned_entities), 0)
