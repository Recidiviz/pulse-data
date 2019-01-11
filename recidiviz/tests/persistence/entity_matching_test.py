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
"""Tests for entity_matching.py."""
import copy
from datetime import datetime
from unittest import TestCase

from recidiviz import Session
from recidiviz.common.constants.booking import CustodyStatus
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.persistence.database import schema, database_utils
from recidiviz.persistence import entities, entity_matching
from recidiviz.persistence.entity_matching import EntityMatchingError
from recidiviz.tests.utils import fakes

_ID = 1
_ID_ANOTHER = 2
_PERSON_ID = '2'
_PERSON_ID_ANOTHER = '3'
_BOOKING_ID = '12'
_BOOKING_ID_ANOTHER = '13'
_CHARGE_ID = '112'
_CHARGE_ID_ANOTHER = '113'
_BOND_ID = '212'
_BOND_ID_ANOTHER = '213'
_SENTENCE_ID = '312'
_SENTENCE_ID_ANOTHER = '313'
_REGION = 'region'
_EXTERNAL_ID = 'external_id'
_EXTERNAL_ID_ANOTHER = 'external_id_another'
_FACILITY = 'facility'
_GIVEN_NAMES = 'given_names'
_GIVEN_NAMES_ANOTHER = 'different names'
_SURNAME = 'surname'
_PLACE_1 = 'place'
_PLACE_2 = 'another'
_DATE = datetime(2018, 12, 13)
_DATE_ANOTHER = datetime(2017, 12, 13)
_NAME = "name_1"
_NAME_2 = "name_2"
_NAME_3 = "name_3"


class TestEntityMatching(TestCase):
    """Tests for entity matching logic"""

    def setup_method(self, _test_method):
        fakes.use_in_memory_sqlite_database()

    def test_match_entites(self):
        schema_charge = schema.Charge(status=ChargeStatus.PENDING.value,
                                      charge_id=_CHARGE_ID,
                                      name=_NAME)
        schema_charge_another = schema.Charge(status=ChargeStatus.PENDING.value,
                                              charge_id=_CHARGE_ID_ANOTHER,
                                              name=_NAME_2)
        open_schema_booking = schema.Booking(
            admission_date=_DATE,
            booking_id=_BOOKING_ID,
            admission_date_inferred=True,
            custody_status=CustodyStatus.IN_CUSTODY.value,
            last_seen_time=_DATE,
            charges=[schema_charge,
                     schema_charge_another])

        closed_schema_booking = schema.Booking(
            admission_date=_DATE_ANOTHER,
            booking_id=_BOOKING_ID_ANOTHER,
            admission_date_inferred=True,
            release_date=_DATE,
            custody_status=CustodyStatus.RELEASED.value,
            last_seen_time=_DATE)

        schema_person = schema.Person(person_id=_PERSON_ID,
                                      surname=_SURNAME,
                                      given_names=_GIVEN_NAMES,
                                      birthdate=_DATE,
                                      place_of_residence=_PLACE_1,
                                      region=_REGION,
                                      bookings=[open_schema_booking,
                                                closed_schema_booking])

        schema_person_another = schema.Person(
            person_id=_PERSON_ID_ANOTHER,
            given_names=_GIVEN_NAMES_ANOTHER,
            surname=_SURNAME,
            birthdate=_DATE,
            place_of_residence=_PLACE_1,
            region=_REGION)

        session = Session()
        session.add(schema_person)
        session.add(schema_person_another)
        session.commit()

        ingested_existing_charge = database_utils.convert_charge(
            schema_charge)
        ingested_existing_charge.charge_id = None
        ingested_new_charge = entities.Charge.new_with_defaults(
            status=ChargeStatus.PENDING,
            name=_NAME_3
        )

        ingested_open_booking = database_utils.convert_booking(
            open_schema_booking)
        ingested_open_booking.admission_date = None
        ingested_open_booking.booking_id = None
        ingested_open_booking.facility = _FACILITY
        ingested_open_booking.charges = [ingested_existing_charge,
                                         ingested_new_charge]

        ingested_person = database_utils.convert_person(schema_person)
        ingested_person.person_id = None
        ingested_person.place_of_residence = _PLACE_2
        ingested_person.bookings = [ingested_open_booking]

        entity_matching.match_entities(Session(), _REGION, [ingested_person])

        expected_existing_charge = copy.deepcopy(ingested_existing_charge)
        expected_new_charge = copy.deepcopy(ingested_new_charge)

        expected_dropped_charge = database_utils.convert_charge(
            schema_charge_another)
        expected_dropped_charge.status = ChargeStatus.DROPPED
        expected_open_booking = copy.deepcopy(ingested_open_booking)
        expected_open_booking.charges = [expected_existing_charge,
                                         expected_new_charge,
                                         expected_dropped_charge]
        expected_closed_booking = \
            database_utils.convert_booking(closed_schema_booking)
        expected_person = copy.deepcopy(ingested_person)
        expected_person.bookings = \
            [expected_open_booking, expected_closed_booking]

        assert ingested_person == expected_person

    def test_match_entities_external_ids(self):
        closed_schema_booking = schema.Booking(
            admission_date=_DATE_ANOTHER,
            booking_id=_BOOKING_ID,
            release_date=_DATE,
            custody_status=CustodyStatus.RELEASED.value,
            last_seen_time=_DATE)

        schema_person = schema.Person(person_id=_PERSON_ID,
                                      external_id=_EXTERNAL_ID,
                                      surname=_SURNAME,
                                      given_names=_GIVEN_NAMES,
                                      birthdate=_DATE,
                                      place_of_residence=_PLACE_1,
                                      region=_REGION,
                                      bookings=[closed_schema_booking])

        session = Session()
        session.add(schema_person)
        session.commit()

        ingested_closed_booking = database_utils.convert_booking(
            closed_schema_booking)
        ingested_closed_booking.booking_id = None
        ingested_closed_booking.facility = _FACILITY

        ingested_person = database_utils.convert_person(schema_person)
        ingested_person.person_id = None
        ingested_person.bookings = [ingested_closed_booking]

        entity_matching.match_entities(Session(), _REGION, [ingested_person])

        expected_closed_booking = copy.deepcopy(ingested_closed_booking)
        expected_person = copy.deepcopy(ingested_person)
        expected_person.bookings = [expected_closed_booking]

        assert ingested_person == expected_person

    def test_matchHolds_duplicateMatch_throws(self):
        db_hold = entities.Hold.new_with_defaults(
            hold_id=_ID, jurisdiction_name=_NAME)
        ingested_hold = entities.Hold.new_with_defaults(jurisdiction_name=_NAME)
        ingested_hold_another = copy.deepcopy(ingested_hold)

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

        expected_matched_hold = copy.deepcopy(ingested_hold)
        expected_matched_hold.hold_id = db_hold.hold_id
        expected_new_hold = copy.deepcopy(ingested_hold_new)
        expected_dropped_hold = copy.deepcopy(db_hold_to_drop)

        entity_matching.match_holds(db_booking=db_booking,
                                    ingested_booking=ingested_booking)

        self.assertCountEqual(ingested_booking.holds,
                              [expected_matched_hold, expected_dropped_hold,
                               expected_new_hold])

    def test_matchArrests(self):
        db_arrest = entities.Arrest.new_with_defaults(arrest_id=_ID)
        ingested_arrest = entities.Arrest.new_with_defaults()

        expected_arrest = copy.deepcopy(ingested_arrest)
        expected_arrest.arrest_id = db_arrest.arrest_id

        entity_matching.match_arrest(
            db_booking=entities.Booking.new_with_defaults(arrest=db_arrest),
            ingested_booking=entities.Booking.new_with_defaults(
                arrest=ingested_arrest))

        self.assertEqual(ingested_arrest, expected_arrest)

    def test_matchCharges(self):
        db_charge = entities.Charge.new_with_defaults(charge_id=_ID,
                                                      name=_NAME)
        db_identical_charge = entities.Charge.new_with_defaults(
            charge_id=_ID_ANOTHER, name=_NAME)

        ingested_charge = entities.Charge.new_with_defaults(name=_NAME,
                                                            judge_name=_NAME_2)
        ingested_charge_new = entities.Charge.new_with_defaults(
            name=_NAME_2)

        expected_matched_charge = copy.deepcopy(ingested_charge)
        expected_matched_charge.charge_id = db_charge.charge_id
        expected_new_charge = copy.deepcopy(ingested_charge_new)
        expected_dropped_charge = copy.deepcopy(db_identical_charge)
        expected_dropped_charge.status = ChargeStatus.DROPPED

        db_booking = entities.Booking.new_with_defaults(
            charges=[db_charge, db_identical_charge])
        ingested_booking = entities.Booking.new_with_defaults(
            charges=[ingested_charge, ingested_charge_new])

        entity_matching.match_charges(db_booking=db_booking,
                                      ingested_booking=ingested_booking)

        self.assertCountEqual(ingested_booking.charges,
                              [expected_matched_charge, expected_new_charge,
                               expected_dropped_charge])

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

        expected_matched_bond = copy.deepcopy(db_bond_shared)
        expected_unmatched_bond = copy.deepcopy(ingested_bond_newly_shared)

        expected_charge_1 = entities.Charge.new_with_defaults(
            charge_id=_CHARGE_ID, bond=expected_matched_bond)
        expected_charge_2 = entities.Charge.new_with_defaults(
            charge_id=_CHARGE_ID_ANOTHER, bond=expected_matched_bond)
        expected_charge_3 = entities.Charge.new_with_defaults(
            charge_id=_ID, bond=expected_unmatched_bond)
        expected_charge_4 = entities.Charge.new_with_defaults(
            charge_id=_ID_ANOTHER, bond=expected_unmatched_bond)

        entity_matching.match_bonds(db_booking=db_booking,
                                    ingested_booking=ingested_booking)

        self.assertCountEqual(ingested_booking.charges,
                              [expected_charge_1, expected_charge_2,
                               expected_charge_3, expected_charge_4])
