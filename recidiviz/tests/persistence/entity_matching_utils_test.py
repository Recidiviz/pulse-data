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
"""Tests for entity_matching_utils.py."""
from datetime import datetime
from unittest import TestCase

import attr
from dateutil.relativedelta import relativedelta

from recidiviz.common.constants.bond import BondType, BondStatus
from recidiviz.common.constants.booking import CustodyStatus
from recidiviz.persistence import entities, entity_matching_utils

_PERSON_ID = 2
_PERSON_ID_OTHER = 3
_BOOKING_ID = 12
_BOOKING_ID_OTHER = 13
_HOLD_ID = 1000
_CHARGE_ID = 112
_CHARGE_ID_OTHER = 113
_BOND_ID = 212
_BOND_ID_OTHER = 213
_SENTENCE_ID = 312
_SENTENCE_ID_OTHER = 313
_REGION = 'region'
_EXTERNAL_ID = 'external_id'
_EXTERNAL_ID_OTHER = 'external_id_another'
_FACILITY = 'facility'
_PLACE_1 = 'place'
_PLACE_2 = 'another'
_FULL_NAME = 'full_name'
_DATE = datetime(2018, 12, 13)
_DATE_OTHER = datetime(2017, 12, 13)
_CHARGE_NAME = "Charge1"
_CHARGE_NAME_2 = "Charge2"
_CHARGE_NAME_3 = "Charge3"
_JUDGE_NAME = 'judd'
_JUDGE_NAME_OTHER = 'jujj'


class TestEntityMatchingUtils(TestCase):
    """Tests for entity matching logic"""

    def test_person_match_name(self):
        db_person = entities.Person.new_with_defaults(
            full_name=_FULL_NAME, resident_of_region=True)
        ingested_person = entities.Person.new_with_defaults(
            full_name=_FULL_NAME, resident_of_region=False)
        self.assertTrue(entity_matching_utils.is_person_match(
            db_entity=db_person, ingested_entity=ingested_person))

    def test_person_match_name_and_birthdate(self):
        db_person = entities.Person.new_with_defaults(
            full_name=_FULL_NAME,
            birthdate=_DATE,
        )
        ingested_person = entities.Person.new_with_defaults(
            full_name=_FULL_NAME,
            birthdate=_DATE
        )
        self.assertTrue(entity_matching_utils.is_person_match(
            db_entity=db_person, ingested_entity=ingested_person))
        ingested_person.birthdate = _DATE_OTHER
        self.assertFalse(entity_matching_utils.is_person_match(
            db_entity=db_person, ingested_entity=ingested_person))

    def test_person_match_name_and_inferred_birthdate(self):
        date_plus_one_year = _DATE + relativedelta(years=1)
        date_plus_two_years = _DATE + relativedelta(years=2)

        db_person = entities.Person.new_with_defaults(
            full_name=_FULL_NAME,
            birthdate=_DATE,
            birthdate_inferred_from_age=True
        )

        ingested_person = entities.Person.new_with_defaults(
            full_name=_FULL_NAME,
            birthdate=date_plus_one_year,
            birthdate_inferred_from_age=True
        )
        self.assertTrue(entity_matching_utils.is_person_match(
            db_entity=db_person, ingested_entity=ingested_person))
        ingested_person.birthdate = date_plus_two_years
        self.assertFalse(entity_matching_utils.is_person_match(
            db_entity=db_person, ingested_entity=ingested_person))

    def test_person_match_external_id(self):
        db_person = entities.Person.new_with_defaults(
            external_id=_EXTERNAL_ID
        )
        ingested_person = entities.Person.new_with_defaults(
            external_id=_EXTERNAL_ID
        )
        self.assertTrue(entity_matching_utils.is_person_match(
            db_entity=db_person, ingested_entity=ingested_person))
        ingested_person.external_id = _EXTERNAL_ID_OTHER
        self.assertFalse(entity_matching_utils.is_person_match(
            db_entity=db_person, ingested_entity=ingested_person))

    def test_booking_match_external_id(self):
        db_booking = entities.Booking.new_with_defaults(
            external_id=_EXTERNAL_ID
        )
        ingested_booking = entities.Booking.new_with_defaults(
            external_id=_EXTERNAL_ID
        )
        self.assertTrue(entity_matching_utils.is_booking_match(
            db_entity=db_booking, ingested_entity=ingested_booking))
        ingested_booking.external_id = _EXTERNAL_ID_OTHER
        self.assertFalse(entity_matching_utils.is_booking_match(
            db_entity=db_booking, ingested_entity=ingested_booking))

    def test_booking_match_admission_date(self):
        db_booking = entities.Booking.new_with_defaults(
            admission_date=_DATE
        )
        ingested_booking = entities.Booking.new_with_defaults(
            admission_date=_DATE
        )
        self.assertTrue(entity_matching_utils.is_booking_match(
            db_entity=db_booking, ingested_entity=ingested_booking))
        ingested_booking.admission_date = None
        self.assertFalse(entity_matching_utils.is_booking_match(
            db_entity=db_booking, ingested_entity=ingested_booking))

    def test_booking_match_open_bookings(self):
        db_booking = entities.Booking.new_with_defaults(
            admission_date=_DATE,
            admission_date_inferred=True
        )
        ingested_booking = entities.Booking.new_with_defaults()
        self.assertTrue(entity_matching_utils.is_booking_match(
            db_entity=db_booking, ingested_entity=ingested_booking))
        ingested_booking.custody_status = CustodyStatus.RELEASED
        self.assertFalse(entity_matching_utils.is_booking_match(
            db_entity=db_booking, ingested_entity=ingested_booking))

    def test_hold_match_external_id(self):
        db_hold = entities.Hold.new_with_defaults(external_id=_EXTERNAL_ID)
        ingested_hold = entities.Hold.new_with_defaults(
            external_id=_EXTERNAL_ID)
        self.assertTrue(
            entity_matching_utils.is_hold_match(db_entity=db_hold,
                                                ingested_entity=ingested_hold))
        ingested_hold.external_id = _EXTERNAL_ID_OTHER
        self.assertFalse(
            entity_matching_utils.is_hold_match(db_entity=db_hold,
                                                ingested_entity=ingested_hold))

    def test_hold_match(self):
        db_hold = entities.Hold.new_with_defaults(
            hold_id=_HOLD_ID, jurisdiction_name=_PLACE_1)
        ingested_hold = entities.Hold.new_with_defaults(
            jurisdiction_name=_PLACE_1)
        self.assertTrue(
            entity_matching_utils.is_hold_match(db_entity=db_hold,
                                                ingested_entity=ingested_hold))
        ingested_hold.jurisdiction_name = _PLACE_2
        self.assertFalse(
            entity_matching_utils.is_hold_match(db_entity=db_hold,
                                                ingested_entity=ingested_hold))

    def test_charge_match_external_ids(self):
        db_charge = entities.Charge.new_with_defaults(external_id=_EXTERNAL_ID)
        ingested_charge = entities.Charge.new_with_defaults(
            external_id=_EXTERNAL_ID)
        self.assertTrue(entity_matching_utils.is_charge_match(
            db_entity=db_charge, ingested_entity=ingested_charge))
        ingested_charge.external_id = _EXTERNAL_ID_OTHER
        self.assertFalse(entity_matching_utils.is_charge_match(
            db_entity=db_charge, ingested_entity=ingested_charge))

    def test_charge_match_without_children(self):
        db_charge = entities.Charge.new_with_defaults(
            charge_id=_CHARGE_ID,
            name=_CHARGE_NAME,
            judge_name=_JUDGE_NAME,
            bond=entities.Bond.new_with_defaults(
                bond_type=BondType.NOT_REQUIRED))
        ingested_charge = entities.Charge.new_with_defaults(
            charge_id=_CHARGE_ID_OTHER,
            name=_CHARGE_NAME,
            judge_name=_JUDGE_NAME_OTHER,
            bond=entities.Bond.new_with_defaults(bond_type=BondType.CASH))
        self.assertTrue(entity_matching_utils.is_charge_match(
            db_entity=db_charge, ingested_entity=ingested_charge))
        ingested_charge.name = _CHARGE_NAME_2
        self.assertFalse(entity_matching_utils.is_charge_match(
            db_entity=db_charge, ingested_entity=ingested_charge))

    def test_charge_match_with_children(self):
        db_bond = entities.Bond.new_with_defaults(
            bond_id=_BOND_ID, external_id=_EXTERNAL_ID)
        db_bond_another = entities.Bond.new_with_defaults(
            bond_id=_BOND_ID_OTHER, external_id=_EXTERNAL_ID_OTHER)
        db_charge = entities.Charge.new_with_defaults(
            charge_id=_CHARGE_ID, bond=db_bond)

        ingested_charge = entities.Charge.new_with_defaults(
            bond=attr.evolve(db_bond, bond_id=None))

        self.assertTrue(entity_matching_utils.is_charge_match_with_children(
            db_entity=db_charge, ingested_entity=ingested_charge))
        ingested_charge.bond = db_bond_another
        self.assertFalse(entity_matching_utils.is_charge_match_with_children(
            db_entity=db_charge, ingested_entity=ingested_charge))

    def test_bond_match_external_ids(self):
        db_bond = entities.Bond.new_with_defaults(external_id=_EXTERNAL_ID)
        ingested_bond = entities.Bond.new_with_defaults(
            external_id=_EXTERNAL_ID)
        self.assertTrue(entity_matching_utils.is_bond_match(
            db_entity=db_bond, ingested_entity=ingested_bond))
        ingested_bond.external_id = _EXTERNAL_ID_OTHER
        self.assertFalse(entity_matching_utils.is_bond_match(
            db_entity=db_bond, ingested_entity=ingested_bond))

    def test_bond_match(self):
        db_bond = entities.Bond.new_with_defaults(
            bond_id=_BOND_ID, bond_type=BondType.CASH, status=BondStatus.SET)
        ingested_bond = entities.Bond.new_with_defaults(
            bond_type=BondType.CASH,
            status=BondStatus.POSTED
        )
        self.assertTrue(entity_matching_utils.is_bond_match(
            db_entity=db_bond, ingested_entity=ingested_bond))
        ingested_bond.bond_type = BondType.SECURED
        self.assertFalse(entity_matching_utils.is_bond_match(
            db_entity=db_bond, ingested_entity=ingested_bond))

    def test_sentence_match_external_ids(self):
        db_sentence = entities.Sentence.new_with_defaults(
            external_id=_EXTERNAL_ID)
        ingested_sentence = entities.Sentence.new_with_defaults(
            external_id=_EXTERNAL_ID)
        self.assertTrue(entity_matching_utils.is_sentence_match(
            db_entity=db_sentence, ingested_entity=ingested_sentence))
        ingested_sentence.external_id = _EXTERNAL_ID_OTHER
        self.assertFalse(entity_matching_utils.is_sentence_match(
            db_entity=db_sentence, ingested_entity=ingested_sentence))

    def test_sentence_match(self):
        # TODO(350): expand tests after more robust equality function
        self.assertTrue(entity_matching_utils.is_sentence_match(
            db_entity=entities.Sentence.new_with_defaults(
                sentence_id=_SENTENCE_ID),
            ingested_entity=entities.Sentence.new_with_defaults(
                sentence_id=_SENTENCE_ID_OTHER)))

    def test_person_similarity(self):
        person = entities.Person.new_with_defaults(
            person_id=1, birthdate=_DATE,
            bookings=[entities.Booking.new_with_defaults(
                booking_id=2, custody_status=CustodyStatus.RELEASED)])

        person_another = attr.evolve(person, birthdate=_DATE_OTHER)
        self.assertEqual(
            entity_matching_utils.diff_count(person, person_another), 1)
