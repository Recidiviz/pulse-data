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
"""Tests for entity_matching_utils.py."""
from datetime import datetime
from unittest import TestCase

from recidiviz.common.constants.bond import BondType, BondStatus
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
_GIVEN_NAMES = 'given_names'
_GIVEN_NAMES_OTHER = 'different names'
_SURNAME = 'surname'
_PLACE_1 = 'place'
_PLACE_2 = 'another'
_DATE = datetime(2018, 12, 13)
_DATE_OTHER = datetime(2017, 12, 13)
_CHARGE_NAME = "Charge1"
_CHARGE_NAME_2 = "Charge2"
_CHARGE_NAME_3 = "Charge3"
_JUDGE_NAME = 'judd'
_JUDGE_NAME_OTHER = 'jujj'


class TestEntityMatchingUtils(TestCase):
    """Tests for entity matching logic"""

    def test_person_match_name_and_birthdate(self):
        db_person = entities.Person.new_with_defaults(
            given_names=_GIVEN_NAMES,
            surname=_SURNAME,
            birthdate=_DATE
        )
        ingested_person = entities.Person.new_with_defaults(
            given_names=_GIVEN_NAMES,
            surname=_SURNAME,
            birthdate=_DATE
        )
        self.assertTrue(entity_matching_utils.is_person_match(
            db_entity=db_person, ingested_entity=ingested_person))
        ingested_person.birthdate = _DATE_OTHER
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
        ingested_booking.release_date = _DATE
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

    def test_charge_match(self):
        db_charge = entities.Charge.new_with_defaults(charge_id=_CHARGE_ID,
                                                      name=_CHARGE_NAME,
                                                      judge_name=_JUDGE_NAME,
                                                      bond=entities.Bond(
                                                          bond_id=_BOND_ID))
        ingested_charge = entities.Charge.new_with_defaults(
            charge_id=_CHARGE_ID_OTHER,
            name=_CHARGE_NAME,
            judge_name=_JUDGE_NAME_OTHER,
            bond=entities.Bond(bond_id=
                               _BOND_ID_OTHER))

        self.assertTrue(entity_matching_utils.is_charge_match(
            db_entity=db_charge, ingested_entity=ingested_charge))
        ingested_charge.name = _CHARGE_NAME_2
        self.assertFalse(entity_matching_utils.is_charge_match(
            db_entity=db_charge, ingested_entity=ingested_charge))

    def test_bond_match_external_ids(self):
        db_bond = entities.Bond(external_id=_EXTERNAL_ID)
        ingested_bond = entities.Bond(external_id=_EXTERNAL_ID)
        self.assertTrue(entity_matching_utils.is_bond_match(
            db_entity=db_bond, ingested_entity=ingested_bond))
        ingested_bond.external_id = _EXTERNAL_ID_OTHER
        self.assertFalse(entity_matching_utils.is_bond_match(
            db_entity=db_bond, ingested_entity=ingested_bond))

    def test_bond_match(self):
        db_bond = entities.Bond(bond_id=_BOND_ID, bond_type=BondType.CASH,
                                status=BondStatus.ACTIVE)
        ingested_bond = entities.Bond(bond_type=BondType.CASH,
                                      status=BondStatus.POSTED)
        self.assertTrue(entity_matching_utils.is_bond_match(
            db_entity=db_bond, ingested_entity=ingested_bond))
        ingested_bond.bond_type = BondType.SECURED
        self.assertFalse(entity_matching_utils.is_bond_match(
            db_entity=db_bond, ingested_entity=ingested_bond))

    def test_sentence_match_external_ids(self):
        db_sentence = entities.Sentence(external_id=_EXTERNAL_ID)
        ingested_sentence = entities.Sentence(external_id=_EXTERNAL_ID)
        self.assertTrue(entity_matching_utils.is_sentence_match(
            db_entity=db_sentence, ingested_entity=ingested_sentence))
        ingested_sentence.external_id = _EXTERNAL_ID_OTHER
        self.assertFalse(entity_matching_utils.is_sentence_match(
            db_entity=db_sentence, ingested_entity=ingested_sentence))

    def test_sentence_match(self):
        # TODO(350): expand tests after more robust equality function
        self.assertTrue(entity_matching_utils.is_sentence_match(
            db_entity=entities.Sentence(sentence_id=_SENTENCE_ID),
            ingested_entity=entities.Sentence(sentence_id=_SENTENCE_ID_OTHER)))
