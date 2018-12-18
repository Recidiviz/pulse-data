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
"""Tests for data converter."""

import unittest

from datetime import datetime

from recidiviz.common.constants.bond import BondType, BondStatus
from recidiviz.common.constants.booking import ReleaseReason, CustodyStatus, \
    Classification
from recidiviz.common.constants.charge import ChargeDegree, ChargeClass, \
    ChargeStatus, CourtType
from recidiviz.common.constants.person import Gender, Race, Ethnicity
from recidiviz.ingest.models.ingest_info_pb2 import IngestInfo
from recidiviz.persistence import converter
from recidiviz.persistence.entities import Person, Booking, Arrest, Charge, \
    Bond, Sentence


class TestConverter(unittest.TestCase):
    """Test converting IngestInfo objects to Persistence layer objects."""

    def test_parsesPerson(self):
        # Arrange
        ingest_info = IngestInfo()
        ingest_info.people.add(
            full_name='LAST,FIRST',
            birthdate='12-31-1999',
            gender='MALE',
            race='WHITE',
            ethnicity='HISPANIC',
            place_of_residence='NNN\n  STREET \t ZIP')

        # Act
        result = converter.convert(ingest_info)

        # Assert
        expected_result = [Person(
            given_names='FIRST',
            surname='LAST',
            birthdate=datetime(year=1999, month=12, day=31),
            birthdate_inferred_from_age=False,
            gender=Gender.MALE,
            race=Race.WHITE,
            ethnicity=Ethnicity.HISPANIC,
            place_of_residence='NNN STREET ZIP'
        )]

        self.assertEqual(result, expected_result)

    def test_personWithSurnameAndFullname_throwsException(self):
        # Arrange
        ingest_info = IngestInfo()
        ingest_info.people.add(full_name='LAST,FIRST', surname='LAST')

        with self.assertRaises(ValueError):
            converter.convert(ingest_info)

    def test_parseRaceAsEthnicity(self):
        # Arrange
        ingest_info = IngestInfo()
        ingest_info.people.add(race='HISPANIC')

        # Act
        result = converter.convert(ingest_info)

        # Assert
        expected_result = [Person(
            ethnicity=Ethnicity.HISPANIC
        )]

        self.assertEqual(result, expected_result)

    def test_parseBooking(self):
        # Arrange
        ingest_info = IngestInfo()
        ingest_info.people.add(booking_ids=['BOOKING_ID'])
        ingest_info.bookings.add(
            booking_id='BOOKING_ID',
            release_date='1/1/1111',
            projected_release_date='2/2/2222',
            release_reason='Transfer',
            custody_status='Held Elsewhere',
            classification='Low'
        )

        # Act
        result = converter.convert(ingest_info)

        # Assert
        expected_result = [Person(
            bookings=[Booking(
                external_id='BOOKING_ID',
                release_date=datetime(year=1111, month=1, day=1),
                release_date_inferred=False,
                projected_release_date=datetime(year=2222, month=2, day=2),
                release_reason=ReleaseReason.TRANSFER,
                custody_status=CustodyStatus.HELD_ELSEWHERE,
                classification=Classification.LOW
            )]
        )]

        self.assertEqual(result, expected_result)

    def test_parseArrest(self):
        # Arrange
        ingest_info = IngestInfo()
        ingest_info.people.add(booking_ids=['BOOKING_ID'])
        ingest_info.bookings.add(booking_id='BOOKING_ID', arrest_id='ARREST_ID')
        ingest_info.arrests.add(
            arrest_id='ARREST_ID',
            date='1/2/1111',
            location='FAKE_LOCATION',
            officer_name='FAKE_NAME',
            officer_id='FAKE_ID',
            agency='FAKE_AGENCY'
        )

        # Act
        result = converter.convert(ingest_info)

        # Assert
        expected_result = [Person(
            bookings=[Booking(
                external_id='BOOKING_ID',
                custody_status='IN_CUSTODY',
                arrest=Arrest(
                    external_id='ARREST_ID',
                    date=datetime(year=1111, month=1, day=2),
                    location='FAKE_LOCATION',
                    officer_name='FAKE_NAME',
                    officer_id='FAKE_ID',
                    agency='FAKE_AGENCY'
                )
            )]
        )]

        self.assertEqual(result, expected_result)

    def test_parseCharge(self):
        # Arrange
        ingest_info = IngestInfo()
        ingest_info.people.add(booking_ids=['BOOKING_ID'])
        ingest_info.bookings.add(booking_id='BOOKING_ID',
                                 charge_ids=['CHARGE_ID'])
        ingest_info.charges.add(
            charge_id='CHARGE_ID',
            attempted='True',
            degree='FIRST',
            charge_class='FELONY',
            status='DROPPED',
            court_type='DISTRICT',
            number_of_counts='3'
        )

        # Act
        result = converter.convert(ingest_info)

        # Assert
        expected_result = [Person(
            bookings=[Booking(
                external_id='BOOKING_ID',
                custody_status='IN_CUSTODY',
                charges=[Charge(
                    external_id='CHARGE_ID',
                    attempted=True,
                    degree=ChargeDegree.FIRST,
                    charge_class=ChargeClass.FELONY,
                    status=ChargeStatus.DROPPED,
                    court_type=CourtType.DISTRICT,
                    number_of_counts=3
                )]
            )]
        )]

        self.assertEqual(result, expected_result)

    def test_parseBond(self):
        # Arrange
        ingest_info = IngestInfo()
        ingest_info.people.add(booking_ids=['BOOKING_ID'])
        ingest_info.bookings.add(booking_id='BOOKING_ID',
                                 charge_ids=['CHARGE_ID'])
        ingest_info.charges.add(charge_id='CHARGE_ID', bond_id='BOND_ID')
        ingest_info.bonds.add(
            bond_id='BOND_ID',
            bond_type='CASH',
            status='ACTIVE'
        )

        # Act
        result = converter.convert(ingest_info)

        # Assert
        expected_result = [Person(
            bookings=[Booking(
                external_id='BOOKING_ID',
                custody_status='IN_CUSTODY',
                charges=[Charge(
                    external_id='CHARGE_ID',
                    status='PENDING',
                    bond=Bond(
                        external_id='BOND_ID',
                        bond_type=BondType.CASH,
                        status=BondStatus.ACTIVE
                    )
                )]
            )]
        )]

        self.assertEqual(result, expected_result)

    def test_totalBondNoCharge_createsChargeWithTotalBondAmount(self):
        # Arrange
        ingest_info = IngestInfo()
        ingest_info.people.add(booking_ids=['BOOKING_ID'])
        ingest_info.bookings.add(booking_id='BOOKING_ID',
                                 total_bond_amount='$100')

        # Act
        result = converter.convert(ingest_info)

        # Assert
        expected_result = [Person(
            bookings=[Booking(
                external_id='BOOKING_ID',
                custody_status='IN_CUSTODY',
                charges=[Charge(
                    status='PENDING',
                    bond=Bond(status='POSTED', amount_dollars=100)
                )]
            )]
        )]

        self.assertEqual(result, expected_result)

    def test_totalBondWithCharge_setsTotalBondOnCharge(self):
        # Arrange
        ingest_info = IngestInfo()
        ingest_info.people.add(booking_ids=['BOOKING_ID'])
        ingest_info.bookings.add(booking_id='BOOKING_ID',
                                 total_bond_amount='$100',
                                 charge_ids=['CHARGE_ID'])
        ingest_info.charges.add(charge_id='CHARGE_ID', bond_id='BOND_ID')
        ingest_info.bonds.add(bond_id='BOND_ID')

        # Act
        result = converter.convert(ingest_info)

        # Assert
        expected_result = [Person(
            bookings=[Booking(
                external_id='BOOKING_ID',
                custody_status='IN_CUSTODY',
                charges=[Charge(
                    external_id='CHARGE_ID',
                    status='PENDING',
                    bond=Bond(external_id='BOND_ID', amount_dollars=100,
                              status='POSTED')
                )]
            )]
        )]

        self.assertEqual(result, expected_result)

    def test_parseSentence(self):
        # Arrange
        ingest_info = IngestInfo()
        ingest_info.people.add(booking_ids=['BOOKING_ID'])
        ingest_info.bookings.add(booking_id='BOOKING_ID',
                                 charge_ids=['CHARGE_ID'])
        ingest_info.charges.add(charge_id='CHARGE_ID',
                                sentence_id='SENTENCE_ID')
        ingest_info.sentences.add(
            sentence_id='SENTENCE_ID',
            min_length='1',
            post_release_supervision_length=''
        )

        # Act
        result = converter.convert(ingest_info)

        # Assert
        expected_result = [Person(
            bookings=[Booking(
                external_id='BOOKING_ID',
                custody_status='IN_CUSTODY',
                charges=[Charge(
                    external_id='CHARGE_ID',
                    status='PENDING',
                    sentence=Sentence(
                        external_id='SENTENCE_ID',
                        min_length_days=1,
                        post_release_supervision_length_days=0
                    )
                )]
            )]
        )]

        self.assertEqual(result, expected_result)

    def test_parseFullIngestInfo(self):
        # Arrange
        ingest_info = IngestInfo()
        ingest_info.people.add(person_id='PERSON_ID',
                               booking_ids=['BOOKING_ID'])
        ingest_info.bookings.add(booking_id='BOOKING_ID',
                                 arrest_id='ARREST_ID',
                                 charge_ids=['CHARGE_ID'])
        ingest_info.arrests.add(arrest_id='ARREST_ID', agency='PD')
        ingest_info.charges.add(charge_id='CHARGE_ID', name='DUI',
                                bond_id='BOND_ID', sentence_id='SENTENCE_ID')
        ingest_info.bonds.add(bond_id='BOND_ID')
        ingest_info.sentences.add(sentence_id='SENTENCE_ID', is_life='True')

        # Act
        result = converter.convert(ingest_info)

        # Assert
        expected_result = [Person(
            external_id='PERSON_ID',
            bookings=[Booking(
                external_id='BOOKING_ID',
                custody_status='IN_CUSTODY',
                arrest=Arrest(external_id='ARREST_ID', agency='PD'),
                charges=[Charge(
                    external_id='CHARGE_ID',
                    status='PENDING',
                    name='DUI',
                    bond=Bond(external_id='BOND_ID', status='POSTED'),
                    sentence=Sentence(external_id='SENTENCE_ID', is_life=True)
                )]
            )])]

        self.assertEqual(result, expected_result)

    def test_cannotConvertField_raisesValueError(self):
        ingest_info = IngestInfo()
        ingest_info.people.add(birthdate='NOT_A_DATE')

        with self.assertRaises(ValueError):
            converter.convert(ingest_info)
