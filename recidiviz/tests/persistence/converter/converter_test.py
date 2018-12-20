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

from recidiviz.ingest.models.ingest_info_pb2 import IngestInfo
from recidiviz.persistence.converter import converter
from recidiviz.persistence.entities import Person, Booking, Arrest, Charge, \
    Bond, Sentence


class TestConverter(unittest.TestCase):
    """Test converting IngestInfo objects to Persistence layer objects."""

    def testConvert_FullIngestInfo(self):
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

    def testConvert_TotalBondNoCharge_CreatesChargeWithTotalBondAmount(self):
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

    def testConvert_TotalBondWithCharge_SetsTotalBondOnCharge(self):
        # Arrange
        ingest_info = IngestInfo()
        ingest_info.people.add(booking_ids=['BOOKING_ID'])
        ingest_info.bookings.add(booking_id='BOOKING_ID',
                                 total_bond_amount='$100',
                                 charge_ids=['CHARGE_ID'])
        ingest_info.charges.add(charge_id='CHARGE_ID')

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
                    bond=Bond(amount_dollars=100, status='POSTED')
                )]
            )]
        )]

        self.assertEqual(result, expected_result)

    def testConvert_TotalBondWithMultipleBonds_ThrowsException(self):
        # Arrange
        ingest_info = IngestInfo()
        ingest_info.people.add(booking_ids=['BOOKING_ID'])
        ingest_info.bookings.add(booking_id='BOOKING_ID',
                                 total_bond_amount='$100',
                                 charge_ids=['CHARGE_ID', 'CHARGE_ID_2'])
        ingest_info.charges.add(charge_id='CHARGE_ID', bond_id='BOND_ID')
        ingest_info.charges.add(charge_id='CHARGE_ID_2', bond_id='BOND_ID_2')
        ingest_info.bonds.add(bond_id='BOND_ID')
        ingest_info.bonds.add(bond_id='BOND_ID_2')

        # Act + Assert
        with self.assertRaises(ValueError):
            converter.convert(ingest_info)

    def testConvert_CannotConvertField_RaisesValueError(self):
        ingest_info = IngestInfo()
        ingest_info.people.add(birthdate='NOT_A_DATE')

        with self.assertRaises(ValueError):
            converter.convert(ingest_info)
