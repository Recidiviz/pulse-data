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

from recidiviz.common.constants.bond import BondType, BondStatus
from recidiviz.common.constants.booking import CustodyStatus
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models.ingest_info_pb2 import IngestInfo
from recidiviz.persistence.converter import converter
from recidiviz.persistence.entities import Person, Booking, Arrest, Charge, \
    Bond, Sentence


class TestConverter(unittest.TestCase):
    """Test converting IngestInfo objects to Persistence layer objects."""

    def testConvert_FullIngestInfo(self):
        # Arrange
        metadata = IngestMetadata('REGION', 'LAST_SEEN_TIME')

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
        result = converter.convert(ingest_info, metadata)

        # Assert
        expected_result = [Person.new_with_defaults(
            external_id='PERSON_ID',
            region='REGION',
            bookings=[Booking.new_with_defaults(
                external_id='BOOKING_ID',
                admission_date='LAST_SEEN_TIME',
                admission_date_inferred=True,
                last_seen_time='LAST_SEEN_TIME',
                custody_status=CustodyStatus.IN_CUSTODY,
                arrest=Arrest.new_with_defaults(
                    external_id='ARREST_ID',
                    agency='PD'
                ),
                charges=[Charge(
                    external_id='CHARGE_ID',
                    status=ChargeStatus.PENDING,
                    name='DUI',
                    bond=Bond(external_id='BOND_ID'),
                    sentence=Sentence(external_id='SENTENCE_ID', is_life=True)
                )]
            )])]

        self.assertEqual(result, expected_result)

    def testConvert_FullIngestInfo_GeneratedIds(self):
        # Arrange
        metadata = IngestMetadata('REGION', 'LAST_SEEN_TIME')

        ingest_info = IngestInfo()
        ingest_info.people.add(person_id='PERSON_ID_GENERATE',
                               booking_ids=['BOOKING_ID_GENERATE'])
        ingest_info.bookings.add(booking_id='BOOKING_ID_GENERATE',
                                 arrest_id='ARREST_ID_GENERATE',
                                 charge_ids=['CHARGE_ID_GENERATE'])
        ingest_info.arrests.add(arrest_id='ARREST_ID_GENERATE', agency='PD')
        ingest_info.charges.add(charge_id='CHARGE_ID_GENERATE', name='DUI',
                                bond_id='BOND_ID_GENERATE',
                                sentence_id='SENTENCE_ID_GENERATE')
        ingest_info.bonds.add(bond_id='BOND_ID_GENERATE')
        ingest_info.sentences.add(sentence_id='SENTENCE_ID_GENERATE',
                                  is_life='True')

        # Act
        result = converter.convert(ingest_info, metadata)

        # Assert
        expected_result = [Person.new_with_defaults(
            region='REGION',
            bookings=[Booking.new_with_defaults(
                admission_date='LAST_SEEN_TIME',
                admission_date_inferred=True,
                last_seen_time='LAST_SEEN_TIME',
                custody_status=CustodyStatus.IN_CUSTODY,
                arrest=Arrest.new_with_defaults(agency='PD'),
                charges=[Charge(
                    status=ChargeStatus.PENDING,
                    name='DUI',
                    bond=Bond(),
                    sentence=Sentence(is_life=True)
                )]
            )])]

        self.assertEqual(result, expected_result)

    def testConvert_TotalBondNoCharge_CreatesChargeWithTotalBondAmount(self):
        # Arrange
        metadata = IngestMetadata.new_with_defaults(
            last_seen_time='LAST_SEEN_TIME'
        )

        ingest_info = IngestInfo()
        ingest_info.people.add(booking_ids=['BOOKING_ID'])
        ingest_info.bookings.add(booking_id='BOOKING_ID',
                                 total_bond_amount='$100')

        # Act
        result = converter.convert(ingest_info, metadata)

        # Assert
        expected_result = [Person.new_with_defaults(
            bookings=[Booking.new_with_defaults(
                admission_date='LAST_SEEN_TIME',
                admission_date_inferred=True,
                last_seen_time='LAST_SEEN_TIME',
                external_id='BOOKING_ID',
                custody_status=CustodyStatus.IN_CUSTODY,
                charges=[Charge(
                    status=ChargeStatus.PENDING,
                    bond=Bond(status=BondStatus.ACTIVE, bond_type=BondType.CASH,
                              amount_dollars=100)
                )]
            )]
        )]

        self.assertEqual(result, expected_result)

    def testConvert_TotalBondWithCharge_SetsTotalBondOnCharge(self):
        # Arrange
        metadata = IngestMetadata.new_with_defaults(
            last_seen_time='LAST_SEEN_TIME'
        )

        ingest_info = IngestInfo()
        ingest_info.people.add(booking_ids=['BOOKING_ID'])
        ingest_info.bookings.add(booking_id='BOOKING_ID',
                                 total_bond_amount='$100',
                                 charge_ids=['CHARGE_ID'])
        ingest_info.charges.add(charge_id='CHARGE_ID')

        # Act
        result = converter.convert(ingest_info, metadata)

        # Assert
        expected_result = [Person.new_with_defaults(
            bookings=[Booking.new_with_defaults(
                external_id='BOOKING_ID',
                admission_date='LAST_SEEN_TIME',
                admission_date_inferred=True,
                last_seen_time='LAST_SEEN_TIME',
                custody_status=CustodyStatus.IN_CUSTODY,
                charges=[Charge(
                    external_id='CHARGE_ID',
                    status=ChargeStatus.PENDING,
                    bond=Bond(amount_dollars=100, status=BondStatus.ACTIVE,
                              bond_type=BondType.CASH)
                )]
            )]
        )]

        self.assertEqual(result, expected_result)

    def testConvert_TotalBondWithMultipleBonds_ThrowsException(self):
        # Arrange
        metadata = IngestMetadata.new_with_defaults()

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
            converter.convert(ingest_info, metadata)

    def testConvert_CannotConvertField_RaisesValueError(self):
        # Arrange
        metadata = IngestMetadata.new_with_defaults()

        ingest_info = IngestInfo()
        ingest_info.people.add(birthdate='NOT_A_DATE')

        # Act + Assert
        with self.assertRaises(ValueError):
            converter.convert(ingest_info, metadata)
