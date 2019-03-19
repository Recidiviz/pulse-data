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
import datetime

from recidiviz.common.constants.bond import BondType, BondStatus
from recidiviz.common.constants.booking import CustodyStatus
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.hold import HoldStatus
from recidiviz.common.constants.sentence import SentenceStatus
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models.ingest_info_pb2 import IngestInfo
from recidiviz.persistence.converter import converter
from recidiviz.persistence.entities import Person, Booking, Arrest, Charge, \
    Bond, Sentence, Hold

_INGEST_TIME = datetime.datetime(year=2019, month=2, day=13, hour=12)
_RELEASE_DATE = datetime.date(year=2018, month=3, day=1)
_BIRTHDATE = datetime.date(1990, 3, 5)
_BIRTHDATE_SCRUBBED = datetime.date(1990, 1, 1)
_JURISDICTION_ID = 'JURISDICTION_ID'


class TestConverter(unittest.TestCase):
    """Test converting IngestInfo objects to Persistence layer objects."""

    def testConvert_FullIngestInfo(self):
        # Arrange
        metadata = IngestMetadata('REGION', _JURISDICTION_ID, _INGEST_TIME)

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
            jurisdiction_id='JURISDICTION_ID',
            bookings=[Booking.new_with_defaults(
                external_id='BOOKING_ID',
                admission_date=_INGEST_TIME.date(),
                admission_date_inferred=True,
                last_seen_time=_INGEST_TIME,
                custody_status=CustodyStatus.PRESENT_WITHOUT_INFO,
                arrest=Arrest.new_with_defaults(
                    external_id='ARREST_ID',
                    agency='PD'
                ),
                charges=[Charge.new_with_defaults(
                    external_id='CHARGE_ID',
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    name='DUI',
                    bond=Bond.new_with_defaults(
                        external_id='BOND_ID',
                        status=BondStatus.PRESENT_WITHOUT_INFO
                    ),
                    sentence=Sentence.new_with_defaults(
                        status=SentenceStatus.PRESENT_WITHOUT_INFO,
                        external_id='SENTENCE_ID',
                        is_life=True
                    )
                )]
            )])]

        self.assertEqual(result, expected_result)

    def testConvert_FullIngestInfo_usingPop(self):
        # Arrange
        metadata = IngestMetadata('REGION', _JURISDICTION_ID, _INGEST_TIME)

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
        ii_converter = converter.Converter(ingest_info, metadata)
        result = []
        while not ii_converter.is_complete():
            result.append(ii_converter.convert_and_pop())

        # Assert
        expected_result = [Person.new_with_defaults(
            external_id='PERSON_ID',
            region='REGION',
            jurisdiction_id='JURISDICTION_ID',
            bookings=[Booking.new_with_defaults(
                external_id='BOOKING_ID',
                admission_date=_INGEST_TIME.date(),
                admission_date_inferred=True,
                last_seen_time=_INGEST_TIME,
                custody_status=CustodyStatus.PRESENT_WITHOUT_INFO,
                arrest=Arrest.new_with_defaults(
                    external_id='ARREST_ID',
                    agency='PD'
                ),
                charges=[Charge.new_with_defaults(
                    external_id='CHARGE_ID',
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    name='DUI',
                    bond=Bond.new_with_defaults(
                        external_id='BOND_ID',
                        status=BondStatus.PRESENT_WITHOUT_INFO
                    ),
                    sentence=Sentence.new_with_defaults(
                        status=SentenceStatus.PRESENT_WITHOUT_INFO,
                        external_id='SENTENCE_ID',
                        is_life=True
                    )
                )]
            )])]

        self.assertEqual(result, expected_result)

    def testConvert_FullIngestInfo_NoOpenBookings(self):
        # Arrange
        metadata = IngestMetadata('REGION', _JURISDICTION_ID, _INGEST_TIME)

        ingest_info = IngestInfo()
        ingest_info.people.add(person_id='PERSON_ID', full_name='TEST',
                               birthdate=str(_BIRTHDATE),
                               booking_ids=['BOOKING_ID'])
        ingest_info.bookings.add(booking_id='BOOKING_ID',
                                 arrest_id='ARREST_ID',
                                 release_date=str(_RELEASE_DATE),
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
            jurisdiction_id='JURISDICTION_ID',
            birthdate=_BIRTHDATE_SCRUBBED,
            birthdate_inferred_from_age=False,
            bookings=[Booking.new_with_defaults(
                external_id='BOOKING_ID',
                admission_date=_INGEST_TIME.date(),
                admission_date_inferred=True,
                release_date=_RELEASE_DATE,
                release_date_inferred=False,
                last_seen_time=_INGEST_TIME,
                custody_status=CustodyStatus.RELEASED,
                arrest=Arrest.new_with_defaults(
                    external_id='ARREST_ID',
                    agency='PD'
                ),
                charges=[Charge.new_with_defaults(
                    external_id='CHARGE_ID',
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    name='DUI',
                    bond=Bond.new_with_defaults(
                        external_id='BOND_ID',
                        status=BondStatus.PRESENT_WITHOUT_INFO
                    ),
                    sentence=Sentence.new_with_defaults(
                        status=SentenceStatus.PRESENT_WITHOUT_INFO,
                        external_id='SENTENCE_ID',
                        is_life=True
                    )
                )]
            )])]

        self.assertEqual(result, expected_result)

    def testConvert_FullIngestInfo_GeneratedIds(self):
        # Arrange
        metadata = IngestMetadata('REGION', _JURISDICTION_ID, _INGEST_TIME)

        ingest_info = IngestInfo()
        ingest_info.people.add(person_id='PERSON_ID_GENERATE',
                               booking_ids=['BOOKING_ID_GENERATE'])
        ingest_info.bookings.add(booking_id='BOOKING_ID_GENERATE',
                                 arrest_id='ARREST_ID_GENERATE',
                                 hold_ids=['HOLD_ID_1_GENERATE',
                                           'HOLD_ID_2_GENERATE'],
                                 charge_ids=['CHARGE_ID_GENERATE'])
        ingest_info.holds.add(
            hold_id='HOLD_ID_1_GENERATE', jurisdiction_name='jurisdiction')
        ingest_info.holds.add(
            hold_id='HOLD_ID_2_GENERATE', jurisdiction_name='jurisdiction')
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
            jurisdiction_id='JURISDICTION_ID',
            bookings=[Booking.new_with_defaults(
                admission_date=_INGEST_TIME.date(),
                admission_date_inferred=True,
                last_seen_time=_INGEST_TIME,
                custody_status=CustodyStatus.PRESENT_WITHOUT_INFO,
                arrest=Arrest.new_with_defaults(agency='PD'),
                holds=[
                    Hold.new_with_defaults(
                        jurisdiction_name='JURISDICTION',
                        status=HoldStatus.PRESENT_WITHOUT_INFO)],
                charges=[Charge.new_with_defaults(
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    name='DUI',
                    bond=Bond.new_with_defaults(
                        status=BondStatus.PRESENT_WITHOUT_INFO),
                    sentence=Sentence.new_with_defaults(
                        status=SentenceStatus.PRESENT_WITHOUT_INFO,
                        is_life=True)
                )]
            )])]

        self.assertEqual(result, expected_result)

    def testConvert_TotalBondNoCharge_CreatesChargeWithTotalBondAmount(self):
        # Arrange
        metadata = IngestMetadata.new_with_defaults(
            ingest_time=_INGEST_TIME)

        ingest_info = IngestInfo()
        ingest_info.people.add(booking_ids=['BOOKING_ID'])
        ingest_info.bookings.add(booking_id='BOOKING_ID',
                                 total_bond_amount='$100')

        # Act
        result = converter.convert(ingest_info, metadata)

        # Assert
        expected_result = [Person.new_with_defaults(
            bookings=[Booking.new_with_defaults(
                admission_date=_INGEST_TIME.date(),
                admission_date_inferred=True,
                last_seen_time=_INGEST_TIME,
                external_id='BOOKING_ID',
                custody_status=CustodyStatus.PRESENT_WITHOUT_INFO,
                charges=[Charge.new_with_defaults(
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    bond=Bond.new_with_defaults(
                        status=BondStatus.PRESENT_WITHOUT_INFO,
                        bond_type=BondType.CASH,
                        amount_dollars=100
                    )
                )]
            )]
        )]

        self.assertEqual(result, expected_result)

    def testConvert_TotalBondWithCharge_SetsTotalBondOnCharge(self):
        # Arrange
        metadata = IngestMetadata.new_with_defaults(
            ingest_time=_INGEST_TIME
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
                admission_date=_INGEST_TIME.date(),
                admission_date_inferred=True,
                last_seen_time=_INGEST_TIME,
                custody_status=CustodyStatus.PRESENT_WITHOUT_INFO,
                charges=[Charge.new_with_defaults(
                    external_id='CHARGE_ID',
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    bond=Bond.new_with_defaults(
                        amount_dollars=100,
                        status=BondStatus.PRESENT_WITHOUT_INFO,
                        bond_type=BondType.CASH
                    )
                )]
            )]
        )]

        self.assertEqual(result, expected_result)

    def testConvert_TotalBondWithMultipleBonds_ThrowsException(self):
        # Arrange
        metadata = IngestMetadata.new_with_defaults(
            ingest_time=_INGEST_TIME)

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

    def testConvert_MultipleCountsOfCharge_CreatesDuplicateCharges(self):
        # Arrange
        metadata = IngestMetadata.new_with_defaults(
            ingest_time=_INGEST_TIME)

        ingest_info = IngestInfo()
        ingest_info.people.add(booking_ids=['BOOKING_ID'])
        ingest_info.bookings.add(booking_id='BOOKING_ID',
                                 charge_ids=['CHARGE_ID'])
        ingest_info.charges.add(charge_id='CHARGE_ID',
                                name='CHARGE_NAME',
                                number_of_counts='3')

        # Act
        result = converter.convert(ingest_info, metadata)

        # Assert
        expected_duplicate_charge = Charge.new_with_defaults(
            external_id='CHARGE_ID',
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            name='CHARGE_NAME'
        )

        expected_result = [Person.new_with_defaults(
            bookings=[Booking.new_with_defaults(
                external_id='BOOKING_ID',
                last_seen_time=_INGEST_TIME,
                admission_date_inferred=True,
                admission_date=_INGEST_TIME.date(),
                custody_status=CustodyStatus.PRESENT_WITHOUT_INFO,
                charges=[
                    expected_duplicate_charge,
                    expected_duplicate_charge,
                    expected_duplicate_charge
                ]
            )]
        )]

        self.assertEqual(result, expected_result)

    def testConvert_CannotConvertField_RaisesValueError(self):
        # Arrange
        metadata = IngestMetadata.new_with_defaults()

        ingest_info = IngestInfo()
        ingest_info.people.add(birthdate='NOT_A_DATE')

        # Act + Assert
        with self.assertRaises(ValueError):
            converter.convert(ingest_info, metadata)

    def testConvert_PersonInferredBooking(self):
        # Arrange
        metadata = IngestMetadata.new_with_defaults(
            ingest_time=_INGEST_TIME)

        ingest_info = IngestInfo()
        ingest_info.people.add()

        # Act
        result = converter.convert(ingest_info, metadata)

        # Assert
        expected_result = [Person.new_with_defaults(
            bookings=[Booking.new_with_defaults(
                admission_date_inferred=True,
                last_seen_time=_INGEST_TIME,
                admission_date=_INGEST_TIME.date(),
                custody_status=CustodyStatus.PRESENT_WITHOUT_INFO,
            )]
        )]

        self.assertEqual(result, expected_result)
