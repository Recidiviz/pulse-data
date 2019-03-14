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
"""Tests for converting bookings."""
import unittest
from datetime import date, datetime

from recidiviz.common.constants.booking import ReleaseReason, CustodyStatus, \
    Classification, AdmissionReason
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence import entities
from recidiviz.persistence.converter import booking

_INGEST_TIME = datetime(year=2019, month=2, day=18)


class BookingConverterTest(unittest.TestCase):
    """Tests for converting bookings."""

    def setUp(self):
        self.subject = entities.Booking.builder()

    def testParseBooking(self):
        # Arrange
        metadata = IngestMetadata.new_with_defaults(
            ingest_time=_INGEST_TIME
        )

        ingest_booking = ingest_info_pb2.Booking(
            booking_id='BOOKING_ID',
            admission_date='2/3/1000',
            admission_reason='New Commitment',
            release_date='1/2/3333',
            projected_release_date='5/20/2222',
            release_reason='Transfer',
            custody_status='Held Elsewhere',
            classification='Low'
        )

        # Act
        booking.copy_fields_to_builder(self.subject, ingest_booking, metadata)
        result = self.subject.build()

        # Assert
        expected_result = entities.Booking.new_with_defaults(
            external_id='BOOKING_ID',
            admission_date=date(year=1000, month=2, day=3),
            admission_reason=AdmissionReason.NEW_COMMITMENT,
            admission_reason_raw_text='NEW COMMITMENT',
            admission_date_inferred=False,
            release_date=date(year=3333, month=1, day=2),
            release_date_inferred=False,
            projected_release_date=date(year=2222, month=5, day=20),
            release_reason=ReleaseReason.TRANSFER,
            release_reason_raw_text='TRANSFER',
            custody_status=CustodyStatus.HELD_ELSEWHERE,
            custody_status_raw_text='HELD ELSEWHERE',
            classification=Classification.LOW,
            classification_raw_text='LOW',
            last_seen_time=_INGEST_TIME,
        )

        self.assertEqual(result, expected_result)

    def testParseBooking_SetsDefaults(self):
        # Arrange
        metadata = IngestMetadata.new_with_defaults(
            ingest_time=_INGEST_TIME,
        )
        ingest_booking = ingest_info_pb2.Booking()

        # Act
        booking.copy_fields_to_builder(self.subject, ingest_booking, metadata)
        result = self.subject.build()

        # Assert
        expected_result = entities.Booking.new_with_defaults(
            admission_date=_INGEST_TIME.date(),
            admission_date_inferred=True,
            last_seen_time=_INGEST_TIME,
            custody_status=CustodyStatus.UNKNOWN_FOUND_IN_SOURCE
        )

        self.assertEqual(result, expected_result)
