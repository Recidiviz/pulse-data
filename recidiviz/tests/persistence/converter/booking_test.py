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
from datetime import datetime

from recidiviz.common.constants.booking import ReleaseReason, CustodyStatus, \
    Classification
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence import entities
from recidiviz.persistence.converter import booking


class BookingConverterTest(unittest.TestCase):
    """Tests for converting bookings."""

    def testParseBooking(self):
        # Arrange
        metadata = IngestMetadata.new_with_none_defaults(
            last_seen_time='LAST_SEEN_TIME'
        )

        ingest_booking = ingest_info_pb2.Booking(
            booking_id='BOOKING_ID',
            release_date='1/1/1111',
            projected_release_date='2/2/2222',
            release_reason='Transfer',
            custody_status='Held Elsewhere',
            classification='Low'
        )

        # Act
        result = booking.convert(ingest_booking, metadata)

        # Assert
        expected_result = entities.Booking(
            external_id='BOOKING_ID',
            release_date=datetime(year=1111, month=1, day=1),
            release_date_inferred=False,
            projected_release_date=datetime(year=2222, month=2, day=2),
            release_reason=ReleaseReason.TRANSFER,
            custody_status=CustodyStatus.HELD_ELSEWHERE,
            classification=Classification.LOW,
            last_seen_time='LAST_SEEN_TIME'
        )

        self.assertEqual(result, expected_result)

    def testParseBooking_SetsDefaults(self):
        # Arrange
        metadata = IngestMetadata.new_with_none_defaults()
        ingest_booking = ingest_info_pb2.Booking()

        # Act
        result = booking.convert(ingest_booking, metadata)

        # Assert
        expected_result = entities.Booking(
            custody_status=CustodyStatus.IN_CUSTODY
        )

        self.assertEqual(result, expected_result)
