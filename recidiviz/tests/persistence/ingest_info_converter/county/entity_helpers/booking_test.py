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
"""Tests for converting bookings."""
import unittest
from datetime import date, datetime
from mock import patch, Mock


from recidiviz.common.constants.county.booking import (
    ReleaseReason,
    CustodyStatus,
    Classification,
    AdmissionReason,
)
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence.entity.county import entities
from recidiviz.persistence.ingest_info_converter.county.entity_helpers import booking
from recidiviz.tests.persistence.database.database_test_utils import TestIngestMetadata

_INGEST_TIME = datetime(year=2019, month=2, day=18)


class BookingConverterTest(unittest.TestCase):
    """Tests for converting bookings."""

    def setUp(self):
        self.subject = entities.Booking.builder()

    def testParseBooking(self):
        # Arrange
        metadata = TestIngestMetadata.for_county(
            region="REGION", ingest_time=_INGEST_TIME
        )

        ingest_booking = ingest_info_pb2.Booking(
            booking_id="BOOKING_ID",
            admission_date="2/3/1000",
            admission_reason="New Commitment",
            release_date="1/2/2017",
            projected_release_date="5/20/2020",
            release_reason="Transfer",
            custody_status="Held Elsewhere",
            classification="Low",
        )

        # Act
        booking.copy_fields_to_builder(self.subject, ingest_booking, metadata)
        result = self.subject.build()

        # Assert
        expected_result = entities.Booking.new_with_defaults(
            external_id="BOOKING_ID",
            admission_date=date(year=1000, month=2, day=3),
            admission_reason=AdmissionReason.NEW_COMMITMENT,
            admission_reason_raw_text="NEW COMMITMENT",
            admission_date_inferred=False,
            release_date=date(year=2017, month=1, day=2),
            release_date_inferred=False,
            projected_release_date=date(year=2020, month=5, day=20),
            release_reason=ReleaseReason.TRANSFER,
            release_reason_raw_text="TRANSFER",
            custody_status=CustodyStatus.HELD_ELSEWHERE,
            custody_status_raw_text="HELD ELSEWHERE",
            classification=Classification.LOW,
            classification_raw_text="LOW",
            last_seen_time=_INGEST_TIME,
            first_seen_time=_INGEST_TIME,
        )

        self.assertEqual(result, expected_result)

    def testParseBooking_SetsDefaults(self):
        # Arrange
        metadata = TestIngestMetadata.for_county(
            region="REGION",
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
            first_seen_time=_INGEST_TIME,
            custody_status=CustodyStatus.PRESENT_WITHOUT_INFO,
        )

        self.assertEqual(result, expected_result)

    def testParseBooking_releaseDateWithoutStatus_marksAsReleased(self):
        # Arrange
        metadata = TestIngestMetadata.for_county(
            region="REGION", ingest_time=_INGEST_TIME
        )

        ingest_booking = ingest_info_pb2.Booking(
            booking_id="BOOKING_ID",
            release_date="2/1/2019",
        )

        # Act
        booking.copy_fields_to_builder(self.subject, ingest_booking, metadata)
        result = self.subject.build()

        # Assert
        expected_result = entities.Booking.new_with_defaults(
            external_id="BOOKING_ID",
            admission_date=_INGEST_TIME.date(),
            admission_date_inferred=True,
            release_date=date(year=2019, month=2, day=1),
            release_date_inferred=False,
            custody_status=CustodyStatus.RELEASED,
            last_seen_time=_INGEST_TIME,
            first_seen_time=_INGEST_TIME,
        )

        self.assertEqual(result, expected_result)

    def testParseBooking_releaseDateInFuture_setAsProjected(self):
        # Arrange
        metadata = TestIngestMetadata.for_county(
            region="REGION", ingest_time=_INGEST_TIME
        )

        ingest_booking = ingest_info_pb2.Booking(
            booking_id="BOOKING_ID",
            admission_date="2/3/1000",
            release_date="1/2/2020",
        )

        # Act
        booking.copy_fields_to_builder(self.subject, ingest_booking, metadata)
        result = self.subject.build()

        # Assert
        expected_result = entities.Booking.new_with_defaults(
            external_id="BOOKING_ID",
            admission_date=date(year=1000, month=2, day=3),
            admission_date_inferred=False,
            release_date=None,
            release_date_inferred=None,
            projected_release_date=date(year=2020, month=1, day=2),
            custody_status=CustodyStatus.PRESENT_WITHOUT_INFO,
            last_seen_time=_INGEST_TIME,
            first_seen_time=_INGEST_TIME,
        )

        self.assertEqual(result, expected_result)

    def testParseBooking_MapAcrossFields(self):
        # Arrange
        overrides_builder = EnumOverrides.Builder()
        overrides_builder.add(
            "WORK RELEASE", Classification.WORK_RELEASE, AdmissionReason
        )
        overrides_builder.add("transfer", AdmissionReason.TRANSFER, CustodyStatus)
        metadata = TestIngestMetadata.for_county(
            region="REGION",
            ingest_time=_INGEST_TIME,
            enum_overrides=overrides_builder.build(),
        )
        ingest_booking = ingest_info_pb2.Booking(
            admission_reason="work release", custody_status="transfer"
        )

        # Act
        booking.copy_fields_to_builder(self.subject, ingest_booking, metadata)
        result = self.subject.build()

        # Assert
        expected_result = entities.Booking.new_with_defaults(
            admission_date=_INGEST_TIME.date(),
            admission_reason=AdmissionReason.TRANSFER,
            admission_reason_raw_text="WORK RELEASE",
            admission_date_inferred=True,
            custody_status=CustodyStatus.PRESENT_WITHOUT_INFO,
            custody_status_raw_text="TRANSFER",
            classification=Classification.WORK_RELEASE,
            last_seen_time=_INGEST_TIME,
            first_seen_time=_INGEST_TIME,
        )
        self.assertEqual(result, expected_result)

    @patch("recidiviz.common.fid.fid_exists", Mock(return_value=True))
    def testParseBooking_facilityId(self):
        # Arrange
        metadata = TestIngestMetadata.for_county(
            region="REGION",
            ingest_time=_INGEST_TIME,
        )

        ingest_booking = ingest_info_pb2.Booking(facility_id="1234567890123456")

        # Act
        booking.copy_fields_to_builder(self.subject, ingest_booking, metadata)
        result = self.subject.build()

        # Assert
        expected_result = entities.Booking.new_with_defaults(
            facility_id="1234567890123456",
            admission_date=_INGEST_TIME.date(),
            admission_date_inferred=True,
            last_seen_time=_INGEST_TIME,
            first_seen_time=_INGEST_TIME,
            release_date=None,
            release_date_inferred=None,
            custody_status=CustodyStatus.PRESENT_WITHOUT_INFO,
        )

        self.assertEqual(result, expected_result)

    @patch("recidiviz.common.fid.fid_exists", Mock(return_value=False))
    def testParseBooking_facilityId_doesNotExist(self):
        # Arrange
        metadata = TestIngestMetadata.for_county(
            region="REGION",
            ingest_time=_INGEST_TIME,
        )

        ingest_booking = ingest_info_pb2.Booking(
            facility_id="1234567890123456"  # invalid FID
        )

        with self.assertRaises(ValueError):
            booking.copy_fields_to_builder(self.subject, ingest_booking, metadata)
