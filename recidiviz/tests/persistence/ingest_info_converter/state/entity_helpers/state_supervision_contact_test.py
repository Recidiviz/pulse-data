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
"""Tests for converting state supervision contacts."""

import unittest
from datetime import date

from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactLocation,
    StateSupervisionContactReason,
    StateSupervisionContactStatus,
    StateSupervisionContactType,
)
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.entity.state.deserialize_entity_factories import (
    StateSupervisionContactFactory,
)
from recidiviz.persistence.ingest_info_converter.state.entity_helpers import (
    state_supervision_contact,
)
from recidiviz.tests.persistence.database.database_test_utils import FakeIngestMetadata

_EMPTY_METADATA = FakeIngestMetadata.for_state("us_ca")


class StateSupervisionContactConverterTest(unittest.TestCase):
    """Tests for converting supervision contacts."""

    def testParseStateSupervisionContacts(self):
        # Arrange
        ingest_contact = ingest_info_pb2.StateSupervisionContact(
            state_supervision_contact_id="CONTACT_ID",
            contact_type="FACE_TO_FACE",
            status="COMPLETED",
            contact_reason="GENERAL_CONTACT",
            location="RESIDENCE",
            contact_date="1/2/1111",
            state_code="us_ca",
            verified_employment="True",
            resulted_in_arrest="False",
        )

        # Act
        contact_builder = entities.StateSupervisionContact.builder()
        state_supervision_contact.copy_fields_to_builder(
            contact_builder, ingest_contact, _EMPTY_METADATA
        )
        result = contact_builder.build(StateSupervisionContactFactory.deserialize)

        # Assert
        expected_result = entities.StateSupervisionContact(
            external_id="CONTACT_ID",
            status=StateSupervisionContactStatus.COMPLETED,
            status_raw_text="COMPLETED",
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            contact_type_raw_text="FACE_TO_FACE",
            contact_date=date(year=1111, month=1, day=2),
            state_code="US_CA",
            contact_reason=StateSupervisionContactReason.GENERAL_CONTACT,
            contact_reason_raw_text="GENERAL_CONTACT",
            location=StateSupervisionContactLocation.RESIDENCE,
            location_raw_text="RESIDENCE",
            verified_employment=True,
            resulted_in_arrest=False,
        )

        self.assertEqual(result, expected_result)
