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
"""Tests for converting state assessments."""

import unittest
from datetime import date

from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentParticipationStatus,
    StateProgramAssignmentDischargeReason,
)
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.state.entity_helpers import (
    state_program_assignment,
)

_EMPTY_METADATA = IngestMetadata.new_with_defaults()


class StateProgramAssignmentConverterTest(unittest.TestCase):
    """Tests for converting program assignments."""

    def testParseProgramAssignment(self):
        # Arrange
        ingest_program_assignment = ingest_info_pb2.StateProgramAssignment(
            participation_status="IN PROGRESS",
            discharge_reason="COMPLETED",
            state_program_assignment_id="PROGRAM_ASSIGNMENT_ID",
            referral_date="1/2/2111",
            start_date="1/3/2111",
            discharge_date="1/4/2111",
            program_id="PROGRAM_ID",
            program_location_id="LOCATION_ID",
            state_code="US_ND",
        )

        # Act
        program_assignment_builder = entities.StateProgramAssignment.builder()
        state_program_assignment.copy_fields_to_builder(
            program_assignment_builder, ingest_program_assignment, _EMPTY_METADATA
        )
        result = program_assignment_builder.build()

        # Assert
        expected_result = entities.StateProgramAssignment.new_with_defaults(
            participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
            participation_status_raw_text="IN PROGRESS",
            discharge_reason=StateProgramAssignmentDischargeReason.COMPLETED,
            discharge_reason_raw_text="COMPLETED",
            external_id="PROGRAM_ASSIGNMENT_ID",
            referral_date=date(year=2111, month=1, day=2),
            start_date=date(year=2111, month=1, day=3),
            discharge_date=date(year=2111, month=1, day=4),
            program_id="PROGRAM_ID",
            program_location_id="LOCATION_ID",
            state_code="US_ND",
        )

        self.assertEqual(result, expected_result)
