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
"""Tests for converting state supervision violations."""

import unittest
from datetime import date

from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.state.entity_helpers import (
    state_supervision_violation,
)
from recidiviz.tests.persistence.database.database_test_utils import FakeIngestMetadata

_EMPTY_METADATA = FakeIngestMetadata.for_state("us_nd")


class StateSupervisionViolationConverterTest(unittest.TestCase):
    """Tests for converting state supervision violations."""

    def testParseStateSupervisionViolation(self):
        # Arrange
        ingest_violation = ingest_info_pb2.StateSupervisionViolation(
            state_supervision_violation_id="VIOLATION_ID",
            violation_date="1/2/2111",
            state_code="us_nd",
            is_violent="false",
            violated_conditions="CURFEW, TOX-SCREEN",
        )

        # Act
        violation_builder = entities.StateSupervisionViolation.builder()
        state_supervision_violation.copy_fields_to_builder(
            violation_builder, ingest_violation, _EMPTY_METADATA
        )
        result = violation_builder.build()

        # Assert
        expected_result = entities.StateSupervisionViolation.new_with_defaults(
            external_id="VIOLATION_ID",
            violation_date=date(year=2111, month=1, day=2),
            state_code="US_ND",
            is_violent=False,
            violated_conditions="CURFEW, TOX-SCREEN",
        )

        self.assertEqual(result, expected_result)
