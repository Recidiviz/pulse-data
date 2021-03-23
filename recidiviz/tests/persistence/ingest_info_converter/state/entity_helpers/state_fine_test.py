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
"""Tests for converting state fines."""

import unittest
from datetime import date

from recidiviz.common.constants.state.state_fine import StateFineStatus
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.state.entity_helpers import state_fine
from recidiviz.tests.persistence.database.database_test_utils import TestIngestMetadata

_EMPTY_METADATA = TestIngestMetadata.for_state("us_nd")


class StateFineConverterTest(unittest.TestCase):
    """Tests for converting state fines."""

    def testParseStateFine(self):
        # Arrange
        ingest_fine = ingest_info_pb2.StateFine(
            status="PAID",
            state_fine_id="FINE_ID",
            date_paid="1/10/2111",
            state_code="us_nd",
            county_code="CO",
            fine_dollars="3.50",
        )

        # Act
        fine_builder = entities.StateFine.builder()
        state_fine.copy_fields_to_builder(fine_builder, ingest_fine, _EMPTY_METADATA)
        result = fine_builder.build()

        # Assert
        expected_result = entities.StateFine.new_with_defaults(
            status=StateFineStatus.PAID,
            status_raw_text="PAID",
            external_id="FINE_ID",
            date_paid=date(year=2111, month=1, day=10),
            state_code="US_ND",
            county_code="CO",
            fine_dollars=3,
        )

        self.assertEqual(result, expected_result)
