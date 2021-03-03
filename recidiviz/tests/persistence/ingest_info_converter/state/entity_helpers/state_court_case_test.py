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
"""Tests for converting state court cases."""

import unittest
from datetime import date

from recidiviz.common.constants.state.state_court_case import (
    StateCourtCaseStatus,
    StateCourtType,
)
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.state.entity_helpers import (
    state_court_case,
)

_EMPTY_METADATA = IngestMetadata.new_with_defaults()


class StateCourtCaseConverterTest(unittest.TestCase):
    """Tests for converting charges."""

    def testParseStateCourtCase(self):
        # Arrange
        ingest_case = ingest_info_pb2.StateCourtCase(
            status=None,
            court_type=None,
            state_court_case_id="CASE_ID",
            date_convicted="1/2/2111",
            next_court_date="1/10/2111",
            state_code="us_nd",
            county_code="111",
            court_fee_dollars="1000",
        )

        # Act
        court_case_builder = entities.StateCourtCase.builder()
        state_court_case.copy_fields_to_builder(
            court_case_builder, ingest_case, _EMPTY_METADATA
        )
        result = court_case_builder.build()

        # Assert
        expected_result = entities.StateCourtCase.new_with_defaults(
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            status_raw_text=None,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            court_type_raw_text=None,
            external_id="CASE_ID",
            date_convicted=date(year=2111, month=1, day=2),
            next_court_date=date(year=2111, month=1, day=10),
            state_code="US_ND",
            county_code="111",
            court_fee_dollars=1000,
        )

        self.assertEqual(result, expected_result)
