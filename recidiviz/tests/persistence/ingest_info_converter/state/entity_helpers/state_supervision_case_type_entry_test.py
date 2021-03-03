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

from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.state.entity_helpers import (
    state_supervision_case_type_entry,
)

_EMPTY_METADATA = IngestMetadata.new_with_defaults()


class StateSupervisionCaseTypeEntryTest(unittest.TestCase):
    """Tests for converting state supervision case type entries."""

    def testParseStateSupervisionViolation(self):
        # Arrange
        ingest_case_type_entry = ingest_info_pb2.StateSupervisionCaseTypeEntry(
            state_supervision_case_type_entry_id="entry_id",
            state_code="state",
            case_type="DOMESTIC_VIOLENCE",
        )

        # Act
        result = state_supervision_case_type_entry.convert(
            ingest_case_type_entry, _EMPTY_METADATA
        )

        # Assert
        expected_result = entities.StateSupervisionCaseTypeEntry(
            state_code="STATE",
            case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
            case_type_raw_text="DOMESTIC_VIOLENCE",
            external_id="ENTRY_ID",
        )

        self.assertEqual(result, expected_result)
