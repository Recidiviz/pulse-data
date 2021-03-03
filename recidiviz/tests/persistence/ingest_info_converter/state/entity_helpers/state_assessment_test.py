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

from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentType,
    StateAssessmentLevel,
)
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.state.entity_helpers import (
    state_assessment,
)

_EMPTY_METADATA = IngestMetadata.new_with_defaults()


class StateAssessmentConverterTest(unittest.TestCase):
    """Tests for converting assessments."""

    def testParseStateAssessment(self):
        # Arrange
        ingest_assessment = ingest_info_pb2.StateAssessment(
            assessment_class="RISK",
            assessment_type="LSIR",
            state_assessment_id="ASSESSMENT_ID",
            assessment_date="1/2/2111",
            state_code="US_ND",
            assessment_score="17",
            assessment_level="MEDIUM",
            assessment_metadata='{"high_score_domains": ["a", "c", "q"]}',
        )

        # Act
        assessment_builder = entities.StateAssessment.builder()
        state_assessment.copy_fields_to_builder(
            assessment_builder, ingest_assessment, _EMPTY_METADATA
        )
        result = assessment_builder.build()

        # Assert
        expected_result = entities.StateAssessment.new_with_defaults(
            assessment_class=StateAssessmentClass.RISK,
            assessment_class_raw_text="RISK",
            assessment_type=StateAssessmentType.LSIR,
            assessment_type_raw_text="LSIR",
            assessment_level=StateAssessmentLevel.MEDIUM,
            assessment_level_raw_text="MEDIUM",
            external_id="ASSESSMENT_ID",
            assessment_date=date(year=2111, month=1, day=2),
            state_code="US_ND",
            assessment_score=17,
            assessment_metadata='{"HIGH_SCORE_DOMAINS": ["A", "C", "Q"]}',
        )

        self.assertEqual(result, expected_result)
