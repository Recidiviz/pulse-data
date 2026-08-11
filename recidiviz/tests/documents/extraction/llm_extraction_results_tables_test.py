# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for the LLM extraction result BQ table definitions."""
import datetime
import unittest

import pytz

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.llm_extraction_results_tables import (
    ExtractionRawResultsBQTable,
    ExtractionValidatedResultsBQTable,
    ExtractionValidationAuditBQTable,
)

_STATE_CODE = StateCode.US_XX
_JOB_ID = "job1"
_RESULT_DATETIME = datetime.datetime(2026, 1, 1, 12, 0, tzinfo=pytz.UTC)
_VALIDATION_DATETIME = datetime.datetime(2026, 1, 1, 12, 5, tzinfo=pytz.UTC)
_RAW_RESULT_JSON = {"is_relevant": True, "location": {"value": "here"}}


class RowMatchesSchemaTest(unittest.TestCase):
    """Guards that each table's to_row emits exactly the columns its schema
    declares, so a schema column added without adapting to_row (or vice versa)
    fails loudly rather than silently dropping data."""

    def test_raw_row_matches_schema(self) -> None:
        row = ExtractionRawResultsBQTable.to_row(
            state_code_str=_STATE_CODE.value,
            job_id=_JOB_ID,
            extractor_id="ex1",
            extractor_version_id="ev1",
            document_contents_id="doc1",
            result_datetime_utc=_RESULT_DATETIME,
            result_json=_RAW_RESULT_JSON,
        )
        self.assertEqual(
            {f.name for f in ExtractionRawResultsBQTable.schema()}, set(row)
        )

    def test_validated_row_matches_schema(self) -> None:
        row = ExtractionValidatedResultsBQTable.to_row(
            state_code_str=_STATE_CODE.value,
            document_contents_id="doc1",
            job_id=_JOB_ID,
            extractor_version_id="ev1",
            validation_config_version_id="vc1",
            validation_datetime_utc=_VALIDATION_DATETIME,
            is_relevant=True,
            validated_output_json={"is_relevant": True},
        )
        self.assertEqual(
            {f.name for f in ExtractionValidatedResultsBQTable.schema()}, set(row)
        )

    def test_audit_row_matches_schema(self) -> None:
        row = ExtractionValidationAuditBQTable.to_row(
            state_code_str=_STATE_CODE.value,
            document_contents_id="doc1",
            job_id=_JOB_ID,
            extractor_version_id="ev1",
            validation_config_version_id="vc1",
            validation_datetime_utc=_VALIDATION_DATETIME,
            passed_validation=True,
            will_retry=False,
            is_relevant=True,
            audit_issues_json=[],
        )
        self.assertEqual(
            {f.name for f in ExtractionValidationAuditBQTable.schema()}, set(row)
        )
