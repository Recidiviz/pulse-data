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
"""Tests for LLMDocumentValidationResult."""
import datetime
import unittest

import pytz

from recidiviz.common.constants.operations.llm_extraction_job import (
    LLMExtractionJobDocumentResultType,
)
from recidiviz.documents.extraction.llm_document_validation_result import (
    LLMDocumentValidationResult,
    ValidationCheckType,
    ValidationIssue,
)

_VALIDATION_DATETIME = datetime.datetime(2026, 1, 1, 12, 0, tzinfo=pytz.UTC)
_ISSUE = ValidationIssue(
    check_type=ValidationCheckType.SCHEMA_CONFORMANCE,
    field_name="location",
    detail="missing required field",
)


def _validation_result(
    *,
    validated_content: dict | None,
    result_type_override: LLMExtractionJobDocumentResultType | None,
    audit_issues: list[ValidationIssue],
) -> LLMDocumentValidationResult:
    return LLMDocumentValidationResult(
        validated_content=validated_content,
        audit_issues=audit_issues,
        result_type_override=result_type_override,
        validation_config_version_id="vc1",
        validation_datetime_utc=_VALIDATION_DATETIME,
    )


class LLMDocumentValidationResultTest(unittest.TestCase):
    """Tests for the derived properties of LLMDocumentValidationResult."""

    def test_passed_validation_tracks_validated_content(self) -> None:
        self.assertTrue(
            _validation_result(
                validated_content={"is_relevant": True},
                result_type_override=None,
                audit_issues=[],
            ).passed_validation
        )
        self.assertFalse(
            _validation_result(
                validated_content=None,
                result_type_override=LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT,
                audit_issues=[_ISSUE],
            ).passed_validation
        )

    def test_will_retry_only_on_transient_override(self) -> None:
        self.assertTrue(
            _validation_result(
                validated_content=None,
                result_type_override=LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT,
                audit_issues=[_ISSUE],
            ).will_retry
        )
        # A clean pass and a permanent failure both mean no retry.
        self.assertFalse(
            _validation_result(
                validated_content={"is_relevant": True},
                result_type_override=None,
                audit_issues=[],
            ).will_retry
        )
        self.assertFalse(
            _validation_result(
                validated_content=None,
                result_type_override=LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_PERMANENT,
                audit_issues=[_ISSUE],
            ).will_retry
        )
