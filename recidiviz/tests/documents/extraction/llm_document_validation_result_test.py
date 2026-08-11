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
from recidiviz.documents.extraction.models.llm_request_output_schema import (
    LLMRequestOutputSchema,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    LLMOutputFieldType,
    PrimitiveScalarLLMRequestOutputSchemaField,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    IS_RELEVANT_FIELD_NAME,
    RESULT_KEY,
)
from recidiviz.documents.extraction.models.llm_request_output_values import (
    LLMRequestOutputValues,
)

_VALIDATION_DATETIME = datetime.datetime(2026, 1, 1, 12, 0, tzinfo=pytz.UTC)
_ISSUE = ValidationIssue(
    check_type=ValidationCheckType.SCHEMA_CONFORMANCE,
    field_name="location",
    detail="missing required field",
)
_OUTPUT_SCHEMA = LLMRequestOutputSchema(
    full_batch_description="Batch of parsed fake documents for testing.",
    result_level_description="One parsed fake document for testing.",
    relevance_criteria="Whether the document is a test fixture.",
    user_defined_fields=[
        PrimitiveScalarLLMRequestOutputSchemaField(
            name="status_note",
            description="A bare note about status.",
            required=True,
            inferred_field_config=None,
            scalar_type=LLMOutputFieldType.STRING,
        )
    ],
)


def _validation_result(
    *,
    validated_output_json: dict | None,
    result_type_override: LLMExtractionJobDocumentResultType | None,
    audit_issues: list[ValidationIssue],
) -> LLMDocumentValidationResult:
    return LLMDocumentValidationResult(
        validated_output=(
            None
            if validated_output_json is None
            else LLMRequestOutputValues(
                output_schema=_OUTPUT_SCHEMA, output_json=validated_output_json
            )
        ),
        audit_issues=audit_issues,
        result_type_override=result_type_override,
        validation_config_version_id="vc1",
        validation_datetime_utc=_VALIDATION_DATETIME,
    )


class LLMDocumentValidationResultTest(unittest.TestCase):
    """Tests for the derived properties of LLMDocumentValidationResult."""

    def test_passed_validation_tracks_validated_output(self) -> None:
        self.assertTrue(
            _validation_result(
                validated_output_json={RESULT_KEY: {IS_RELEVANT_FIELD_NAME: True}},
                result_type_override=None,
                audit_issues=[],
            ).passed_validation
        )
        self.assertFalse(
            _validation_result(
                validated_output_json=None,
                result_type_override=LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT,
                audit_issues=[_ISSUE],
            ).passed_validation
        )

    def test_is_relevant_delegates_to_validated_output(self) -> None:
        # Relevance is read off the validated output rather than tracked
        # separately, and is unknown when nothing validated.
        for is_relevant in (True, False):
            with self.subTest(is_relevant=is_relevant):
                self.assertIs(
                    is_relevant,
                    _validation_result(
                        validated_output_json={
                            RESULT_KEY: {IS_RELEVANT_FIELD_NAME: is_relevant}
                        },
                        result_type_override=None,
                        audit_issues=[],
                    ).is_relevant,
                )
        self.assertIsNone(
            _validation_result(
                validated_output_json=None,
                result_type_override=LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT,
                audit_issues=[_ISSUE],
            ).is_relevant
        )

    def test_result_type_falls_back_to_success(self) -> None:
        # An override is passed through; its absence resolves to SUCCESS.
        self.assertEqual(
            LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT,
            _validation_result(
                validated_output_json=None,
                result_type_override=LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT,
                audit_issues=[_ISSUE],
            ).result_type,
        )
        self.assertEqual(
            LLMExtractionJobDocumentResultType.SUCCESS,
            _validation_result(
                validated_output_json={RESULT_KEY: {IS_RELEVANT_FIELD_NAME: True}},
                result_type_override=None,
                audit_issues=[],
            ).result_type,
        )

    def test_will_retry_only_on_transient_override(self) -> None:
        self.assertTrue(
            _validation_result(
                validated_output_json=None,
                result_type_override=LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT,
                audit_issues=[_ISSUE],
            ).will_retry
        )
        # A clean pass and a permanent failure both mean no retry.
        self.assertFalse(
            _validation_result(
                validated_output_json={RESULT_KEY: {IS_RELEVANT_FIELD_NAME: True}},
                result_type_override=None,
                audit_issues=[],
            ).will_retry
        )
        self.assertFalse(
            _validation_result(
                validated_output_json=None,
                result_type_override=LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_PERMANENT,
                audit_issues=[_ISSUE],
            ).will_retry
        )
