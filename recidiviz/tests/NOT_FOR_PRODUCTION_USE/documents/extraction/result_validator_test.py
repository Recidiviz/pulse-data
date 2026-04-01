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
"""Tests for result_validator.py."""
import datetime
import json
import unittest

from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.extraction_output_schema import (
    ExtractionFieldType,
    ExtractionInferredField,
    ExtractionOutputSchema,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.document_extraction_result_metadata import (
    DocumentExtractionResultMetadata,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.extraction_exclusion_type import (
    ExtractionExclusionType,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.result_validator import (
    ValidationResult,
    validate_extraction_result,
)

_THRESHOLD = 0.8
_DATETIME = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)


def _make_result(result_data: dict) -> DocumentExtractionResultMetadata:
    return DocumentExtractionResultMetadata(
        job_id="job-1",
        document_id="doc-1",
        extractor_id="US_XX_TEST",
        extractor_version_id="v1",
        extraction_datetime=_DATETIME,
        status="SUCCESS",
        result_json=json.dumps(result_data),
        error_message=None,
        error_type=None,
    )


def _make_schema(
    fields: list[ExtractionInferredField],
) -> ExtractionOutputSchema:
    is_relevant = ExtractionInferredField(
        name="is_relevant",
        field_type=ExtractionFieldType.BOOLEAN,
        unaugmented_description="Whether relevant",
        required=True,
    )
    by_name = {"is_relevant": is_relevant}
    for f in fields:
        by_name[f.name] = f
    return ExtractionOutputSchema(
        full_batch_description="batch",
        result_level_description="result",
        inferred_fields_by_name=by_name,
    )


def _envelope(value: object, confidence: float = 0.9) -> dict:
    return {
        "value": value,
        "confidence_score": confidence,
        "null_reason": None,
        "citations": None,
    }


class TestValidateExtractionResultDocumentLevel(unittest.TestCase):
    """Tests for document-level exclusion in validate_extraction_result."""

    def setUp(self) -> None:
        self.schema = _make_schema(
            [
                ExtractionInferredField(
                    name="employer_name",
                    field_type=ExtractionFieldType.STRING,
                    unaugmented_description="Employer name",
                    required=False,
                ),
                ExtractionInferredField(
                    name="employment_status",
                    field_type=ExtractionFieldType.ENUM,
                    unaugmented_description="Employment status",
                    required=True,
                    enum_values=("EMPLOYED", "UNEMPLOYED"),
                ),
            ]
        )

    def _validate(self, result_data: dict) -> ValidationResult:
        return validate_extraction_result(
            result=_make_result(result_data),
            output_schema=self.schema,
            confidence_threshold=_THRESHOLD,
        )

    def test_valid_result_passes(self) -> None:
        result = self._validate(
            {
                "is_relevant": _envelope(True),
                "employer_name": _envelope("Walmart"),
                "employment_status": _envelope("EMPLOYED"),
            }
        )
        self.assertIsNotNone(result.validated_result_json)
        self.assertEqual(result.failures, [])

    def test_not_relevant_excluded(self) -> None:
        result = self._validate(
            {
                "is_relevant": _envelope(False),
                "employer_name": _envelope("Walmart"),
                "employment_status": _envelope("EMPLOYED"),
            }
        )
        self.assertIsNone(result.validated_result_json)
        self.assertEqual(len(result.failures), 1)
        self.assertEqual(
            result.failures[0].exclusion_type, ExtractionExclusionType.NOT_RELEVANT
        )

    def test_low_confidence_field_excluded(self) -> None:
        result = self._validate(
            {
                "is_relevant": _envelope(True),
                "employer_name": _envelope("Walmart", confidence=0.5),
                "employment_status": _envelope("EMPLOYED"),
            }
        )
        self.assertIsNone(result.validated_result_json)
        self.assertEqual(len(result.failures), 1)
        self.assertEqual(
            result.failures[0].exclusion_type, ExtractionExclusionType.LOW_CONFIDENCE
        )
        assert result.failures[0].exclusion_details_json is not None
        details = json.loads(result.failures[0].exclusion_details_json)
        self.assertEqual(details["field_name"], "employer_name")
        self.assertAlmostEqual(details["confidence_score"], 0.5)

    def test_multiple_low_confidence_fields_all_recorded(self) -> None:
        result = self._validate(
            {
                "is_relevant": _envelope(True),
                "employer_name": _envelope("Walmart", confidence=0.5),
                "employment_status": _envelope("EMPLOYED", confidence=0.3),
            }
        )
        self.assertIsNone(result.validated_result_json)
        self.assertEqual(len(result.failures), 2)
        failed_fields = {
            json.loads(f.exclusion_details_json)["field_name"]
            for f in result.failures
            if f.exclusion_details_json is not None
        }
        self.assertEqual(failed_fields, {"employer_name", "employment_status"})

    def test_not_relevant_and_low_confidence_both_recorded(self) -> None:
        result = self._validate(
            {
                "is_relevant": _envelope(False),
                "employer_name": _envelope("Walmart", confidence=0.5),
                "employment_status": _envelope("EMPLOYED"),
            }
        )
        self.assertIsNone(result.validated_result_json)
        exclusion_types = {f.exclusion_type for f in result.failures}
        self.assertIn(ExtractionExclusionType.NOT_RELEVANT, exclusion_types)
        self.assertIn(ExtractionExclusionType.LOW_CONFIDENCE, exclusion_types)

    def test_is_relevant_stripped_from_validated_result(self) -> None:
        result = self._validate(
            {
                "is_relevant": _envelope(True),
                "employer_name": _envelope("Walmart"),
                "employment_status": _envelope("EMPLOYED"),
            }
        )
        assert result.validated_result_json is not None
        validated = json.loads(result.validated_result_json)
        self.assertNotIn("is_relevant", validated)

    def test_envelopes_preserved_in_validated_result(self) -> None:
        result = self._validate(
            {
                "is_relevant": _envelope(True),
                "employer_name": _envelope("Walmart", confidence=0.95),
                "employment_status": _envelope("EMPLOYED", confidence=0.88),
            }
        )
        assert result.validated_result_json is not None
        validated = json.loads(result.validated_result_json)
        self.assertEqual(validated["employer_name"]["value"], "Walmart")
        self.assertAlmostEqual(validated["employer_name"]["confidence_score"], 0.95)
        self.assertEqual(validated["employment_status"]["value"], "EMPLOYED")

    def test_array_of_struct_low_confidence_subfield_excluded(self) -> None:
        schema = _make_schema(
            [
                ExtractionInferredField(
                    name="employers",
                    field_type=ExtractionFieldType.ARRAY_OF_STRUCT,
                    unaugmented_description="Employers",
                    required=False,
                    struct_fields=(
                        ExtractionInferredField(
                            name="employer_name",
                            field_type=ExtractionFieldType.STRING,
                            unaugmented_description="Name",
                            required=False,
                        ),
                        ExtractionInferredField(
                            name="pay_rate",
                            field_type=ExtractionFieldType.FLOAT,
                            unaugmented_description="Pay rate",
                            required=False,
                        ),
                    ),
                ),
            ]
        )
        result_data = {
            "is_relevant": _envelope(True),
            "employers": [
                {
                    "employer_name": _envelope("Walmart"),
                    "pay_rate": _envelope(15.0, confidence=0.3),
                }
            ],
        }
        result = validate_extraction_result(
            result=_make_result(result_data),
            output_schema=schema,
            confidence_threshold=_THRESHOLD,
        )
        self.assertIsNone(result.validated_result_json)
        self.assertEqual(len(result.failures), 1)
        assert result.failures[0].exclusion_details_json is not None
        details = json.loads(result.failures[0].exclusion_details_json)
        self.assertEqual(details["field_name"], "employers[0].pay_rate")
