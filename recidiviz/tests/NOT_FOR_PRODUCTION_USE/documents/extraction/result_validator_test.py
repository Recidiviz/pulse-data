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
    RESERVED_FIELD_NAME_IS_RELEVANT,
    ExtractionFieldType,
    ExtractionInferredField,
    ExtractionOutputSchema,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.document_extraction_result_metadata import (
    DocumentExtractionResultMetadata,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.document_result_exclusion_metadata import (
    DocumentResultExclusionMetadata,
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


def _wrap_value(
    value: str | bool | None,
    confidence: float = 0.95,
    null_reason: str | None = None,
) -> dict:
    """Creates a standard {value, null_reason, confidence_score, citations} wrapper."""
    return {
        "value": value,
        "null_reason": null_reason,
        "confidence_score": confidence,
        "citations": (
            [{"text": "example", "start": 0, "end": 7}] if value is not None else None
        ),
    }


def _make_is_relevant_field() -> ExtractionInferredField:
    return ExtractionInferredField(
        name=RESERVED_FIELD_NAME_IS_RELEVANT,
        field_type=ExtractionFieldType.BOOLEAN,
        unaugmented_description="Is relevant.",
        required=True,
    )


def _parse_failure_details(failure: DocumentResultExclusionMetadata) -> dict:
    """Parses the exclusion_details_json, asserting it is not None."""
    assert failure.exclusion_details_json is not None
    return json.loads(failure.exclusion_details_json)


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


class TestSemanticConsistencyAllowedIfValue(unittest.TestCase):
    """Tests for allowed_if_value semantic consistency checks."""

    def _make_housing_schema(self) -> ExtractionOutputSchema:
        """Schema where housing_address and added_info have allowed_if_value
        constraints and added_info also has allowed_value_combinations."""
        return ExtractionOutputSchema(
            full_batch_description="Housing info.",
            result_level_description="Housing info for one note.",
            inferred_fields_by_name={
                RESERVED_FIELD_NAME_IS_RELEVANT: _make_is_relevant_field(),
                "primary_status": ExtractionInferredField(
                    name="primary_status",
                    field_type=ExtractionFieldType.ENUM,
                    unaugmented_description="Housing status.",
                    required=True,
                    enum_values=("housed", "unhoused", "other"),
                ),
                "added_info": ExtractionInferredField(
                    name="added_info",
                    field_type=ExtractionFieldType.ENUM,
                    unaugmented_description="Additional info.",
                    required=False,
                    enum_values=(
                        "unhoused",
                        "transitional_housing",
                        "dependent",
                        "stable",
                        "other",
                    ),
                    # only valid when primary_status is housed or unhoused
                    allowed_if_value={"primary_status": ("housed", "unhoused")},
                    # when primary_status=housed, added_info cannot be 'unhoused'
                    allowed_value_combinations={
                        "primary_status": {
                            "housed": (
                                "transitional_housing",
                                "dependent",
                                "stable",
                                "other",
                            ),
                            "unhoused": (
                                "unhoused",
                                "transitional_housing",
                                "dependent",
                                "stable",
                                "other",
                            ),
                        }
                    },
                ),
                "housing_address": ExtractionInferredField(
                    name="housing_address",
                    field_type=ExtractionFieldType.STRING,
                    unaugmented_description="Address.",
                    required=False,
                    allowed_if_value={"primary_status": ("housed",)},
                ),
            },
        )

    def test_valid_housed_with_address(self) -> None:
        schema = self._make_housing_schema()
        result = _make_result(
            {
                RESERVED_FIELD_NAME_IS_RELEVANT: _wrap_value(True),
                "primary_status": _wrap_value("housed"),
                "added_info": _wrap_value("stable"),
                "housing_address": _wrap_value("123 Main St"),
            }
        )
        validation = validate_extraction_result(result, schema, 0.8)
        self.assertIsNotNone(validation.validated_result_json)
        self.assertEqual(len(validation.failures), 0)

    def test_valid_unhoused_with_added_info_unhoused(self) -> None:
        schema = self._make_housing_schema()
        result = _make_result(
            {
                RESERVED_FIELD_NAME_IS_RELEVANT: _wrap_value(True),
                "primary_status": _wrap_value("unhoused"),
                "added_info": _wrap_value("unhoused"),
                "housing_address": _wrap_value(None, null_reason="not_applicable"),
            }
        )
        validation = validate_extraction_result(result, schema, 0.8)
        self.assertIsNotNone(validation.validated_result_json)

    def test_fails_address_when_not_housed(self) -> None:
        """housing_address should fail when primary_status is not 'housed'."""
        schema = self._make_housing_schema()
        result = _make_result(
            {
                RESERVED_FIELD_NAME_IS_RELEVANT: _wrap_value(True),
                "primary_status": _wrap_value("unhoused"),
                "added_info": _wrap_value("unhoused"),
                "housing_address": _wrap_value("123 Main St"),
            }
        )
        validation = validate_extraction_result(result, schema, 0.8)
        self.assertIsNone(validation.validated_result_json)
        semantic_failures = [
            f
            for f in validation.failures
            if f.exclusion_type == ExtractionExclusionType.SEMANTIC_CONSISTENCY_FAILURE
        ]
        self.assertEqual(len(semantic_failures), 1)
        details = _parse_failure_details(semantic_failures[0])
        self.assertEqual(details["field_name"], "housing_address")
        self.assertEqual(details["constraint_type"], "allowed_if_value")

    def test_fails_added_info_unhoused_when_primary_housed(self) -> None:
        """added_info='unhoused' should fail when primary_status is 'housed'."""
        schema = self._make_housing_schema()
        result = _make_result(
            {
                RESERVED_FIELD_NAME_IS_RELEVANT: _wrap_value(True),
                "primary_status": _wrap_value("housed"),
                "added_info": _wrap_value("unhoused"),
                "housing_address": _wrap_value("123 Main St"),
            }
        )
        validation = validate_extraction_result(result, schema, 0.8)
        self.assertIsNone(validation.validated_result_json)
        semantic_failures = [
            f
            for f in validation.failures
            if f.exclusion_type == ExtractionExclusionType.SEMANTIC_CONSISTENCY_FAILURE
        ]
        self.assertEqual(len(semantic_failures), 1)
        details = _parse_failure_details(semantic_failures[0])
        self.assertEqual(details["field_name"], "added_info")
        self.assertEqual(details["field_value"], "unhoused")
        self.assertEqual(details["constraint_type"], "allowed_value_combinations")

    def test_fails_added_info_when_primary_other(self) -> None:
        """added_info should fail entirely when primary_status is 'other'
        (allowed_if_value constraint)."""
        schema = self._make_housing_schema()
        result = _make_result(
            {
                RESERVED_FIELD_NAME_IS_RELEVANT: _wrap_value(True),
                "primary_status": _wrap_value("other"),
                "added_info": _wrap_value("stable"),
                "housing_address": _wrap_value(None, null_reason="not_applicable"),
            }
        )
        validation = validate_extraction_result(result, schema, 0.8)
        self.assertIsNone(validation.validated_result_json)
        semantic_failures = [
            f
            for f in validation.failures
            if f.exclusion_type == ExtractionExclusionType.SEMANTIC_CONSISTENCY_FAILURE
        ]
        self.assertEqual(len(semantic_failures), 1)
        details = _parse_failure_details(semantic_failures[0])
        self.assertEqual(details["field_name"], "added_info")
        self.assertEqual(details["constraint_type"], "allowed_if_value")

    def test_null_field_passes_allowed_if_value(self) -> None:
        """A null field should not trigger allowed_if_value violations."""
        schema = self._make_housing_schema()
        result = _make_result(
            {
                RESERVED_FIELD_NAME_IS_RELEVANT: _wrap_value(True),
                "primary_status": _wrap_value("other"),
                "added_info": _wrap_value(None, null_reason="not_applicable"),
                "housing_address": _wrap_value(None, null_reason="not_applicable"),
            }
        )
        validation = validate_extraction_result(result, schema, 0.8)
        self.assertIsNotNone(validation.validated_result_json)


class TestSemanticConsistencyAllowedIfNonnull(unittest.TestCase):
    """Tests for allowed_if_nonnull semantic consistency checks."""

    def _make_schema(self) -> ExtractionOutputSchema:
        return ExtractionOutputSchema(
            full_batch_description="Test.",
            result_level_description="Test.",
            inferred_fields_by_name={
                RESERVED_FIELD_NAME_IS_RELEVANT: _make_is_relevant_field(),
                "change_type": ExtractionInferredField(
                    name="change_type",
                    field_type=ExtractionFieldType.STRING,
                    unaugmented_description="Type of change.",
                    required=False,
                ),
                "change_date": ExtractionInferredField(
                    name="change_date",
                    field_type=ExtractionFieldType.STRING,
                    unaugmented_description="Date of the change.",
                    required=False,
                    allowed_if_nonnull="change_type",
                ),
            },
        )

    def test_valid_both_present(self) -> None:
        schema = self._make_schema()
        result = _make_result(
            {
                RESERVED_FIELD_NAME_IS_RELEVANT: _wrap_value(True),
                "change_type": _wrap_value("promotion"),
                "change_date": _wrap_value("2026-01-15"),
            }
        )
        validation = validate_extraction_result(result, schema, 0.8)
        self.assertIsNotNone(validation.validated_result_json)

    def test_valid_both_null(self) -> None:
        schema = self._make_schema()
        result = _make_result(
            {
                RESERVED_FIELD_NAME_IS_RELEVANT: _wrap_value(True),
                "change_type": _wrap_value(None, null_reason="no_info_found"),
                "change_date": _wrap_value(None, null_reason="no_info_found"),
            }
        )
        validation = validate_extraction_result(result, schema, 0.8)
        self.assertIsNotNone(validation.validated_result_json)

    def test_fails_date_without_type(self) -> None:
        schema = self._make_schema()
        result = _make_result(
            {
                RESERVED_FIELD_NAME_IS_RELEVANT: _wrap_value(True),
                "change_type": _wrap_value(None, null_reason="no_info_found"),
                "change_date": _wrap_value("2026-01-15"),
            }
        )
        validation = validate_extraction_result(result, schema, 0.8)
        self.assertIsNone(validation.validated_result_json)
        semantic_failures = [
            f
            for f in validation.failures
            if f.exclusion_type == ExtractionExclusionType.SEMANTIC_CONSISTENCY_FAILURE
        ]
        self.assertEqual(len(semantic_failures), 1)
        details = _parse_failure_details(semantic_failures[0])
        self.assertEqual(details["field_name"], "change_date")
        self.assertEqual(details["constraint_type"], "allowed_if_nonnull")
        self.assertIsNone(details["allowed_condition_values"])


class TestSemanticConsistencyArrayOfStruct(unittest.TestCase):
    """Tests for semantic constraints on ARRAY_OF_STRUCT fields."""

    def _make_employment_schema(self) -> ExtractionOutputSchema:
        """Schema with an ARRAY_OF_STRUCT employers field."""
        return ExtractionOutputSchema(
            full_batch_description="Employment info.",
            result_level_description="Employment info for one note.",
            inferred_fields_by_name={
                RESERVED_FIELD_NAME_IS_RELEVANT: _make_is_relevant_field(),
                "primary_status": ExtractionInferredField(
                    name="primary_status",
                    field_type=ExtractionFieldType.ENUM,
                    unaugmented_description="Employment status.",
                    required=True,
                    enum_values=("employed", "unemployed"),
                ),
                "employers": ExtractionInferredField(
                    name="employers",
                    field_type=ExtractionFieldType.ARRAY_OF_STRUCT,
                    unaugmented_description="Employers.",
                    required=False,
                    allowed_if_value={"primary_status": ("employed",)},
                    struct_fields=(
                        ExtractionInferredField(
                            name="status",
                            field_type=ExtractionFieldType.ENUM,
                            unaugmented_description="Current or former.",
                            required=True,
                            enum_values=("current", "former"),
                        ),
                        ExtractionInferredField(
                            name="end_date",
                            field_type=ExtractionFieldType.STRING,
                            unaugmented_description="End date.",
                            required=False,
                            allowed_if_value={"status": ("former",)},
                        ),
                        ExtractionInferredField(
                            name="employer_name",
                            field_type=ExtractionFieldType.STRING,
                            unaugmented_description="Employer name.",
                            required=True,
                        ),
                    ),
                ),
            },
        )

    def test_valid_employed_with_employers(self) -> None:
        schema = self._make_employment_schema()
        result = _make_result(
            {
                RESERVED_FIELD_NAME_IS_RELEVANT: _wrap_value(True),
                "primary_status": _wrap_value("employed"),
                "employers": [
                    {
                        "status": _wrap_value("current"),
                        "end_date": _wrap_value(None, null_reason="not_applicable"),
                        "employer_name": _wrap_value("Acme Corp"),
                    },
                ],
            }
        )
        validation = validate_extraction_result(result, schema, 0.8)
        self.assertIsNotNone(validation.validated_result_json)
        self.assertEqual(len(validation.failures), 0)

    def test_fails_employers_when_unemployed(self) -> None:
        """employers array should fail when primary_status is 'unemployed'."""
        schema = self._make_employment_schema()
        result = _make_result(
            {
                RESERVED_FIELD_NAME_IS_RELEVANT: _wrap_value(True),
                "primary_status": _wrap_value("unemployed"),
                "employers": [
                    {
                        "status": _wrap_value("current"),
                        "end_date": _wrap_value(None, null_reason="not_applicable"),
                        "employer_name": _wrap_value("Acme Corp"),
                    },
                ],
            }
        )
        validation = validate_extraction_result(result, schema, 0.8)
        self.assertIsNone(validation.validated_result_json)
        semantic_failures = [
            f
            for f in validation.failures
            if f.exclusion_type == ExtractionExclusionType.SEMANTIC_CONSISTENCY_FAILURE
        ]
        self.assertEqual(len(semantic_failures), 1)
        details = _parse_failure_details(semantic_failures[0])
        self.assertEqual(details["field_name"], "employers")
        self.assertEqual(details["constraint_type"], "allowed_if_value")

    def test_fails_end_date_when_status_current(self) -> None:
        """end_date sub-field should fail when sibling status is 'current'."""
        schema = self._make_employment_schema()
        result = _make_result(
            {
                RESERVED_FIELD_NAME_IS_RELEVANT: _wrap_value(True),
                "primary_status": _wrap_value("employed"),
                "employers": [
                    {
                        "status": _wrap_value("current"),
                        "end_date": _wrap_value("2025-12-31"),
                        "employer_name": _wrap_value("Acme Corp"),
                    },
                ],
            }
        )
        validation = validate_extraction_result(result, schema, 0.8)
        self.assertIsNone(validation.validated_result_json)
        semantic_failures = [
            f
            for f in validation.failures
            if f.exclusion_type == ExtractionExclusionType.SEMANTIC_CONSISTENCY_FAILURE
        ]
        self.assertEqual(len(semantic_failures), 1)
        details = _parse_failure_details(semantic_failures[0])
        self.assertEqual(details["field_name"], "employers[0].end_date")
        self.assertEqual(details["condition_field"], "employers[0].status")

    def test_valid_end_date_when_status_former(self) -> None:
        schema = self._make_employment_schema()
        result = _make_result(
            {
                RESERVED_FIELD_NAME_IS_RELEVANT: _wrap_value(True),
                "primary_status": _wrap_value("employed"),
                "employers": [
                    {
                        "status": _wrap_value("former"),
                        "end_date": _wrap_value("2025-12-31"),
                        "employer_name": _wrap_value("Acme Corp"),
                    },
                ],
            }
        )
        validation = validate_extraction_result(result, schema, 0.8)
        self.assertIsNotNone(validation.validated_result_json)

    def test_null_employers_when_unemployed_is_valid(self) -> None:
        """Null/empty employers should pass even when primary_status is
        'unemployed' — the constraint only fires when the field is present."""
        schema = self._make_employment_schema()
        result = _make_result(
            {
                RESERVED_FIELD_NAME_IS_RELEVANT: _wrap_value(True),
                "primary_status": _wrap_value("unemployed"),
                "employers": None,
            }
        )
        validation = validate_extraction_result(result, schema, 0.8)
        self.assertIsNotNone(validation.validated_result_json)

    def test_multiple_elements_independent_failures(self) -> None:
        """Each array element is validated independently."""
        schema = self._make_employment_schema()
        result = _make_result(
            {
                RESERVED_FIELD_NAME_IS_RELEVANT: _wrap_value(True),
                "primary_status": _wrap_value("employed"),
                "employers": [
                    {
                        "status": _wrap_value("current"),
                        "end_date": _wrap_value("2025-06-01"),  # invalid
                        "employer_name": _wrap_value("Acme"),
                    },
                    {
                        "status": _wrap_value("former"),
                        "end_date": _wrap_value("2024-12-31"),  # valid
                        "employer_name": _wrap_value("Old Corp"),
                    },
                    {
                        "status": _wrap_value("current"),
                        "end_date": _wrap_value("2025-07-01"),  # invalid
                        "employer_name": _wrap_value("Beta Inc"),
                    },
                ],
            }
        )
        validation = validate_extraction_result(result, schema, 0.8)
        self.assertIsNone(validation.validated_result_json)
        semantic_failures = [
            f
            for f in validation.failures
            if f.exclusion_type == ExtractionExclusionType.SEMANTIC_CONSISTENCY_FAILURE
        ]
        self.assertEqual(len(semantic_failures), 2)
        field_names = {
            _parse_failure_details(f)["field_name"] for f in semantic_failures
        }
        self.assertEqual(
            field_names, {"employers[0].end_date", "employers[2].end_date"}
        )


class TestSemanticConsistencyAllowedValueCombinations(unittest.TestCase):
    """Tests for allowed_value_combinations semantic consistency checks."""

    def _make_schema(self) -> ExtractionOutputSchema:
        return ExtractionOutputSchema(
            full_batch_description="Employment info.",
            result_level_description="Employment info for one note.",
            inferred_fields_by_name={
                RESERVED_FIELD_NAME_IS_RELEVANT: _make_is_relevant_field(),
                "employment_status": ExtractionInferredField(
                    name="employment_status",
                    field_type=ExtractionFieldType.ENUM,
                    unaugmented_description="Employment status.",
                    required=True,
                    enum_values=("employed", "unemployed", "other"),
                ),
                "change_type": ExtractionInferredField(
                    name="change_type",
                    field_type=ExtractionFieldType.ENUM,
                    unaugmented_description="Type of change.",
                    required=False,
                    enum_values=("hired", "fired", "quit", "retired"),
                    allowed_value_combinations={
                        "employment_status": {
                            "employed": ("hired", "fired", "quit", "retired"),
                            "unemployed": ("fired", "quit", "retired"),
                            "other": ("fired", "quit", "retired"),
                        }
                    },
                ),
            },
        )

    def test_valid_hired_when_employed(self) -> None:
        schema = self._make_schema()
        result = _make_result(
            {
                RESERVED_FIELD_NAME_IS_RELEVANT: _wrap_value(True),
                "employment_status": _wrap_value("employed"),
                "change_type": _wrap_value("hired"),
            }
        )
        validation = validate_extraction_result(result, schema, 0.8)
        self.assertIsNotNone(validation.validated_result_json)

    def test_valid_fired_when_unemployed(self) -> None:
        schema = self._make_schema()
        result = _make_result(
            {
                RESERVED_FIELD_NAME_IS_RELEVANT: _wrap_value(True),
                "employment_status": _wrap_value("unemployed"),
                "change_type": _wrap_value("fired"),
            }
        )
        validation = validate_extraction_result(result, schema, 0.8)
        self.assertIsNotNone(validation.validated_result_json)

    def test_fails_hired_when_unemployed(self) -> None:
        """'hired' is not allowed when employment_status is 'unemployed'."""
        schema = self._make_schema()
        result = _make_result(
            {
                RESERVED_FIELD_NAME_IS_RELEVANT: _wrap_value(True),
                "employment_status": _wrap_value("unemployed"),
                "change_type": _wrap_value("hired"),
            }
        )
        validation = validate_extraction_result(result, schema, 0.8)
        self.assertIsNone(validation.validated_result_json)
        semantic_failures = [
            f
            for f in validation.failures
            if f.exclusion_type == ExtractionExclusionType.SEMANTIC_CONSISTENCY_FAILURE
        ]
        self.assertEqual(len(semantic_failures), 1)
        details = _parse_failure_details(semantic_failures[0])
        self.assertEqual(details["field_name"], "change_type")
        self.assertEqual(details["field_value"], "hired")
        self.assertEqual(details["constraint_type"], "allowed_value_combinations")
        self.assertEqual(details["condition_field"], "employment_status")
        self.assertEqual(details["condition_field_actual_value"], "unemployed")

    def test_null_change_type_skips_combinations_check(self) -> None:
        """Null change_type should not trigger allowed_value_combinations."""
        schema = self._make_schema()
        result = _make_result(
            {
                RESERVED_FIELD_NAME_IS_RELEVANT: _wrap_value(True),
                "employment_status": _wrap_value("unemployed"),
                "change_type": _wrap_value(None, null_reason="no_info_found"),
            }
        )
        validation = validate_extraction_result(result, schema, 0.8)
        self.assertIsNotNone(validation.validated_result_json)


class TestExtractionExclusionTypeIsLlmError(unittest.TestCase):
    """Tests that SEMANTIC_CONSISTENCY_FAILURE is not an LLM error."""

    def test_semantic_consistency_failure_is_not_llm_error(self) -> None:
        self.assertFalse(
            ExtractionExclusionType.SEMANTIC_CONSISTENCY_FAILURE.is_llm_error
        )
