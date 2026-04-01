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
"""Validation logic for document extraction results.

Validates successful extraction results against the output schema and
confidence threshold. Produces DocumentResultExclusionMetadata for excluded
results and validated output for passing results.
"""
import json

import attr

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


@attr.define
class ValidationResult:
    """Result of validating an extraction result."""

    failures: list[DocumentResultExclusionMetadata]
    validated_result_json: str | None


def failure_from_extraction_error(
    result: DocumentExtractionResultMetadata,
) -> DocumentResultExclusionMetadata:
    """Creates a DocumentResultExclusionMetadata from a failed extraction result
    (status != SUCCESS). Uses the same ExtractionExclusionType directly."""
    assert result.error_type is not None
    return DocumentResultExclusionMetadata(
        job_id=result.job_id,
        document_id=result.document_id,
        extractor_id=result.extractor_id,
        extractor_version_id=result.extractor_version_id,
        extraction_datetime=result.extraction_datetime,
        exclusion_type=result.error_type,
        exclusion_details_json=json.dumps(
            {
                "error_message": result.error_message,
            }
        ),
    )


def _check_is_relevant(
    result_data: dict,
    result: DocumentExtractionResultMetadata,
) -> DocumentResultExclusionMetadata | None:
    """Returns a NOT_RELEVANT failure if is_relevant is not True."""
    is_relevant_wrapper = result_data.get(RESERVED_FIELD_NAME_IS_RELEVANT, {})
    value = is_relevant_wrapper.get("value")
    # The LLM returns value as a string; handle both string and bool
    if isinstance(value, str):
        is_relevant = value.lower() == "true"
    elif isinstance(value, bool):
        is_relevant = value
    else:
        is_relevant = False

    if not is_relevant:
        return DocumentResultExclusionMetadata(
            job_id=result.job_id,
            document_id=result.document_id,
            extractor_id=result.extractor_id,
            extractor_version_id=result.extractor_version_id,
            extraction_datetime=result.extraction_datetime,
            exclusion_type=ExtractionExclusionType.NOT_RELEVANT,
            exclusion_details_json=json.dumps(
                {
                    "is_relevant_value": value,
                    "is_relevant_confidence": is_relevant_wrapper.get(
                        "confidence_score"
                    ),
                }
            ),
        )
    return None


def _check_field_confidence(
    result_data: dict,
    field: ExtractionInferredField,
    confidence_threshold: float,
    result: DocumentExtractionResultMetadata,
) -> list[DocumentResultExclusionMetadata]:
    """Returns LOW_CONFIDENCE failures for fields below threshold.

    For flat fields, checks the field's confidence_score directly.
    For ARRAY_OF_STRUCT fields, checks each sub-field's confidence_score
    within each array element.
    """
    if field.field_type == ExtractionFieldType.ARRAY_OF_STRUCT:
        return _check_array_of_struct_confidence(
            result_data, field, confidence_threshold, result
        )

    field_wrapper = result_data.get(field.name, {})
    confidence_score = field_wrapper.get("confidence_score")

    if confidence_score is None or float(confidence_score) < confidence_threshold:
        return [
            DocumentResultExclusionMetadata(
                job_id=result.job_id,
                document_id=result.document_id,
                extractor_id=result.extractor_id,
                extractor_version_id=result.extractor_version_id,
                extraction_datetime=result.extraction_datetime,
                exclusion_type=ExtractionExclusionType.LOW_CONFIDENCE,
                exclusion_details_json=json.dumps(
                    {
                        "field_name": field.name,
                        "confidence_score": confidence_score,
                        "threshold": confidence_threshold,
                    }
                ),
            )
        ]
    return []


def _check_array_of_struct_confidence(
    result_data: dict,
    field: ExtractionInferredField,
    confidence_threshold: float,
    result: DocumentExtractionResultMetadata,
) -> list[DocumentResultExclusionMetadata]:
    """Checks confidence scores for each sub-field in each element of an
    ARRAY_OF_STRUCT field."""
    array_data = result_data.get(field.name)
    if not array_data or not field.struct_fields:
        return []

    failures: list[DocumentResultExclusionMetadata] = []
    for element_index, element in enumerate(array_data):
        for sf in field.struct_fields:
            sf_wrapper = element.get(sf.name, {})
            confidence_score = sf_wrapper.get("confidence_score")
            if (
                confidence_score is None
                or float(confidence_score) < confidence_threshold
            ):
                failures.append(
                    DocumentResultExclusionMetadata(
                        job_id=result.job_id,
                        document_id=result.document_id,
                        extractor_id=result.extractor_id,
                        extractor_version_id=result.extractor_version_id,
                        extraction_datetime=result.extraction_datetime,
                        exclusion_type=ExtractionExclusionType.LOW_CONFIDENCE,
                        exclusion_details_json=json.dumps(
                            {
                                "field_name": f"{field.name}[{element_index}].{sf.name}",
                                "confidence_score": confidence_score,
                                "threshold": confidence_threshold,
                            }
                        ),
                    )
                )
    return failures


def _build_validated_result(
    result_data: dict,
    output_schema: ExtractionOutputSchema,
) -> dict:
    """Builds the validated result dict, keeping LLM envelopes intact.

    Strips is_relevant (used only for filtering) and retains all other fields
    with their original {value, confidence_score, null_reason, citations}
    envelope so that confidence scores remain available downstream.
    """
    return {
        field.name: result_data[field.name]
        for field in output_schema.inferred_fields
        if field.name != RESERVED_FIELD_NAME_IS_RELEVANT and field.name in result_data
    }


def validate_extraction_result(
    result: DocumentExtractionResultMetadata,
    output_schema: ExtractionOutputSchema,
    confidence_threshold: float,
) -> ValidationResult:
    """Validates a successful extraction result against the schema and threshold.

    Checks applied in order:
    1. is_relevant must be True
    2. All fields must have confidence_score >= threshold (including
       sub-fields within ARRAY_OF_STRUCT elements)

    Returns a ValidationResult with is_valid=True and flattened result_json
    if all checks pass, or is_valid=False with failure details otherwise.
    """
    assert result.result_json is not None
    result_data = json.loads(result.result_json)
    failures: list[DocumentResultExclusionMetadata] = []

    # Check 1: is_relevant
    relevance_failure = _check_is_relevant(result_data, result)
    if relevance_failure:
        failures.append(relevance_failure)

    # Check 2: confidence threshold for each field (including array sub-fields)
    for field in output_schema.inferred_fields:
        if field.name == RESERVED_FIELD_NAME_IS_RELEVANT:
            continue
        confidence_failures = _check_field_confidence(
            result_data, field, confidence_threshold, result
        )
        failures.extend(confidence_failures)

    if failures:
        return ValidationResult(failures=failures, validated_result_json=None)

    # All checks passed — build validated output (envelopes preserved)
    validated_result = _build_validated_result(result_data, output_schema)
    return ValidationResult(
        failures=[],
        validated_result_json=json.dumps(validated_result),
    )
