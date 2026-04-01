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


def _make_semantic_failure(
    result: DocumentExtractionResultMetadata,
    field_name: str,
    constraint_type: str,
    condition_field: str,
    allowed_condition_values: list[str] | None,
    sibling_value: str | None,
    field_value: str | None = None,
) -> DocumentResultExclusionMetadata:
    """Creates a SEMANTIC_CONSISTENCY_FAILURE DocumentResultExclusionMetadata."""
    if constraint_type == "allowed_if_nonnull":
        message = (
            f"'{field_name}' is only valid when '{condition_field}' has a value, "
            f"but '{condition_field}' is null"
        )
    elif constraint_type == "allowed_if_value":
        message = (
            f"'{field_name}' is only valid when '{condition_field}' is one of "
            f"{allowed_condition_values}, but '{condition_field}' is '{sibling_value}'"
        )
    else:
        message = (
            f"'{field_name}' value '{field_value}' is not allowed when "
            f"'{condition_field}' is '{sibling_value}'; "
            f"allowed values: {allowed_condition_values}"
        )

    details: dict = {
        "field_name": field_name,
        "constraint_type": constraint_type,
        "condition_field": condition_field,
        "allowed_condition_values": allowed_condition_values,
        "condition_field_actual_value": sibling_value,
        "message": message,
    }
    if field_value is not None:
        details["field_value"] = field_value

    return DocumentResultExclusionMetadata(
        job_id=result.job_id,
        document_id=result.document_id,
        extractor_id=result.extractor_id,
        extractor_version_id=result.extractor_version_id,
        extraction_datetime=result.extraction_datetime,
        exclusion_type=ExtractionExclusionType.SEMANTIC_CONSISTENCY_FAILURE,
        exclusion_details_json=json.dumps(details),
    )


def _check_allowed_conditions(
    sibling_data: dict,
    field_display_name: str,
    field: ExtractionInferredField,
    field_is_present: bool,
    field_value: str | None,
    condition_field_prefix: str,
    result: DocumentExtractionResultMetadata,
) -> list[DocumentResultExclusionMetadata]:
    """Checks allowed_if_nonnull, allowed_if_value, and allowed_value_combinations
    constraints for a single field against its sibling values."""
    failures: list[DocumentResultExclusionMetadata] = []

    # allowed_if_nonnull: field can only be nonnull if sibling is nonnull
    if field.allowed_if_nonnull and field_is_present:
        sibling_wrapper = sibling_data.get(field.allowed_if_nonnull, {})
        sibling_value = sibling_wrapper.get("value")
        if sibling_value is None:
            failures.append(
                _make_semantic_failure(
                    result=result,
                    field_name=field_display_name,
                    constraint_type="allowed_if_nonnull",
                    condition_field=f"{condition_field_prefix}{field.allowed_if_nonnull}",
                    allowed_condition_values=None,
                    sibling_value=None,
                )
            )

    # allowed_if_value: field can only be nonnull if each sibling has an allowed value
    if field.allowed_if_value and field_is_present:
        for (
            sibling_field_name,
            allowed_sibling_values,
        ) in field.allowed_if_value.items():
            sibling_wrapper = sibling_data.get(sibling_field_name, {})
            sibling_value = sibling_wrapper.get("value")
            if (
                sibling_value is None
                or str(sibling_value) not in allowed_sibling_values
            ):
                failures.append(
                    _make_semantic_failure(
                        result=result,
                        field_name=field_display_name,
                        constraint_type="allowed_if_value",
                        condition_field=f"{condition_field_prefix}{sibling_field_name}",
                        allowed_condition_values=list(allowed_sibling_values),
                        sibling_value=str(sibling_value)
                        if sibling_value is not None
                        else None,
                    )
                )

    # allowed_value_combinations: when field has a value, it must be in the allowed
    # set for the sibling's current value
    if field.allowed_value_combinations and field_value is not None:
        for sibling_field_name, value_map in field.allowed_value_combinations.items():
            sibling_wrapper = sibling_data.get(sibling_field_name, {})
            sibling_value = sibling_wrapper.get("value")
            if sibling_value is not None:
                sibling_value_str = str(sibling_value)
                if sibling_value_str in value_map:
                    allowed_current_vals = value_map[sibling_value_str]
                    if str(field_value) not in allowed_current_vals:
                        failures.append(
                            _make_semantic_failure(
                                result=result,
                                field_name=field_display_name,
                                constraint_type="allowed_value_combinations",
                                condition_field=f"{condition_field_prefix}{sibling_field_name}",
                                allowed_condition_values=list(allowed_current_vals),
                                sibling_value=sibling_value_str,
                                field_value=str(field_value),
                            )
                        )

    return failures


def _check_field_semantic_consistency(
    result_data: dict,
    field: ExtractionInferredField,
    result: DocumentExtractionResultMetadata,
) -> list[DocumentResultExclusionMetadata]:
    """Checks semantic consistency constraints for a top-level field against
    its sibling values in result_data."""
    # For ARRAY_OF_STRUCT, the value in result_data is a list (not a wrapped
    # dict), so field_value is always None and we only check field_is_present.
    if field.field_type == ExtractionFieldType.ARRAY_OF_STRUCT:
        array_data = result_data.get(field.name)
        field_is_present = bool(array_data)
        field_value = None
    else:
        field_wrapper = result_data.get(field.name, {})
        field_value = field_wrapper.get("value")
        field_is_present = field_value is not None

    return _check_allowed_conditions(
        sibling_data=result_data,
        field_display_name=field.name,
        field=field,
        field_is_present=field_is_present,
        field_value=field_value,
        condition_field_prefix="",
        result=result,
    )


def _check_array_of_struct_sub_field_semantic_consistency(
    result_data: dict,
    field: ExtractionInferredField,
    result: DocumentExtractionResultMetadata,
) -> list[DocumentResultExclusionMetadata]:
    """Checks semantic consistency constraints for sub-fields within each
    element of an ARRAY_OF_STRUCT field."""
    array_data = result_data.get(field.name)
    if not array_data or not field.struct_fields:
        return []

    failures: list[DocumentResultExclusionMetadata] = []
    for element_index, element in enumerate(array_data):
        for sf in field.struct_fields:
            sf_wrapper = element.get(sf.name, {})
            sf_value = sf_wrapper.get("value")

            failures.extend(
                _check_allowed_conditions(
                    sibling_data=element,
                    field_display_name=f"{field.name}[{element_index}].{sf.name}",
                    field=sf,
                    field_is_present=sf_value is not None,
                    field_value=sf_value,
                    condition_field_prefix=f"{field.name}[{element_index}].",
                    result=result,
                )
            )

    return failures


def _check_semantic_consistency(
    result_data: dict,
    output_schema: ExtractionOutputSchema,
    result: DocumentExtractionResultMetadata,
) -> list[DocumentResultExclusionMetadata]:
    """Checks all semantic consistency constraints across the schema."""
    failures: list[DocumentResultExclusionMetadata] = []

    for field in output_schema.inferred_fields:
        if field.name == RESERVED_FIELD_NAME_IS_RELEVANT:
            continue

        # Check constraints on top-level fields
        failures.extend(_check_field_semantic_consistency(result_data, field, result))

        # Check sub-field constraints within ARRAY_OF_STRUCT elements
        if field.field_type == ExtractionFieldType.ARRAY_OF_STRUCT:
            failures.extend(
                _check_array_of_struct_sub_field_semantic_consistency(
                    result_data, field, result
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
    3. Semantic consistency: allowed_if_nonnull, allowed_if_value, and
       allowed_value_combinations constraints must be satisfied

    Returns a ValidationResult with validated_result_json set if all checks
    pass, or failures populated otherwise.
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

    # Check 3: semantic consistency constraints
    semantic_failures = _check_semantic_consistency(result_data, output_schema, result)
    failures.extend(semantic_failures)

    if failures:
        return ValidationResult(failures=failures, validated_result_json=None)

    # All checks passed — build validated output (envelopes preserved)
    validated_result = _build_validated_result(result_data, output_schema)
    return ValidationResult(
        failures=[],
        validated_result_json=json.dumps(validated_result),
    )
