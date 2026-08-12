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
"""The validation portion of a processed document: the outcome of running the
validator over one raw extraction result.
"""

import datetime
from enum import StrEnum
from typing import Any

import attr

from recidiviz.common import attr_validators
from recidiviz.common.constants.operations.llm_extraction_job import (
    LLMExtractionJobDocumentResultType,
)
from recidiviz.documents.extraction.extraction_results_columns import (
    VALIDATION_ISSUE_CHECK_TYPE_FIELD,
    VALIDATION_ISSUE_DETAIL_FIELD,
    VALIDATION_ISSUE_FIELD_NAME_FIELD,
)
from recidiviz.documents.extraction.models.llm_request_output_values import (
    LLMRequestOutputValues,
)


class ValidationCheckType(StrEnum):
    """The validation check that produced an audit finding, recorded on every
    `ValidationIssue` so findings can be grouped by the check that raised them."""

    SCHEMA_CONFORMANCE = "SCHEMA_CONFORMANCE"
    SEMANTIC_CONSISTENCY = "SEMANTIC_CONSISTENCY"
    REQUIRED_FIELD_BELOW_MINIMUM_CONFIDENCE_LEVEL = (
        "REQUIRED_FIELD_BELOW_MINIMUM_CONFIDENCE_LEVEL"
    )
    RELEVANT_BUT_ALL_NULL = "RELEVANT_BUT_ALL_NULL"
    ADVERSARIAL_INTERPRETATION_CONFIDENCE_LEVEL_INCONSISTENCY = (
        "ADVERSARIAL_INTERPRETATION_CONFIDENCE_LEVEL_INCONSISTENCY"
    )


@attr.define(frozen=True, kw_only=True)
class ValidationIssue:
    """One audit finding from validation of a single document."""

    check_type: ValidationCheckType = attr.ib(
        validator=attr.validators.in_(ValidationCheckType)
    )
    """The validation check that produced this finding."""

    field_name: str | None = attr.ib(validator=attr_validators.is_opt_str)
    """The extracted field the finding is about, or None for a document-level
    finding not tied to a single field."""

    detail: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """A human-readable description of what the check found."""

    def to_dict(self) -> dict[str, Any]:
        """Returns a dict representation of the issue, suitable for JSON
        serialization."""
        return {
            VALIDATION_ISSUE_CHECK_TYPE_FIELD: self.check_type,
            VALIDATION_ISSUE_FIELD_NAME_FIELD: self.field_name,
            VALIDATION_ISSUE_DETAIL_FIELD: self.detail,
        }


@attr.define(frozen=True, kw_only=True)
class LLMDocumentValidationResult:
    """The validation portion of a processed document"""

    validated_output: LLMRequestOutputValues | None = attr.ib(
        validator=attr_validators.is_opt(LLMRequestOutputValues)
    )
    """The validated output, wrapping the JSON with quality filters applied, or
    None when the document failed an extraction-error check (nothing usable to
    persist)."""

    audit_issues: list[ValidationIssue] = attr.ib(
        validator=attr_validators.is_list_of(ValidationIssue)
    )
    """Every finding raised during validation, empty when the document was
    clean."""

    result_type_override: LLMExtractionJobDocumentResultType | None = attr.ib(
        validator=attr_validators.is_opt(LLMExtractionJobDocumentResultType)
    )
    """The per-document result an extraction-error check overrides a raw SUCCESS
    result type, or None when validation leaves the SUCCESS classification intact."""

    validation_config_version_id: str = attr.ib(
        validator=attr_validators.is_non_empty_str
    )
    """The version of the threshold/quality-filter config the validator ran
    against, persisted alongside the outcome so a result can be tied back to the
    rules that produced it."""

    validation_datetime_utc: datetime.datetime = attr.ib(
        validator=attr_validators.is_utc_timezone_aware_datetime
    )
    """When validation ran."""

    def __attrs_post_init__(self) -> None:
        if self.validated_output is None and not self.audit_issues:
            raise ValueError(
                f"Document failed validation but has no audit issues: "
                f"validated_output=[{self.validated_output}], "
                f"audit_issues=[{self.audit_issues}]."
            )
        if self.result_type_override is not None and not self.audit_issues:
            raise ValueError(
                f"Document has a result_type_override but no audit issues: "
                f"result_type_override=[{self.result_type_override}], "
                f"audit_issues=[{self.audit_issues}]."
            )

    @property
    def passed_validation(self) -> bool:
        """Returns whether the document passed all extraction-error checks."""
        return self.validated_output is not None

    @property
    def result_type(self) -> LLMExtractionJobDocumentResultType:
        """Returns the per-document result type validation resolves the document
        to: the override an extraction-error check set, or SUCCESS when validation
        left the raw SUCCESS classification intact."""
        if self.result_type_override is not None:
            return self.result_type_override
        return LLMExtractionJobDocumentResultType.SUCCESS

    @property
    def will_retry(self) -> bool:
        """Returns whether validation queued the document for another LLM call."""
        return (
            self.result_type_override
            is LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT
        )

    @property
    def is_relevant(self) -> bool | None:
        """Returns the model's relevance classification for a document that
        extracted and validated cleanly, or None when the document failed an
        extraction-error check (nothing usable to persist).
        """
        if self.validated_output is None:
            return None
        return self.validated_output.is_relevant
