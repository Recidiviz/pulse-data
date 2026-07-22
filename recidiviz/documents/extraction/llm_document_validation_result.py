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
from typing import Any

import attr

from recidiviz.common import attr_validators
from recidiviz.common.constants.operations.llm_extraction_job import (
    LLMExtractionJobDocumentResultType,
)
from recidiviz.documents.extraction.extraction_results_columns import (
    VALIDATION_ISSUE_CHECK_NAME_FIELD,
    VALIDATION_ISSUE_DETAIL_FIELD,
    VALIDATION_ISSUE_FIELD_NAME_FIELD,
)


@attr.define(frozen=True, kw_only=True)
class ValidationIssue:
    """One audit finding from validation of a single document."""

    check_name: str = attr.ib(validator=attr_validators.is_non_empty_str)
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
            VALIDATION_ISSUE_CHECK_NAME_FIELD: self.check_name,
            VALIDATION_ISSUE_FIELD_NAME_FIELD: self.field_name,
            VALIDATION_ISSUE_DETAIL_FIELD: self.detail,
        }


@attr.define(frozen=True, kw_only=True)
class LLMDocumentValidationResult:
    """The validation portion of a processed document"""

    validated_content: dict[str, Any] | None = attr.ib(
        validator=attr_validators.is_opt_dict
    )
    """The validated JSON with quality filters applied, or None when the
    document failed an extraction-error check (nothing usable to persist)."""

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
        if self.validated_content is None and not self.audit_issues:
            raise ValueError(
                f"Document failed validation but has no audit issues: "
                f"validated_content=[{self.validated_content}], "
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
        return self.validated_content is not None

    @property
    def will_retry(self) -> bool:
        """Returns whether validation queued the document for another LLM call."""
        return (
            self.result_type_override
            is LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT
        )
