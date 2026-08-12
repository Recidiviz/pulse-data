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
"""The required-field confidence check: flags a required INFERRED field whose
extracted value did not meet the field's minimum confidence level, one
`ValidationIssue` per such field.
"""

from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    ConfidenceLevel,
)
from recidiviz.documents.extraction.models.llm_request_output_values import (
    LLMRequestOutputValues,
)
from recidiviz.documents.extraction.validation.llm_document_validation_result import (
    ValidationCheckType,
    ValidationIssue,
)
from recidiviz.utils.types import assert_type


class RequiredFieldConfidenceCheck:
    """The required-field confidence check: flags a required INFERRED field whose
    extracted value did not meet the field's minimum confidence level.

    This is a contradiction rather than a quality judgment — the model was
    confident enough to call the document relevant, but not confident enough
    about a field the schema says every relevant document must answer — so it is
    an extraction error and the document is retried.

    A required field that took its null branch has no extracted value to hold to
    a confidence bar, so this check passes it over; its confidence level
    describes the model's certainty that nothing was there to extract.
    """

    @classmethod
    def issues(cls, *, output: LLMRequestOutputValues) -> list[ValidationIssue]:
        """Returns one `ValidationIssue` per required INFERRED field in |output|
        whose extracted value fell below its minimum confidence level.
        """
        issues: list[ValidationIssue] = []
        for field_output in output.inferred_field_outputs():
            if not field_output.field.required or not field_output.has_value:
                continue
            minimum = assert_type(
                field_output.field.minimum_confidence_level, ConfidenceLevel
            )
            confidence_level = field_output.confidence_level
            if confidence_level.meets_minimum(minimum):
                continue
            issues.append(
                ValidationIssue(
                    check_type=(
                        ValidationCheckType.REQUIRED_FIELD_BELOW_MINIMUM_CONFIDENCE_LEVEL
                    ),
                    field_name=field_output.display_name,
                    detail=(
                        f"Required field [{field_output.display_name}] was "
                        f"extracted with confidence level "
                        f"[{confidence_level.value}], below its minimum of "
                        f"[{minimum.value}]."
                    ),
                )
            )
        return issues
