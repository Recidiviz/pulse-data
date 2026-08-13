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
"""The confidence-threshold quality adjustment: withholds the value of an
optional INFERRED field that did not meet its minimum confidence level.
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


class ConfidenceThresholdAdjustment:
    """The confidence-threshold quality adjustment: nulls out the value of an
    optional INFERRED field whose confidence level fell below the field's
    minimum, one `ValidationIssue` per field adjusted.

    The model followed its instructions here — it extracted a value and reported
    honestly how sure it was — so the document is kept and never retried; only
    the value is withheld from product-facing output. A required field falling
    short is handled by `RequiredFieldConfidenceCheck`; between them the two
    cover every INFERRED field.

    The adjusted value is nulled in the returned copy, leaving the field's
    companion metadata — its confidence level and citations — intact, so
    downstream the null is explicable rather than indistinguishable from a value
    the model never found.
    """

    @classmethod
    def apply(
        cls, *, output: LLMRequestOutputValues
    ) -> tuple[LLMRequestOutputValues, list[ValidationIssue]]:
        """Returns a copy of |output| with the value of every optional INFERRED
        field whose confidence level fell below its minimum nulled out, paired
        with one `ValidationIssue` per field adjusted.
        """
        adjusted = output.deep_copy()
        issues: list[ValidationIssue] = []
        for field_output in adjusted.inferred_field_outputs():
            if field_output.field.required or not field_output.has_value:
                continue
            minimum = assert_type(
                field_output.field.minimum_confidence_level, ConfidenceLevel
            )
            confidence_level = field_output.confidence_level
            if confidence_level.meets_minimum(minimum):
                continue
            field_output.null_out_value()
            issues.append(
                ValidationIssue(
                    check_type=ValidationCheckType.BELOW_MINIMUM_CONFIDENCE_LEVEL,
                    field_name=field_output.display_name,
                    detail=(
                        f"Field [{field_output.display_name}] was extracted with "
                        f"confidence level [{confidence_level.value}], below its "
                        f"minimum of [{minimum.value}]; its value was withheld "
                        f"from the validated output."
                    ),
                )
            )
        return adjusted, issues
