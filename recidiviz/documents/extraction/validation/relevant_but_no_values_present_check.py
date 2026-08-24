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
"""A validation check that fails if the model labels a document "relevant"
but returns no value (i.e. fields all null or empty arrays) for any
user-defined field.
"""

from recidiviz.documents.extraction.models.llm_request_output_values import (
    LLMRequestOutputValues,
)
from recidiviz.documents.extraction.validation.llm_document_validation_result import (
    ValidationCheckType,
    ValidationIssue,
)


class RelevantButNoValuesPresentCheck:
    """A validation check that fails if the model labels a document "relevant"
    but returns no value (i.e. fields all null or empty arrays) for any
    user-defined field.

    Calling a document relevant asserts it says something the extractor was
    built to capture, so extracting nothing from it contradicts that assertion.
    """

    @classmethod
    def issues(cls, *, output: LLMRequestOutputValues) -> list[ValidationIssue]:
        """Returns a single `ValidationIssue` when |output| is relevant but
        carries no value for any user-defined field, or an empty list otherwise.
        """
        if not output.is_relevant:
            return []
        user_defined_fields = output.output_schema.user_defined_fields
        if any(output.is_value_present(field=field) for field in user_defined_fields):
            return []
        return [
            ValidationIssue(
                check_type=ValidationCheckType.RELEVANT_BUT_NO_VALUES_PRESENT,
                field_name=None,
                detail=(
                    f"Document was extracted as relevant, but no extracted "
                    f"field carries a value: "
                    f"{sorted(field.name for field in user_defined_fields)}."
                ),
            )
        ]
