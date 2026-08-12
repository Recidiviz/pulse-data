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
"""The adversarial-interpretation consistency check: flags an INFERRED field
whose `adversarial_interpretation` and `confidence_level` contradict each other.
"""

from recidiviz.documents.extraction.models.llm_inferred_field_output import (
    InferredFieldOutput,
)
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


class AdversarialInterpretationConsistencyCheck:
    """The adversarial-interpretation consistency check: flags an INFERRED field
    whose `adversarial_interpretation` and `confidence_level` contradict each
    other, one `ValidationIssue` per field.

    The two are defined to move together: `speculative` means precisely that a
    plausible alternative reading exists, and the prompt instructs the model to
    set `confidence_level` to `speculative` whenever it records an
    `adversarial_interpretation`. So a field is inconsistent in either direction
    — an alternative reading recorded at a higher confidence level, or
    `speculative` confidence with no alternative reading to justify it. Either
    way the model contradicted its instructions, so the document is retried.

    Both branches of an INFERRED field carry both keys, so both are checked: the
    model may record an alternative reading of why a value might have been
    present when it extracted none.
    """

    @classmethod
    def issues(cls, *, output: LLMRequestOutputValues) -> list[ValidationIssue]:
        """Returns one `ValidationIssue` per INFERRED field in |output| whose
        `adversarial_interpretation` and `confidence_level` contradict each
        other, or an empty list when every field is consistent.
        """
        return [
            issue
            for field_output in output.inferred_field_outputs()
            if (issue := cls._issue_for_field(field_output)) is not None
        ]

    @staticmethod
    def _issue_for_field(
        field_output: InferredFieldOutput,
    ) -> ValidationIssue | None:
        """Returns the finding for |field_output|'s adversarial/confidence pair,
        or None when the two are consistent.
        """
        has_adversarial_interpretation = (
            field_output.adversarial_interpretation is not None
        )
        is_speculative = field_output.confidence_level is ConfidenceLevel.SPECULATIVE
        if has_adversarial_interpretation == is_speculative:
            return None

        if has_adversarial_interpretation:
            detail = (
                f"Field [{field_output.display_name}] recorded an adversarial "
                f"interpretation but has confidence level "
                f"[{field_output.confidence_level.value}]; an alternative "
                f"reading requires [{ConfidenceLevel.SPECULATIVE.value}]."
            )
        else:
            detail = (
                f"Field [{field_output.display_name}] has confidence level "
                f"[{ConfidenceLevel.SPECULATIVE.value}] but recorded no "
                f"adversarial interpretation to justify it."
            )
        return ValidationIssue(
            check_type=(
                ValidationCheckType.ADVERSARIAL_INTERPRETATION_CONFIDENCE_LEVEL_INCONSISTENCY
            ),
            field_name=field_output.display_name,
            detail=detail,
        )
