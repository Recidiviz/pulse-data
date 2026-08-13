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
"""The citation-grounding check: flags a hallucinated citation — a quote the
model attributes to the source document that appears nowhere near where it says
it does.
"""

from recidiviz.documents.extraction.models.llm_inferred_field_output import (
    FieldCitation,
)
from recidiviz.documents.extraction.models.llm_request_output_values import (
    LLMRequestOutputValues,
)
from recidiviz.documents.extraction.validation.citation_matching import (
    MAX_CITATION_OFFSET_DRIFT,
    SourceDocumentText,
)
from recidiviz.documents.extraction.validation.llm_document_validation_result import (
    ValidationCheckType,
    ValidationIssue,
)
from recidiviz.utils.string_formatting import truncate_string_if_necessary

# How much of a hallucinated quote to echo in the audit finding, so a long
# fabricated passage does not bloat every audit row that carries it.
_MAX_QUOTED_TEXT_CHARS = 100


class CitationGroundingCheck:
    """The citation-grounding check: flags a quote the model attributes to the
    source document that appears nowhere near where it says, one
    `ValidationIssue` per ungrounded citation.

    A fabricated quote is the clearest signal that a value was invented rather
    than extracted, so a document carrying one is discarded and retried. A quote
    that appears in the document, but only far from its reported offset, is no
    more trustworthy than one that appears nowhere — the model may have latched
    onto an unrelated occurrence — so it is failed here too. A quote grounded
    close to its reported offset is a correctable defect, handled by
    `CitationOffsetDriftAdjustment` rather than treated as an error.

    Every citation is checked, on both branches of an INFERRED field: the null
    branch's citations are quotes offered as evidence that no value was there to
    extract, and a fabricated one is no more trustworthy than on the value
    branch.
    """

    @classmethod
    def issues(
        cls,
        *,
        output: LLMRequestOutputValues,
        source_document_text: SourceDocumentText,
    ) -> list[ValidationIssue]:
        """Returns one `ValidationIssue` per citation in |output| whose quoted
        text cannot be found within `MAX_CITATION_OFFSET_DRIFT` characters of its
        reported start offset in |source_document_text|, or an empty list when
        every citation is grounded.
        """
        issues: list[ValidationIssue] = []
        for field_output in output.inferred_field_outputs():
            for citation in field_output.citations:
                span = source_document_text.find_citation_span(
                    citation_text=citation.text,
                    reported_start=citation.start,
                    allowed_actual_start_drift=MAX_CITATION_OFFSET_DRIFT,
                )
                if span is not None:
                    continue
                issues.append(
                    ValidationIssue(
                        check_type=ValidationCheckType.HALLUCINATED_CITATION,
                        field_name=citation.field_display_name,
                        detail=cls._issue_detail(citation=citation),
                    )
                )
        return issues

    @staticmethod
    def _issue_detail(*, citation: FieldCitation) -> str:
        """Returns the audit detail message for an ungrounded |citation|."""
        quoted_text = truncate_string_if_necessary(
            citation.text, max_length=_MAX_QUOTED_TEXT_CHARS
        )
        return (
            f"Citation [{citation.index}] on field [{citation.field_display_name}] "
            f"quotes text that is not grounded within "
            f"{MAX_CITATION_OFFSET_DRIFT} characters of its reported offset "
            f"[{citation.start}]: [{quoted_text}]."
        )
