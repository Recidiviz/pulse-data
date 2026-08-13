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
"""The citation offset-drift quality adjustment: corrects the character offsets
of a grounded citation the model reported at the wrong place in the document.
"""

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


class CitationOffsetDriftAdjustment:
    """The citation offset-drift quality adjustment: rewrites the reported
    offsets of a citation whose quoted text is grounded in the source document
    but sits somewhere other than where the model said, one `ValidationIssue`
    per corrected citation.

    Counting characters is not something a language model does reliably, and the
    quote itself — not the offsets — is the evidence that the extraction is real,
    so drift is corrected rather than treated as an error: the document is kept
    and never retried. Correcting it here means every consumer that highlights a
    citation in the source document can trust `document_text[start:end]`.

    A citation whose quote is not in the document at all — or is only found far
    from its reported offset — is a hallucination, not drift; `CitationGroundingCheck`
    fails the document before this adjustment runs, so any citation reaching it
    is grounded within `MAX_CITATION_OFFSET_DRIFT` of its reported offset — the
    same window this adjustment searches.
    """

    @classmethod
    def apply(
        cls,
        *,
        output: LLMRequestOutputValues,
        source_document_text: SourceDocumentText,
    ) -> tuple[LLMRequestOutputValues, list[ValidationIssue]]:
        """Returns a copy of |output| with the offsets rewritten on every citation
        that is grounded in |source_document_text| but reported at the wrong
        offsets, paired with one `ValidationIssue` per citation corrected.
        """
        adjusted = output.deep_copy()
        issues: list[ValidationIssue] = []
        for field_output in adjusted.inferred_field_outputs():
            for citation in field_output.citations:
                span = source_document_text.find_citation_span(
                    citation_text=citation.text,
                    reported_start=citation.start,
                    allowed_actual_start_drift=MAX_CITATION_OFFSET_DRIFT,
                )
                if span is None:
                    continue
                if citation.start == span.start and citation.end == span.end:
                    continue
                issues.append(
                    ValidationIssue(
                        check_type=ValidationCheckType.CITATION_OFFSET_DRIFT,
                        field_name=citation.field_display_name,
                        detail=(
                            f"Citation [{citation.index}] on field "
                            f"[{citation.field_display_name}] was reported at "
                            f"offsets [{citation.start}, {citation.end}) but its "
                            f"quoted text sits at [{span.start}, {span.end}); "
                            f"the offsets were corrected."
                        ),
                    )
                )
                citation.correct_offsets(start=span.start, end=span.end)
        return adjusted, issues
