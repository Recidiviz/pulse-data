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
"""Locates an extracted field's cited quote within the document it was extracted
from — the shared machinery behind both citation checks: the extraction-error
check that a quote is grounded in the document at all, and the quality filter
that corrects the offsets a grounded quote was reported at.
"""
import attr

from recidiviz.common import attr_validators


@attr.define(frozen=True, kw_only=True)
class CitationSpan:
    """Where a cited quote actually appears in the source document: the character
    offsets of the span, in the document's own coordinates.
    """

    start: int = attr.ib(validator=attr_validators.is_non_negative_int)
    """Offset of the span's first character."""

    end: int = attr.ib(validator=attr_validators.is_non_negative_int)
    """Offset one past the span's last character, so `document_text[start:end]`
    is the quoted span."""

    def __attrs_post_init__(self) -> None:
        if self.end <= self.start:
            raise ValueError(
                f"Citation span must be non-empty, but end [{self.end}] is not "
                f"after start [{self.start}]."
            )


@attr.define(frozen=True, kw_only=True)
class SourceDocumentText:
    """The text of the document an extraction's citations are matched against.

    Matching is exact: the model is instructed to cite verbatim, so a quote that
    differs from the document in wording, casing, or whitespace is treated as
    ungrounded rather than fuzzily matched to the nearest text.
    """

    text: str = attr.ib(validator=attr_validators.is_str)
    """The document text exactly as it was handed to the model."""

    def find_citation_span(
        self,
        *,
        citation_text: str,
        reported_start: int,
        allowed_actual_start_drift: int | None = None,
    ) -> CitationSpan | None:
        """Returns where |citation_text| appears in this document, or None when it
        appears nowhere (the quote is not grounded in the document).

        When the quote appears more than once, the occurrence whose start is
        nearest |reported_start| — the offset the model reported — wins, so a
        model that quotes a repeated phrase but drifts on its offsets is
        corrected toward the occurrence it most plausibly meant, and a model
        whose offsets are already right is left alone.

        A quote that is empty (or nothing but whitespace) matches nothing: there
        is no span of the document it could be grounded in.

        |allowed_actual_start_drift|, when given, restricts the search to
        occurrences whose start lies within that many characters of
        |reported_start|, so a caller that only cares whether the quote is
        grounded near where it was reported does not pay for a full-document
        scan. Leave it None to search the whole document, e.g. when the caller
        wants the nearest occurrence regardless of how far it drifted.
        """
        if not citation_text.strip():
            return None

        search_from = 0
        search_to = None
        if allowed_actual_start_drift is not None:
            search_from = max(0, reported_start - allowed_actual_start_drift)
            search_to = reported_start + allowed_actual_start_drift + len(citation_text)

        spans = [
            CitationSpan(start=start, end=start + len(citation_text))
            for start in _all_occurrences(
                haystack=self.text,
                needle=citation_text,
                search_from=search_from,
                search_to=search_to,
            )
        ]
        if not spans:
            return None
        return min(spans, key=lambda span: abs(span.start - reported_start))


def _all_occurrences(
    *, haystack: str, needle: str, search_from: int = 0, search_to: int | None = None
) -> list[int]:
    """Returns the start offset of every occurrence of |needle| in |haystack|
    whose start lies in [|search_from|, |search_to|) — or from |search_from| to
    the end of |haystack| when |search_to| is None — including overlapping
    occurrences, in ascending order.
    """
    offsets: list[int] = []
    while (found_at := haystack.find(needle, search_from, search_to)) != -1:
        offsets.append(found_at)
        search_from = found_at + 1
    return offsets
