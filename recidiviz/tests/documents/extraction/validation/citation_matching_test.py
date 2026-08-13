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
"""Tests for SourceDocumentText, the citation-locating primitive both citation
checks are built on: whether a quote is grounded in the document at all (the
hallucination check) and, when it is, where it actually sits (the offset-drift
adjustment).
"""

from unittest import TestCase

from recidiviz.documents.extraction.validation.citation_matching import (
    CitationSpan,
    SourceDocumentText,
)

_DOCUMENT = "Client reported starting work at the Kitchen on Monday."
# Larger than any offset gap exercised below, so the matching-behavior tests
# that use `_find` exercise find_citation_span without its window ever
# constraining the result; the allowed-drift tests further down call
# find_citation_span directly with a realistic window instead.
_FIND_WINDOW = 10_000


class CitationSpanTest(TestCase):
    """Tests for CitationSpan."""

    def test_span_is_end_exclusive(self) -> None:
        # The convention the whole citation pipeline is written to: end is one
        # past the last character, so slicing the document by the span yields the
        # quote.
        span = CitationSpan(start=37, end=44)
        self.assertEqual("Kitchen", _DOCUMENT[span.start : span.end])

    def test_empty_span_rejected(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"^Citation span must be non-empty, but end \[5\] is not"
        ):
            CitationSpan(start=5, end=5)

    def test_inverted_span_rejected(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"^Citation span must be non-empty, but end \[3\] is not"
        ):
            CitationSpan(start=5, end=3)


class SourceDocumentTextTest(TestCase):
    """Tests for SourceDocumentText.find_citation_span."""

    def _find(
        self, citation_text: str, *, document: str = _DOCUMENT, reported_start: int = 0
    ) -> CitationSpan | None:
        return SourceDocumentText(text=document).find_citation_span(
            citation_text=citation_text,
            reported_start=reported_start,
            allowed_actual_start_drift=_FIND_WINDOW,
        )

    def test_exact_quote_found(self) -> None:
        self.assertEqual(CitationSpan(start=37, end=44), self._find("Kitchen"))

    def test_quote_at_document_start_found(self) -> None:
        self.assertEqual(CitationSpan(start=0, end=6), self._find("Client"))

    def test_whole_document_found(self) -> None:
        self.assertEqual(
            CitationSpan(start=0, end=len(_DOCUMENT)), self._find(_DOCUMENT)
        )

    def test_quote_absent_from_document(self) -> None:
        self.assertIsNone(self._find("the Laundry"))

    def test_match_is_case_sensitive(self) -> None:
        # The model is instructed to quote verbatim, so a quote that differs in
        # casing is not the document's text and is left ungrounded rather than
        # fuzzily matched.
        self.assertIsNone(self._find("kitchen"))

    def test_empty_quote_matches_nothing(self) -> None:
        # There is no span of any document an empty quote could be grounded in,
        # even though "" is trivially a substring of everything.
        self.assertIsNone(self._find(""))

    def test_whitespace_only_quote_matches_nothing(self) -> None:
        self.assertIsNone(self._find("   \n  "))

    def test_empty_document_grounds_nothing(self) -> None:
        self.assertIsNone(self._find("Kitchen", document=""))

    def test_quote_with_different_whitespace_not_found(self) -> None:
        # Matching is exact, not whitespace-normalized: a quote that reformats the
        # document's line breaks or spacing is not the document's own text.
        document = "Client reported\n   starting  work"
        self.assertIsNone(
            self._find("Client reported starting work", document=document)
        )

    def test_document_with_different_whitespace_not_found(self) -> None:
        # The mirror image: the document is the one carrying the line breaks the
        # quote doesn't.
        document = "Assigned to the\nKitchen today"
        self.assertIsNone(self._find("the Kitchen", document=document))

    def test_quote_with_surrounding_whitespace_not_found(self) -> None:
        # Leading/trailing whitespace makes the quote no longer the document's
        # exact text, so it is left ungrounded rather than trimmed and matched.
        self.assertIsNone(self._find("  Kitchen\n "))

    def test_quote_differing_in_wording_not_found(self) -> None:
        self.assertIsNone(self._find("Kitchen assignment"))

    def test_repeated_quote_resolves_to_nearest_reported_start(self) -> None:
        document = "Kitchen. " * 3
        # Occurrences start at 0, 9, and 18; the reported offset is closest to
        # the third.
        self.assertEqual(
            CitationSpan(start=18, end=25),
            self._find("Kitchen", document=document, reported_start=17),
        )

    def test_repeated_quote_resolves_to_exact_occurrence_when_offsets_right(
        self,
    ) -> None:
        document = "Kitchen. " * 3
        self.assertEqual(
            CitationSpan(start=9, end=16),
            self._find("Kitchen", document=document, reported_start=9),
        )

    def test_repeated_quote_ties_resolve_to_earlier_occurrence(self) -> None:
        document = "Kitchen...Kitchen"
        # Equidistant from the occurrences at 0 and 10 (distance 5 either way):
        # the earlier one wins, so the choice is deterministic.
        self.assertEqual(
            CitationSpan(start=0, end=7),
            self._find("Kitchen", document=document, reported_start=5),
        )

    def test_overlapping_occurrences_all_considered(self) -> None:
        document = "abababab"
        # "abab" occurs at 0, 2, and 4 — overlapping occurrences the naive
        # non-overlapping scan would miss.
        self.assertEqual(
            CitationSpan(start=4, end=8),
            self._find("abab", document=document, reported_start=5),
        )

    def test_reported_start_beyond_document_still_matches(self) -> None:
        # A wildly wrong offset does not stop the quote from being grounded; it
        # only decides which occurrence is nearest.
        self.assertEqual(
            CitationSpan(start=37, end=44), self._find("Kitchen", reported_start=9_999)
        )

    def test_allowed_drift_finds_occurrence_within_window(self) -> None:
        self.assertEqual(
            CitationSpan(start=37, end=44),
            SourceDocumentText(text=_DOCUMENT).find_citation_span(
                citation_text="Kitchen",
                reported_start=40,
                allowed_actual_start_drift=10,
            ),
        )

    def test_allowed_drift_excludes_occurrence_outside_window(self) -> None:
        # "Kitchen" is grounded, but only far outside the drift window, so it is
        # treated the same as not appearing in the document at all.
        self.assertIsNone(
            SourceDocumentText(text=_DOCUMENT).find_citation_span(
                citation_text="Kitchen",
                reported_start=9_999,
                allowed_actual_start_drift=10,
            )
        )

    def test_allowed_drift_picks_nearest_occurrence_within_window(self) -> None:
        document = "Kitchen. " * 3
        # Occurrences start at 0, 9, and 18; only 9 and 18 fall within drift 10
        # of the reported offset 17, and 18 is nearest.
        self.assertEqual(
            CitationSpan(start=18, end=25),
            SourceDocumentText(text=document).find_citation_span(
                citation_text="Kitchen",
                reported_start=17,
                allowed_actual_start_drift=10,
            ),
        )
