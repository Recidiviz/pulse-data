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
from recidiviz.utils.types import assert_type

_DOCUMENT = "Client reported starting work at the Kitchen on Monday."


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
        self, citation_text: str, *, document: str = _DOCUMENT
    ) -> CitationSpan | None:
        return SourceDocumentText(text=document).find_citation_span(
            citation_text=citation_text
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

    def test_quote_differing_only_in_case_found(self) -> None:
        # Some source systems store notes entirely in uppercase and the model
        # re-cases quotes into sentence case, so case carries no grounding
        # signal and matching ignores it.
        self.assertEqual(CitationSpan(start=37, end=44), self._find("kitchen"))

    def test_recased_quote_found_in_uppercase_document(self) -> None:
        self.assertEqual(
            CitationSpan(start=33, end=44),
            self._find("the Kitchen", document=_DOCUMENT.upper()),
        )

    def test_empty_quote_matches_nothing(self) -> None:
        # There is no span of any document an empty quote could be grounded in,
        # even though "" is trivially a substring of everything.
        self.assertIsNone(self._find(""))

    def test_whitespace_only_quote_matches_nothing(self) -> None:
        self.assertIsNone(self._find("   \n  "))

    def test_empty_document_grounds_nothing(self) -> None:
        self.assertIsNone(self._find("Kitchen", document=""))

    def test_quote_with_different_whitespace_found(self) -> None:
        document = "Client reported\n   starting  work"
        self.assertEqual(
            CitationSpan(start=0, end=len(document)),
            self._find("Client reported starting work", document=document),
        )

    def test_document_with_different_whitespace_found(self) -> None:
        document = "Assigned to the\nKitchen today"
        self.assertEqual(
            CitationSpan(start=12, end=23),
            self._find("the Kitchen", document=document),
        )

    def test_quote_with_surrounding_whitespace_found(self) -> None:
        self.assertEqual(CitationSpan(start=37, end=44), self._find("  Kitchen\n "))

    def test_quote_spanning_blank_line_found(self) -> None:
        document = "123 Example Dr\n\nDenver, CO 80000"
        whole_document = CitationSpan(start=0, end=len(document))
        self.assertEqual(
            whole_document,
            self._find("123 Example Dr Denver, CO 80000", document=document),
        )
        self.assertEqual(
            whole_document,
            self._find("123 Example Dr\nDenver, CO 80000", document=document),
        )

    def test_quote_with_newline_where_document_has_space_found(self) -> None:
        self.assertEqual(CitationSpan(start=33, end=44), self._find("the\nKitchen"))

    def test_span_end_covers_document_whitespace(self) -> None:
        document = "works at the\n\nKitchen now"
        span = assert_type(self._find("the Kitchen", document=document), CitationSpan)
        self.assertEqual("the\n\nKitchen", document[span.start : span.end])

    def test_word_broken_by_document_whitespace_not_found(self) -> None:
        # Some source systems hard-wrap notes mid-word. A quote that rejoins the
        # word has no whitespace at the break, so it does not match.
        self.assertIsNone(
            self._find("COLORADO SPRINGS", document="COLORADO S\n\nPRINGS")
        )

    def test_quote_adding_whitespace_inside_document_word_not_found(self) -> None:
        self.assertIsNone(self._find("Kit chen"))

    def test_regex_metacharacters_in_quote_match_literally(self) -> None:
        document = "makes $200-$300 (weekly) at a.c. repair"
        self.assertEqual(
            CitationSpan(start=6, end=24),
            self._find("$200-$300 (weekly)", document=document),
        )
        self.assertIsNone(self._find("a.c.", document="abcd"))

    def test_recased_quote_with_different_whitespace_found(self) -> None:
        self.assertEqual(
            CitationSpan(start=12, end=24),
            self._find("the Kitchen", document="ASSIGNED TO THE\n\nKITCHEN"),
        )

    def test_quote_differing_in_wording_not_found(self) -> None:
        self.assertIsNone(self._find("Kitchen assignment"))

    def test_repeated_quote_resolves_to_first_occurrence(self) -> None:
        document = "Kitchen. " * 3
        self.assertEqual(
            CitationSpan(start=0, end=7), self._find("Kitchen", document=document)
        )

    def test_quote_deep_in_long_document_found(self) -> None:
        document = "x" * 5_000 + "Kitchen"
        self.assertEqual(
            CitationSpan(start=5_000, end=5_007),
            self._find("Kitchen", document=document),
        )
