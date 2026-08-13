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
"""Tests for CitationOffsetDriftAdjustment, exercised against the fake extractor
collection paired with source document text built to ground its citations (see
`ground_citations_in_fake_source_text`).

Unlike the checks, an adjustment both reports and rewrites: every case here pins
the findings AND the offsets left behind, with the corrected span asserted by
slicing the document with it — which is the guarantee the adjustment exists to
provide. The adjustment rewrites a copy and leaves the output it was handed
alone, so a case sets its drift up on the input JSON and reads the corrected
offsets off the returned copy.
"""

import copy
from typing import Any
from unittest import TestCase

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    get_first_order_llm_extractor_config,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    CITATION_END_FIELD_NAME,
    CITATION_START_FIELD_NAME,
    CITATION_TEXT_FIELD_NAME,
    CITATIONS_FIELD_NAME,
)
from recidiviz.documents.extraction.models.llm_request_output_values import (
    LLMRequestOutputValues,
)
from recidiviz.documents.extraction.validation.citation_matching import (
    SourceDocumentText,
)
from recidiviz.documents.extraction.validation.citation_offset_drift_adjustment import (
    CitationOffsetDriftAdjustment,
)
from recidiviz.documents.extraction.validation.llm_document_validation_result import (
    ValidationCheckType,
    ValidationIssue,
)
from recidiviz.tests.documents import fake_config
from recidiviz.tests.documents.extraction.fake_extractor_result_json import (
    fake_all_fields_result_json,
    fake_irrelevant_result_json,
    fake_minimal_relevant_result_json,
    ground_citations_in_fake_source_text,
)

_STATE_CODE = StateCode.US_XX
_COLLECTION_NAME = "FAKE_EXTRACTOR_COLLECTION"
# How far a test drifts a grounded citation's reported start offset — within
# MAX_CITATION_OFFSET_DRIFT, since a document with drift beyond that window
# fails CitationGroundingCheck before this adjustment ever runs.
_DRIFT_WITHIN_WINDOW = 3


def _citations(result_json: dict[str, Any], field_name: str) -> list[dict[str, Any]]:
    """Returns the mutable citations array of top-level |field_name|."""
    return result_json["result"][field_name][CITATIONS_FIELD_NAME]


class CitationOffsetDriftAdjustmentTest(TestCase):
    """Tests for CitationOffsetDriftAdjustment."""

    def setUp(self) -> None:
        self.output_schema = get_first_order_llm_extractor_config(
            _STATE_CODE, _COLLECTION_NAME, config_module=fake_config
        ).extractor_collection.output_schema

    def _apply(
        self, result_json: dict[str, Any], *, source_document_text: str
    ) -> tuple[list[ValidationIssue], dict[str, Any]]:
        """Returns the findings from adjusting |result_json|, paired with the
        adjusted JSON — a copy, leaving |result_json| itself untouched."""
        adjusted, issues = CitationOffsetDriftAdjustment.apply(
            output=LLMRequestOutputValues(
                output_schema=self.output_schema, output_json=result_json
            ),
            source_document_text=SourceDocumentText(text=source_document_text),
        )
        return issues, adjusted.output_json

    def _assert_unchanged(
        self, result_json: dict[str, Any], *, source_document_text: str
    ) -> None:
        """Asserts adjusting |result_json| reports nothing and returns a copy of
        it unchanged."""
        before = copy.deepcopy(result_json)
        issues, adjusted_json = self._apply(
            result_json, source_document_text=source_document_text
        )
        self.assertEqual([], issues)
        self.assertEqual(before, adjusted_json)
        self.assertEqual(before, result_json)

    def _assert_citation_quotes_itself(
        self, citation_json: dict[str, Any], *, source_document_text: str
    ) -> None:
        """Asserts |citation_json|'s offsets now bracket its own quoted text in
        |source_document_text| — the invariant the adjustment guarantees."""
        self.assertEqual(
            citation_json[CITATION_TEXT_FIELD_NAME],
            source_document_text[
                citation_json[CITATION_START_FIELD_NAME] : citation_json[
                    CITATION_END_FIELD_NAME
                ]
            ],
        )

    def test_grounded_result_untouched(self) -> None:
        grounded = ground_citations_in_fake_source_text(fake_all_fields_result_json())
        self._assert_unchanged(
            grounded.result_json,
            source_document_text=grounded.source_document_text,
        )

    def test_irrelevant_result_untouched(self) -> None:
        self._assert_unchanged(fake_irrelevant_result_json(), source_document_text="")

    def test_drifted_offsets_corrected(self) -> None:
        grounded = ground_citations_in_fake_source_text(
            fake_minimal_relevant_result_json()
        )
        citation = _citations(grounded.result_json, "primary_status")[0]
        correct_start, correct_end = (
            citation[CITATION_START_FIELD_NAME],
            citation[CITATION_END_FIELD_NAME],
        )
        drifted_start = correct_start - _DRIFT_WITHIN_WINDOW
        drifted_end = correct_end - _DRIFT_WITHIN_WINDOW
        citation[CITATION_START_FIELD_NAME] = drifted_start
        citation[CITATION_END_FIELD_NAME] = drifted_end

        issues, adjusted_json = self._apply(
            grounded.result_json,
            source_document_text=grounded.source_document_text,
        )

        self.assertEqual(1, len(issues))
        issue = issues[0]
        self.assertEqual(ValidationCheckType.CITATION_OFFSET_DRIFT, issue.check_type)
        self.assertEqual("primary_status", issue.field_name)
        self.assertIn(
            f"was reported at offsets [{drifted_start}, {drifted_end}) but its "
            f"quoted text sits at [{correct_start}, {correct_end})",
            issue.detail,
        )
        adjusted_citation = _citations(adjusted_json, "primary_status")[0]
        self.assertEqual(correct_start, adjusted_citation[CITATION_START_FIELD_NAME])
        self.assertEqual(correct_end, adjusted_citation[CITATION_END_FIELD_NAME])
        self._assert_citation_quotes_itself(
            adjusted_citation, source_document_text=grounded.source_document_text
        )

    def test_wrong_end_alone_corrected(self) -> None:
        # A model that reports an inclusive end offset drifts by one on every
        # citation; the correction pins the exclusive-end convention.
        grounded = ground_citations_in_fake_source_text(
            fake_minimal_relevant_result_json()
        )
        citation = _citations(grounded.result_json, "primary_status")[0]
        correct_end = citation[CITATION_END_FIELD_NAME]
        citation[CITATION_END_FIELD_NAME] = correct_end - 1

        issues, adjusted_json = self._apply(
            grounded.result_json,
            source_document_text=grounded.source_document_text,
        )

        self.assertEqual(1, len(issues))
        self.assertEqual(
            correct_end,
            _citations(adjusted_json, "primary_status")[0][CITATION_END_FIELD_NAME],
        )

    def test_ungrounded_citation_left_alone(self) -> None:
        # A quote that appears nowhere has no correct offsets to move to. The
        # validator fails such a document on CitationGroundingCheck before this
        # adjustment ever runs.
        grounded = ground_citations_in_fake_source_text(
            fake_minimal_relevant_result_json()
        )
        _citations(grounded.result_json, "primary_status")[0][
            CITATION_TEXT_FIELD_NAME
        ] = "a quote the document never contained"

        self._assert_unchanged(
            grounded.result_json,
            source_document_text=grounded.source_document_text,
        )

    def test_quote_broken_across_lines_left_ungrounded(self) -> None:
        # Matching is exact, not whitespace-normalized: a document that
        # reformats the quote's line breaks no longer contains it verbatim, so
        # there is no span to correct the citation's offsets to.
        grounded = ground_citations_in_fake_source_text(
            fake_minimal_relevant_result_json()
        )
        source_document_text = grounded.source_document_text.replace(
            "citation for active", "citation\n   for active"
        )

        self._assert_unchanged(
            grounded.result_json, source_document_text=source_document_text
        )

    def test_repeated_quote_corrected_to_nearest_occurrence(self) -> None:
        # The quote appears twice; the model's offsets are closer to the second,
        # so that is the occurrence it plausibly meant.
        result_json = fake_minimal_relevant_result_json()
        source_document_text = "citation for active. Later: citation for active."
        citation = _citations(result_json, "primary_status")[0]
        citation[CITATION_START_FIELD_NAME] = 26
        citation[CITATION_END_FIELD_NAME] = 45

        issues, adjusted_json = self._apply(
            result_json, source_document_text=source_document_text
        )

        self.assertEqual(1, len(issues))
        adjusted_citation = _citations(adjusted_json, "primary_status")[0]
        self.assertEqual(28, adjusted_citation[CITATION_START_FIELD_NAME])
        self._assert_citation_quotes_itself(
            adjusted_citation, source_document_text=source_document_text
        )

    def test_drift_in_array_element_reported_with_index(self) -> None:
        grounded = ground_citations_in_fake_source_text(fake_all_fields_result_json())
        citation = grounded.result_json["result"]["assignments"][0]["rate_amount"][
            CITATIONS_FIELD_NAME
        ][0]
        citation[CITATION_START_FIELD_NAME] -= _DRIFT_WITHIN_WINDOW
        citation[CITATION_END_FIELD_NAME] -= _DRIFT_WITHIN_WINDOW

        issues, adjusted_json = self._apply(
            grounded.result_json,
            source_document_text=grounded.source_document_text,
        )

        self.assertEqual(1, len(issues))
        issue = issues[0]
        self.assertEqual("assignments[0].rate_amount", issue.field_name)
        self.assertIn(
            "Citation [0] on field [assignments[0].rate_amount]", issue.detail
        )
        self._assert_citation_quotes_itself(
            adjusted_json["result"]["assignments"][0]["rate_amount"][
                CITATIONS_FIELD_NAME
            ][0],
            source_document_text=grounded.source_document_text,
        )

    def test_only_the_drifted_citation_of_a_field_corrected(self) -> None:
        grounded = ground_citations_in_fake_source_text(
            fake_minimal_relevant_result_json()
        )
        citations = _citations(grounded.result_json, "primary_status")
        citations.append(copy.deepcopy(citations[0]))
        citations[1][CITATION_START_FIELD_NAME] -= _DRIFT_WITHIN_WINDOW
        citations[1][CITATION_END_FIELD_NAME] -= _DRIFT_WITHIN_WINDOW
        grounded_citation = copy.deepcopy(citations[0])

        issues, adjusted_json = self._apply(
            grounded.result_json,
            source_document_text=grounded.source_document_text,
        )

        self.assertEqual(1, len(issues))
        self.assertIn("Citation [1] on field [primary_status]", issues[0].detail)
        # The already-grounded citation is carried across untouched, and the
        # drifted one is corrected onto the same span.
        adjusted_citations = _citations(adjusted_json, "primary_status")
        self.assertEqual(grounded_citation, adjusted_citations[0])
        self.assertEqual(grounded_citation, adjusted_citations[1])

    def test_every_drifted_citation_corrected(self) -> None:
        grounded = ground_citations_in_fake_source_text(fake_all_fields_result_json())
        drifted_citations = [
            _citations(grounded.result_json, "primary_status")[0],
            grounded.result_json["result"]["assignments"][0]["assignment_name"][
                CITATIONS_FIELD_NAME
            ][0],
        ]
        for citation in drifted_citations:
            citation[CITATION_START_FIELD_NAME] -= _DRIFT_WITHIN_WINDOW
            citation[CITATION_END_FIELD_NAME] -= _DRIFT_WITHIN_WINDOW

        issues, adjusted_json = self._apply(
            grounded.result_json,
            source_document_text=grounded.source_document_text,
        )

        self.assertEqual(
            ["primary_status", "assignments[0].assignment_name"],
            [issue.field_name for issue in issues],
        )
        for adjusted_citation in (
            _citations(adjusted_json, "primary_status")[0],
            adjusted_json["result"]["assignments"][0]["assignment_name"][
                CITATIONS_FIELD_NAME
            ][0],
        ):
            self._assert_citation_quotes_itself(
                adjusted_citation,
                source_document_text=grounded.source_document_text,
            )

    def test_given_output_is_left_untouched(self) -> None:
        # The adjustment rewrites a copy, so the raw output the extractor
        # returned keeps the offsets the model reported even on a document the
        # adjustment did correct — which is what lets the validator hand it the
        # raw output directly.
        grounded = ground_citations_in_fake_source_text(
            fake_minimal_relevant_result_json()
        )
        _citations(grounded.result_json, "primary_status")[0][
            CITATION_START_FIELD_NAME
        ] -= _DRIFT_WITHIN_WINDOW
        raw_output = LLMRequestOutputValues(
            output_schema=self.output_schema, output_json=grounded.result_json
        )
        before = copy.deepcopy(grounded.result_json)

        adjusted_output, issues = CitationOffsetDriftAdjustment.apply(
            output=raw_output,
            source_document_text=SourceDocumentText(text=grounded.source_document_text),
        )

        self.assertEqual(1, len(issues))
        self.assertEqual(before, raw_output.output_json)
        self.assertIsNot(raw_output.output_json, adjusted_output.output_json)
        self.assertNotEqual(before, adjusted_output.output_json)
