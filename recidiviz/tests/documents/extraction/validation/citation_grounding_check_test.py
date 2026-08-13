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
"""Tests for CitationGroundingCheck, exercised against the fake extractor
collection paired with source document text built to ground its citations (see
`ground_citations_in_fake_source_text`).

Where a grounded quote sits — as opposed to whether it is grounded at all — is
CitationOffsetDriftAdjustment's business; the matching primitive both share is
tested in citation_matching_test.py, and this check's wiring into the validator
in llm_extraction_result_validator_test.py.
"""

from typing import Any
from unittest import TestCase

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    get_first_order_llm_extractor_config,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    CITATION_TEXT_FIELD_NAME,
    CITATIONS_FIELD_NAME,
)
from recidiviz.documents.extraction.models.llm_request_output_values import (
    LLMRequestOutputValues,
)
from recidiviz.documents.extraction.validation.citation_grounding_check import (
    CitationGroundingCheck,
)
from recidiviz.documents.extraction.validation.citation_matching import (
    SourceDocumentText,
)
from recidiviz.documents.extraction.validation.llm_document_validation_result import (
    ValidationCheckType,
    ValidationIssue,
)
from recidiviz.tests.documents import fake_config
from recidiviz.tests.documents.extraction.fake_extractor_result_json import (
    build_null_inferred_field_result_json,
    fake_all_fields_result_json,
    fake_irrelevant_result_json,
    fake_minimal_relevant_result_json,
    ground_citations_in_fake_source_text,
)
from recidiviz.utils.string_formatting import truncate_string_if_necessary

_STATE_CODE = StateCode.US_XX
_COLLECTION_NAME = "FAKE_EXTRACTOR_COLLECTION"


def _citations(result_json: dict[str, Any], field_name: str) -> list[dict[str, Any]]:
    """Returns the mutable citations array of top-level |field_name|."""
    return result_json["result"][field_name][CITATIONS_FIELD_NAME]


class CitationGroundingCheckTest(TestCase):
    """Tests for CitationGroundingCheck."""

    def setUp(self) -> None:
        self.output_schema = get_first_order_llm_extractor_config(
            _STATE_CODE, _COLLECTION_NAME, config_module=fake_config
        ).extractor_collection.output_schema

    def _issues(
        self, result_json: dict[str, Any], *, source_document_text: str
    ) -> list[ValidationIssue]:
        return CitationGroundingCheck.issues(
            output=LLMRequestOutputValues(
                output_schema=self.output_schema, output_json=result_json
            ),
            source_document_text=SourceDocumentText(text=source_document_text),
        )

    def _issues_for_grounded(
        self, result_json: dict[str, Any]
    ) -> list[ValidationIssue]:
        """Returns the findings against |result_json| paired with a source
        document that grounds every citation it carries."""
        grounded = ground_citations_in_fake_source_text(result_json)
        return self._issues(
            grounded.result_json, source_document_text=grounded.source_document_text
        )

    def _assert_single_issue(
        self,
        result_json: dict[str, Any],
        *,
        source_document_text: str,
        expected_field_name: str,
        expected_detail_substring: str,
    ) -> None:
        issues = self._issues(result_json, source_document_text=source_document_text)
        self.assertEqual(1, len(issues))
        issue = issues[0]
        self.assertEqual(ValidationCheckType.HALLUCINATED_CITATION, issue.check_type)
        self.assertEqual(expected_field_name, issue.field_name)
        self.assertIn(expected_detail_substring, issue.detail)

    def test_grounded_result_has_no_issues(self) -> None:
        self.assertEqual([], self._issues_for_grounded(fake_all_fields_result_json()))

    def test_grounded_minimal_result_has_no_issues(self) -> None:
        self.assertEqual(
            [], self._issues_for_grounded(fake_minimal_relevant_result_json())
        )

    def test_irrelevant_result_has_no_issues(self) -> None:
        # An irrelevant result carries no extracted fields, so no citations.
        self.assertEqual(
            [], self._issues(fake_irrelevant_result_json(), source_document_text="")
        )

    def test_fabricated_citation_flagged(self) -> None:
        grounded = ground_citations_in_fake_source_text(
            fake_minimal_relevant_result_json()
        )
        citation = _citations(grounded.result_json, "primary_status")[0]
        reported_start = citation["start"]
        citation[CITATION_TEXT_FIELD_NAME] = "a quote the document never contained"
        self._assert_single_issue(
            grounded.result_json,
            source_document_text=grounded.source_document_text,
            expected_field_name="primary_status",
            expected_detail_substring=(
                f"Citation [0] on field [primary_status] quotes text that is not "
                f"grounded within 10 characters of its reported offset "
                f"[{reported_start}]: [a quote the document never contained]."
            ),
        )

    def test_grounded_at_slightly_wrong_offset_not_flagged(self) -> None:
        # The quote is in the document, within 10 characters of its reported
        # offset — small drift CitationOffsetDriftAdjustment corrects rather than
        # this check failing the document over.
        grounded = ground_citations_in_fake_source_text(
            fake_minimal_relevant_result_json()
        )
        citation = _citations(grounded.result_json, "primary_status")[0]
        citation["start"] += 10
        citation["end"] += 10
        self.assertEqual(
            [],
            self._issues(
                grounded.result_json,
                source_document_text=grounded.source_document_text,
            ),
        )

    def test_grounded_far_from_reported_offset_flagged(self) -> None:
        # The quote does appear in the document, but nowhere near where the
        # model said — too far for CitationOffsetDriftAdjustment to be trusted to
        # correct, so it is failed here instead.
        grounded = ground_citations_in_fake_source_text(
            fake_minimal_relevant_result_json()
        )
        citation = _citations(grounded.result_json, "primary_status")[0]
        citation["start"] = 0
        citation["end"] = 5
        self._assert_single_issue(
            grounded.result_json,
            source_document_text=grounded.source_document_text,
            expected_field_name="primary_status",
            expected_detail_substring=(
                "Citation [0] on field [primary_status] quotes text that is not "
                "grounded within 10 characters of its reported offset [0]:"
            ),
        )

    def test_quote_broken_across_lines_flagged(self) -> None:
        # Matching is exact, not whitespace-normalized: a document that
        # reformats the quote's line breaks no longer contains it verbatim.
        grounded = ground_citations_in_fake_source_text(
            fake_minimal_relevant_result_json()
        )
        self._assert_single_issue(
            grounded.result_json,
            source_document_text=grounded.source_document_text.replace(
                "citation for", "citation\n   for"
            ),
            expected_field_name="primary_status",
            expected_detail_substring="quotes text that is not grounded within",
        )

    def test_empty_citation_text_flagged(self) -> None:
        # An empty quote cites nothing, so it grounds nowhere. The schema's
        # minItems only forces a citation to exist, not to say anything.
        grounded = ground_citations_in_fake_source_text(
            fake_minimal_relevant_result_json()
        )
        _citations(grounded.result_json, "primary_status")[0][
            CITATION_TEXT_FIELD_NAME
        ] = ""
        self._assert_single_issue(
            grounded.result_json,
            source_document_text=grounded.source_document_text,
            expected_field_name="primary_status",
            expected_detail_substring="quotes text that is not grounded within",
        )

    def test_fabricated_citation_on_null_branch_flagged(self) -> None:
        # A quote offered as evidence that nothing was there to extract is no
        # more trustworthy than one on the value branch.
        result_json = fake_minimal_relevant_result_json()
        location = build_null_inferred_field_result_json("explicitly_unknown")
        location[CITATIONS_FIELD_NAME] = [
            {"text": "employment status unknown", "start": 0, "end": 25}
        ]
        result_json["result"]["location"] = location
        self._assert_single_issue(
            result_json,
            # A document that grounds only the other field's citation.
            source_document_text="citation for active",
            expected_field_name="location",
            expected_detail_substring="[employment status unknown]",
        )

    def test_fabricated_citation_in_array_element_flagged_with_index(self) -> None:
        grounded = ground_citations_in_fake_source_text(fake_all_fields_result_json())
        grounded.result_json["result"]["assignments"][0]["rate_amount"][
            CITATIONS_FIELD_NAME
        ][0][CITATION_TEXT_FIELD_NAME] = "paid $99 an hour"
        self._assert_single_issue(
            grounded.result_json,
            source_document_text=grounded.source_document_text,
            expected_field_name="assignments[0].rate_amount",
            expected_detail_substring=(
                "Citation [0] on field [assignments[0].rate_amount]"
            ),
        )

    def test_second_citation_on_a_field_flagged_by_its_index(self) -> None:
        grounded = ground_citations_in_fake_source_text(
            fake_minimal_relevant_result_json()
        )
        _citations(grounded.result_json, "primary_status").append(
            {"text": "invented follow-up quote", "start": 0, "end": 24}
        )
        self._assert_single_issue(
            grounded.result_json,
            source_document_text=grounded.source_document_text,
            expected_field_name="primary_status",
            expected_detail_substring="Citation [1] on field [primary_status]",
        )

    def test_every_fabricated_citation_reported(self) -> None:
        grounded = ground_citations_in_fake_source_text(fake_all_fields_result_json())
        _citations(grounded.result_json, "primary_status")[0][
            CITATION_TEXT_FIELD_NAME
        ] = "invented status quote"
        grounded.result_json["result"]["assignments"][0]["assignment_name"][
            CITATIONS_FIELD_NAME
        ][0][CITATION_TEXT_FIELD_NAME] = "invented assignment quote"
        self.assertEqual(
            ["primary_status", "assignments[0].assignment_name"],
            [
                issue.field_name
                for issue in self._issues(
                    grounded.result_json,
                    source_document_text=grounded.source_document_text,
                )
            ],
        )

    def test_long_fabricated_quote_truncated_in_detail(self) -> None:
        # A fabricated passage can be arbitrarily long; the audit row echoes
        # enough of it to recognize and no more.
        grounded = ground_citations_in_fake_source_text(
            fake_minimal_relevant_result_json()
        )
        fabricated_quote = "x" * 500
        _citations(grounded.result_json, "primary_status")[0][
            CITATION_TEXT_FIELD_NAME
        ] = fabricated_quote
        issues = self._issues(
            grounded.result_json, source_document_text=grounded.source_document_text
        )
        self.assertEqual(1, len(issues))
        self.assertIn(
            f"[{truncate_string_if_necessary(fabricated_quote, max_length=100)}]",
            issues[0].detail,
        )

    def test_empty_source_document_grounds_nothing(self) -> None:
        result_json = fake_minimal_relevant_result_json()
        issues = self._issues(result_json, source_document_text="")
        self.assertEqual(
            [ValidationCheckType.HALLUCINATED_CITATION],
            [issue.check_type for issue in issues],
        )
