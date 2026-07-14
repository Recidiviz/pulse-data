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
"""Tests for the provider-agnostic LLM extraction client types."""
from unittest import TestCase

from google.genai import types

from recidiviz.documents.extraction.llm_client.types import (
    LLMClientDocumentExtractionResult,
    LLMDocumentExtractionTokenCounts,
    LLMRequestErrorType,
)


class LLMDocumentExtractionTokenCountsTest(TestCase):
    """Tests for LLMDocumentExtractionTokenCounts factories."""

    def test_empty_is_all_zero(self) -> None:
        counts = LLMDocumentExtractionTokenCounts.empty()
        self.assertEqual(0, counts.input_token_count)
        self.assertEqual(0, counts.output_token_count)
        self.assertEqual(0, counts.cached_input_token_count)
        self.assertEqual(0, counts.thinking_token_count)

    def test_populated_usage_is_mapped(self) -> None:
        counts = LLMDocumentExtractionTokenCounts.from_google_genai_usage_metadata(
            usage=types.GenerateContentResponseUsageMetadata(
                prompt_token_count=100,
                candidates_token_count=20,
                cached_content_token_count=80,
                thoughts_token_count=5,
            )
        )
        self.assertEqual(100, counts.input_token_count)
        self.assertEqual(20, counts.output_token_count)
        self.assertEqual(80, counts.cached_input_token_count)
        self.assertEqual(5, counts.thinking_token_count)

    def test_absent_count_fields_coalesce_to_zero(self) -> None:
        # The SDK leaves individual counts as None (e.g. no cached content, no
        # thinking); each must coalesce to 0 rather than propagating None.
        counts = LLMDocumentExtractionTokenCounts.from_google_genai_usage_metadata(
            usage=types.GenerateContentResponseUsageMetadata(prompt_token_count=100)
        )
        self.assertEqual(100, counts.input_token_count)
        self.assertEqual(0, counts.output_token_count)
        self.assertEqual(0, counts.cached_input_token_count)
        self.assertEqual(0, counts.thinking_token_count)


class LLMClientDocumentExtractionResultTest(TestCase):
    """Tests for the LLMClientDocumentExtractionResult invariants and factories."""

    def _token_counts(self) -> LLMDocumentExtractionTokenCounts:
        return LLMDocumentExtractionTokenCounts.empty()

    def test_from_success_and_from_error_factories(self) -> None:
        success = LLMClientDocumentExtractionResult.from_success(
            document_contents_id="doc_1",
            result_json={"result": {}},
            token_counts=self._token_counts(),
        )
        self.assertEqual({"result": {}}, success.result_json)
        self.assertIsNone(success.error_type)
        self.assertIsNone(success.error_message)

        error = LLMClientDocumentExtractionResult.from_error(
            document_contents_id="doc_1",
            error_type=LLMRequestErrorType.TIMEOUT,
            error_message="timed out",
        )
        self.assertIsNone(error.result_json)
        self.assertEqual(LLMRequestErrorType.TIMEOUT, error.error_type)
        # A failure that never billed reports zero counts, not None.
        self.assertEqual(0, error.token_counts.input_token_count)

    def test_result_and_error_are_mutually_exclusive(self) -> None:
        # Neither set, both set, and a half-set error pair each violate the
        # invariant.
        with self.assertRaisesRegex(
            ValueError, r"Exactly one of result_json and error_type"
        ):
            LLMClientDocumentExtractionResult(
                document_contents_id="doc_1",
                result_json=None,
                token_counts=self._token_counts(),
                error_type=None,
                error_message=None,
            )
        with self.assertRaisesRegex(
            ValueError, r"Exactly one of result_json and error_type"
        ):
            LLMClientDocumentExtractionResult(
                document_contents_id="doc_1",
                result_json={"result": {}},
                token_counts=self._token_counts(),
                error_type=LLMRequestErrorType.TIMEOUT,
                error_message="timed out",
            )
        with self.assertRaisesRegex(
            ValueError, r"error_type and error_message must both be set"
        ):
            LLMClientDocumentExtractionResult(
                document_contents_id="doc_1",
                result_json=None,
                token_counts=self._token_counts(),
                error_type=LLMRequestErrorType.TIMEOUT,
                error_message=None,
            )
