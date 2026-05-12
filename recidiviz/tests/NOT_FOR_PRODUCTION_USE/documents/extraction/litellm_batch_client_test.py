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
"""Tests for litellm_batch_client.py and llm_provider_delegate.py."""
import json
import unittest
from typing import Any
from unittest.mock import MagicMock

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.document_provider import (
    GCSDocument,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.extraction_output_schema import (
    ExtractionFieldType,
    ExtractionInferredField,
    ExtractionOutputSchema,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.litellm_batch_client import (
    LiteLLMBatchClient,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.llm_client import (
    LLMExtractionRequest,
    LLMExtractionStatus,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.llm_provider_delegate import (
    VertexAIProviderDelegate,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.llm_prompt_extractor_metadata import (
    LLMPromptExtractorMetadata,
)

_MODULE = "recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.litellm_batch_client"

DOC_ID_A = "a" * 64
DOC_ID_B = "b" * 64
DOC_ID_C = "c" * 64


def _make_extractor() -> LLMPromptExtractorMetadata:
    return LLMPromptExtractorMetadata(
        extractor_id="US_OZ_TEST_EXTRACTOR",
        collection_name="TEST_COLLECTION",
        description="Test extractor",
        input_document_collection_name="US_OZ_TEST_DOCS",
        state_code=StateCode.US_OZ,
        llm_provider="vertex_ai",
        model="gemini-2.5-flash",
        instructions_prompt="Extract test data.",
    )


def _make_schema() -> ExtractionOutputSchema:
    return ExtractionOutputSchema(
        full_batch_description="batch",
        result_level_description="result",
        inferred_fields_by_name={
            "is_relevant": ExtractionInferredField(
                name="is_relevant",
                field_type=ExtractionFieldType.BOOLEAN,
                unaugmented_description="Whether relevant",
                required=True,
            ),
        },
    )


def _make_request(doc_id: str, content: str = "document text") -> LLMExtractionRequest:
    doc = GCSDocument(
        document_id=doc_id,
        gcs_file_path=GcsfsFilePath.from_absolute_path(
            f"gs://test-bucket/docs/{doc_id}.txt"
        ),
        mime_type="text/plain",
    )
    extractor = _make_extractor()
    request = MagicMock(spec=LLMExtractionRequest)
    request.document = doc
    request.model = extractor.model
    request.thinking_budget = None
    schema = _make_schema()
    request.output_schema = schema
    request.build_messages.return_value = [
        {"role": "system", "content": "Extract test data."},
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({"document_contents_id": doc_id}),
                },
                {"type": "text", "text": content},
            ],
        },
    ]
    return request


def _make_vertex_ai_output_line(
    doc_id: str, result_json: dict, include_doc_id_header: bool = True
) -> str:
    """Builds a predictions.jsonl line in Vertex AI's batch output format."""
    if include_doc_id_header:
        parts = [
            {"text": json.dumps({"document_contents_id": doc_id})},
            {"text": "document text"},
        ]
    else:
        parts = [{"text": "document text"}]
    return json.dumps(
        {
            "request": {
                "contents": [{"parts": parts}],
                "generationConfig": {},
                "system_instruction": {"parts": [{"text": "Extract test data."}]},
            },
            "status": "ok",
            "response": {
                "candidates": [
                    {"content": {"parts": [{"text": json.dumps(result_json)}]}}
                ],
                "usageMetadata": {"promptTokenCount": 100, "candidatesTokenCount": 50},
            },
            "processed_time": "2026-01-01T00:00:00Z",
        }
    )


class TestVertexAIProviderDelegateExtractCustomId(unittest.TestCase):
    """Tests for VertexAIProviderDelegate.extract_custom_id_from_batch_result_line."""

    def setUp(self) -> None:
        self.delegate = VertexAIProviderDelegate(
            gcs_bucket="test-bucket", project="test-project"
        )

    def test_extracts_doc_id_from_header(self) -> None:
        entry = json.loads(
            _make_vertex_ai_output_line(DOC_ID_A, {}, include_doc_id_header=True)
        )
        self.assertEqual(
            self.delegate.extract_custom_id_from_batch_result_line(entry), DOC_ID_A
        )

    def test_returns_none_when_no_header(self) -> None:
        entry = json.loads(
            _make_vertex_ai_output_line(DOC_ID_A, {}, include_doc_id_header=False)
        )
        self.assertIsNone(self.delegate.extract_custom_id_from_batch_result_line(entry))

    def test_returns_none_when_no_request_field(self) -> None:
        entry: dict[str, Any] = {"response": {"candidates": []}}
        self.assertIsNone(self.delegate.extract_custom_id_from_batch_result_line(entry))

    def test_returns_none_when_contents_empty(self) -> None:
        entry: dict[str, Any] = {"request": {"contents": []}}
        self.assertIsNone(self.delegate.extract_custom_id_from_batch_result_line(entry))

    def test_returns_none_when_parts_empty(self) -> None:
        entry: dict[str, Any] = {"request": {"contents": [{"parts": []}]}}
        self.assertIsNone(self.delegate.extract_custom_id_from_batch_result_line(entry))


class TestLiteLLMBatchClientBuildRequestLine(unittest.TestCase):
    """Tests for LiteLLMBatchClient._build_batch_request_line."""

    def setUp(self) -> None:
        delegate = MagicMock(spec=VertexAIProviderDelegate)
        delegate.custom_llm_provider = "vertex_ai"
        delegate.get_env_vars.return_value = {}
        self.client = LiteLLMBatchClient(provider_delegate=delegate)

    def test_user_message_has_document_id_metadata_part(self) -> None:
        request = _make_request(DOC_ID_A, content="some document text")
        # pylint: disable-next=protected-access
        line = self.client._build_batch_request_line(request)
        content_parts = line["body"]["messages"][-1]["content"]
        self.assertIsInstance(content_parts, list)
        self.assertEqual(len(content_parts), 2)
        metadata = json.loads(content_parts[0]["text"])
        self.assertEqual(metadata["document_contents_id"], DOC_ID_A)
        self.assertEqual(content_parts[1]["text"], "some document text")

    def test_custom_id_matches_document_id(self) -> None:
        request = _make_request(DOC_ID_A)
        # pylint: disable-next=protected-access
        line = self.client._build_batch_request_line(request)
        self.assertEqual(line["custom_id"], DOC_ID_A)

    def test_system_message_unchanged(self) -> None:
        request = _make_request(DOC_ID_A)
        # pylint: disable-next=protected-access
        line = self.client._build_batch_request_line(request)
        system_msg = line["body"]["messages"][0]
        self.assertEqual(system_msg["role"], "system")
        self.assertEqual(system_msg["content"], "Extract test data.")


class TestLiteLLMBatchClientParseResults(unittest.TestCase):
    """Tests for LiteLLMBatchClient._parse_batch_results.

    The key scenario: Vertex AI returns results in a different order from
    submission. With the document_id embedded in the request header and echoed
    back in the output, each result is attributed to the correct document
    regardless of output ordering.
    """

    def setUp(self) -> None:
        delegate = VertexAIProviderDelegate(
            gcs_bucket="test-bucket", project="test-project"
        )
        self.client = LiteLLMBatchClient(provider_delegate=delegate)

    def _parse(self, jsonl_lines: list[str], submitted_ids: list[str]) -> dict:
        content = "\n".join(jsonl_lines)
        # pylint: disable-next=protected-access
        results = self.client._parse_batch_results(content, submitted_ids)
        return {r.document_id: r for r in results}

    def test_correct_attribution_when_output_reordered(self) -> None:
        # Submitted A, B, C — output arrives C, A, B (reordered by Vertex AI).
        result_a = {
            "is_relevant": {
                "value": "true",
                "null_reason": None,
                "confidence_level": "explicit",
                "citations": None,
            }
        }
        result_b = {
            "is_relevant": {
                "value": "false",
                "null_reason": "no_info_found",
                "confidence_level": "explicit",
                "citations": None,
            }
        }
        result_c = {
            "is_relevant": {
                "value": "true",
                "null_reason": None,
                "confidence_level": "explicit",
                "citations": None,
            }
        }

        lines = [
            _make_vertex_ai_output_line(DOC_ID_C, result_c),
            _make_vertex_ai_output_line(DOC_ID_A, result_a),
            _make_vertex_ai_output_line(DOC_ID_B, result_b),
        ]
        submitted_ids = [DOC_ID_A, DOC_ID_B, DOC_ID_C]

        by_doc_id = self._parse(lines, submitted_ids)

        self.assertEqual(by_doc_id[DOC_ID_A].status, LLMExtractionStatus.SUCCESS)
        self.assertEqual(by_doc_id[DOC_ID_B].status, LLMExtractionStatus.SUCCESS)
        self.assertEqual(by_doc_id[DOC_ID_C].status, LLMExtractionStatus.SUCCESS)
        self.assertEqual(by_doc_id[DOC_ID_A].extracted_data, result_a)
        self.assertEqual(by_doc_id[DOC_ID_B].extracted_data, result_b)
        self.assertEqual(by_doc_id[DOC_ID_C].extracted_data, result_c)

    def test_falls_back_to_positional_when_no_header(self) -> None:
        # Old-format batch output (no DOCUMENT_ID header) falls back to
        # positional correlation, preserving backward compatibility.
        result_a = {
            "is_relevant": {
                "value": "true",
                "null_reason": None,
                "confidence_level": "explicit",
                "citations": None,
            }
        }
        lines = [
            _make_vertex_ai_output_line(DOC_ID_A, result_a, include_doc_id_header=False)
        ]
        submitted_ids = [DOC_ID_A]

        by_doc_id = self._parse(lines, submitted_ids)

        self.assertIn(DOC_ID_A, by_doc_id)
        self.assertEqual(by_doc_id[DOC_ID_A].status, LLMExtractionStatus.SUCCESS)


if __name__ == "__main__":
    unittest.main()
