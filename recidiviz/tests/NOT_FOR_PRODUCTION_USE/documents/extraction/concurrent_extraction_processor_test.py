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
"""Tests for concurrent_extraction_processor.py."""
import json
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from litellm.exceptions import RateLimitError  # type: ignore[import-not-found]

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.concurrent_extraction_processor import (
    ConcurrentExtractionProcessor,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.document_provider import (
    GCSDocument,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.extraction_output_schema import (
    ExtractionFieldType,
    ExtractionInferredField,
    ExtractionOutputSchema,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.llm_client import (
    LLMExtractionResult,
    LLMExtractionStatus,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.document_extractor_collection_metadata import (
    DocumentExtractorCollectionMetadata,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.extraction_exclusion_type import (
    ExtractionExclusionType,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.llm_prompt_extractor_metadata import (
    LLMPromptExtractorMetadata,
)

_MODULE = "recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.concurrent_extraction_processor"


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
            "test_field": ExtractionInferredField(
                name="test_field",
                field_type=ExtractionFieldType.STRING,
                unaugmented_description="A test field",
                required=False,
            ),
        },
    )


def _make_collection() -> DocumentExtractorCollectionMetadata:
    return DocumentExtractorCollectionMetadata(
        name="TEST_COLLECTION",
        description="Test collection",
        output_schema=_make_schema(),
        confidence_threshold=0.8,
    )


def _make_document(doc_id: str) -> GCSDocument:
    return GCSDocument(
        document_id=doc_id,
        gcs_file_path=GcsfsFilePath.from_absolute_path(
            f"gs://test-bucket/US_OZ/{doc_id}.txt"
        ),
        mime_type="text/plain",
    )


def _make_llm_response(is_relevant: bool = True, confidence: float = 0.9) -> dict:
    return {
        "is_relevant": {
            "value": is_relevant,
            "confidence_score": confidence,
            "null_reason": None,
            "citations": None,
        },
        "test_field": {
            "value": "test_value",
            "confidence_score": confidence,
            "null_reason": None,
            "citations": None,
        },
    }


class TestConcurrentExtractionProcessor(unittest.TestCase):
    """Tests for ConcurrentExtractionProcessor."""

    def setUp(self) -> None:
        self.extractor = _make_extractor()
        self.collection = _make_collection()
        self.processor = ConcurrentExtractionProcessor(
            extractor=self.extractor,
            collection=self.collection,
            sandbox_dataset_prefix="test_prefix",
            max_llm_concurrency=5,
            max_gcs_read_workers=2,
            bq_write_batch_size=3,
            max_retries_per_doc=1,
        )
        self.bq_client = MagicMock()
        self.bq_client.project_id = "recidiviz-staging"

    def test_get_already_extracted_returns_empty_when_table_missing(self) -> None:
        self.bq_client.table_exists.return_value = False
        result = self.processor.get_already_extracted_document_ids(self.bq_client)
        self.assertEqual(result, set())

    def test_get_already_extracted_returns_doc_ids(self) -> None:
        self.bq_client.table_exists.return_value = True
        self.bq_client.run_query_async.return_value = [
            {"document_id": "doc-1"},
            {"document_id": "doc-2"},
        ]
        result = self.processor.get_already_extracted_document_ids(self.bq_client)
        self.assertEqual(result, {"doc-1", "doc-2"})

    @patch(f"{_MODULE}.acompletion")
    def test_resume_skips_already_extracted_docs(
        self, mock_acompletion: AsyncMock
    ) -> None:
        """Documents that already have results in BQ are skipped."""
        docs = [
            _make_document("doc-1"),
            _make_document("doc-2"),
            _make_document("doc-3"),
        ]

        # doc-1 already extracted
        self.bq_client.table_exists.return_value = True
        self.bq_client.run_query_async.return_value = [{"document_id": "doc-1"}]

        llm_response = MagicMock()
        llm_response.choices = [MagicMock()]
        llm_response.choices[0].message.content = json.dumps(_make_llm_response())
        mock_acompletion.return_value = llm_response

        with patch.object(self.processor, "_read_documents_parallel") as mock_read:
            mock_read.return_value = ({"doc-2": "content", "doc-3": "content"}, [])
            result = self.processor.process_documents(self.bq_client, docs)

        assert result is not None
        # Only 2 docs processed (doc-1 skipped)
        self.assertEqual(result.num_documents_originally_submitted, 2)
        # acompletion called for doc-2 and doc-3
        self.assertEqual(mock_acompletion.call_count, 2)

    @patch(f"{_MODULE}.acompletion")
    def test_all_docs_already_extracted_is_noop(
        self, mock_acompletion: AsyncMock
    ) -> None:
        """When all docs are already extracted, returns None and does no work."""
        docs = [_make_document("doc-1")]

        self.bq_client.table_exists.return_value = True
        self.bq_client.run_query_async.return_value = [{"document_id": "doc-1"}]

        result = self.processor.process_documents(self.bq_client, docs)

        self.assertIsNone(result)
        mock_acompletion.assert_not_called()

    @patch(f"{_MODULE}.acompletion")
    def test_gcs_read_failure_produces_permanent_failure(
        self, mock_acompletion: AsyncMock
    ) -> None:
        """A GCS read failure becomes a PERMANENT_FAILURE result."""
        docs = [_make_document("doc-good"), _make_document("doc-bad")]

        self.bq_client.table_exists.return_value = False

        llm_response = MagicMock()
        llm_response.choices = [MagicMock()]
        llm_response.choices[0].message.content = json.dumps(_make_llm_response())
        mock_acompletion.return_value = llm_response

        # Mock _read_documents_parallel to return one success and one failure
        gcs_failure = LLMExtractionResult(
            document_id="doc-bad",
            status=LLMExtractionStatus.PERMANENT_FAILURE,
            extracted_data=None,
            error_message="Failed to read document from GCS: not found",
            error_type=ExtractionExclusionType.DOCUMENT_NOT_FOUND,
        )
        with patch.object(self.processor, "_read_documents_parallel") as mock_read:
            mock_read.return_value = ({"doc-good": "content"}, [gcs_failure])
            result = self.processor.process_documents(self.bq_client, docs)

        assert result is not None
        self.assertEqual(result.num_successful_extractions, 1)
        self.assertEqual(result.num_failed_extractions, 1)
        # Only 1 LLM call (the good doc)
        self.assertEqual(mock_acompletion.call_count, 1)

    @patch(f"{_MODULE}.acompletion")
    def test_llm_rate_limit_retries_then_succeeds(
        self, mock_acompletion: AsyncMock
    ) -> None:
        """Rate-limited calls are retried and can succeed."""
        docs = [_make_document("doc-1")]

        self.bq_client.table_exists.return_value = False

        good_response = MagicMock()
        good_response.choices = [MagicMock()]
        good_response.choices[0].message.content = json.dumps(_make_llm_response())

        mock_acompletion.side_effect = [
            RateLimitError("rate limited", "vertex_ai", "gemini"),
            good_response,
        ]

        # pylint: disable=protected-access
        self.processor._retry_base_delay = 0.01

        with patch.object(self.processor, "_read_documents_parallel") as mock_read:
            mock_read.return_value = ({"doc-1": "content"}, [])
            result = self.processor.process_documents(self.bq_client, docs)

        assert result is not None
        self.assertEqual(result.num_successful_extractions, 1)
        self.assertEqual(result.num_failed_extractions, 0)
        self.assertEqual(mock_acompletion.call_count, 2)

    @patch(f"{_MODULE}.acompletion")
    def test_llm_permanent_failure_after_max_retries(
        self, mock_acompletion: AsyncMock
    ) -> None:
        """After max retries, the doc gets PERMANENT_FAILURE."""
        docs = [_make_document("doc-1")]

        self.bq_client.table_exists.return_value = False

        # Always fails — processor has max_retries_per_doc=1, so 2 attempts total
        mock_acompletion.side_effect = RateLimitError(
            "rate limited", "vertex_ai", "gemini"
        )

        # pylint: disable=protected-access
        self.processor._retry_base_delay = 0.01

        with patch.object(self.processor, "_read_documents_parallel") as mock_read:
            mock_read.return_value = ({"doc-1": "content"}, [])
            result = self.processor.process_documents(self.bq_client, docs)

        assert result is not None
        self.assertEqual(result.num_successful_extractions, 0)
        self.assertEqual(result.num_failed_extractions, 1)
        self.assertEqual(mock_acompletion.call_count, 2)  # initial + 1 retry

    @patch(f"{_MODULE}.acompletion")
    def test_bq_writes_batched(self, mock_acompletion: AsyncMock) -> None:
        """Results are flushed to BQ in batches of bq_write_batch_size."""
        # 5 docs with batch_size=3 -> 2 flushes (3 + 2)
        docs = [_make_document(f"doc-{i}") for i in range(5)]

        self.bq_client.table_exists.return_value = False

        llm_response = MagicMock()
        llm_response.choices = [MagicMock()]
        llm_response.choices[0].message.content = json.dumps(_make_llm_response())
        mock_acompletion.return_value = llm_response

        with patch.object(self.processor, "_read_documents_parallel") as mock_read:
            mock_read.return_value = (
                {f"doc-{i}": "content" for i in range(5)},
                [],
            )
            result = self.processor.process_documents(self.bq_client, docs)

        assert result is not None
        self.assertEqual(result.num_documents_originally_submitted, 5)

        # Count stream_into_table calls for the raw results table
        raw_table_calls = [
            call
            for call in self.bq_client.stream_into_table.call_args_list
            if "document_extraction_results__raw" in str(call)
        ]
        # At least 2 flushes from _extract_all_async (batch of 3 + batch of 2)
        self.assertGreaterEqual(len(raw_table_calls), 2)

    @patch(f"{_MODULE}.acompletion")
    def test_metadata_tables_written(self, mock_acompletion: AsyncMock) -> None:
        """Job metadata, submitted docs, and job results are written to BQ."""
        docs = [_make_document("doc-1")]

        self.bq_client.table_exists.return_value = False

        llm_response = MagicMock()
        llm_response.choices = [MagicMock()]
        llm_response.choices[0].message.content = json.dumps(_make_llm_response())
        mock_acompletion.return_value = llm_response

        with patch.object(self.processor, "_read_documents_parallel") as mock_read:
            mock_read.return_value = ({"doc-1": "content"}, [])
            result = self.processor.process_documents(self.bq_client, docs)

        assert result is not None

        # Collect all table addresses written to
        written_tables = {
            str(call[0][0]) for call in self.bq_client.stream_into_table.call_args_list
        }
        table_strs = " ".join(written_tables)

        # Verify metadata tables were written
        self.assertIn("extraction_jobs", table_strs)
        self.assertIn("extraction_job_submitted_documents", table_strs)
        self.assertIn("extraction_job_results", table_strs)
        self.assertIn("extractor_collections", table_strs)
