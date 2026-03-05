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
"""LiteLLM-based client for document extraction with GCS persistence."""
import asyncio
import json
import uuid
from typing import Any

import nest_asyncio  # type: ignore[import-untyped]
from google.cloud import storage  # type: ignore[import-untyped, attr-defined]
from litellm import acompletion  # type: ignore[import-not-found]

from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.document_extractor_configs import (
    get_extractor,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.llm_client import (
    BatchNotFoundError,
    LLMClient,
    LLMExtractionBatchResult,
    LLMExtractionInProgressBatchStatus,
    LLMExtractionRequest,
    LLMExtractionResult,
    LLMExtractionStatus,
    LLMResultReader,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.document_extraction_error_type import (
    DocumentExtractionErrorType,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.extraction_job_error_type import (
    ExtractionJobErrorType,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.extraction_job_metadata import (
    ExtractionJobMetadata,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.extraction_job_submitted_document_metadata import (
    ExtractionJobSubmittedDocumentMetadata,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.llm_prompt_extractor_metadata import (
    LLMPromptExtractorMetadata,
)


class LiteLLMClient(LLMClient, LLMResultReader):
    """LLM client that uses LiteLLM to make real API calls to language models.

    This client reads documents from GCS, sends them to the configured LLM via
    LiteLLM, and parses structured JSON responses. Results are persisted to
    GCS to survive process restarts.

    Supports any model supported by LiteLLM (Gemini, Claude, OpenAI, etc.).
    Uses llm_provider and model from extraction requests to build LiteLLM
    model strings (e.g., "vertex_ai/gemini-2.5-flash").

    Uses Application Default Credentials for authentication with Vertex AI.
    Run `gcloud auth application-default login` before using.

    GCS Persistence Structure:
        gs://{bucket}/{prefix}/{job_id}/
            └── results.json    # Extraction results (updated incrementally)

    Job metadata (job_id, total_count) comes from ExtractionJob in BQ.
    Requests are reconstructed from submitted_documents + extractor at result
    retrieval time.
    """

    def __init__(
        self,
        batches_gcs_bucket: str,
        batches_gcs_prefix: str = "extraction_batches",
        temperature: float = 0.001,
        max_output_tokens: int = 8192,
        top_p: float = 1.0,
        batch_size: int = 10,
    ) -> None:
        """Initialize the LiteLLM client.

        Args:
            batches_gcs_bucket: GCS bucket for storing batch state.
            batches_gcs_prefix: Prefix within bucket for batch directories.
            temperature: Sampling temperature (0.0 = deterministic, 2.0 = random).
            max_output_tokens: Maximum number of tokens to generate.
            top_p: Nucleus sampling parameter.
            batch_size: Number of concurrent async requests to process at once.
        """
        self._batches_gcs_bucket = batches_gcs_bucket
        self._batches_gcs_prefix = batches_gcs_prefix
        self._temperature = temperature
        self._max_output_tokens = max_output_tokens
        self._top_p = top_p
        self._batch_size = batch_size
        self._storage_client: storage.Client | None = None
        # Reuse the same event loop to avoid LiteLLM's logging queue issues
        self._event_loop: asyncio.AbstractEventLoop | None = None

    def _get_storage_client(self) -> storage.Client:
        """Lazily initialize and return the GCS storage client."""
        if self._storage_client is None:
            self._storage_client = storage.Client()
        return self._storage_client

    # GCS path helpers
    def _batch_dir_path(self, job_id: str) -> str:
        """Returns the GCS path prefix for a batch directory."""
        return f"{self._batches_gcs_prefix}/{job_id}"

    def _results_blob_path(self, job_id: str) -> str:
        """Returns the GCS blob path for results.json."""
        return f"{self._batch_dir_path(job_id)}/results.json"

    # Serialization helpers
    def _serialize_result(self, result: LLMExtractionResult) -> dict[str, Any]:
        """Serialize an extraction result for GCS storage."""
        return {
            "document_id": result.document_id,
            "status": result.status.value,
            "extracted_data": result.extracted_data,
            "error_message": result.error_message,
            "error_type": result.error_type.value if result.error_type else None,
        }

    def _deserialize_result(self, data: dict[str, Any]) -> LLMExtractionResult:
        """Deserialize an extraction result from GCS storage."""
        status = LLMExtractionStatus(data["status"])
        raw_error_type = data.get("error_type")
        if raw_error_type is not None:
            error_type: DocumentExtractionErrorType | None = (
                DocumentExtractionErrorType(raw_error_type)
            )
        elif status != LLMExtractionStatus.SUCCESS:
            # Backwards compat: old serialized data may not have error_type
            error_type = DocumentExtractionErrorType.UNKNOWN
        else:
            error_type = None
        return LLMExtractionResult(
            document_id=data["document_id"],
            status=status,
            extracted_data=data.get("extracted_data"),
            error_message=data.get("error_message"),
            error_type=error_type,
        )

    # GCS read/write helpers
    def _write_json_to_gcs(self, blob_path: str, data: Any) -> None:
        """Write JSON data to a GCS blob."""
        client = self._get_storage_client()
        bucket = client.bucket(self._batches_gcs_bucket)
        blob = bucket.blob(blob_path)
        blob.upload_from_string(json.dumps(data), content_type="application/json")

    def _read_json_from_gcs(self, blob_path: str) -> Any:
        """Read JSON data from a GCS blob. Returns None if blob doesn't exist."""
        client = self._get_storage_client()
        bucket = client.bucket(self._batches_gcs_bucket)
        blob = bucket.blob(blob_path)
        if not blob.exists():
            return None
        return json.loads(blob.download_as_text())

    def _prepare_response_format(self, schema: dict[str, Any]) -> dict[str, Any]:
        """Prepare response format for LiteLLM structured output.

        Args:
            schema: JSON schema for structured responses.

        Returns:
            Response format dictionary for LiteLLM.
        """
        return {
            "type": "json_schema",
            "json_schema": {
                "name": "extraction_response",
                "strict": True,
                "schema": schema,
            },
        }

    async def _process_request_async(
        self,
        request: LLMExtractionRequest,
    ) -> LLMExtractionResult:
        """Process a single extraction request asynchronously.

        Args:
            request: The extraction request.

        Returns:
            LLMExtractionResult with extracted data or error.
        """
        messages = request.build_messages()
        schema = request.output_schema.to_llm_json_schema()
        response_format = self._prepare_response_format(schema)

        completion_kwargs: dict[str, Any] = {
            "model": f"{request.llm_provider}/{request.model}",
            "messages": messages,
            "temperature": self._temperature,
            "max_tokens": self._max_output_tokens,
            "top_p": self._top_p,
            "response_format": response_format,
        }

        try:
            response = await acompletion(**completion_kwargs)
            response_text = response.choices[0].message.content

            # Parse JSON response
            try:
                response_data = json.loads(response_text)
            except json.JSONDecodeError as e:
                return LLMExtractionResult(
                    document_id=request.document.document_id,
                    status=LLMExtractionStatus.PERMANENT_FAILURE,
                    extracted_data=None,
                    error_message=f"Failed to parse JSON response: {e}. Raw response: {response_text[:500]}",
                    error_type=DocumentExtractionErrorType.LLM_MALFORMED_RESPONSE,
                )

            # The schema returns a single object (one result per document).
            if isinstance(response_data, dict):
                return LLMExtractionResult(
                    document_id=request.document.document_id,
                    status=LLMExtractionStatus.SUCCESS,
                    extracted_data=response_data,
                    error_message=None,
                    error_type=None,
                )

            return LLMExtractionResult(
                document_id=request.document.document_id,
                status=LLMExtractionStatus.PERMANENT_FAILURE,
                extracted_data=None,
                error_message=f"Unexpected response format from LLM. Got type: {type(response_data).__name__}. Raw: {response_text[:500]}",
                error_type=DocumentExtractionErrorType.LLM_MALFORMED_RESPONSE,
            )

        except Exception as e:
            return LLMExtractionResult(
                document_id=request.document.document_id,
                status=LLMExtractionStatus.TRANSIENT_FAILURE,
                extracted_data=None,
                error_message=str(e),
                error_type=DocumentExtractionErrorType.UNKNOWN,
            )

    async def _process_batch_async(
        self,
        requests: list[LLMExtractionRequest],
    ) -> list[LLMExtractionResult]:
        """Process multiple requests asynchronously in batches.

        Args:
            requests: List of extraction requests.

        Returns:
            List of extraction results.
        """
        tasks = [self._process_request_async(request) for request in requests]

        # Process in batches to avoid overwhelming the API
        results: list[LLMExtractionResult] = []
        for i in range(0, len(tasks), self._batch_size):
            batch = tasks[i : i + self._batch_size]
            batch_results = await asyncio.gather(*batch)
            results.extend(batch_results)

            # Brief pause between batches
            if i + self._batch_size < len(tasks):
                await asyncio.sleep(1)

        return results

    def _get_event_loop(self) -> asyncio.AbstractEventLoop:
        """Get or create a reusable event loop.

        Reusing the same event loop across calls avoids issues with LiteLLM's
        internal logging queue getting bound to a stale loop.
        """
        if self._event_loop is None or self._event_loop.is_closed():
            self._event_loop = asyncio.new_event_loop()
        return self._event_loop

    def _run_async(self, coro: Any) -> Any:
        """Run an async coroutine, handling nested event loops."""
        try:
            # Check if we're in a running event loop (e.g., Jupyter notebook)
            asyncio.get_running_loop()
            # Use nest_asyncio to allow nested event loops
            nest_asyncio.apply()
            return asyncio.run(coro)
        except RuntimeError:
            # No running event loop - use our reusable loop
            loop = self._get_event_loop()
            return loop.run_until_complete(coro)

    def submit_batch(self, requests: list[LLMExtractionRequest]) -> str:
        """Submits a batch of extraction requests to the LLM service.

        Returns a job_id immediately. Actual processing happens when
        get_results() is called. The requests are not persisted here; they are
        reconstructed from submitted_documents + extractor at result retrieval.

        Args:
            requests: The extraction requests to submit (unused, reconstructed from BQ).

        Returns:
            The job_id for tracking this batch.
        """
        # requests are not used here - they are reconstructed from
        # submitted_documents at result retrieval time.
        _ = requests
        job_id = str(uuid.uuid4())

        # Initialize empty results file
        self._write_json_to_gcs(self._results_blob_path(job_id), [])

        return job_id

    def get_results(
        self,
        extraction_job: ExtractionJobMetadata,
        submitted_documents: list[ExtractionJobSubmittedDocumentMetadata],
    ) -> LLMExtractionBatchResult | LLMExtractionInProgressBatchStatus:
        """Retrieves the results for a previously submitted batch.

        If the batch is not complete, processes one batch of pending requests
        and returns progress status.

        Args:
            extraction_job: The ExtractionJob (read from BQ). Provides job_id
                and extractor_id for rebuilding requests.
            submitted_documents: Documents submitted with this job (read from BQ).
                Used to reconstruct extraction requests for pending documents.

        Returns:
            Either LLMExtractionBatchResult if complete, or
            LLMExtractionInProgressBatchStatus if still processing.

        Raises:
            BatchNotFoundError: If the job_id is not found.
        """
        job_id = extraction_job.job_id
        total_count = len(submitted_documents)

        # Look up the extractor from the registry
        extractor = get_extractor(extraction_job.extractor_id)
        if not isinstance(extractor, LLMPromptExtractorMetadata):
            raise ValueError(
                f"Extractor {extraction_job.extractor_id} is not an LLMPromptExtractorMetadata"
            )

        # Load existing results from GCS
        results_path = self._results_blob_path(job_id)
        results_data = self._read_json_from_gcs(results_path)
        if results_data is None:
            raise BatchNotFoundError(job_id)

        existing_results = [self._deserialize_result(r) for r in results_data]
        completed_doc_ids = {r.document_id for r in existing_results}

        # Check if already complete
        if len(existing_results) >= total_count:
            return LLMExtractionBatchResult(
                job_id=job_id,
                results=existing_results,
                batch_error_type=None,
                batch_error_message=None,
            )

        # Find pending documents and reconstruct requests
        pending_requests = []
        for doc in submitted_documents:
            if doc.document_id not in completed_doc_ids:
                gcs_document = doc.to_gcs_document()
                request = LLMExtractionRequest(
                    extractor=extractor, document=gcs_document
                )
                pending_requests.append(request)

        # Process one batch of pending requests
        batch_to_process = pending_requests[: self._batch_size]

        try:
            new_results = self._run_async(self._process_batch_async(batch_to_process))
        except Exception as e:
            return LLMExtractionBatchResult(
                job_id=job_id,
                results=None,
                batch_error_type=ExtractionJobErrorType.JOB_UNKNOWN_ERROR,
                batch_error_message=str(e),
            )

        # Append and save results
        all_results = existing_results + new_results
        self._write_json_to_gcs(
            results_path,
            [self._serialize_result(r) for r in all_results],
        )

        # Check if now complete
        if len(all_results) >= total_count:
            return LLMExtractionBatchResult(
                job_id=job_id,
                results=all_results,
                batch_error_type=None,
                batch_error_message=None,
            )

        # Still in progress
        return LLMExtractionInProgressBatchStatus(
            job_id=job_id,
            completed_count=len(all_results),
            total_count=total_count,
        )
