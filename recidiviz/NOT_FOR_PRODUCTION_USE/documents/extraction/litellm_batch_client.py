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
"""LiteLLM Batch API client for server-side async document extraction.

This client uses LiteLLM's Batch API (/v1/files + /v1/batches) to submit
extraction requests for true server-side asynchronous processing.

Unlike LiteLLMClient which processes requests concurrently in the local process,
this client uploads requests as a JSONL file and lets the provider (e.g., Vertex AI)
handle batch processing asynchronously.

Provider-specific logic (env var setup, response parsing, file ID encoding) is
delegated to an LLMProviderDelegate.
"""
import io
import json
import logging
import os
from typing import Any

import litellm  # type: ignore[import-not-found]

from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.llm_client import (
    LLMClient,
    LLMExtractionBatchResult,
    LLMExtractionInProgressBatchStatus,
    LLMExtractionRequest,
    LLMExtractionResult,
    LLMExtractionStatus,
    LLMResultReader,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.llm_provider_delegate import (
    LLMProviderDelegate,
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


class LiteLLMBatchClient(LLMClient, LLMResultReader):
    """LLM client that uses LiteLLM's Batch API for server-side async processing.

    This client submits extraction requests via LiteLLM's OpenAI-compatible batch
    endpoints. The workflow is:
    1. Build JSONL file with requests in OpenAI batch format
    2. Upload JSONL via litellm.create_file()
    3. Create batch job via litellm.create_batch()
    4. Poll batch status via litellm.retrieve_batch()
    5. Retrieve results via litellm.file_content()

    The batch is processed server-side (e.g., on Vertex AI), which is more efficient
    for large batches and allows the job to continue even if this process exits.
    """

    def __init__(
        self,
        provider_delegate: LLMProviderDelegate,
        temperature: float = 0.001,
        max_output_tokens: int = 8192,
    ) -> None:
        """Initialize the LiteLLM Batch client.

        Args:
            provider_delegate: Delegate that encapsulates provider-specific behavior
                (env vars, response parsing, file ID processing).
            temperature: Sampling temperature (0.0 = deterministic, 2.0 = random).
            max_output_tokens: Maximum number of tokens to generate per request.
        """
        self._provider_delegate = provider_delegate
        self._temperature = temperature
        self._max_output_tokens = max_output_tokens

        # Configure environment variables via the provider delegate
        for key, value in provider_delegate.get_env_vars().items():
            os.environ[key] = value

    def _build_batch_request_line(
        self, request: LLMExtractionRequest
    ) -> dict[str, Any]:
        """Build a single line for the batch JSONL file in OpenAI format.

        Args:
            request: The extraction request.

        Returns:
            Dictionary representing one line in the batch JSONL.
        """
        messages = request.build_messages()

        body: dict[str, Any] = {
            "model": request.model,
            "messages": messages,
            "max_tokens": self._max_output_tokens,
            "temperature": self._temperature,
        }

        # Add response format for structured output
        schema = request.output_schema.to_llm_json_schema()
        body["response_format"] = {
            "type": "json_schema",
            "json_schema": {
                "name": "extraction_response",
                "strict": True,
                "schema": schema,
            },
        }

        return {
            "custom_id": request.document.document_id,
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": body,
        }

    def submit_batch(self, requests: list[LLMExtractionRequest]) -> str:
        """Submits a batch of extraction requests via LiteLLM Batch API.

        Args:
            requests: The extraction requests to submit.

        Returns:
            The job_id for tracking this batch.
        """
        if not requests:
            raise ValueError("Cannot submit empty batch")

        llm_provider = self._provider_delegate.custom_llm_provider

        # Build JSONL content with one request per line
        lines = []
        for request in requests:
            line = self._build_batch_request_line(request)
            lines.append(json.dumps(line))
        jsonl_content = "\n".join(lines)

        # Upload JSONL file via LiteLLM
        file_obj = litellm.create_file(
            file=io.BytesIO(jsonl_content.encode("utf-8")),
            purpose="batch",
            custom_llm_provider=llm_provider,
        )

        # Create batch job via LiteLLM
        batch = litellm.create_batch(
            input_file_id=file_obj.id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
            custom_llm_provider=llm_provider,
        )

        return batch.id

    def _parse_batch_results(
        self, content: str, document_ids: list[str]
    ) -> list[LLMExtractionResult]:
        """Parse batch results from JSONL content.

        Uses the provider delegate to extract custom_id (for ID-based correlation)
        or falls back to positional correlation when the provider doesn't return
        custom_id.

        Args:
            content: JSONL string with one result per line.
            document_ids: List of document IDs in the order they were submitted.

        Returns:
            List of LLMExtractionResult objects.
        """
        results = []
        lines = [line for line in content.strip().split("\n") if line]

        for idx, line in enumerate(lines):
            entry = json.loads(line)

            # Try to get custom_id from the provider; fall back to positional
            custom_id = (
                self._provider_delegate.extract_custom_id_from_batch_result_line(entry)
            )
            if custom_id is not None:
                doc_id = custom_id
            else:
                if idx >= len(document_ids):
                    logging.warning(
                        "More results than document IDs: got %d results but only %d document IDs",
                        len(lines),
                        len(document_ids),
                    )
                    break
                doc_id = document_ids[idx]

            # Extract text via the provider delegate
            content_text = self._provider_delegate.extract_text_from_batch_result_line(
                entry
            )

            if content_text is not None:
                try:
                    extracted = json.loads(content_text)
                    if not isinstance(extracted, dict):
                        results.append(
                            LLMExtractionResult(
                                document_id=doc_id,
                                status=LLMExtractionStatus.PERMANENT_FAILURE,
                                extracted_data=None,
                                error_message=f"Expected dict response, got {type(extracted).__name__}. Raw: {content_text[:500]}",
                                error_type=DocumentExtractionErrorType.LLM_MALFORMED_RESPONSE,
                            )
                        )
                        continue

                    results.append(
                        LLMExtractionResult(
                            document_id=doc_id,
                            status=LLMExtractionStatus.SUCCESS,
                            extracted_data=extracted,
                            error_message=None,
                            error_type=None,
                        )
                    )
                except json.JSONDecodeError as e:
                    results.append(
                        LLMExtractionResult(
                            document_id=doc_id,
                            status=LLMExtractionStatus.PERMANENT_FAILURE,
                            extracted_data=None,
                            error_message=f"Failed to parse JSON: {e}. Raw: {content_text[:500]}",
                            error_type=DocumentExtractionErrorType.LLM_MALFORMED_RESPONSE,
                        )
                    )
            else:
                results.append(
                    LLMExtractionResult(
                        document_id=doc_id,
                        status=LLMExtractionStatus.PERMANENT_FAILURE,
                        extracted_data=None,
                        error_message="No text content in response",
                        error_type=DocumentExtractionErrorType.LLM_EMPTY_RESPONSE,
                    )
                )

        return results

    def get_results(
        self,
        extraction_job: ExtractionJobMetadata,
        submitted_documents: list[ExtractionJobSubmittedDocumentMetadata],
    ) -> LLMExtractionBatchResult | LLMExtractionInProgressBatchStatus:
        """Retrieves the results for a previously submitted batch.

        Args:
            extraction_job: The ExtractionJob (read from BQ). Provides job_id.
            submitted_documents: Documents submitted with this job (read from BQ).
                Must be ordered by job_index. Used for positional correlation
                with batch results.

        Returns:
            Either LLMExtractionBatchResult if the batch is complete (or failed),
            or LLMExtractionInProgressBatchStatus if still processing.
        """
        job_id = extraction_job.job_id
        document_ids = [doc.document_id for doc in submitted_documents]

        llm_provider = self._provider_delegate.custom_llm_provider

        # Retrieve batch status from LiteLLM
        batch = litellm.retrieve_batch(
            batch_id=job_id,
            custom_llm_provider=llm_provider,
        )

        if batch.status == "completed":
            # Retrieve and parse output file
            output_file_id = self._provider_delegate.process_output_file_id(
                batch.output_file_id
            )
            content = litellm.file_content(
                file_id=output_file_id,
                custom_llm_provider=llm_provider,
            )
            content_str = self._provider_delegate.decode_file_content(content)

            results = self._parse_batch_results(content_str, document_ids)
            return LLMExtractionBatchResult(
                job_id=job_id,
                results=results,
                batch_error_type=None,
                batch_error_message=None,
            )

        if batch.status in ("failed", "cancelled", "expired"):
            # Batch-level failure
            errors = getattr(batch, "errors", None)
            error_msg = f"Batch {batch.status}"
            if errors:
                error_msg = f"{error_msg}: {errors}"

            if batch.status == "expired":
                error_type = ExtractionJobErrorType.JOB_TIMEOUT
            else:
                error_type = ExtractionJobErrorType.JOB_EXECUTION_FAILURE

            return LLMExtractionBatchResult(
                job_id=job_id,
                results=None,
                batch_error_type=error_type,
                batch_error_message=error_msg,
            )

        # Still processing (validating, in_progress, finalizing)
        completed_count = 0
        total_count = len(document_ids)
        if hasattr(batch, "request_counts") and batch.request_counts:
            completed_count = getattr(batch.request_counts, "completed", 0)
            total_count = getattr(batch.request_counts, "total", len(document_ids))

        return LLMExtractionInProgressBatchStatus(
            job_id=job_id,
            completed_count=completed_count,
            total_count=total_count,
        )
