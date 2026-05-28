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
"""High-throughput concurrent document extraction processor.

Replaces the poll-based LiteLLMClient submit/get_results pattern with a
single-pass architecture optimized for processing up to 100K+ documents:

1. Pre-filters already-extracted documents via BQ raw results table (resume)
2. Reads document contents from GCS in parallel via ThreadPoolExecutor
3. Calls the LLM with high concurrency via asyncio.Semaphore + acompletion()
4. Writes results to BQ in streaming batches as they complete
5. Logs progress at regular intervals

Unlike LiteLLMClient, this processor does NOT use GCS for intermediate state.
Resume is handled by querying the BQ raw results table for already-extracted
document IDs, making it safe for multiple workers to run in parallel on
different document segments.
"""
import asyncio
import datetime
import json
import logging
import random
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import pytz
import requests
from aiolimiter import AsyncLimiter
from google.cloud import storage  # type: ignore[import-untyped, attr-defined]
from litellm import acompletion  # type: ignore[import-not-found]
from litellm.exceptions import RateLimitError, ServiceUnavailableError
from litellm.exceptions import (
    Timeout as LiteLLMTimeout,  # type: ignore[import-not-found]
)
from tqdm import tqdm

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.document_provider import (
    GCSDocument,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.extraction_output_schema import (
    ExtractionOutputSchema,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.llm_client import (
    LLMExtractionResult,
    LLMExtractionStatus,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.document_extraction_result_metadata import (
    DocumentExtractionResultMetadata,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.document_extractor_collection_metadata import (
    DocumentExtractorCollectionMetadata,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.document_result_exclusion_metadata import (
    DocumentResultExclusionMetadata,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.extraction_exclusion_type import (
    ExtractionExclusionType,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.extraction_job_metadata import (
    ExtractionJobMetadata,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.extraction_job_result_metadata import (
    ExtractionJobResultMetadata,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.extraction_job_submitted_document_metadata import (
    ExtractionJobSubmittedDocumentMetadata,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.llm_prompt_extractor_metadata import (
    LLMPromptExtractorMetadata,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.validated_extraction_result_metadata import (
    ValidatedExtractionResultMetadata,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.result_validator import (
    failure_from_extraction_error,
    validate_extraction_result,
)

# Maximum rows per BQ streaming insert call (well below the 50K API limit)
_MAX_BQ_STREAMING_INSERT_ROWS = 10_000


class ConcurrentExtractionProcessor:
    """High-throughput document extraction processor for concurrent mode.

    Processes all documents in a single pass with:
    - Parallel GCS reads (ThreadPoolExecutor)
    - High-concurrency async LLM calls (asyncio.Semaphore + acompletion)
    - Streaming BQ writes in batches

    Resume is handled by querying the raw results table for already-extracted
    documents, making it safe to restart after crashes.
    """

    def __init__(  # pylint: disable=too-many-positional-arguments
        self,
        extractor: LLMPromptExtractorMetadata,
        collection: DocumentExtractorCollectionMetadata,
        sandbox_dataset_prefix: str,
        max_llm_concurrency: int = 200,
        # 50 workers avoids overwhelming the oauth2 token endpoint with
        # concurrent auth refreshes (100 workers caused SSL errors).
        max_gcs_read_workers: int = 50,
        bq_write_batch_size: int = 500,
        temperature: float = 0.001,
        # Gemini 2.5 Flash supports up to 65K output tokens. We've seen
        # truncated-JSON failures at 8192 when the model emits long citation
        # arrays — bumping high to give headroom.
        max_output_tokens: int = 32768,
        max_retries_per_doc: int = 5,
        retry_base_delay: float = 5.0,
        # Max requests per second to send to the LLM provider. Caps burst rate
        # independently of concurrency — Vertex AI enforces per-second burst
        # limits well below the per-minute quota (typically ~100-300 RPS for
        # Gemini Flash). With 200 concurrency and ~3s latency, steady-state
        # throughput is ~66 RPS, but asyncio would otherwise fire 200 requests
        # simultaneously at job start, overrunning the burst limit.
        max_llm_rps: float = 50.0,
    ) -> None:
        self._extractor = extractor
        self._collection = collection
        self._sandbox_dataset_prefix = sandbox_dataset_prefix
        self._max_llm_concurrency = max_llm_concurrency
        self._max_gcs_read_workers = max_gcs_read_workers
        self._bq_write_batch_size = bq_write_batch_size
        self._temperature = temperature
        self._max_output_tokens = max_output_tokens
        self._max_retries_per_doc = max_retries_per_doc
        self._retry_base_delay = retry_base_delay
        self._max_llm_rps = max_llm_rps

    def get_already_extracted_document_ids(
        self,
        bq_client: BigQueryClient,
    ) -> set[str]:
        """Returns document IDs that already have successful extraction results
        for the current extractor version.

        Only skips documents with status=SUCCESS so that failed extractions
        (e.g. from rate limiting) are retried on the next run.
        """
        raw_address = DocumentExtractionResultMetadata.raw_table_address(
            state_code=self._extractor.state_code,
            collection_name=self._extractor.collection_name,
            sandbox_dataset_prefix=self._sandbox_dataset_prefix,
        )
        full_address = raw_address.to_project_specific_address(bq_client.project_id)

        if not bq_client.table_exists(raw_address):
            return set()

        query = f"""
            SELECT DISTINCT document_id
            FROM `{full_address.to_str()}`
            WHERE extractor_version_id = '{self._extractor.extractor_version_id()}'
              AND status = 'SUCCESS'
        """
        rows = bq_client.run_query_async(query_str=query, use_query_cache=True)
        return {row["document_id"] for row in rows}

    @staticmethod
    def _build_shared_storage_client() -> storage.Client:
        """Builds a single GCS storage client with connection pooling.

        Shared across all threads in _read_documents_parallel to avoid the
        overhead of creating a new client (HTTP session, TLS handshake, auth
        token fetch) per document read. The google-cloud-storage Client is
        thread-safe for read operations.
        """
        client = storage.Client()
        adapter = requests.adapters.HTTPAdapter(pool_connections=128, pool_maxsize=128)
        # pylint: disable=protected-access
        client._http.mount("https://", adapter)
        client._http._auth_request.session.mount("https://", adapter)
        return client

    def _read_documents_parallel(
        self,
        documents: list[GCSDocument],
    ) -> tuple[dict[str, str], list[LLMExtractionResult]]:
        """Reads document contents from GCS in parallel.

        Uses a single shared storage.Client across all threads to avoid
        per-read client creation overhead (which was the dominant cost at
        scale — ~9K reads took 10+ minutes with per-read clients).

        Returns:
            Tuple of (successful reads as {doc_id: content}, failed reads as
            LLMExtractionResult list).
        """
        contents: dict[str, str] = {}
        failures: list[LLMExtractionResult] = []
        shared_client = self._build_shared_storage_client()

        max_read_retries = 3

        def _read_one(doc: GCSDocument) -> tuple[str, str | None, str | None]:
            last_err: str | None = None
            for attempt in range(max_read_retries):
                try:
                    bucket = shared_client.bucket(doc.gcs_file_path.bucket_name)
                    blob = bucket.blob(doc.gcs_file_path.blob_name)
                    content = blob.download_as_text()
                    return (doc.document_id, content, None)
                except Exception as e:
                    last_err = str(e)
                    if attempt < max_read_retries - 1:
                        time.sleep(1 * (attempt + 1))
            return (doc.document_id, None, last_err)

        logging.info(
            "Reading %d documents from GCS (%d workers)...",
            len(documents),
            self._max_gcs_read_workers,
        )
        start_time = time.time()

        with ThreadPoolExecutor(max_workers=self._max_gcs_read_workers) as executor:
            futures = {executor.submit(_read_one, doc): doc for doc in documents}
            with tqdm(total=len(documents), desc="GCS reads", unit="doc") as progress:
                for future in as_completed(futures):
                    doc_id, content, error = future.result()
                    if content is not None:
                        contents[doc_id] = content
                    else:
                        failures.append(
                            LLMExtractionResult(
                                document_id=doc_id,
                                status=LLMExtractionStatus.PERMANENT_FAILURE,
                                extracted_data=None,
                                error_message=f"Failed to read document from GCS: {error}",
                                error_type=ExtractionExclusionType.DOCUMENT_NOT_FOUND,
                            )
                        )
                    progress.update(1)

        elapsed = time.time() - start_time
        logging.info(
            "GCS reads complete: %d ok, %d failed (%.1fs)",
            len(contents),
            len(failures),
            elapsed,
        )
        return contents, failures

    def _prepare_response_format(self, schema: dict[str, Any]) -> dict[str, Any]:
        """Prepare response format for LiteLLM structured output."""
        return {
            "type": "json_schema",
            "json_schema": {
                "name": "extraction_response",
                "strict": True,
                "schema": schema,
            },
        }

    async def _extract_single_doc_async(  # pylint: disable=too-many-positional-arguments
        self,
        semaphore: asyncio.Semaphore,
        rate_limiter: AsyncLimiter,
        document_id: str,
        document_content: str,
        output_schema: ExtractionOutputSchema,
    ) -> LLMExtractionResult:
        """Process a single document with retry logic."""
        schema = output_schema.to_llm_json_schema()
        response_format = self._prepare_response_format(schema)

        completion_kwargs: dict[str, Any] = {
            "model": f"{self._extractor.llm_provider}/{self._extractor.model}",
            "messages": [
                {"role": "system", "content": self._extractor.instructions_prompt},
                {"role": "user", "content": document_content},
            ],
            "temperature": self._temperature,
            "max_tokens": self._max_output_tokens,
            "response_format": response_format,
        }

        last_error: str | None = None
        for attempt in range(self._max_retries_per_doc + 1):
            try:
                # Acquire both the concurrency cap (semaphore) and the
                # per-second burst cap (rate_limiter) before firing. Order
                # matters: semaphore first so we don't hold a rate-limit slot
                # while blocked on concurrency.
                async with semaphore:
                    async with rate_limiter:
                        response = await acompletion(**completion_kwargs)

                response_text = response.choices[0].message.content
                finish_reason = getattr(response.choices[0], "finish_reason", None)
                if response_text is None:
                    # Vertex AI signals safety/content filter blocks via
                    # finish_reason=content_filter (or "safety"). Distinguish
                    # these from other empty completions for cleaner reporting.
                    if finish_reason in ("content_filter", "safety"):
                        error_type = ExtractionExclusionType.LLM_CONTENT_FILTERED
                        error_message = (
                            f"LLM response blocked by content/safety filter. "
                            f"finish_reason={finish_reason}"
                        )
                    else:
                        error_type = ExtractionExclusionType.LLM_EMPTY_RESPONSE
                        error_message = (
                            f"LLM returned no content. finish_reason={finish_reason}"
                        )
                    return LLMExtractionResult(
                        document_id=document_id,
                        status=LLMExtractionStatus.PERMANENT_FAILURE,
                        extracted_data=None,
                        error_message=error_message,
                        error_type=error_type,
                    )
                try:
                    response_data = json.loads(response_text)
                except json.JSONDecodeError as e:
                    return LLMExtractionResult(
                        document_id=document_id,
                        status=LLMExtractionStatus.PERMANENT_FAILURE,
                        extracted_data=None,
                        error_message=(
                            f"Failed to parse JSON: {e}. "
                            f"finish_reason={finish_reason}. "
                            f"Raw: {response_text[:500]}"
                        ),
                        error_type=ExtractionExclusionType.LLM_MALFORMED_RESPONSE,
                    )

                if isinstance(response_data, dict):
                    return LLMExtractionResult(
                        document_id=document_id,
                        status=LLMExtractionStatus.SUCCESS,
                        extracted_data=response_data,
                        error_message=None,
                        error_type=None,
                    )

                return LLMExtractionResult(
                    document_id=document_id,
                    status=LLMExtractionStatus.PERMANENT_FAILURE,
                    extracted_data=None,
                    error_message=f"Unexpected response type: {type(response_data).__name__}. Raw: {response_text[:500]}",
                    error_type=ExtractionExclusionType.LLM_MALFORMED_RESPONSE,
                )

            except (RateLimitError, ServiceUnavailableError, LiteLLMTimeout) as e:
                last_error = str(e)
                if attempt < self._max_retries_per_doc:
                    # Exponential backoff with jitter to avoid thundering herd
                    # when many concurrent requests get 429'd simultaneously.
                    base_delay = self._retry_base_delay * (2**attempt)
                    delay = base_delay * (0.5 + random.random())
                    logging.warning(
                        "Retryable error for doc %s (attempt %d/%d), "
                        "retrying in %.1fs: %s",
                        document_id,
                        attempt + 1,
                        self._max_retries_per_doc + 1,
                        delay,
                        last_error,
                    )
                    await asyncio.sleep(delay)
                    continue

            except Exception as e:
                return LLMExtractionResult(
                    document_id=document_id,
                    status=LLMExtractionStatus.PERMANENT_FAILURE,
                    extracted_data=None,
                    error_message=str(e),
                    error_type=ExtractionExclusionType.LLM_UNKNOWN_ERROR,
                )

        return LLMExtractionResult(
            document_id=document_id,
            status=LLMExtractionStatus.PERMANENT_FAILURE,
            extracted_data=None,
            error_message=f"Failed after {self._max_retries_per_doc + 1} attempts: {last_error}",
            error_type=ExtractionExclusionType.LLM_RATE_LIMITED,
        )

    def _flush_results_to_bq(
        self,
        job_id: str,
        results: list[LLMExtractionResult],
        bq_client: BigQueryClient,
        quiet: bool = False,
        document_contents: dict[str, str] | None = None,
    ) -> tuple[int, int]:
        """Writes a batch of results to BQ raw, exclusions, and validated tables.

        If quiet=True, suppresses the per-table "Inserting N rows" log messages
        to keep progress bar output clean.

        Returns (num_success, num_failed) for this batch.
        """
        now = datetime.datetime.now(tz=pytz.UTC)
        extraction_results: list[DocumentExtractionResultMetadata] = []
        num_success = 0
        num_failed = 0

        for llm_result in results:
            if llm_result.status == LLMExtractionStatus.SUCCESS:
                num_success += 1
                result_data = llm_result.extracted_data or {}
                extraction_results.append(
                    DocumentExtractionResultMetadata(
                        job_id=job_id,
                        document_id=llm_result.document_id,
                        extractor_id=self._extractor.extractor_id,
                        extractor_version_id=self._extractor.extractor_version_id(),
                        extraction_datetime=now,
                        status=llm_result.status.value,
                        result_json=json.dumps(result_data),
                        error_message=None,
                        error_type=None,
                    )
                )
            else:
                num_failed += 1
                extraction_results.append(
                    DocumentExtractionResultMetadata(
                        job_id=job_id,
                        document_id=llm_result.document_id,
                        extractor_id=self._extractor.extractor_id,
                        extractor_version_id=self._extractor.extractor_version_id(),
                        extraction_datetime=now,
                        status=llm_result.status.value,
                        result_json=None,
                        error_message=llm_result.error_message,
                        error_type=llm_result.error_type,
                    )
                )

        # Temporarily suppress per-table "Inserting N rows" logs during
        # extraction so progress bars stay clean.
        root_logger = logging.getLogger()
        prev_level = root_logger.level
        if quiet:
            root_logger.setLevel(logging.WARNING)

        try:
            # Write to raw results table (chunked)
            raw_address = DocumentExtractionResultMetadata.raw_table_address(
                state_code=self._extractor.state_code,
                collection_name=self._extractor.collection_name,
                sandbox_dataset_prefix=self._sandbox_dataset_prefix,
            )
            raw_rows = [r.as_metadata_row() for r in extraction_results]
            for i in range(0, len(raw_rows), _MAX_BQ_STREAMING_INSERT_ROWS):
                bq_client.stream_into_table(
                    raw_address, raw_rows[i : i + _MAX_BQ_STREAMING_INSERT_ROWS]
                )

            # Validate and write to exclusions + validated tables
            all_failures: list[DocumentResultExclusionMetadata] = []
            validated_results: list[ValidatedExtractionResultMetadata] = []

            for result in extraction_results:
                if result.status != "SUCCESS":
                    all_failures.append(failure_from_extraction_error(result))
                    continue

                validation = validate_extraction_result(
                    result=result,
                    output_schema=self._collection.output_schema,
                    minimum_confidence_level=self._collection.minimum_confidence_level,
                    document_text=document_contents.get(result.document_id)
                    if document_contents
                    else None,
                )

                if validation.validated_result_json is not None:
                    validated_results.append(
                        ValidatedExtractionResultMetadata(
                            job_id=result.job_id,
                            document_id=result.document_id,
                            extractor_id=result.extractor_id,
                            extractor_version_id=result.extractor_version_id,
                            extraction_datetime=result.extraction_datetime,
                            state_code=self._extractor.state_code.value,
                            result_json=validation.validated_result_json,
                        )
                    )
                else:
                    all_failures.extend(validation.failures)

            # Write exclusions (chunked)
            if all_failures:
                exclusions_address = DocumentResultExclusionMetadata.table_address(
                    state_code=self._extractor.state_code,
                    collection_name=self._extractor.collection_name,
                    sandbox_dataset_prefix=self._sandbox_dataset_prefix,
                )
                failure_rows = [f.as_metadata_row() for f in all_failures]
                for i in range(0, len(failure_rows), _MAX_BQ_STREAMING_INSERT_ROWS):
                    bq_client.stream_into_table(
                        exclusions_address,
                        failure_rows[i : i + _MAX_BQ_STREAMING_INSERT_ROWS],
                    )

            # Write validated results (chunked)
            if validated_results:
                validated_address = ValidatedExtractionResultMetadata.table_address(
                    state_code=self._extractor.state_code,
                    collection_name=self._extractor.collection_name,
                    sandbox_dataset_prefix=self._sandbox_dataset_prefix,
                )
                validated_rows = [r.as_metadata_row() for r in validated_results]
                for i in range(0, len(validated_rows), _MAX_BQ_STREAMING_INSERT_ROWS):
                    bq_client.stream_into_table(
                        validated_address,
                        validated_rows[i : i + _MAX_BQ_STREAMING_INSERT_ROWS],
                    )
        finally:
            if quiet:
                root_logger.setLevel(prev_level)

        return num_success, num_failed

    def _write_job_metadata(
        self,
        job_id: str,
        documents: list[GCSDocument],
        bq_client: BigQueryClient,
    ) -> None:
        """Writes extraction job and submitted document metadata to BQ."""
        start_datetime = datetime.datetime.now(tz=pytz.UTC)

        # Write collection metadata
        collection_address = DocumentExtractorCollectionMetadata.metadata_table_address(
            self._sandbox_dataset_prefix
        )
        bq_client.stream_into_table(
            collection_address, [self._collection.as_metadata_row()]
        )

        # Write extractor metadata
        extractor_address = self._extractor.metadata_table_address(
            self._sandbox_dataset_prefix
        )
        bq_client.stream_into_table(
            extractor_address, [self._extractor.as_metadata_row()]
        )

        # Write job metadata
        job = ExtractionJobMetadata(
            job_id=job_id,
            start_datetime=start_datetime,
            extractor_id=self._extractor.extractor_id,
            extractor_version_id=self._extractor.extractor_version_id(),
            state_code=self._extractor.state_code,
            num_documents_submitted=len(documents),
        )
        jobs_address = ExtractionJobMetadata.metadata_table_address(
            self._sandbox_dataset_prefix
        )
        bq_client.stream_into_table(jobs_address, [job.as_metadata_row()])

        # Write submitted documents in chunks
        submitted_address = (
            ExtractionJobSubmittedDocumentMetadata.metadata_table_address(
                self._sandbox_dataset_prefix
            )
        )
        submitted_rows = [
            {
                "job_id": job_id,
                "document_id": doc.document_id,
                "submitted_datetime": start_datetime,
                "job_index": idx,
                "gcs_uri": doc.gcs_file_path.uri(),
                "mime_type": doc.mime_type,
            }
            for idx, doc in enumerate(documents)
        ]
        for i in range(0, len(submitted_rows), _MAX_BQ_STREAMING_INSERT_ROWS):
            bq_client.stream_into_table(
                submitted_address,
                submitted_rows[i : i + _MAX_BQ_STREAMING_INSERT_ROWS],
            )

        logging.info(
            "Wrote job metadata: job_id=%s, %d documents submitted",
            job_id,
            len(documents),
        )

    async def _extract_all_async(
        self,
        job_id: str,
        document_contents: dict[str, str],
        bq_client: BigQueryClient,
    ) -> tuple[int, int]:
        """Runs LLM extraction on all documents with bounded concurrency.

        Returns (total_success, total_failed).
        """
        output_schema = self._collection.output_schema
        semaphore = asyncio.Semaphore(self._max_llm_concurrency)
        # Token-bucket rate limiter: caps requests-per-second to avoid Vertex
        # AI burst limits. max_rate tokens per time_period (seconds).
        rate_limiter = AsyncLimiter(max_rate=self._max_llm_rps, time_period=1.0)
        total_docs = len(document_contents)
        total_success = 0
        total_failed = 0
        results_buffer: list[LLMExtractionResult] = []
        start_time = time.time()

        tasks = {
            asyncio.ensure_future(
                self._extract_single_doc_async(
                    semaphore, rate_limiter, doc_id, content, output_schema
                )
            ): doc_id
            for doc_id, content in document_contents.items()
        }

        progress = tqdm(total=total_docs, desc="LLM extraction", unit="doc")
        for coro in asyncio.as_completed(tasks.keys()):
            result = await coro
            results_buffer.append(result)
            progress.update(1)

            if len(results_buffer) >= self._bq_write_batch_size:
                batch_success, batch_failed = self._flush_results_to_bq(
                    job_id,
                    results_buffer,
                    bq_client,
                    quiet=True,
                    document_contents=document_contents,
                )
                total_success += batch_success
                total_failed += batch_failed
                results_buffer = []

        progress.close()

        # Flush remaining results
        if results_buffer:
            batch_success, batch_failed = self._flush_results_to_bq(
                job_id,
                results_buffer,
                bq_client,
                document_contents=document_contents,
            )
            total_success += batch_success
            total_failed += batch_failed

        elapsed = time.time() - start_time
        logging.info(
            "Extraction complete: %d docs | %d ok, %d failed | %.0fs total",
            total_docs,
            total_success,
            total_failed,
            elapsed,
        )
        return total_success, total_failed

    def process_documents(
        self,
        bq_client: BigQueryClient,
        documents: list[GCSDocument],
    ) -> ExtractionJobResultMetadata | None:
        """Processes a list of documents through extraction.

        This is the main entry point. It:
        1. Filters out already-extracted documents (resume support)
        2. Writes job metadata to BQ
        3. Reads documents from GCS in parallel
        4. Runs LLM extraction with high concurrency
        5. Writes results to BQ in streaming batches
        6. Writes final job result metadata

        Returns ExtractionJobResultMetadata, or None if all documents were
        already extracted.
        """
        # Suppress noisy per-request LiteLLM logs and per-batch BQ insert logs
        # so the tqdm progress bars remain readable.
        logging.getLogger("LiteLLM").setLevel(logging.WARNING)
        logging.getLogger("litellm").setLevel(logging.WARNING)

        # Step 1: Filter already-extracted documents
        already_extracted = self.get_already_extracted_document_ids(bq_client)
        if already_extracted:
            original_count = len(documents)
            documents = [
                doc for doc in documents if doc.document_id not in already_extracted
            ]
            skipped = original_count - len(documents)
            logging.info(
                "Skipping %d already-extracted documents (%d remaining)",
                skipped,
                len(documents),
            )

        if not documents:
            logging.info("All documents already extracted. Nothing to do.")
            return None

        # Step 2: Write job metadata
        job_id = str(uuid.uuid4())
        self._write_job_metadata(job_id, documents, bq_client)

        # Step 3: Read documents from GCS in parallel
        document_contents, gcs_failures = self._read_documents_parallel(documents)

        # Step 4 & 5: Run extraction and write results
        # First flush any GCS read failures
        gcs_success = 0
        gcs_failed = 0
        if gcs_failures:
            gcs_success, gcs_failed = self._flush_results_to_bq(
                job_id, gcs_failures, bq_client
            )

        # Run async extraction
        llm_success, llm_failed = asyncio.run(
            self._extract_all_async(job_id, document_contents, bq_client)
        )

        total_success = gcs_success + llm_success
        total_failed = gcs_failed + llm_failed

        # Step 6: Write job result
        job_result = ExtractionJobResultMetadata(
            job_id=job_id,
            result_datetime=datetime.datetime.now(tz=pytz.UTC),
            num_documents_originally_submitted=len(documents),
            num_successful_extractions=total_success,
            num_failed_extractions=total_failed,
            error_type=None,
            error_message=None,
        )
        job_results_address = ExtractionJobResultMetadata.metadata_table_address(
            self._sandbox_dataset_prefix
        )
        bq_client.stream_into_table(job_results_address, [job_result.as_metadata_row()])

        logging.info(
            "Job %s complete: %d submitted, %d success, %d failed",
            job_id,
            len(documents),
            total_success,
            total_failed,
        )
        return job_result
