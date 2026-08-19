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
"""Runs a sandbox extraction job's pending documents through the LLM for one
extractor: builds a request per document, runs them, classifies each result, and
flushes the results to BigQuery and Postgres in chunks, tallying the outcome into
a run summary as it goes."""

import logging
import time
from collections.abc import Iterator

import attr

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.entity_resolution.entity_resolution_composite_document_query_builder import (
    ENTRY_NUM_FIELD_NAME,
)
from recidiviz.documents.extraction.entity_resolution.entity_resolution_document_collection_config import (
    EntityResolutionDocumentCollectionConfig,
)
from recidiviz.documents.extraction.llm_client.llm_document_extraction_request_builder import (
    LLMDocumentExtractionRequestBuilder,
)
from recidiviz.documents.extraction.llm_client.sync_llm_client import SyncLLMClient
from recidiviz.documents.extraction.llm_client.sync_llm_document_extraction_request_runner import (
    SyncLLMDocumentExtractionRequestRunner,
)
from recidiviz.documents.extraction.llm_client.types import (
    LLMClientDocumentExtractionResult,
    LLMDocumentExtractionRequest,
)
from recidiviz.documents.extraction.llm_extraction_job_manager import (
    LLMExtractionJobManager,
    LLMJobDocumentExtractionResult,
)
from recidiviz.documents.extraction.llm_extraction_result_processor import (
    LLMExtractionResultProcessor,
)
from recidiviz.documents.extraction.llm_extraction_results_persister import (
    LLMExtractionResultsPersister,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.documents.extraction.validation.llm_extraction_result_validator import (
    LLMExtractionResultValidator,
)
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
)
from recidiviz.documents.store.document_store_sandbox_context import (
    DocumentStoreSandboxContext,
)
from recidiviz.persistence.entity.operations.entities import LLMExtractionJobDocument
from recidiviz.utils.future_executor import map_with_bounded_concurrency


def _format_progress(*, processed: int, total: int, elapsed_seconds: float) -> str:
    """Returns a human-readable progress line covering how far through the batch
    the run is, its throughput, and a rough estimate of the time remaining."""
    percent = processed / total * 100
    rate = processed / elapsed_seconds if elapsed_seconds > 0 else 0.0
    remaining = (total - processed) / rate if rate > 0 else 0.0
    return (
        f"{processed}/{total} documents ({percent:.1f}%) — "
        f"{rate:.1f} docs/sec, ~{remaining / 60:.1f} min remaining"
    )


@attr.define(kw_only=True)
class _ExtractionProgressLogger:
    """Logs a throughput line as extraction results complete, at most once per
    |interval_seconds|, so a long run shows steady progress without emitting a
    line per document.

    Also counts request-level failures as it goes, so a run that is moving along
    quickly but failing every request is visible while it happens rather than only
    in the closing summary. Validation runs after this sees a result, so a failure
    counted here is specifically one whose LLM request failed, not one whose
    result failed validation.
    """

    total_documents: int = attr.ib(validator=attr_validators.is_positive_int)
    """How many pending documents the run started with. Documents that never
    reach the LLM (empty text, request failed to build) are removed from the
    denominator via exclude_document as they are discovered, so a run with skips
    still converges to 100%."""

    interval_seconds: float = attr.ib(validator=attr_validators.is_non_negative_float)
    """Minimum seconds between progress lines; 0 logs on every result."""

    _results_seen: int = attr.ib(default=0, init=False)
    _failed_requests_seen: int = attr.ib(default=0, init=False)
    _excluded_documents: int = attr.ib(default=0, init=False)
    _started_at: float = attr.ib(factory=time.monotonic, init=False)
    _last_logged_at: float = attr.ib(factory=time.monotonic, init=False)

    def on_result(self, result: LLMClientDocumentExtractionResult) -> None:
        """Records a completed result, logging progress if |interval_seconds| has
        elapsed since the last line.

        Handed to the request runner as its progress_callback, which calls it once
        per result, serially, from the thread consuming the results — so it needs
        no locking despite the requests themselves running concurrently.
        """
        self._results_seen += 1
        if result.is_error_result:
            self._failed_requests_seen += 1

        now = time.monotonic()
        if now - self._last_logged_at < self.interval_seconds:
            return
        self._last_logged_at = now
        logging.info("Progress: %s", self._progress_line())

    def exclude_document(self) -> None:
        """Drops one document from the progress denominator — a document that
        will never produce a result because it was skipped or failed to build."""
        self._excluded_documents += 1

    def log_final(self) -> None:
        """Logs the run's closing throughput line, regardless of the interval."""
        logging.info("Finished LLM processing: %s", self._progress_line())

    @property
    def elapsed_seconds(self) -> float:
        """Returns seconds elapsed since the first result was awaited."""
        return time.monotonic() - self._started_at

    def _progress_line(self) -> str:
        effective_total = self.total_documents - self._excluded_documents
        if effective_total <= 0:
            # Every document was skipped or failed to build, so none reached the
            # LLM; there is no throughput to report.
            return "0/0 documents — no documents reached the LLM"
        progress = _format_progress(
            processed=self._results_seen,
            total=effective_total,
            elapsed_seconds=time.monotonic() - self._started_at,
        )
        if not self._failed_requests_seen:
            return progress
        # Phrased with a colon rather than "N failed LLM requests" so the line
        # reads correctly when the count is 1.
        return f"{progress}, failed LLM requests: {self._failed_requests_seen}"


@attr.define(kw_only=True)
class SandboxExtractionSummary:
    """The tally of one extraction thread's run, paired with the extractor
    collection it covers so the run's first-order and per-entity-group phases can
    each be printed under their own header rather than rolled into one total."""

    extractor_config_name: str = attr.ib(validator=attr_validators.is_str)
    """Extractor collection name the summary covers, used as its printed header."""

    processed: int = attr.ib(default=0, validator=attr_validators.is_non_negative_int)
    """Documents that reached the LLM and were classified (success or failure)."""

    succeeded: int = attr.ib(default=0, validator=attr_validators.is_non_negative_int)
    """Processed documents that extracted and validated cleanly."""

    failed_llm_request: int = attr.ib(
        default=0, validator=attr_validators.is_non_negative_int
    )
    """Processed documents whose LLM request itself failed (timeout, rate limit,
    server error, content filter, malformed/empty response) — no result JSON came
    back to validate."""

    failed_validation: int = attr.ib(
        default=0, validator=attr_validators.is_non_negative_int
    )
    """Processed documents whose LLM request returned a result that then failed
    validation."""

    skipped_empty: int = attr.ib(
        default=0, validator=attr_validators.is_non_negative_int
    )
    """Documents skipped before the LLM because their text was empty."""

    failed_to_build: int = attr.ib(
        default=0, validator=attr_validators.is_non_negative_int
    )
    """Documents that could not be assembled into a request (e.g. missing GCS
    text). Left unmarked in Postgres, so they are re-selected on the next run."""

    input_tokens: int = attr.ib(
        default=0, validator=attr_validators.is_non_negative_int
    )
    output_tokens: int = attr.ib(
        default=0, validator=attr_validators.is_non_negative_int
    )
    cached_input_tokens: int = attr.ib(
        default=0, validator=attr_validators.is_non_negative_int
    )
    thinking_tokens: int = attr.ib(
        default=0, validator=attr_validators.is_non_negative_int
    )

    # Excluded from equality: it is a non-deterministic timing measurement used
    # only for display, not part of the run's logical outcome.
    llm_phase_seconds: float = attr.ib(
        default=0.0, eq=False, validator=attr_validators.is_non_negative_float
    )
    """Wall-clock seconds spent building requests and running them through the
    LLM — the run's dominant phase."""

    def log(self) -> None:
        """Logs the run's summary under a header naming the extractor collection
        it covers."""
        logging.info(
            "=== Sandbox extraction complete: %s ===", self.extractor_config_name
        )
        minutes, seconds = divmod(round(self.llm_phase_seconds), 60)
        logging.info("LLM requests phase took %dm %ds.", minutes, seconds)

        # Documents that never reached the LLM because a request could not be
        # built for them. Only surfaced when non-empty, so a clean run stays quiet.
        job_creation_error_rows = [
            ("❌ Empty text in GCS (skipped)", self.skipped_empty),
            ("❌ Failed to build LLM request", self.failed_to_build),
        ]
        if any(count for _, count in job_creation_error_rows):
            logging.info("Documents with job creation errors:")
            for label, count in job_creation_error_rows:
                if count:
                    logging.info("    %s: %d", label, count)

        logging.info("Documents processed via LLM: %d", self.processed)
        for label, count in [
            ("✅ Succeeded", self.succeeded),
            ("❌ Failed (LLM request)", self.failed_llm_request),
            ("❌ Failed (validation)", self.failed_validation),
        ]:
            if count:
                logging.info("    %s: %d", label, count)

        logging.info("Token usage:")
        logging.info("  Input: %d", self.input_tokens)
        logging.info("  Output: %d", self.output_tokens)
        logging.info("  Cached input: %d", self.cached_input_tokens)
        logging.info("  Thinking: %d", self.thinking_tokens)


def read_expected_entry_nums_by_document(
    *,
    config: LLMExtractorConfig,
    document_store_sandbox: DocumentStoreSandboxContext | None,
    bq_client: BigQueryClient,
) -> dict[str, set[int]] | None:
    """Returns the complete entry set of each composite document, keyed by
    document_contents_id, read from the entry→source map table — or None for a
    first-order extractor, whose documents have no numbered entries.

    The validator requires the entry set for every entity-resolution result so
    the entry-partition check can validate the clustering against it. The map
    table is read from wherever the run wrote the ER collection's document store
    (the production document store when the run has no sandbox one), which the
    document store process hydrated before this extraction runs.
    """
    if config.entity_group is None:
        return None
    er_collection = config.input_document_collection
    if not isinstance(er_collection, EntityResolutionDocumentCollectionConfig):
        raise ValueError(
            f"Extractor [{config.extractor_id}] is an entity-resolution "
            f"extractor, but its input document collection "
            f"[{er_collection.name}] is not an "
            f"EntityResolutionDocumentCollectionConfig."
        )
    source_read_prefix = (
        document_store_sandbox.source_read_prefix_for_document_collection(
            er_collection.name
        )
        if document_store_sandbox is not None
        else None
    )
    map_table_address = er_collection.entry_source_map_table_address(
        sandbox_dataset_prefix=source_read_prefix
    ).to_project_specific_address(bq_client.project_id)

    expected_entry_nums_by_document: dict[str, set[int]] = {}
    query_job = bq_client.run_query_async(
        query_str=map_table_address.select_query(
            select_statement=(
                f"SELECT {DOCUMENT_CONTENTS_ID_COLUMN_NAME}, {ENTRY_NUM_FIELD_NAME}"
            )
        ),
        use_query_cache=False,
    )
    for row in query_job:
        expected_entry_nums_by_document.setdefault(
            row[DOCUMENT_CONTENTS_ID_COLUMN_NAME], set()
        ).add(row[ENTRY_NUM_FIELD_NAME])
    return expected_entry_nums_by_document


class DocumentExtractionProcessor:
    """Runs a job's pending documents through the LLM for one extractor and
    sandbox: builds a request per document, runs them, classifies each result, and
    flushes the results to BigQuery and Postgres in chunks. Owns the run's summary,
    accumulating each document's outcome and token usage into it as results
    complete.
    """

    def __init__(
        self,
        *,
        # The narrowed extractor config every stage reads from.
        config: LLMExtractorConfig,
        # Prefix scoping the BQ result datasets this processor writes.
        results_sandbox_prefix: str,
        # The sandbox document store its request builder reads each document's text
        # from, or None to read the production document store.
        document_store_sandbox: DocumentStoreSandboxContext | None,
        # Billing labels attached to each Vertex AI request.
        labels: dict[str, str],
        # Client for the sandbox result tables.
        bq_client: BigQueryClient,
        # Filesystem the document text is read from.
        fs: GCSFileSystem,
        # Client that makes the live Vertex AI extraction calls.
        sync_client: SyncLLMClient,
        # Marks each job document's result as it is processed.
        job_manager: LLMExtractionJobManager,
        # How many results to buffer before flushing to BigQuery and Postgres.
        persist_chunk_size: int,
        # How many requests to build (reading their text from GCS) concurrently.
        request_build_concurrency: int,
        # Minimum seconds between progress lines while the LLM requests run.
        progress_log_interval_seconds: float,
    ) -> None:
        self.config = config
        self.results_sandbox_prefix = results_sandbox_prefix
        self.document_store_sandbox = document_store_sandbox
        self.labels = labels
        self.bq_client = bq_client
        self.fs = fs
        self.sync_client = sync_client
        self.job_manager = job_manager
        self.persist_chunk_size = persist_chunk_size
        self.request_build_concurrency = request_build_concurrency
        self.progress_log_interval_seconds = progress_log_interval_seconds

        self.summary = SandboxExtractionSummary(
            extractor_config_name=config.extractor_collection.name
        )
        self.processor = LLMExtractionResultProcessor(
            validator=LLMExtractionResultValidator()
        )
        self.persister = LLMExtractionResultsPersister(
            sandbox_prefix=results_sandbox_prefix, bq_client=bq_client
        )

    @property
    def state_code(self) -> StateCode:
        return self.config.state_code

    def process(
        self, *, job_id: str, pending_documents: list[LLMExtractionJobDocument]
    ) -> SandboxExtractionSummary:
        """Builds a request per pending document, runs them through the LLM,
        classifies each result, and flushes the results to BigQuery and Postgres
        in chunks. Returns the summary of the processed documents."""
        source_document_sandbox_prefix = (
            self.document_store_sandbox.source_read_prefix_for_document_collection(
                self.config.input_document_collection.name
            )
            if self.document_store_sandbox is not None
            else None
        )
        expected_entry_nums_by_document = read_expected_entry_nums_by_document(
            config=self.config,
            document_store_sandbox=self.document_store_sandbox,
            bq_client=self.bq_client,
        )
        request_builder = LLMDocumentExtractionRequestBuilder(
            fs=self.fs,
            project_id=self.bq_client.project_id,
            state_code=self.state_code,
            collection_name=self.config.input_document_collection.name,
            instructions_prompt=self.config.instructions_prompt,
            response_json_schema=self.config.extractor_collection.generate_json_schema(),
            request_parameters=LLMDocumentExtractionRequestBuilder.build_request_parameters(
                model_config=self.config.model_config, labels=self.labels
            ),
            source_sandbox_prefix=source_document_sandbox_prefix,
        )
        runner = SyncLLMDocumentExtractionRequestRunner(client=self.sync_client)

        total_documents = len(pending_documents)
        logging.info("Processing [%d] documents through the LLM.", total_documents)

        # Each buildable document's source text, keyed by document_contents_id.
        # The request generator writes an entry as it builds each request and the
        # classify step pops it once the result comes back, so this holds only the
        # in-flight window's worth of text (bounded by the runner's concurrency),
        # not every document's text at once.
        #
        # TODO(OBT-42971) Passing the source text in-process from request-building to
        # classification works for this single-process sandbox script, but the
        # Airflow version splits these across tasks and must not push document_text
        # through XCom. The classify step will need to re-read the text rather than
        # receive it in memory — likely from the BigQuery table that already holds
        # the document text.
        source_text_by_document: dict[str, str] = {}

        # Progress is reported through the runner's progress_callback, which fires
        # as each result completes — decoupling how often the run says something
        # from how often it flushes to BigQuery. Tying the two together meant a run
        # shorter than one persist chunk (the common sandbox case) printed no
        # progress at all until it finished.
        progress = _ExtractionProgressLogger(
            total_documents=total_documents,
            interval_seconds=self.progress_log_interval_seconds,
        )
        chunk: list[LLMJobDocumentExtractionResult] = []
        with runner.execute_document_extraction_requests(
            requests=self._iter_requests(
                job_documents=pending_documents,
                request_builder=request_builder,
                source_text_by_document=source_text_by_document,
                progress=progress,
            ),
            progress_callback=progress.on_result,
        ) as results:
            for raw_result in results:
                # The document is done once its result is classified, so popping
                # its source text here bounds the map to the in-flight window
                # rather than the whole run.
                chunk.append(
                    self._classify_result(
                        raw_result=raw_result,
                        job_id=job_id,
                        source_document_text=source_text_by_document.pop(
                            raw_result.document_contents_id
                        ),
                        expected_entry_nums=(
                            None
                            if expected_entry_nums_by_document is None
                            else expected_entry_nums_by_document[
                                raw_result.document_contents_id
                            ]
                        ),
                    )
                )
                if len(chunk) >= self.persist_chunk_size:
                    self._write_results_to_bq_and_postgres(results=chunk)
                    chunk = []
        # Flush the final partial chunk.
        self._write_results_to_bq_and_postgres(results=chunk)
        progress.log_final()
        self.summary.llm_phase_seconds = progress.elapsed_seconds
        return self.summary

    def _iter_requests(
        self,
        *,
        job_documents: list[LLMExtractionJobDocument],
        request_builder: LLMDocumentExtractionRequestBuilder,
        source_text_by_document: dict[str, str],
        progress: _ExtractionProgressLogger,
    ) -> Iterator[LLMDocumentExtractionRequest]:
        """Yields one extraction request per buildable document, lazily, recording
        each document's source text in |source_text_by_document| for the classify
        step to read back.

        Builds requests on a small thread pool rather than inline. Each build
        reads its document's text from GCS, which costs two sequential HTTP round
        trips, and this generator is consumed on the thread driving the request
        runner — so building inline serialized every read and starved the runner,
        holding in-flight LLM requests far below its max_concurrency. Building on
        a pool overlaps those round trips with each other and with the LLM calls.

        Still lazy: map_with_bounded_concurrency keeps at most
        `request_build_concurrency` reads in flight and pulls documents only as
        the consumer takes requests, so a large run holds that many documents'
        text rather than every document's at once. Requests come out in
        completion order rather than job order, which the runner does not depend
        on.

        The summary counters mutated below are only ever touched from this thread
        (the one consuming the completed builds), not from the build pool.
        """
        with map_with_bounded_concurrency(
            work_fn=lambda job_document: request_builder.build_request(
                job_document=job_document
            ),
            items=job_documents,
            max_concurrency=self.request_build_concurrency,
        ) as completed_builds:
            for completed in completed_builds:
                document_contents_id = completed.item.document_contents_id
                try:
                    request = completed.result
                except Exception:  # pylint: disable=broad-except
                    # Any failure building one document's request — the expected
                    # LLMDocumentExtractionRequestError, or an unexpected error
                    # like a transient GCS read failure — is survivable: count it
                    # and move on rather than letting it abort the whole run. A
                    # systematic build bug still surfaces loudly as every document
                    # landing in failed_to_build.
                    logging.exception(
                        "Could not build a request for document [%s]; leaving it "
                        "unmarked for re-selection on the next run.",
                        document_contents_id,
                    )
                    self.summary.failed_to_build += 1
                    progress.exclude_document()
                    continue
                if request is None:
                    # An empty-text document is skipped without a terminal
                    # result_type, so under --keep-postgres it is re-selected into
                    # a fresh job on every resume (re-read, re-skipped, job marked
                    # SUCCESS) and never converges. Harmless but wasteful, and it
                    # masks true completion. TODO(OBT-42807) give empty docs a
                    # terminal result so they stop re-selecting.
                    logging.info(
                        "Document [%s] has empty text; skipping.", document_contents_id
                    )
                    self.summary.skipped_empty += 1
                    progress.exclude_document()
                    continue
                source_text_by_document[document_contents_id] = request.document_text
                yield request

    def _classify_result(
        self,
        *,
        raw_result: LLMClientDocumentExtractionResult,
        job_id: str,
        source_document_text: str,
        # This document's complete composite-document entry set, or None for a
        # first-order extractor whose documents have no numbered entries.
        expected_entry_nums: set[int] | None,
    ) -> LLMJobDocumentExtractionResult:
        """Returns the processed result for one raw extraction result, classified
        and validated, and folds its counts and token usage into the summary.
        """
        result = self.processor.validate_and_classify(
            config=self.config,
            raw_result=raw_result,
            job_id=job_id,
            source_document_text=source_document_text,
            expected_entry_nums=expected_entry_nums,
            # TODO(OBT-41779) since we are allowing postgres to persist across runs,
            # we should properly pass in the prior transient failure count from the postgres table here
            prior_transient_failure_count=0,
        )

        self.summary.processed += 1
        if result.is_validated_result:
            self.summary.succeeded += 1
        elif result.raw_result.is_error_result:
            # The LLM request itself failed, so no result JSON reached the
            # validator.
            self.summary.failed_llm_request += 1
        else:
            # The request returned a result that then failed validation.
            self.summary.failed_validation += 1

        token_counts = result.raw_result.token_counts
        self.summary.input_tokens += token_counts.input_token_count
        self.summary.output_tokens += token_counts.output_token_count
        self.summary.cached_input_tokens += token_counts.cached_input_token_count
        self.summary.thinking_tokens += token_counts.thinking_token_count
        return result

    def _write_results_to_bq_and_postgres(
        self, *, results: list[LLMJobDocumentExtractionResult]
    ) -> None:
        """Persists a list of processed results to BigQuery and then marks their
        Postgres job-document results, in that order.
        """
        if not results:
            return
        self.persister.persist_results(config=self.config, results=results)
        self.job_manager.set_job_document_results(
            state_code=self.config.state_code, results=results
        )
