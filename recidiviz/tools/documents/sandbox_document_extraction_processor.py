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
from recidiviz.documents.extraction.expected_entry_nums_source import (
    InMemoryExpectedEntryNumsSource,
)
from recidiviz.documents.extraction.llm_client.llm_document_extraction_request_builder import (
    GCSDocumentTextSource,
)
from recidiviz.documents.extraction.llm_client.sync_llm_client import SyncLLMClient
from recidiviz.documents.extraction.llm_client.types import (
    LLMClientDocumentExtractionResult,
)
from recidiviz.documents.extraction.llm_extraction_job_manager import (
    LLMExtractionJobManager,
    LLMJobDocumentExtractionResult,
)
from recidiviz.documents.extraction.llm_extraction_results_persister import (
    LLMExtractionResultsPersister,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.documents.extraction.sync_llm_document_extraction_session import (
    SyncLLMDocumentExtractionSession,
    SyncLLMDocumentExtractionSessionSummary,
)
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
)
from recidiviz.documents.store.document_store_sandbox_context import (
    DocumentStoreSandboxContext,
)
from recidiviz.persistence.entity.operations.entities import LLMExtractionJobDocument


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

        Called by the session delegate once per result, serially, from the thread
        consuming the results — so it needs no locking despite the requests
        themselves running concurrently.
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
class _SandboxExtractionSessionDelegate:
    """Routes the extraction session's per-document events into the sandbox run's
    logging and progress reporting. Every event is survivable: the session counts
    the document and moves on rather than aborting the whole run.
    """

    progress: _ExtractionProgressLogger = attr.ib(
        validator=attr.validators.instance_of(_ExtractionProgressLogger)
    )
    """The run's progress logger, told about each result and each excluded
    document."""

    def on_empty_document(self, *, document_contents_id: str) -> None:
        """Logs the skip and drops the document from the progress denominator.

        An empty-text document is skipped without a terminal result_type, so under
        --keep-postgres it is re-selected into a fresh job on every resume
        (re-read, re-skipped, job marked SUCCESS) and never converges — see
        TODO(OBT-42807) in the session's request generator.
        """
        logging.info("Document [%s] has empty text; skipping.", document_contents_id)
        self.progress.exclude_document()

    def on_document_request_build_failure(
        self, *, document_contents_id: str, error: Exception
    ) -> None:
        """Logs the failure and drops the document from the progress denominator.
        The document's job row is left unmarked in Postgres, so it is re-selected
        on the next run.
        """
        logging.error(
            "Could not build a request for document [%s]; leaving it unmarked for "
            "re-selection on the next run.",
            document_contents_id,
            exc_info=error,
        )
        self.progress.exclude_document()

    def on_raw_document_extraction_result(
        self, result: LLMClientDocumentExtractionResult
    ) -> None:
        """Feeds each completed raw result to the progress logger."""
        self.progress.on_result(result)


@attr.define(frozen=True, kw_only=True)
class SandboxExtractionSummary:
    """The tally of one extraction thread's run, paired with the extractor
    collection it covers so the run's first-order and per-entity-group phases can
    each be printed under their own header rather than rolled into one total."""

    extractor_config_name: str = attr.ib(validator=attr_validators.is_str)
    """Extractor collection name the summary covers, used as its printed header."""

    session_summary: SyncLLMDocumentExtractionSessionSummary = attr.ib(
        factory=SyncLLMDocumentExtractionSessionSummary,
        validator=attr.validators.instance_of(SyncLLMDocumentExtractionSessionSummary),
    )
    """The run's per-document outcome counts and token usage, accumulated by the
    extraction session. Defaults to all zeros for a run that processed nothing."""

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
            ("❌ Empty text in GCS (skipped)", self.session_summary.skipped_empty),
            ("❌ Failed to build LLM request", self.session_summary.failed_to_build),
        ]
        if any(count for _, count in job_creation_error_rows):
            logging.info("Documents with job creation errors:")
            for label, count in job_creation_error_rows:
                if count:
                    logging.info("    %s: %d", label, count)

        logging.info("Documents processed via LLM: %d", self.session_summary.processed)
        for label, count in [
            ("✅ Succeeded", self.session_summary.succeeded),
            ("❌ Failed (LLM request)", self.session_summary.failed_llm_request),
            ("❌ Failed (validation)", self.session_summary.failed_validation),
        ]:
            if count:
                logging.info("    %s: %d", label, count)

        token_counts = self.session_summary.token_counts
        logging.info("Token usage:")
        logging.info("  Input: %d", token_counts.input_token_count)
        logging.info("  Output: %d", token_counts.output_token_count)
        logging.info("  Cached input: %d", token_counts.cached_input_token_count)
        logging.info("  Thinking: %d", token_counts.thinking_token_count)


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
    flushes the results to BigQuery and Postgres in chunks. Returns a summary
    wrapping the extraction session's outcome tally.
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
        documents = [
            GCSDocumentTextSource(
                document_contents_id=job_document.document_contents_id,
                fs=self.fs,
                project_id=self.bq_client.project_id,
                state_code=self.state_code,
                collection_name=self.config.input_document_collection.name,
                source_sandbox_prefix=source_document_sandbox_prefix,
            )
            for job_document in pending_documents
        ]
        total_documents = len(pending_documents)
        logging.info("Processing [%d] documents through the LLM.", total_documents)

        # Progress is reported through the session's per-result event, which fires
        # as each result completes — decoupling how often the run says something
        # from how often it flushes to BigQuery. Tying the two together meant a run
        # shorter than one persist chunk (the common sandbox case) printed no
        # progress at all until it finished.
        progress = _ExtractionProgressLogger(
            total_documents=total_documents,
            interval_seconds=self.progress_log_interval_seconds,
        )
        session = SyncLLMDocumentExtractionSession(
            config=self.config,
            job_id=job_id,
            billing_labels=self.labels,
            sync_client=self.sync_client,
            delegate=_SandboxExtractionSessionDelegate(progress=progress),
            expected_entry_nums_source=(
                None
                if expected_entry_nums_by_document is None
                else InMemoryExpectedEntryNumsSource(
                    entry_nums_by_document=expected_entry_nums_by_document
                )
            ),
            request_build_concurrency=self.request_build_concurrency,
        )

        chunk: list[LLMJobDocumentExtractionResult] = []
        for result in session.extract(documents=documents):
            chunk.append(result)
            if len(chunk) >= self.persist_chunk_size:
                self._write_results_to_bq_and_postgres(results=chunk)
                chunk = []
        # Flush the final partial chunk.
        self._write_results_to_bq_and_postgres(results=chunk)
        progress.log_final()
        return SandboxExtractionSummary(
            extractor_config_name=self.config.extractor_collection.name,
            session_summary=session.finish_session(),
            llm_phase_seconds=progress.elapsed_seconds,
        )

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
