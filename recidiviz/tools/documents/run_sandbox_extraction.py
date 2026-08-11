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
"""Drives the whole first-order LLM document-extraction end-to-end against
a single state's extractor.

It runs against job/document tracking in a local Postgres that this script spins
up itself, reads the document segment from the state's real document store in
BigQuery + GCS, makes live Vertex AI calls, and writes the extraction result rows
into the sandbox-prefixed BigQuery result tables and deploys the parsed views over
them.

TODO(OBT-42680): There is no way to point this at a sandbox document collection yet —
it always reads the state's real document store.

TODO(OBT-42972): Harden against Google ADC expiry mid-run. A large run can outlive the
Google Application Default Credentials token, and today that just fails the requests (or
the whole run) rather than recovering. Periodically re-check credentials during the LLM
phase and, when they are expired, pause and prompt for re-auth instead of failing out —
see verify_google_adc_credentials in recidiviz.tools.utils.script_helpers, which today
only fails fast at startup via sys.exit(1).

TODO(OBT-42973): Clear both the sandbox-prefixed BQ result rows and the Postgres job/document
tracking at the start of each run by default, so every run starts clean rather than
accumulating append-only BQ rows and (with --keep-postgres) reusing prior tracking. Add
a temporary opt-out flag (e.g. --dont-clear-state-i-swear-im-rerunning-with-same-args) to
preserve state for the deliberate resume/accumulate case. This overlaps with the existing
--keep-postgres flag — reconcile the two when implementing.

By default the Postgres job/document tracking is thrown away at the end of the
run, so it does NOT persist across runs: every run starts with no prior jobs, so
there is no cross-run resume or retry accounting. Pass --keep-postgres to keep
the tracking in a stable data directory across runs instead; a later run then
reconnects to it and skips documents already finished, which is what makes a large
run resumable after a crash. (Delete that data directory to reset.)

The BigQuery result rows this script writes DO persist across runs and are scoped by
--sandbox-prefix, so there are two behaviors worth understanding together:

  - The BQ result tables ARE scoped by --sandbox-prefix (the raw / validated / audit
    tables live in sandbox-prefixed datasets), so a different prefix writes to a
    fresh set of result tables while the same prefix accumulates into one set
    across runs (the prefix does not scope the Postgres tracking, which is keyed on
    extractor version).
  - Writes are append-only and at-least-once: this script never deletes prior
    result rows, and re-persisting a document (e.g. a chunk re-extracted after a
    crash, since the persist-to-BQ-then-mark-Postgres ordering makes recovery
    re-run the whole in-flight chunk) just appends duplicate rows. The parsed
    views dedup to the latest row per document_contents_id, so duplicates are
    expected and harmless when querying through the views.

Example usage:
    python -m recidiviz.tools.documents.run_sandbox_extraction \
        --sandbox-prefix my_prefix \
        --state-code US_OZ \
        --collection PLAYGROUND_EMPLOYMENT_INFO \
        --document-limit 500 \
        --labels reason=test

To narrow to specific root entities:
    python -m recidiviz.tools.documents.run_sandbox_extraction \
        --sandbox-prefix my_prefix \
        --state-code US_OZ \
        --collection PLAYGROUND_EMPLOYMENT_INFO \
        --external-id-type US_OZ_LOTR_ID \
        --root-entity-ids 12345 67890
"""

import argparse
import logging
import os
import re
import sys
import tempfile
import time
from collections.abc import Generator, Iterator
from contextlib import contextmanager

import attr

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.big_query.big_query_view_dag_walker import (
    BigQueryViewDagWalkerProcessingFailureMode,
)
from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.common.git import get_normalized_git_username
from recidiviz.documents.extraction.llm_client.llm_document_extraction_request_builder import (
    LLMDocumentExtractionRequestBuilder,
    LLMDocumentExtractionRequestError,
)
from recidiviz.documents.extraction.llm_client.sync_llm_client import SyncLLMClient
from recidiviz.documents.extraction.llm_client.sync_llm_document_extraction_request_runner import (
    SyncLLMDocumentExtractionRequestRunner,
)
from recidiviz.documents.extraction.llm_client.types import (
    LLMClientDocumentExtractionResult,
    LLMDocumentExtractionRequest,
)
from recidiviz.documents.extraction.llm_client.vertex_ai_sync_llm_client import (
    VertexAISyncLLMClient,
)
from recidiviz.documents.extraction.llm_extraction_eligible_document_query_builder import (
    LLMExtractionEligibleDocumentQueryBuilder,
)
from recidiviz.documents.extraction.llm_extraction_job_generator import (
    LLMExtractionJobGenerator,
)
from recidiviz.documents.extraction.llm_extraction_job_manager import (
    LLMExtractionJobManager,
    LLMJobDocumentExtractionResult,
)
from recidiviz.documents.extraction.llm_extraction_result_processor import (
    LLMExtractionResultProcessor,
)
from recidiviz.documents.extraction.llm_extraction_result_validator import (
    LLMExtractionResultValidator,
)
from recidiviz.documents.extraction.llm_extraction_results_persister import (
    LLMExtractionResultsPersister,
)
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    get_first_order_llm_extractor_config,
)
from recidiviz.documents.extraction.llm_extractor_metadata_manager import (
    LLMExtractorMetadataManager,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.documents.extraction.views.llm_extraction_results_view_collector import (
    collect_first_order_llm_extraction_results_view_builders,
)
from recidiviz.ingest.direct.external_id_type_helpers import (
    external_id_types_by_state_code,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.operations.entities import LLMExtractionJobDocument
from recidiviz.source_tables.extraction_results_source_table_collection import (
    collect_extraction_results_source_table_collections,
)
from recidiviz.source_tables.source_table_config import SourceTableCollection
from recidiviz.source_tables.source_table_update_manager import SourceTableUpdateManager
from recidiviz.tools.load_views_to_sandbox import load_collected_views_to_sandbox
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.utils.script_helpers import requires_google_adc
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.future_executor import map_with_bounded_concurrency
from recidiviz.utils.metadata import local_project_id_override, project_id
from recidiviz.utils.params import non_negative_int, parse_key_value_args, positive_int

DEFAULT_TABLE_EXPIRATION_DAYS = 14

# Refuse to start a run whose job holds more than this many documents.
# TODO(OBT-42789) decrease this in the future since this script is intended
# for bounded sandbox runs, not large-scale production runs.
MAX_DOCUMENTS = 500_000

# How many processed results to buffer before flushing them to BigQuery and then
# marking their Postgres results. Bounds both the per-flush memory footprint and
# the re-extraction window on a crash.
#
# Sized for the current streaming-insert write path, whose per-request payload
# BigQuery caps at 10 MB: too large a chunk risks a 413 on the raw-results write.
# When the persister moves to load jobs, this should grow substantially —
# load jobs have no payload cap but a 1,500-loads-per-table-per-
# day quota, which instead rewards fewer, larger flushes.
DEFAULT_PERSIST_CHUNK_SIZE = 250

# How many extraction requests to build — i.e. read document text from GCS for —
# concurrently. Building requests one at a time on the thread driving the request
# runner is what otherwise holds the run's in-flight LLM requests far below the
# runner's own max_concurrency, since each build costs two sequential GCS round
# trips (one for the blob metadata, one for the bytes).
#
# Sized from measuring a real collection's ~300-byte documents: serial reads
# managed ~2 documents/sec, 16 threads reached ~15/sec, and 64 threads were no
# faster — the storage client's own connection pool becomes the limit past this
# point, so raising it buys nothing.
DEFAULT_REQUEST_BUILD_CONCURRENCY = 16

# How often, at most, to log a progress line while the LLM requests run. Paced on
# a time interval rather than every N documents so the cadence is the same whether
# the run holds 200 documents or 200,000.
DEFAULT_PROGRESS_LOG_INTERVAL_SECONDS = 10.0


REQUESTER_LABEL_KEY = "requester"
SANDBOX_PREFIX_LABEL_KEY = "sandbox_prefix"


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
    """How many documents the run will put through the LLM."""

    interval_seconds: float = attr.ib(validator=attr_validators.is_non_negative_float)
    """Minimum seconds between progress lines; 0 logs on every result."""

    _results_seen: int = attr.ib(default=0, init=False)
    _failed_requests_seen: int = attr.ib(default=0, init=False)
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

    def log_final(self) -> None:
        """Logs the run's closing throughput line, regardless of the interval."""
        logging.info("Finished LLM processing: %s", self._progress_line())

    @property
    def elapsed_seconds(self) -> float:
        """Returns seconds elapsed since the first result was awaited."""
        return time.monotonic() - self._started_at

    def _progress_line(self) -> str:
        progress = _format_progress(
            processed=self._results_seen,
            total=self.total_documents,
            elapsed_seconds=time.monotonic() - self._started_at,
        )
        if not self._failed_requests_seen:
            return progress
        # Phrased with a colon rather than "N failed LLM requests" so the line
        # reads correctly when the count is 1.
        return f"{progress}, failed LLM requests: {self._failed_requests_seen}"


# TODO(OBT-42711) Formalize concept of labels
def _build_labels(*, user_labels: list[str], sandbox_prefix: str) -> dict[str, str]:
    """Returns the billing labels attached to every Vertex request: the parsed
    user-supplied labels, plus an automatically-added requester and sandbox prefix.
    """

    def _sanitize_label_value(value: str) -> str:
        """Returns |value| coerced into a valid GCP label value: lowercased, with any
        character outside [a-z0-9_-] replaced by '_', truncated to GCP's 63-character
        limit."""
        sanitized = re.sub(r"[^a-z0-9_-]", "_", value.lower())
        return sanitized[:63]

    def _sanitize_label_key(key: str) -> str:
        """Returns |key| coerced into a valid GCP label key. Sanitizes like a value
        but additionally enforces GCP's stricter key rules — a key must be non-empty
        and start with a lowercase letter — raising loudly rather than sending an
        invalid key that would fail every Vertex request in the run."""
        sanitized = _sanitize_label_value(key)
        if not re.fullmatch(r"[a-z][a-z0-9_-]*", sanitized):
            raise ValueError(
                f"Label key [{key}] sanitizes to [{sanitized}], which is not a valid "
                f"GCP label key (must be non-empty and start with a lowercase letter)."
            )
        return sanitized

    labels = parse_key_value_args(user_labels)
    labels.setdefault(REQUESTER_LABEL_KEY, get_normalized_git_username())
    labels[SANDBOX_PREFIX_LABEL_KEY] = sandbox_prefix
    return {
        _sanitize_label_key(key): _sanitize_label_value(value)
        for key, value in labels.items()
    }


_PERSISTENT_POSTGRES_DATA_DIR = os.path.join(
    tempfile.gettempdir(), "recidiviz_sandbox_extraction_operations_db"
)


@contextmanager
def _local_operations_postgres(*, keep_postgres: bool) -> Generator[None, None, None]:
    """Spins up an on-disk Postgres bound to the OPERATIONS schema (where the
    job/document tracking lives) for the duration of the run.

    When |keep_postgres| is False, the database is thrown away on
    exit: job/document tracking does NOT persist across runs.

    When |keep_postgres| is True, the cluster lives in a stable data directory that
    survives the process, so a later run on the same machine reconnects to the same
    tracking and skips documents already finished. The server is stopped on exit but
    the data is left in place; to reset, delete the data directory below.
    """
    if not local_postgres_helpers.can_start_on_disk_postgresql_database():
        logging.error(
            "pg_ctl is not installed; cannot start a local Postgres. Install "
            "postgres (e.g. `brew install postgresql`) and try again."
        )
        sys.exit(1)

    logging.info(
        "Starting local Postgres for OPERATIONS schema%s",
        (
            f". Data directory: {_PERSISTENT_POSTGRES_DATA_DIR}"
            if keep_postgres
            else " (ephemeral)"
        ),
    )
    with local_persistence_helpers.local_postgres(
        database_key=SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS),
        persistent_data_dir=(_PERSISTENT_POSTGRES_DATA_DIR if keep_postgres else None),
    ):
        yield


@attr.define(kw_only=True)
class SandboxExtractionSummary:
    """The tally of a sandbox extraction run, printed at the end."""

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
        """Logs the run's summary"""
        logging.info("=== Sandbox extraction complete ===")
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


def _extraction_results_source_table_collections(
    config: LLMExtractorConfig,
) -> list[SourceTableCollection]:
    """Returns the (un-prefixed) source table collections holding the extractor's
    raw / validated / audit result tables."""
    return collect_extraction_results_source_table_collections(
        configs={config.state_code: {config.extractor_collection.name: config}}
    )


def create_extraction_results_tables(
    *,
    config: LLMExtractorConfig,
    sandbox_prefix: str,
    table_expiration_ms: int,
    bq_client: BigQueryClient,
) -> None:
    """Creates the raw / validated / audit result tables for the extractor in
    the sandbox-prefixed datasets, with the given table expiration.

    The datasets are created up front with the requested expiration; the
    source table update manager's own create-if-necessary then finds them
    already present and does not override it with its default sandbox
    expiration.
    """
    logging.info("Creating sandbox result tables under prefix [%s].", sandbox_prefix)
    update_manager = SourceTableUpdateManager(bq_client)
    for collection in _extraction_results_source_table_collections(config):
        sandbox_collection = collection.as_sandbox_collection(
            sandbox_dataset_prefix=sandbox_prefix
        )
        bq_client.create_dataset_if_necessary(
            sandbox_collection.dataset_id,
            default_table_expiration_ms=table_expiration_ms,
        )
        update_manager.update(sandbox_collection)


def create_extraction_results_views(
    *,
    config: LLMExtractorConfig,
    sandbox_prefix: str,
    table_expiration_ms: int,
) -> None:
    """Collects the extractor's parsed views and deploys them to the
    sandbox-prefixed datasets, reading from the sandbox result tables the run
    wrote.

    Goes through the shared load_collected_views_to_sandbox wrapper (rather
    than calling create_managed_dataset_and_deploy_views_for_view_builders
    directly) so the sandbox deploy matches every other sandbox view load, and
    so the state_code_filter installs the state-filtering parent-address
    formatter the cross-state union views will need.
    """
    logging.info(
        "Deploying parsed views over the sandbox result tables under prefix [%s].",
        sandbox_prefix,
    )
    state_code = config.state_code
    view_builders = collect_first_order_llm_extraction_results_view_builders([config])

    def _sandbox_source_table_overrides() -> BigQueryAddressOverrides:
        """Returns overrides pointing the parsed views' source tables at the
        sandbox-prefixed result datasets the run wrote."""
        builder = BigQueryAddressOverrides.Builder(sandbox_prefix=sandbox_prefix)
        for collection in _extraction_results_source_table_collections(config):
            builder.register_sandbox_override_for_entire_dataset(collection.dataset_id)
        return builder.build()

    load_collected_views_to_sandbox(
        sandbox_dataset_prefix=sandbox_prefix,
        state_code_filter=state_code,
        collected_builders=view_builders,
        input_source_table_dataset_overrides_dict=None,
        # Point the views' source tables (the raw/validated result tables) at
        # the sandbox-prefixed datasets the run wrote.
        input_source_table_overrides=_sandbox_source_table_overrides(),
        allow_slow_views=True,
        rematerialize_changed_views_only=False,
        failure_mode=BigQueryViewDagWalkerProcessingFailureMode.FAIL_FAST,
        schemas_only=False,
        default_table_expiration_ms=table_expiration_ms,
    )


class SandboxExtractionRunner:
    """Drives the first-order extraction thread end-to-end for one extractor and
    sandbox, holding the run's shared inputs so the per-stage steps don't have to
    thread them through as arguments.
    """

    def __init__(
        self,
        *,
        # The narrowed extractor config every stage reads from.
        config: LLMExtractorConfig,
        # Prefix applied to every BQ dataset this run writes.
        sandbox_prefix: str,
        # Billing labels attached to each Vertex AI request.
        labels: dict[str, str],
        # Client for the sandbox result tables and eligible-document query.
        bq_client: BigQueryClient,
        # Filesystem the document text is read from.
        fs: GCSFileSystem,
        # Client that makes the live Vertex AI extraction calls.
        sync_client: SyncLLMClient,
        # How many results to buffer before flushing to BigQuery and Postgres.
        persist_chunk_size: int,
        # How many requests to build (reading their text from GCS) concurrently.
        request_build_concurrency: int,
        # Minimum seconds between progress lines while the LLM requests run.
        progress_log_interval_seconds: float,
    ) -> None:
        self.config = config
        self.sandbox_prefix = sandbox_prefix
        self.labels = labels
        self.bq_client = bq_client
        self.fs = fs
        self.sync_client = sync_client
        self.persist_chunk_size = persist_chunk_size
        self.request_build_concurrency = request_build_concurrency
        self.progress_log_interval_seconds = progress_log_interval_seconds

        self.summary = SandboxExtractionSummary()
        self.job_manager = LLMExtractionJobManager()
        self.processor = LLMExtractionResultProcessor(
            validator=LLMExtractionResultValidator()
        )
        self.persister = LLMExtractionResultsPersister(
            sandbox_prefix=sandbox_prefix, bq_client=bq_client
        )

    @property
    def state_code(self) -> StateCode:
        return self.config.state_code

    def run(self) -> SandboxExtractionSummary:
        """Runs the extraction thread end-to-end and returns the run's summary.

        Expects the sandbox result tables to already exist; deploying the parsed
        views over the tables this writes is left to the caller.
        """
        active_version = LLMExtractorMetadataManager().set_active_extractor_version(
            config=self.config
        )
        logging.info(
            "Running sandbox extraction for state [%s], collection [%s], "
            "extractor version [%s].",
            self.state_code.value,
            self.config.extractor_collection.name,
            active_version.extractor_version_id,
        )
        # A resumed job (with --keep-postgres) is keyed only on the extractor
        # version, which excludes the --document-limit / --root-entity-ids
        # narrowing. So a resume with different narrowing flags continues the
        # original job's already-selected documents rather than the newly
        # requested ones; the narrowing appears applied but is effectively
        # ignored on resume. TODO(OBT-42806) surface or key on the narrowing.
        job = LLMExtractionJobGenerator(
            eligible_documents_query_builder=LLMExtractionEligibleDocumentQueryBuilder(
                document_filter=self.config.document_filter,
                input_document_collection=self.config.input_document_collection,
            ),
            job_manager=self.job_manager,
            bq_client=self.bq_client,
        ).generate_job(config=self.config, active_version=active_version)

        if job is None:
            logging.info("No documents need processing; nothing to do.")
            return self.summary

        # mark_job_started is a no-op on an already-started (resumed) job, so
        # calling it before the pending-document read lets the try/except own the
        # whole job lifecycle: any failure from here on marks the job failed
        # rather than leaving it open, which under --keep-postgres would block
        # every future run for this extractor version.
        self.job_manager.mark_job_started(state_code=self.state_code, job_id=job.job_id)
        try:
            pending_documents = self.job_manager.get_pending_job_documents(
                state_code=self.state_code, job_id=job.job_id
            )
            if len(pending_documents) > MAX_DOCUMENTS:
                raise ValueError(
                    f"Job [{job.job_id}] holds [{len(pending_documents)}] pending "
                    f"documents, above the MAX_DOCUMENTS ceiling of "
                    f"[{MAX_DOCUMENTS}]. This script's write path is not yet sized "
                    f"for runs this large; narrow the run (e.g. --document-limit) "
                    f"or raise MAX_DOCUMENTS deliberately."
                )
            if pending_documents:
                self._execute_document_extraction(
                    job_id=job.job_id, pending_documents=pending_documents
                )
            else:
                # A resumed job whose documents all finished before a crash still
                # needs completing, or it stays open and blocks every future run.
                logging.info(
                    "Job [%s] has no pending documents; completing it.", job.job_id
                )
            self.job_manager.mark_job_completed(
                state_code=self.state_code, job_id=job.job_id
            )
        except Exception as e:
            self.job_manager.mark_job_failed(
                state_code=self.state_code, job_id=job.job_id, error_message=str(e)
            )
            raise

        return self.summary

    def _execute_document_extraction(
        self, *, job_id: str, pending_documents: list[LLMExtractionJobDocument]
    ) -> None:
        """Builds a request per pending document, runs them through the LLM,
        classifies each result, and flushes the results to BigQuery and Postgres
        in chunks."""
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
                    )
                )
                if len(chunk) >= self.persist_chunk_size:
                    self._write_results_to_bq_and_postgres(results=chunk)
                    chunk = []
        # Flush the final partial chunk.
        self._write_results_to_bq_and_postgres(results=chunk)
        progress.log_final()
        self.summary.llm_phase_seconds = progress.elapsed_seconds

    def _iter_requests(
        self,
        *,
        job_documents: list[LLMExtractionJobDocument],
        request_builder: LLMDocumentExtractionRequestBuilder,
        source_text_by_document: dict[str, str],
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
                except LLMDocumentExtractionRequestError:
                    logging.error(
                        "Could not build a request for document [%s]; leaving it "
                        "unmarked for re-selection on the next run.",
                        document_contents_id,
                    )
                    self.summary.failed_to_build += 1
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
                    continue
                source_text_by_document[document_contents_id] = request.document_text
                yield request

    def _classify_result(
        self,
        *,
        raw_result: LLMClientDocumentExtractionResult,
        job_id: str,
        source_document_text: str,
    ) -> LLMJobDocumentExtractionResult:
        """Returns the processed result for one raw extraction result, classified
        and validated, and folds its counts and token usage into the summary.
        """
        result = self.processor.validate_and_classify(
            config=self.config,
            raw_result=raw_result,
            job_id=job_id,
            source_document_text=source_document_text,
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


def _validate_external_id_type(
    *, external_id_type: str | None, state_code: StateCode
) -> None:
    """Validates that |external_id_type|, if given, is an external ID type
    registered for |state_code| in external_id_types.py.
    """
    if external_id_type is None:
        return
    allowed_id_types = external_id_types_by_state_code()[state_code]
    if external_id_type not in allowed_id_types:
        raise ValueError(
            f"Got --external-id-type [{external_id_type}], which is not an "
            f"external ID type for [{state_code.value}]. Registered types: "
            f"{sorted(allowed_id_types)}."
        )


def parse_arguments() -> argparse.Namespace:
    """Parses the command-line arguments for a sandbox extraction run."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project-id",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        default=GCP_PROJECT_STAGING,
        help="The GCP project whose document store and Vertex AI this run uses.",
    )
    parser.add_argument(
        "--sandbox-prefix",
        required=True,
        help="Prefix applied to every BQ dataset this run writes (result tables "
        "and views).",
    )
    parser.add_argument(
        "--state-code",
        type=StateCode,
        required=True,
        help="The state whose extractor to run.",
    )
    parser.add_argument(
        "--collection",
        required=True,
        help="The extractor collection to run (e.g. PLAYGROUND_EMPLOYMENT_INFO).",
    )
    # TODO(OBT-32176): When entity resolution is added to this script, skip the ER
    # phase when --document-limit is set. Entity resolution across a random,
    # arbitrarily-truncated set of documents doesn't make sense — the limit pulls
    # documents without regard to which entities they mention.
    parser.add_argument(
        "--document-limit",
        type=int,
        default=None,
        help="Cap the number of documents processed.",
    )
    parser.add_argument(
        "--root-entity-ids",
        nargs="+",
        default=None,
        help="Restrict processing to documents for these root entities.",
    )
    parser.add_argument(
        "--external-id-type",
        default=None,
        help="The id type qualifying every --root-entity-ids value (e.g. "
        "US_CO_OFFENDERID). Required when the extractor's document collection is "
        "keyed by an external root entity id, since the same id string can belong "
        "to different people under different id types. Omit for a collection keyed "
        "by an internal person_id / staff_id.",
    )
    parser.add_argument(
        "--table-expiration-days",
        type=positive_int,
        default=DEFAULT_TABLE_EXPIRATION_DAYS,
        help="How long the sandbox BQ datasets live before expiring.",
    )
    parser.add_argument(
        "--labels",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Additional billing labels for cost attribution, on top of the "
        "requester and sandbox-prefix labels the run attaches automatically.",
    )
    parser.add_argument(
        "--keep-postgres",
        action="store_true",
        help="Keep the local Postgres job/document tracking across runs (in a "
        "stable data directory) instead of throwing it away at the end. Lets a "
        "large run resume after a crash, skipping documents already finished. "
        "Delete the data directory to reset.",
    )
    parser.add_argument(
        "--pre-view-materialization-delay-minutes",
        type=non_negative_int,
        default=1,
        help="How long to wait after the run before materializing the parsed "
        "views, to give the result rows time to leave BigQuery's streaming buffer "
        "and become queryable. Defaults to 1 minute; 0 disables the wait.",
    )
    args = parser.parse_args()

    _validate_external_id_type(
        external_id_type=args.external_id_type, state_code=args.state_code
    )
    return args


def run_sandbox_extraction(args: argparse.Namespace) -> SandboxExtractionSummary:
    """Runs one sandbox extraction end-to-end: creates the result tables, drives
    the extraction thread, and deploys the parsed views over what it wrote."""
    config = get_first_order_llm_extractor_config(
        args.state_code, args.collection
    ).with_sandbox_narrowing(
        document_limit=args.document_limit,
        root_entity_ids=args.root_entity_ids,
        external_id_type=args.external_id_type,
    )
    table_expiration_ms = args.table_expiration_days * 24 * 60 * 60 * 1000
    bq_client = BigQueryClientImpl(project_id=project_id())

    create_extraction_results_tables(
        config=config,
        sandbox_prefix=args.sandbox_prefix,
        table_expiration_ms=table_expiration_ms,
        bq_client=bq_client,
    )

    summary = SandboxExtractionRunner(
        config=config,
        sandbox_prefix=args.sandbox_prefix,
        labels=_build_labels(
            user_labels=args.labels, sandbox_prefix=args.sandbox_prefix
        ),
        bq_client=bq_client,
        fs=GcsfsFactory.build(),
        sync_client=VertexAISyncLLMClient(model_config=config.model_config),
        persist_chunk_size=DEFAULT_PERSIST_CHUNK_SIZE,
        request_build_concurrency=DEFAULT_REQUEST_BUILD_CONCURRENCY,
        progress_log_interval_seconds=DEFAULT_PROGRESS_LOG_INTERVAL_SECONDS,
    ).run()

    # A run that processed nothing (no eligible documents, or a resumed job whose
    # work was already done) wrote no new result rows, so there is nothing to wait
    # for the streaming buffer on and nothing new for the views to reflect. Skip
    # the delay and the view redeploy in that case.
    if summary.processed == 0:
        logging.info("No documents were processed; skipping view materialization.")
        return summary

    # TODO(OBT-41801): The view materialization below queries the result tables the
    # run just wrote — but those writes go through streaming inserts, whose rows can
    # sit in the streaming buffer and be invisible to a query for minutes. So the
    # materialized views can come back empty or short a few rows even though the run
    # reported success. --pre-view-materialization-delay-minutes waits before
    # materializing as a partial mitigation (the buffer can take longer than any
    # reasonable wait, rarely up to ~90 minutes); moving the persister to load jobs
    # (which commit atomically) is the real fix that removes this race.
    if args.pre_view_materialization_delay_minutes:
        delay_seconds = args.pre_view_materialization_delay_minutes * 60
        logging.info(
            "Waiting %d minute(s) for streamed result rows to become queryable "
            "before materializing views.",
            args.pre_view_materialization_delay_minutes,
        )
        time.sleep(delay_seconds)

    create_extraction_results_views(
        config=config,
        sandbox_prefix=args.sandbox_prefix,
        table_expiration_ms=table_expiration_ms,
    )
    return summary


@requires_google_adc
def main() -> None:
    logging.getLogger().setLevel(logging.INFO)
    # google-genai dispatches one HTTP request per document through httpx, which
    # logs a line per request at INFO — one for every document in the run, which
    # buries the run's own progress output. WARNING keeps transport failures
    # visible while dropping the successful-request chatter.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    args = parse_arguments()
    with (
        local_project_id_override(args.project_id),
        _local_operations_postgres(keep_postgres=args.keep_postgres),
    ):
        summary = run_sandbox_extraction(args)
    summary.log()


if __name__ == "__main__":
    main()
