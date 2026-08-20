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
from collections.abc import Generator
from contextlib import contextmanager

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.common.constants.states import StateCode
from recidiviz.common.git import get_normalized_git_username
from recidiviz.documents.extraction.llm_client.vertex_ai_sync_llm_client import (
    VertexAISyncLLMClient,
)
from recidiviz.documents.extraction.llm_extraction_job_manager import (
    LLMExtractionJobManager,
)
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    get_first_order_llm_extractor_config,
)
from recidiviz.documents.extraction.views.llm_extraction_results_view_collector import (
    collect_first_order_llm_extraction_results_view_builders,
)
from recidiviz.documents.store.document_store_sandbox_context import (
    DocumentStoreSandboxContext,
)
from recidiviz.ingest.direct.external_id_type_helpers import (
    external_id_types_by_state_code,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.documents.sandbox_document_extraction_processor import (
    DocumentExtractionProcessor,
    SandboxExtractionSummary,
)
from recidiviz.tools.documents.sandbox_extraction_bq_helpers import (
    create_extraction_results_tables,
    deploy_extraction_results_views,
    first_order_input_overrides,
)
from recidiviz.tools.documents.sandbox_extraction_runners import SandboxExtractionRunner
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.utils.script_helpers import requires_google_adc
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override, project_id
from recidiviz.utils.params import non_negative_int, parse_key_value_args, positive_int

DEFAULT_TABLE_EXPIRATION_DAYS = 14

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


def run_sandbox_extraction(
    args: argparse.Namespace,
) -> SandboxExtractionSummary | None:
    """Runs one sandbox extraction end-to-end: creates the result tables, drives
    the extraction thread, and deploys the parsed views over what it wrote. Returns
    None if there was no work to do (no eligible documents, or a resumed job whose
    documents all finished before a crash)."""
    config = get_first_order_llm_extractor_config(
        args.state_code, args.collection
    ).with_sandbox_narrowing(
        document_limit=args.document_limit,
        root_entity_ids=args.root_entity_ids,
        external_id_type=args.external_id_type,
    )
    table_expiration_ms = args.table_expiration_days * 24 * 60 * 60 * 1000
    bq_client = BigQueryClientImpl(project_id=project_id())

    # TODO(OBT-42680) This script currently only supports extractions against the real
    # document store, so there is no sandbox document store to read from. When sandbox
    # documents are supported, this will point at the sandbox copy the run seeded.
    document_store_sandbox: DocumentStoreSandboxContext | None = None

    logging.info(
        "Creating sandbox result tables under prefix [%s].", args.sandbox_prefix
    )
    create_extraction_results_tables(
        config=config,
        sandbox_prefix=args.sandbox_prefix,
        table_expiration_ms=table_expiration_ms,
        bq_client=bq_client,
    )

    job_manager = LLMExtractionJobManager()
    summary = SandboxExtractionRunner(
        config=config,
        document_store_sandbox=document_store_sandbox,
        bq_client=bq_client,
        job_manager=job_manager,
        processor=DocumentExtractionProcessor(
            config=config,
            results_sandbox_prefix=args.sandbox_prefix,
            document_store_sandbox=document_store_sandbox,
            labels=_build_labels(
                user_labels=args.labels, sandbox_prefix=args.sandbox_prefix
            ),
            bq_client=bq_client,
            fs=GcsfsFactory.build(),
            sync_client=VertexAISyncLLMClient(model_config=config.model_config),
            job_manager=job_manager,
            persist_chunk_size=DEFAULT_PERSIST_CHUNK_SIZE,
            request_build_concurrency=DEFAULT_REQUEST_BUILD_CONCURRENCY,
            progress_log_interval_seconds=DEFAULT_PROGRESS_LOG_INTERVAL_SECONDS,
        ),
    ).run()

    # A run that processed nothing (no eligible documents, or a resumed job whose
    # work was already done) returns no summary: it wrote no new result rows, so
    # there is nothing to wait for the streaming buffer on and nothing new for the
    # views to reflect. Skip the delay and the view redeploy in that case.
    if summary is None or summary.processed == 0:
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

    logging.info(
        "Deploying parsed views over the sandbox result tables under prefix [%s].",
        args.sandbox_prefix,
    )
    deploy_extraction_results_views(
        config=config,
        view_builders=collect_first_order_llm_extraction_results_view_builders(
            [config]
        ),
        results_sandbox_prefix=args.sandbox_prefix,
        input_source_table_overrides=first_order_input_overrides(
            config=config,
            results_sandbox_prefix=args.sandbox_prefix,
            document_store_sandbox=document_store_sandbox,
        ),
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

    if summary is None:
        logging.info(
            "=== Sandbox extraction complete: no work to do (no eligible documents, "
            "or a resumed run whose documents all finished). ==="
        )
        return
    summary.log()


if __name__ == "__main__":
    main()
