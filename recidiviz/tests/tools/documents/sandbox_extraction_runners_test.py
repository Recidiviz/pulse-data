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
"""Tests for sandbox_extraction_runners.py"""

import copy
import csv
import io
import threading
from pathlib import Path
from typing import Any
from unittest import mock

import attr
from google.cloud import bigquery
from google.cloud.bigquery.enums import SqlTypeNames

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.common import attr_validators
from recidiviz.common.constants.operations.llm_extraction_job import (
    LLMDocumentExtractionErrorType,
    LLMExtractionJobDocumentResultType,
    LLMExtractionJobResultType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.extraction_results_columns import (
    EXTRACTION_JOB_ID_COLUMN_NAME,
    RESULT_DATETIME_UTC_COLUMN_NAME,
    VALIDATION_DATETIME_UTC_COLUMN_NAME,
)
from recidiviz.documents.extraction.llm_client.types import (
    LLMClientDocumentExtractionResult,
    LLMDocumentExtractionRequest,
    LLMDocumentExtractionTokenCounts,
    LLMRequestErrorType,
)
from recidiviz.documents.extraction.llm_extraction_job_manager import (
    LLMExtractionJobManager,
)
from recidiviz.documents.extraction.llm_extraction_results_persister import (
    LLMExtractionResultsPersister,
)
from recidiviz.documents.extraction.llm_extraction_results_tables import (
    ExtractionRawResultsBQTable,
    ExtractionValidatedResultsBQTable,
    ExtractionValidationAuditBQTable,
)
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    get_first_order_llm_extractor_config,
)
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
)
from recidiviz.documents.store.document_collection_config_collectors import (
    get_document_collection_config,
)
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
)
from recidiviz.documents.store.document_store_gcs_path_utils import (
    gcs_path_for_document,
)
from recidiviz.documents.store.document_store_sandbox_context import (
    DocumentCollectionSandboxLocation,
    DocumentStoreSandboxContext,
)
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.source_tables.document_store_source_table_collection import (
    collect_document_store_source_tables,
)
from recidiviz.source_tables.extraction_results_source_table_collection import (
    collect_extraction_results_source_table_collections,
)
from recidiviz.source_tables.source_table_config import (
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tests.documents import fake_config
from recidiviz.tests.documents.extraction.entity_resolution.entity_resolution_test_utils import (
    patch_fake_entity_resolution_model_config_name,
)
from recidiviz.tests.documents.extraction.fake_extractor_result_json import (
    fake_minimal_relevant_result_json,
    ground_citations_in_fake_source_text,
)
from recidiviz.tests.documents.extraction.llm_client.fake_sync_llm_client import (
    FakeSyncLLMClient,
)
from recidiviz.tests.ingest.direct.fixture_util import load_dataframe_from_path
from recidiviz.tests.tools.documents import fixtures
from recidiviz.tools.documents import sandbox_extraction_runners
from recidiviz.tools.documents.sandbox_document_extraction_processor import (
    DocumentExtractionProcessor,
    SandboxExtractionSummary,
)
from recidiviz.tools.documents.sandbox_extraction_runners import (
    SandboxDocumentStoreRunner,
    SandboxExtractionRunner,
)
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult

_STATE_CODE = StateCode.US_XX
_COLLECTION_NAME = "FAKE_EXTRACTOR_COLLECTION"
_INPUT_COLLECTION_NAME = "FAKE_INPUT_NOTES"
_SANDBOX_PREFIX = "test_prefix"

_DOC_A = "CID_A"
_DOC_B = "CID_B"


# The canned success the fake LLM client returns, paired with the document text
# seeded into GCS for the documents it is returned for. Validation checks a
# result's citations against the document it was extracted from, so the two have
# to be built together for a result to pass as usable.
_GROUNDED_SUCCESS_RESULT = ground_citations_in_fake_source_text(
    fake_minimal_relevant_result_json()
)
_DOCUMENT_TEXT = _GROUNDED_SUCCESS_RESULT.source_document_text


def _token_counts() -> LLMDocumentExtractionTokenCounts:
    return LLMDocumentExtractionTokenCounts(
        input_token_count=10,
        output_token_count=5,
        cached_input_token_count=2,
        thinking_token_count=0,
    )


def _success_result_fn(
    request: LLMDocumentExtractionRequest,
) -> LLMClientDocumentExtractionResult:
    """Returns a canned success whose JSON conforms to the fake collection's
    schema, so the real validator passes it as a usable relevant result."""
    return LLMClientDocumentExtractionResult.from_success(
        document_contents_id=request.document_contents_id,
        result_json=copy.deepcopy(_GROUNDED_SUCCESS_RESULT.result_json),
        token_counts=_token_counts(),
    )


@attr.define(frozen=True, kw_only=True)
class _ExtractionJobDocumentSnapshot:
    """A detached copy of an LLMExtractionJobDocument's result columns, read out
    of the session so it survives the session close and can be compared by value."""

    result_type: str | None = attr.ib(validator=attr_validators.is_opt_str)
    is_relevant: bool | None = attr.ib(validator=attr_validators.is_opt_bool)
    error_type: str | None = attr.ib(validator=attr_validators.is_opt_str)
    error_message: str | None = attr.ib(validator=attr_validators.is_opt_str)
    input_token_count: int | None = attr.ib(validator=attr_validators.is_opt_int)
    output_token_count: int | None = attr.ib(validator=attr_validators.is_opt_int)
    cached_input_token_count: int | None = attr.ib(validator=attr_validators.is_opt_int)
    thinking_token_count: int | None = attr.ib(validator=attr_validators.is_opt_int)


@attr.define(frozen=True, kw_only=True)
class _ExtractionJobSnapshot:
    """A detached copy of the extraction job's state. The completion
    datetime is reduced to a boolean since its exact value is run-stamped."""

    result_type: str | None = attr.ib(validator=attr_validators.is_opt_str)
    is_completed: bool = attr.ib(validator=attr_validators.is_bool)


# A document that was never processed leaves every result column null.
_UNMARKED_DOCUMENT = _ExtractionJobDocumentSnapshot(
    result_type=None,
    is_relevant=None,
    error_type=None,
    error_message=None,
    input_token_count=None,
    output_token_count=None,
    cached_input_token_count=None,
    thinking_token_count=None,
)


# TODO(OBT-32105) Add more tests: partial failures, cross-run postgres state etc.
class RunSandboxExtractionTest(BigQueryEmulatorTestCase):
    """Runs the extraction thread end-to-end against a real Postgres and the BQ
    emulator."""

    # The seeded tables (result tables + doc-store tables) are created once in
    # setUpClass; keep them across tests and only clear their rows between tests.
    wipe_emulator_data_on_teardown = False

    postgres_launch_result: OnDiskPostgresLaunchResult

    @classmethod
    def get_source_tables(cls) -> list[SourceTableCollection]:
        # The real persister writes to sandbox-prefixed result tables, and the
        # real eligible-document query reads the (un-prefixed) doc-store metadata
        # and contents tables — seed both up front.
        result_tables = [
            collection.as_sandbox_collection(_SANDBOX_PREFIX)
            for collection in collect_extraction_results_source_table_collections(
                configs={
                    _STATE_CODE: {
                        _COLLECTION_NAME: get_first_order_llm_extractor_config(
                            _STATE_CODE, _COLLECTION_NAME, config_module=fake_config
                        )
                    }
                }
            )
        ]
        return result_tables + collect_document_store_source_tables(fake_config)

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.postgres_launch_result = (
            local_postgres_helpers.start_on_disk_postgresql_database()
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.postgres_launch_result
        )
        super().tearDownClass()

    def setUp(self) -> None:
        super().setUp()
        self.database_key = SQLAlchemyDatabaseKey.for_schema(
            LLMExtractionJobManager().database_key.schema_type
        )
        local_persistence_helpers.use_on_disk_postgresql_database(
            self.postgres_launch_result, self.database_key
        )

        self.config = get_first_order_llm_extractor_config(
            _STATE_CODE, _COLLECTION_NAME, config_module=fake_config
        )
        self.fs = FakeGCSFileSystem()

        self._seed_document_store_tables()

    def tearDown(self) -> None:
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )
        # Rows persist across tests (wipe_emulator_data_on_teardown is False), so
        # clear every seeded table's rows; setUp reseeds the doc-store tables.
        self._clear_emulator_table_data()
        super().tearDown()

    def _input_collection_config(self) -> DocumentCollectionConfig:
        return get_document_collection_config(
            _STATE_CODE, _INPUT_COLLECTION_NAME, fake_config
        )

    def _seed_document_store_tables(self) -> None:
        """Loads the metadata and contents tables the eligible-document query
        reads. The metadata table's tables already exist (seeded at emulator
        startup) but _clear_emulator_table_data empties their rows between tests,
        so this reloads the rows each time."""
        config = self._input_collection_config()
        fixtures_dir = Path(fixtures.__file__).parent
        self._load_rows_from_fixture(
            address=config.metadata_table_address(sandbox_dataset_prefix=None),
            fixture_path=fixtures_dir / "fake_input_notes_metadata.csv",
        )
        self._load_rows_from_fixture(
            address=config.document_contents_table_address(sandbox_dataset_prefix=None),
            fixture_path=fixtures_dir / "fake_input_notes_contents.csv",
        )

    def _load_rows_from_fixture(
        self, *, address: BigQueryAddress, fixture_path: Path
    ) -> None:
        """Streams a CSV fixture's rows into an already-created emulator table,
        typing each column against the table's schema."""
        df = load_dataframe_from_path(
            fixture_path, fixture_columns=None, allow_comments=False
        )
        if not df.empty:
            self.load_rows_into_table(address, df.to_dict("records"))

    def _seed_gcs(self, document_contents_id: str, text: str) -> None:
        self.fs.upload_from_string(
            path=gcs_path_for_document(
                project_id=self.project_id,
                state_code=_STATE_CODE,
                collection_name=self.config.input_document_collection.name,
                document_contents_id=document_contents_id,
                # TODO(OBT-42680) Test reading from sandbox doc-store bucket
                sandbox_prefix=None,
            ),
            contents=text,
            content_type="text/plain",
        )

    def _run(
        self,
        sync_client: FakeSyncLLMClient,
        persist_chunk_size: int = 1000,
        request_build_concurrency: int = 4,
        progress_log_interval_seconds: float = 0.0,
    ) -> SandboxExtractionSummary | None:
        # Drives just the extraction thread against the seeded result and doc-store
        # tables; the table-create, upload, and view-deploy steps live in
        # run_sandbox_extraction and are not exercised here.
        config = self.config.with_sandbox_narrowing(
            document_limit=5, root_entity_ids=None, external_id_type=None
        )
        job_manager = LLMExtractionJobManager()
        # A first-order run always reads its input from the production document store;
        # there is no supported first-order sandbox document store yet
        # (TODO(OBT-42680)). A None document store sandbox reads the input collection
        # from production.
        return SandboxExtractionRunner(
            config=config,
            document_store_sandbox=None,
            bq_client=self.bq_client,
            job_manager=job_manager,
            processor=DocumentExtractionProcessor(
                config=config,
                results_sandbox_prefix=_SANDBOX_PREFIX,
                document_store_sandbox=None,
                labels={"reason": "test"},
                bq_client=self.bq_client,
                fs=self.fs,
                sync_client=sync_client,
                job_manager=job_manager,
                persist_chunk_size=persist_chunk_size,
                request_build_concurrency=request_build_concurrency,
                progress_log_interval_seconds=progress_log_interval_seconds,
            ),
        ).run()

    def _fake_client(
        self,
        result_fn: Any = _success_result_fn,
    ) -> FakeSyncLLMClient:
        return FakeSyncLLMClient(
            model_config=self.config.model_config, result_fn=result_fn
        )

    def _assert_result_tables_match_fixtures(self, scenario: str) -> None:
        """Asserts the full contents of the raw, validated, and audit result
        tables equal the CSV fixtures for |scenario|. The run-stamped columns —
        the per-run job UUID and the result/validation datetimes — are excluded
        from the comparison since they vary between runs."""
        fixtures_dir = Path(fixtures.__file__).parent
        # The three tables share a table_id, so key the fixture on the table's
        # role (raw / validated / audit) instead.
        for role, table, run_stamped_datetime_column in (
            ("raw", ExtractionRawResultsBQTable, RESULT_DATETIME_UTC_COLUMN_NAME),
            (
                "validated",
                ExtractionValidatedResultsBQTable,
                VALIDATION_DATETIME_UTC_COLUMN_NAME,
            ),
            (
                "audit",
                ExtractionValidationAuditBQTable,
                VALIDATION_DATETIME_UTC_COLUMN_NAME,
            ),
        ):
            ignore_columns = [
                EXTRACTION_JOB_ID_COLUMN_NAME,
                run_stamped_datetime_column,
            ]
            fixture_path = fixtures_dir / scenario / f"{role}_output.csv"
            with self.subTest(fixture=str(fixture_path)):
                self.compare_table_to_fixture(
                    address=table.address(
                        state_code=_STATE_CODE,
                        collection_name=_COLLECTION_NAME,
                        sandbox_prefix=_SANDBOX_PREFIX,
                    ),
                    columns_to_ignore=ignore_columns,
                    expected_output_fixture_path=fixture_path,
                    expect_missing_fixtures_on_empty_results=True,
                    create_expected=False,
                    expect_unique_output_rows=True,
                )

    def _extraction_job_document_by_id(
        self,
    ) -> dict[str, _ExtractionJobDocumentSnapshot]:
        with SessionFactory.using_database(self.database_key) as session:
            return {
                doc.document_contents_id: _ExtractionJobDocumentSnapshot(
                    result_type=doc.result_type,
                    is_relevant=doc.is_relevant,
                    error_type=doc.error_type,
                    error_message=doc.error_message,
                    input_token_count=doc.input_token_count,
                    output_token_count=doc.output_token_count,
                    cached_input_token_count=doc.cached_input_token_count,
                    thinking_token_count=doc.thinking_token_count,
                )
                for doc in session.query(schema.LLMExtractionJobDocument).all()
            }

    def _extraction_job_snapshot(self) -> _ExtractionJobSnapshot:
        with SessionFactory.using_database(self.database_key) as session:
            job = session.query(schema.LLMExtractionJob).one()
            return _ExtractionJobSnapshot(
                result_type=job.result_type,
                is_completed=job.completion_datetime_utc is not None,
            )

    def test_happy_path_persists_bq_rows_and_marks_postgres(self) -> None:
        self._seed_gcs(_DOC_A, _DOCUMENT_TEXT)
        self._seed_gcs(_DOC_B, _DOCUMENT_TEXT)

        summary = self._run(self._fake_client())

        # Both documents succeed, so their token counts are summed across the run.
        self.assertEqual(
            SandboxExtractionSummary(
                extractor_config_name=_COLLECTION_NAME,
                processed=2,
                succeeded=2,
                input_tokens=20,
                output_tokens=10,
                cached_input_tokens=4,
                thinking_tokens=0,
            ),
            summary,
        )

        # Both documents' content landed in the sandbox-prefixed BQ result tables.
        self._assert_result_tables_match_fixtures("happy_path")

        # Both documents were marked SUCCESS in Postgres, each with its own token
        # counts recorded on its own row (not just summed into the run totals).
        expected_doc = _ExtractionJobDocumentSnapshot(
            result_type=LLMExtractionJobDocumentResultType.SUCCESS.value,
            is_relevant=True,
            error_type=None,
            error_message=None,
            input_token_count=10,
            output_token_count=5,
            cached_input_token_count=2,
            thinking_token_count=0,
        )
        self.assertEqual(
            {_DOC_A: expected_doc, _DOC_B: expected_doc},
            self._extraction_job_document_by_id(),
        )
        self.assertEqual(
            _ExtractionJobSnapshot(
                result_type=LLMExtractionJobResultType.SUCCESS.value,
                is_completed=True,
            ),
            self._extraction_job_snapshot(),
        )

    def test_requests_are_built_concurrently(self) -> None:
        # Building requests inline on the thread driving the request runner
        # serialized each document's GCS read and starved the LLM pool. Both
        # documents' reads must now be in flight at once: this barrier only clears
        # when two threads reach it, so a serialized build breaks the barrier on
        # its timeout and fails the test rather than passing slowly.
        both_reads_in_flight = threading.Barrier(2, timeout=10)
        real_download_as_string = self.fs.download_as_string

        def download_as_string(path: GcsfsFilePath, encoding: str = "utf-8") -> str:
            # Waits before delegating, so neither thread is holding the fake
            # filesystem's mutex while parked here.
            both_reads_in_flight.wait()
            return real_download_as_string(path, encoding)

        self._seed_gcs(_DOC_A, _DOCUMENT_TEXT)
        self._seed_gcs(_DOC_B, _DOCUMENT_TEXT)

        with mock.patch.object(self.fs, "download_as_string", download_as_string):
            summary = self._run(self._fake_client(), request_build_concurrency=2)

        self.assertEqual(
            SandboxExtractionSummary(
                extractor_config_name=_COLLECTION_NAME,
                processed=2,
                succeeded=2,
                input_tokens=20,
                output_tokens=10,
                cached_input_tokens=4,
                thinking_tokens=0,
            ),
            summary,
        )

    def test_failed_request_counts_as_llm_request_failure(self) -> None:
        self._seed_gcs(_DOC_A, _DOCUMENT_TEXT)
        self._seed_gcs(_DOC_B, _DOCUMENT_TEXT)

        def result_fn(
            request: LLMDocumentExtractionRequest,
        ) -> LLMClientDocumentExtractionResult:
            return LLMClientDocumentExtractionResult.from_error(
                document_contents_id=request.document_contents_id,
                error_type=LLMRequestErrorType.CONTENT_FILTERED,
                error_message="blocked",
            )

        summary = self._run(self._fake_client(result_fn))

        # An LLM-request-level error is counted separately from a validation
        # failure. The error result carries no usage, so no tokens are summed.
        self.assertEqual(
            SandboxExtractionSummary(
                extractor_config_name=_COLLECTION_NAME,
                processed=2,
                failed_llm_request=2,
            ),
            summary,
        )

        # A content-filtered request never produces a result to validate, so no
        # rows land in any of the three result tables.
        self._assert_result_tables_match_fixtures("llm_request_error")

        # Both documents are marked with a permanent document-level failure
        # carrying the error type/message and no is_relevant. The error result
        # carries no usage, so its token counts are zero.
        expected_doc = _ExtractionJobDocumentSnapshot(
            result_type=LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_PERMANENT.value,
            is_relevant=None,
            error_type=LLMDocumentExtractionErrorType.LLM_REQUEST_CONTENT_FILTERED.value,
            error_message="blocked",
            input_token_count=0,
            output_token_count=0,
            cached_input_token_count=0,
            thinking_token_count=0,
        )
        self.assertEqual(
            {_DOC_A: expected_doc, _DOC_B: expected_doc},
            self._extraction_job_document_by_id(),
        )
        # Every document failed, so the job completes as a partial failure.
        self.assertEqual(
            _ExtractionJobSnapshot(
                result_type=LLMExtractionJobResultType.PARTIAL_FAILURE.value,
                is_completed=True,
            ),
            self._extraction_job_snapshot(),
        )

    def test_failed_validations(self) -> None:
        self._seed_gcs(_DOC_A, _DOCUMENT_TEXT)
        self._seed_gcs(_DOC_B, _DOCUMENT_TEXT)

        def result_fn(
            request: LLMDocumentExtractionRequest,
        ) -> LLMClientDocumentExtractionResult:
            # A successful request whose JSON does not conform to the collection's
            # schema, so it reaches the validator and fails there.
            return LLMClientDocumentExtractionResult.from_success(
                document_contents_id=request.document_contents_id,
                result_json={"not_a_valid_field": "nope"},
                token_counts=_token_counts(),
            )

        summary = self._run(self._fake_client(result_fn))

        # Both documents' requests succeeded but their results failed validation,
        # so they land in failed_validation, not failed_llm_request. The requests'
        # token counts are still summed across the run.
        self.assertEqual(
            SandboxExtractionSummary(
                extractor_config_name=_COLLECTION_NAME,
                processed=2,
                failed_validation=2,
                input_tokens=20,
                output_tokens=10,
                cached_input_tokens=4,
                thinking_tokens=0,
            ),
            summary,
        )

        # A validation failure persists the raw result and an audit row marking
        # the failure, but writes no validated row.
        self._assert_result_tables_match_fixtures("validation_failure")

        # Both documents are marked with a transient document-level failure (the
        # structural check flags it retryable), with no error type/message and
        # no is_relevant. The request's token counts are still recorded.
        expected_doc = _ExtractionJobDocumentSnapshot(
            result_type=LLMExtractionJobDocumentResultType.DOCUMENT_LEVEL_FAILURE_TRANSIENT.value,
            is_relevant=None,
            error_type=None,
            error_message=None,
            input_token_count=10,
            output_token_count=5,
            cached_input_token_count=2,
            thinking_token_count=0,
        )
        self.assertEqual(
            {_DOC_A: expected_doc, _DOC_B: expected_doc},
            self._extraction_job_document_by_id(),
        )
        # Every document failed, so the job completes as a partial failure.
        self.assertEqual(
            _ExtractionJobSnapshot(
                result_type=LLMExtractionJobResultType.PARTIAL_FAILURE.value,
                is_completed=True,
            ),
            self._extraction_job_snapshot(),
        )

    def test_missing_gcs_document_left_unmarked(self) -> None:
        # doc-a has GCS text; doc-b was never uploaded to GCS, so its request
        # cannot be built.
        self._seed_gcs(_DOC_A, _DOCUMENT_TEXT)

        summary = self._run(self._fake_client())

        # doc-a processed and succeeded (contributing its token counts); doc-b
        # could not be assembled into a request.
        self.assertEqual(
            SandboxExtractionSummary(
                extractor_config_name=_COLLECTION_NAME,
                processed=1,
                succeeded=1,
                failed_to_build=1,
                input_tokens=10,
                output_tokens=5,
                cached_input_tokens=2,
                thinking_tokens=0,
            ),
            summary,
        )
        # doc-a processed and marked SUCCESS; doc-b left entirely unmarked for
        # re-selection on the next run.
        self.assertEqual(
            {
                _DOC_A: _ExtractionJobDocumentSnapshot(
                    result_type=LLMExtractionJobDocumentResultType.SUCCESS.value,
                    is_relevant=True,
                    error_type=None,
                    error_message=None,
                    input_token_count=10,
                    output_token_count=5,
                    cached_input_token_count=2,
                    thinking_token_count=0,
                ),
                _DOC_B: _UNMARKED_DOCUMENT,
            },
            self._extraction_job_document_by_id(),
        )
        # An unmarked document is not a failure, so the job completes as SUCCESS.
        self.assertEqual(
            _ExtractionJobSnapshot(
                result_type=LLMExtractionJobResultType.SUCCESS.value,
                is_completed=True,
            ),
            self._extraction_job_snapshot(),
        )
        # Only doc-a's content was persisted.
        self._assert_result_tables_match_fixtures("missing_gcs")

    def test_empty_document_skipped(self) -> None:
        self._seed_gcs(_DOC_A, "")
        self._seed_gcs(_DOC_B, _DOCUMENT_TEXT)

        summary = self._run(self._fake_client())

        # doc-a has empty text and is skipped before the LLM; doc-b processes and
        # succeeds, contributing its token counts.
        self.assertEqual(
            SandboxExtractionSummary(
                extractor_config_name=_COLLECTION_NAME,
                processed=1,
                succeeded=1,
                skipped_empty=1,
                input_tokens=10,
                output_tokens=5,
                cached_input_tokens=2,
                thinking_tokens=0,
            ),
            summary,
        )
        # doc-a skipped and left entirely unmarked; doc-b marked SUCCESS.
        self.assertEqual(
            {
                _DOC_A: _UNMARKED_DOCUMENT,
                _DOC_B: _ExtractionJobDocumentSnapshot(
                    result_type=LLMExtractionJobDocumentResultType.SUCCESS.value,
                    is_relevant=True,
                    error_type=None,
                    error_message=None,
                    input_token_count=10,
                    output_token_count=5,
                    cached_input_token_count=2,
                    thinking_token_count=0,
                ),
            },
            self._extraction_job_document_by_id(),
        )
        # A skipped document is not a failure, so the job completes as SUCCESS.
        self.assertEqual(
            _ExtractionJobSnapshot(
                result_type=LLMExtractionJobResultType.SUCCESS.value,
                is_completed=True,
            ),
            self._extraction_job_snapshot(),
        )
        # Only doc-b's content was persisted.
        self._assert_result_tables_match_fixtures("empty_document")

    def test_no_work_creates_no_job_and_exits(self) -> None:
        # No live documents in the doc store, so the eligible-document query
        # returns nothing, no job is created, and the run exits with an empty
        # summary. Empty just the two tables the query reads rather than wiping
        # the whole emulator.
        config = self._input_collection_config()
        for address in (
            config.metadata_table_address(sandbox_dataset_prefix=None),
            config.document_contents_table_address(sandbox_dataset_prefix=None),
        ):
            self.bq_client.delete_from_table_async(address).result()

        summary = self._run(self._fake_client())

        self.assertIsNone(summary)
        with SessionFactory.using_database(self.database_key) as session:
            self.assertEqual(0, session.query(schema.LLMExtractionJob).count())

    def test_resumed_job_with_no_pending_documents_is_completed(self) -> None:
        # Simulate a crash between marking the last document and completing the
        # job: the first run processes every document (marking them all) but its
        # mark_job_completed is stubbed out, leaving an open job. The next run
        # resumes that job, finds nothing pending, and must complete it rather
        # than leave it open forever.
        self._seed_gcs(_DOC_A, _DOCUMENT_TEXT)
        self._seed_gcs(_DOC_B, _DOCUMENT_TEXT)

        with mock.patch.object(LLMExtractionJobManager, "mark_job_completed"):
            first_summary = self._run(self._fake_client())
        self.assertEqual(
            SandboxExtractionSummary(
                extractor_config_name=_COLLECTION_NAME,
                processed=2,
                succeeded=2,
                input_tokens=20,
                output_tokens=10,
                cached_input_tokens=4,
                thinking_tokens=0,
            ),
            first_summary,
        )
        # mark_job_completed was stubbed, so the job is left open and unresolved.
        self.assertEqual(
            _ExtractionJobSnapshot(result_type=None, is_completed=False),
            self._extraction_job_snapshot(),
        )

        second_summary = self._run(self._fake_client())

        # Nothing left to process, but the resumed job is now completed.
        self.assertIsNone(second_summary)
        self.assertEqual(
            _ExtractionJobSnapshot(
                result_type=LLMExtractionJobResultType.SUCCESS.value,
                is_completed=True,
            ),
            self._extraction_job_snapshot(),
        )

    def test_job_failure_marks_job_failed(self) -> None:
        self._seed_gcs(_DOC_A, _DOCUMENT_TEXT)
        self._seed_gcs(_DOC_B, _DOCUMENT_TEXT)

        with mock.patch.object(
            LLMExtractionResultsPersister,
            "persist_results",
            side_effect=RuntimeError("BQ is down"),
        ):
            with self.assertRaises(RuntimeError):
                self._run(self._fake_client())

        self.assertEqual(
            _ExtractionJobSnapshot(
                result_type=LLMExtractionJobResultType.FAILURE.value,
                is_completed=True,
            ),
            self._extraction_job_snapshot(),
        )

    def test_exceeding_max_documents_marks_job_failed(self) -> None:
        # Two eligible documents but a ceiling of one, so the run refuses to do
        # any LLM work. The ceiling check runs inside the job lifecycle, so the
        # job is marked failed (not left open) and its documents stay unmarked.
        self._seed_gcs(_DOC_A, _DOCUMENT_TEXT)
        self._seed_gcs(_DOC_B, _DOCUMENT_TEXT)

        with (
            mock.patch.object(sandbox_extraction_runners, "MAX_DOCUMENTS", 1),
            self.assertRaisesRegex(ValueError, r"above the MAX_DOCUMENTS ceiling"),
        ):
            self._run(self._fake_client())

        self.assertEqual(
            _ExtractionJobSnapshot(
                result_type=LLMExtractionJobResultType.FAILURE.value,
                is_completed=True,
            ),
            self._extraction_job_snapshot(),
        )
        # No documents were processed, so they stay unmarked for re-selection.
        self.assertEqual(
            {_DOC_A: _UNMARKED_DOCUMENT, _DOC_B: _UNMARKED_DOCUMENT},
            self._extraction_job_document_by_id(),
        )


_RAW_INPUT_NOTES_ADDRESS = BigQueryAddress(
    dataset_id="us_xx_raw_data", table_id="fake_input_notes"
)
_RAW_INPUT_NOTES_SCHEMA = [
    bigquery.SchemaField("person_id", SqlTypeNames.STRING.value),
    bigquery.SchemaField("note_id", SqlTypeNames.STRING.value),
    bigquery.SchemaField("note_body", SqlTypeNames.STRING.value),
    bigquery.SchemaField("created_at", SqlTypeNames.STRING.value),
]


class _FakeGcsCsvLoader:
    """Stand-in for BigQueryClientImpl.load_table_from_cloud_storage that reads the
    upload-status CSVs the uploader wrote to a FakeGCSFileSystem and streams them into
    the destination emulator table. The emulator cannot load from a gs:// URI backed by
    the fake filesystem, so this replaces only that GCS→BQ transport step; every other
    query in the recorder runs for real against the emulator."""

    def __init__(self, *, fs: FakeGCSFileSystem, bq_client: Any) -> None:
        self.fs = fs
        self.bq_client = bq_client

    def __call__(
        self,
        *,
        source_uris: list[str],
        destination_address: BigQueryAddress,
        destination_table_schema: list[bigquery.SchemaField],
        **_kwargs: Any,
    ) -> mock.MagicMock:
        column_names = [field.name for field in destination_table_schema]
        rows = [
            dict(zip(column_names, values))
            for uri in source_uris
            for values in self._read_csv_rows(uri)
        ]
        if rows:
            self.bq_client.stream_into_table(destination_address, rows=rows)
        return mock.MagicMock()

    def _read_csv_rows(self, source_uri: str) -> list[list[str | None]]:
        # source_uri is a "<directory>/*.csv" glob; read every CSV the uploader wrote
        # under that directory out of the fake filesystem.
        directory = GcsfsDirectoryPath.from_absolute_path(source_uri.rsplit("/", 1)[0])
        rows: list[list[str | None]] = []
        for path in self.fs.ls(
            directory.bucket_name, blob_prefix=directory.relative_path
        ):
            if not isinstance(path, GcsfsFilePath) or not path.abs_path().endswith(
                ".csv"
            ):
                continue
            contents = self.fs.download_as_string(path)
            rows.extend(
                [value or None for value in row]
                for row in csv.reader(io.StringIO(contents))
            )
        return rows


class SandboxDocumentStoreRunnerTest(BigQueryEmulatorTestCase):
    """Runs SandboxDocumentStoreRunner's discovery → batch → GCS upload → record
    pipeline end-to-end against the BQ emulator and a fake GCS for a first-order
    collection, asserting on the metadata/contents rows and GCS text it writes.

    TODO(OBT-42814): there is no end-to-end entity-resolution case here. The ER
    composite generation writes its entry→source map via a CREATE TABLE AS SELECT that
    emits an ARRAY<STRUCT<...TIMESTAMP...>>, which the bigquery-emulator cannot
    round-trip (it fails scanning the stored value). Add the ER case once that emulator
    bug is fixed; the ER pass's control flow is covered meanwhile by
    RunEntityResolutionPassTest.
    """

    # The source tables are created once in setUpClass; keep them across tests and only
    # clear their rows between tests.
    wipe_emulator_data_on_teardown = False

    _RUN_ID = "sandbox_test_prefix"

    @classmethod
    def get_source_tables(cls) -> list[SourceTableCollection]:
        # The ER model config patch is needed only so the ER composite collections'
        # document-store tables can be collected; those tables are created but unused by
        # the first-order test below.
        with patch_fake_entity_resolution_model_config_name():
            document_store_collections = collect_document_store_source_tables(
                fake_config
            )
        # The runner writes every collection's document-store tables under the sandbox
        # prefix; a first-order collection additionally diffs discovery against the
        # production tables. Seed production + sandbox copies of the doc-store tables,
        # plus the raw input table the first-order generation query reads.
        raw_input = SourceTableCollection(
            dataset_id=_RAW_INPUT_NOTES_ADDRESS.dataset_id,
            update_config=SourceTableCollectionUpdateConfig.regenerable(),
            description="Raw input table the fake first-order generation query reads.",
        )
        raw_input.add_source_table(
            table_id=_RAW_INPUT_NOTES_ADDRESS.table_id,
            schema_fields=_RAW_INPUT_NOTES_SCHEMA,
        )
        return [
            *document_store_collections,
            *[
                collection.as_sandbox_collection(_SANDBOX_PREFIX)
                for collection in document_store_collections
            ],
            raw_input,
        ]

    def setUp(self) -> None:
        super().setUp()
        self.fs = FakeGCSFileSystem()
        # SandboxDocumentStoreRunner delegates to NewDocumentDiscoverer /
        # DocumentUploadResultRecorder, which re-resolve the collection config from the
        # production config module; point that resolution at the fake module.
        self.config_module_patcher = mock.patch(
            "recidiviz.documents.store.document_collection_config_collectors."
            "default_config_module",
            fake_config,
        )
        self.config_module_patcher.start()
        # The recorder loads upload-status CSVs from GCS into BQ; the emulator can't read
        # a gs:// URI backed by the fake filesystem, so replace only that transport step.
        self.load_csvs_patcher = mock.patch.object(
            self.bq_client,
            "load_table_from_cloud_storage",
            _FakeGcsCsvLoader(fs=self.fs, bq_client=self.bq_client),
        )
        self.load_csvs_patcher.start()

    def tearDown(self) -> None:
        self.load_csvs_patcher.stop()
        self.config_module_patcher.stop()
        self._clear_emulator_table_data()
        super().tearDown()

    def _first_order_collection(self) -> DocumentCollectionConfig:
        return get_document_collection_config(
            _STATE_CODE, _INPUT_COLLECTION_NAME, fake_config
        )

    def _first_order_sandbox(self) -> DocumentStoreSandboxContext:
        # A first-order collection writes its outputs to the sandbox but diffs discovery
        # against production (diff_read_prefix=None), so a brand-new document is
        # discovered against the empty production store and seeded into the sandbox.
        return DocumentStoreSandboxContext(
            document_collection_locations={
                _INPUT_COLLECTION_NAME: DocumentCollectionSandboxLocation(
                    output_prefix=_SANDBOX_PREFIX, diff_read_prefix=None
                )
            },
            extractor_collection_read_prefixes={},
        )

    def _run(
        self,
        *,
        document_collection: DocumentCollectionConfig,
        document_store_sandbox: DocumentStoreSandboxContext,
    ) -> None:
        SandboxDocumentStoreRunner(
            document_collection=document_collection,
            document_store_sandbox=document_store_sandbox,
            bq_client=self.bq_client,
            fs=self.fs,
            run_id=self._RUN_ID,
        ).run()

    def _table_rows(
        self, address: BigQueryAddress, columns: list[str]
    ) -> list[dict[str, Any]]:
        project_specific = address.to_project_specific_address(self.project_id)
        df = self.query(
            project_specific.select_query(
                select_statement=f"SELECT {', '.join(columns)}"
            )
        )
        return df.sort_values(by=columns).to_dict("records")

    def _gcs_text_by_contents_id(
        self, collection: DocumentCollectionConfig, contents_ids: list[str]
    ) -> dict[str, str]:
        return {
            contents_id: self.fs.download_as_string(
                gcs_path_for_document(
                    project_id=self.project_id,
                    state_code=_STATE_CODE,
                    collection_name=collection.name,
                    document_contents_id=contents_id,
                    sandbox_prefix=_SANDBOX_PREFIX,
                )
            )
            for contents_id in contents_ids
        }

    def _seed_raw_input_notes(self, rows: list[dict[str, Any]]) -> None:
        self.load_rows_into_table(_RAW_INPUT_NOTES_ADDRESS, rows)

    def test_first_order_generates_uploads_and_records(self) -> None:
        collection = self._first_order_collection()
        self._seed_raw_input_notes(
            [
                {
                    "person_id": "111",
                    "note_id": "N1",
                    "note_body": "first note body",
                    "created_at": "2026-01-01 10:00:00",
                },
                {
                    "person_id": "222",
                    "note_id": "N2",
                    "note_body": "second note body",
                    "created_at": "2026-02-01 10:00:00",
                },
            ]
        )

        self._run(
            document_collection=collection,
            document_store_sandbox=self._first_order_sandbox(),
        )

        # Both generated documents land in the sandbox metadata and contents tables under
        # a content-addressed document_contents_id, and their text is uploaded to GCS at
        # the sandbox path.
        metadata_rows = self._table_rows(
            collection.metadata_table_address(sandbox_dataset_prefix=_SANDBOX_PREFIX),
            ["person_external_id", "note_id"],
        )
        self.assertEqual(
            [
                {"person_external_id": "111", "note_id": "N1"},
                {"person_external_id": "222", "note_id": "N2"},
            ],
            metadata_rows,
        )
        contents_rows = self._table_rows(
            collection.document_contents_table_address(
                sandbox_dataset_prefix=_SANDBOX_PREFIX
            ),
            [DOCUMENT_CONTENTS_ID_COLUMN_NAME],
        )
        contents_ids = [row[DOCUMENT_CONTENTS_ID_COLUMN_NAME] for row in contents_rows]
        self.assertEqual(2, len(contents_ids))
        self.assertEqual(
            {"first note body", "second note body"},
            set(self._gcs_text_by_contents_id(collection, contents_ids).values()),
        )

    def test_no_documents_discovered_writes_nothing(self) -> None:
        collection = self._first_order_collection()
        # No raw rows, so the generation query produces nothing and discovery returns
        # before uploading or recording.
        self._run(
            document_collection=collection,
            document_store_sandbox=self._first_order_sandbox(),
        )

        self.assertEqual(
            [],
            self._table_rows(
                collection.metadata_table_address(
                    sandbox_dataset_prefix=_SANDBOX_PREFIX
                ),
                ["person_external_id", "note_id"],
            ),
        )
        self.assertEqual([], self.fs.all_paths)
