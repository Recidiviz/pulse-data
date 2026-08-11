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
"""Tests for the run_sandbox_extraction orchestration script"""

import threading
from pathlib import Path
from typing import Any
from unittest import mock

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
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
from recidiviz.documents.store.document_store_gcs_path_utils import (
    gcs_path_for_document,
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
from recidiviz.source_tables.source_table_config import SourceTableCollection
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tests.documents import fake_config
from recidiviz.tests.documents.extraction.fake_extractor_result_json import (
    fake_minimal_relevant_result_json,
)
from recidiviz.tests.documents.extraction.llm_client.fake_sync_llm_client import (
    FakeSyncLLMClient,
)
from recidiviz.tests.ingest.direct.fixture_util import load_dataframe_from_path
from recidiviz.tests.tools.documents import fixtures
from recidiviz.tools.documents import run_sandbox_extraction
from recidiviz.tools.documents.run_sandbox_extraction import (
    SandboxExtractionRunner,
    SandboxExtractionSummary,
)
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult

_STATE_CODE = StateCode.US_XX
_COLLECTION_NAME = "FAKE_EXTRACTOR_COLLECTION"
_INPUT_COLLECTION_NAME = "FAKE_INPUT_NOTES"
_SANDBOX_PREFIX = "test_prefix"

_DOC_A = "CID_A"
_DOC_B = "CID_B"


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
        result_json=fake_minimal_relevant_result_json(),
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
            address=config.metadata_table_address,
            fixture_path=fixtures_dir / "fake_input_notes_metadata.csv",
        )
        self._load_rows_from_fixture(
            address=config.document_contents_table_address,
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
    ) -> SandboxExtractionSummary:
        # Drives just the extraction thread against the seeded result tables; the
        # table-create and view-deploy steps live in run_sandbox_extraction and are
        # covered by RunSandboxExtractionOrchestrationTest.
        return SandboxExtractionRunner(
            config=self.config.with_sandbox_narrowing(
                document_limit=5, root_entity_ids=None, external_id_type=None
            ),
            sandbox_prefix=_SANDBOX_PREFIX,
            labels={"reason": "test"},
            bq_client=self.bq_client,
            fs=self.fs,
            sync_client=sync_client,
            persist_chunk_size=persist_chunk_size,
            request_build_concurrency=request_build_concurrency,
            progress_log_interval_seconds=progress_log_interval_seconds,
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
        self._seed_gcs(_DOC_A, "some case note text")
        self._seed_gcs(_DOC_B, "another case note")

        summary = self._run(self._fake_client())

        # Both documents succeed, so their token counts are summed across the run.
        self.assertEqual(
            SandboxExtractionSummary(
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

        self._seed_gcs(_DOC_A, "some case note text")
        self._seed_gcs(_DOC_B, "another case note")

        with mock.patch.object(self.fs, "download_as_string", download_as_string):
            summary = self._run(self._fake_client(), request_build_concurrency=2)

        self.assertEqual(
            SandboxExtractionSummary(
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
        self._seed_gcs(_DOC_A, "some case note text")
        self._seed_gcs(_DOC_B, "another case note")

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
            SandboxExtractionSummary(processed=2, failed_llm_request=2),
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
        self._seed_gcs(_DOC_A, "some case note text")
        self._seed_gcs(_DOC_B, "another case note")

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
        self._seed_gcs(_DOC_A, "some case note text")

        summary = self._run(self._fake_client())

        # doc-a processed and succeeded (contributing its token counts); doc-b
        # could not be assembled into a request.
        self.assertEqual(
            SandboxExtractionSummary(
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
        self._seed_gcs(_DOC_B, "another case note")

        summary = self._run(self._fake_client())

        # doc-a has empty text and is skipped before the LLM; doc-b processes and
        # succeeds, contributing its token counts.
        self.assertEqual(
            SandboxExtractionSummary(
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
            config.metadata_table_address,
            config.document_contents_table_address,
        ):
            self.bq_client.delete_from_table_async(address).result()

        summary = self._run(self._fake_client())

        self.assertEqual(SandboxExtractionSummary(), summary)
        with SessionFactory.using_database(self.database_key) as session:
            self.assertEqual(0, session.query(schema.LLMExtractionJob).count())

    def test_resumed_job_with_no_pending_documents_is_completed(self) -> None:
        # Simulate a crash between marking the last document and completing the
        # job: the first run processes every document (marking them all) but its
        # mark_job_completed is stubbed out, leaving an open job. The next run
        # resumes that job, finds nothing pending, and must complete it rather
        # than leave it open forever.
        self._seed_gcs(_DOC_A, "some case note text")
        self._seed_gcs(_DOC_B, "another case note")

        with mock.patch.object(LLMExtractionJobManager, "mark_job_completed"):
            first_summary = self._run(self._fake_client())
        self.assertEqual(
            SandboxExtractionSummary(
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
        self.assertEqual(SandboxExtractionSummary(), second_summary)
        self.assertEqual(
            _ExtractionJobSnapshot(
                result_type=LLMExtractionJobResultType.SUCCESS.value,
                is_completed=True,
            ),
            self._extraction_job_snapshot(),
        )

    def test_job_failure_marks_job_failed(self) -> None:
        self._seed_gcs(_DOC_A, "some case note text")
        self._seed_gcs(_DOC_B, "another case note")

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
        self._seed_gcs(_DOC_A, "some case note text")
        self._seed_gcs(_DOC_B, "another case note")

        with (
            mock.patch.object(run_sandbox_extraction, "MAX_DOCUMENTS", 1),
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
