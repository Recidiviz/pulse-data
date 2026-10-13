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
from pathlib import Path
from typing import Any
from unittest import mock

from google.cloud import bigquery
from google.cloud.bigquery.enums import SqlTypeNames

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.cloud_resources.resource_label import ResourceLabel
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.document_extraction_processor import (
    DocumentExtractionProcessor,
    ExtractionRunSummary,
)
from recidiviz.documents.extraction.llm_client.types import (
    LLMClientDocumentExtractionResult,
    LLMDocumentExtractionRequest,
    LLMDocumentExtractionTokenCounts,
)
from recidiviz.documents.extraction.llm_extraction_job_manager import (
    LLMExtractionJobManager,
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
    SourceTableUpdateGroup,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.big_query.big_query_emulator_with_gcs_test_case import (
    BigQueryEmulatorWithGCSTestCase,
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
from recidiviz.tests.documents.store.document_store_test_utils import (
    patch_fake_config_states_exist_in_env,
)
from recidiviz.tests.ingest.direct.fixture_util import load_dataframe_from_path
from recidiviz.tests.tools.documents import fixtures
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


# The canned success the fake LLM client returns, paired with the document text
# seeded into GCS for the documents it is returned for. Validation checks a
# result's citations against the document it was extracted from, so the two have
# to be built together for a result to pass as usable.
_GROUNDED_SUCCESS_RESULT = ground_citations_in_fake_source_text(
    fake_minimal_relevant_result_json()
)


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


class RunSandboxExtractionTest(BigQueryEmulatorTestCase):
    """Verifies SandboxExtractionRunner's job generation — that an empty eligible-
    document set creates no job — against a real Postgres and the BQ emulator."""

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
        with patch_fake_config_states_exist_in_env():
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

    def _run(
        self,
        sync_client: FakeSyncLLMClient,
        persist_chunk_size: int = 1000,
        request_build_concurrency: int = 4,
        progress_log_interval_seconds: float = 0.0,
    ) -> ExtractionRunSummary | None:
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
                labels=[ResourceLabel(key="reason", value="test")],
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


_RAW_INPUT_NOTES_ADDRESS = BigQueryAddress(
    dataset_id="us_xx_raw_data", table_id="fake_input_notes"
)
_RAW_INPUT_NOTES_SCHEMA = [
    bigquery.SchemaField("person_id", SqlTypeNames.STRING.value),
    bigquery.SchemaField("note_id", SqlTypeNames.STRING.value),
    bigquery.SchemaField("note_body", SqlTypeNames.STRING.value),
    bigquery.SchemaField("created_at", SqlTypeNames.STRING.value),
]


class SandboxDocumentStoreRunnerTest(BigQueryEmulatorWithGCSTestCase):
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
        with (
            patch_fake_entity_resolution_model_config_name(),
            patch_fake_config_states_exist_in_env(),
        ):
            document_store_collections = collect_document_store_source_tables(
                fake_config
            )
        # The runner writes every collection's document-store tables under the sandbox
        # prefix; a first-order collection additionally diffs discovery against the
        # production tables. Seed production + sandbox copies of the doc-store tables,
        # plus the raw input table the first-order generation query reads.
        raw_input = SourceTableCollection(
            update_groups={SourceTableUpdateGroup.CALC},
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
        # SandboxDocumentStoreRunner delegates to NewDocumentDiscoverer /
        # DocumentUploadResultRecorder, which re-resolve the collection config from the
        # production config module; point that resolution at the fake module.
        self.config_module_patcher = mock.patch(
            "recidiviz.documents.store.document_collection_config_collectors."
            "default_config_module",
            fake_config,
        )
        self.config_module_patcher.start()

    def tearDown(self) -> None:
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
