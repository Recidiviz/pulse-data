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
"""End-to-end tests for run_sandbox_extraction: drives the real orchestration
against the BQ emulator and a local Postgres, faking only the Vertex client, the
GCS->BQ CSV load transport, and the one composite-generation step the emulator
cannot execute.
"""

import argparse
import copy
import datetime
from pathlib import Path
from typing import Any
from unittest import mock

from google.cloud import bigquery
from google.cloud.bigquery.enums import SqlTypeNames

from recidiviz.big_query.big_query_address import (
    BigQueryAddress,
    ProjectSpecificBigQueryAddress,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.entity_resolution.entity_resolution_document_collection_config import (
    EntityResolutionDocumentCollectionConfig,
)
from recidiviz.documents.extraction.entity_resolution.entity_resolution_entry_source_map_table import (
    EntityResolutionEntrySourceMapBQTable,
)
from recidiviz.documents.extraction.extraction_results_columns import (
    VALIDATION_DATETIME_UTC_COLUMN_NAME,
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
    load_first_order_llm_extractor_configs,
)
from recidiviz.documents.extraction.views.llm_extractor_array_level_results_view_builders import (
    LLMExtractorPreResolutionArrayFieldResultsViewBuilder,
)
from recidiviz.documents.extraction.views.llm_extractor_doc_level_results_view_builders import (
    LLMExtractorPreResolutionResultsViewBuilder,
)
from recidiviz.documents.extraction.views.llm_extractor_entities_view_builder import (
    LLMExtractorEntitiesViewBuilder,
)
from recidiviz.documents.extraction.views.llm_extractor_entity_mentions_view_builder import (
    LLMExtractorEntityMentionsViewBuilder,
)
from recidiviz.documents.store.document_store_gcs_path_utils import (
    gcs_path_for_document,
)
from recidiviz.documents.store.new_document_discovery import NewDocumentDiscoverer
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.activity import normalized_entities
from recidiviz.persistence.entity.activity.normalized_entities import (
    NormalizedStatePersonExternalId,
)
from recidiviz.persistence.entity.activity.normalized_entities_utils import (
    queryable_address_for_normalized_entity,
)
from recidiviz.persistence.entity.entities_bq_schema import (
    get_bq_schema_for_entity_table,
)
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.serialization import serialize_entity_into_json
from recidiviz.source_tables.collect_all_source_table_configs import (
    build_source_table_repository_for_collected_schemata,
)
from recidiviz.source_tables.document_store_source_table_collection import (
    collect_document_store_source_tables,
)
from recidiviz.source_tables.source_table_config import (
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
)
from recidiviz.tests.big_query.big_query_emulator_with_gcs_test_case import (
    BigQueryEmulatorWithGCSTestCase,
)
from recidiviz.tests.documents import fake_config
from recidiviz.tests.documents.extraction.entity_resolution.entity_resolution_test_utils import (
    FAKE_ASSIGNMENT_ER_COLLECTION_NAME,
    FAKE_LOCATION_ER_COLLECTION_NAME,
    FAKE_PAY_RATE_ER_COLLECTION_NAME,
    build_fake_composite_generation_output_row,
    build_fake_entry_source_map_entry,
    fake_first_order_extractor_config,
    patch_fake_entity_resolution_model_config_name,
)
from recidiviz.tests.documents.extraction.fake_extractor_result_json import (
    build_fake_entity_resolution_entity_result_json,
    build_fake_entity_resolution_result_content,
    build_fake_extractor_assignment_result_json,
    build_fake_extractor_result_content,
    ground_citations_in_fake_source_text,
    wrap_in_result_key,
)
from recidiviz.tests.documents.extraction.llm_client.fake_sync_llm_client import (
    FakeSyncLLMClient,
)
from recidiviz.tests.documents.extraction.views.fake_extractor_result_helpers import (
    FAKE_EXTRACTOR_COLLECTION,
    build_fake_extractor_person_external_id,
)
from recidiviz.tests.documents.store.document_store_test_utils import (
    FAKE_INPUT_DOCUMENT_COLLECTION_NAME,
    get_fake_first_order_document_collection_config,
)
from recidiviz.tests.test_setup_utils import BQ_EMULATOR_PROJECT_ID
from recidiviz.tests.tools.documents import fixtures
from recidiviz.tools.documents import run_sandbox_extraction
from recidiviz.tools.documents.sandbox_document_extraction_processor import (
    SandboxExtractionSummary,
)
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult
from recidiviz.utils.metadata import local_project_id_override

STATE_CODE = StateCode.US_XX
EXTRACTOR_COLLECTION_NAME = FAKE_EXTRACTOR_COLLECTION
SANDBOX_PREFIX = "test_prefix"

DOC_A = "CID_A"
DOC_B = "CID_B"

RAW_INPUT_NOTES_ADDRESS = BigQueryAddress(
    dataset_id="us_xx_raw_data", table_id="fake_input_notes"
)
RAW_INPUT_NOTES_SCHEMA = [
    bigquery.SchemaField("person_id", SqlTypeNames.STRING.value),
    bigquery.SchemaField("note_id", SqlTypeNames.STRING.value),
    bigquery.SchemaField("note_body", SqlTypeNames.STRING.value),
    bigquery.SchemaField("created_at", SqlTypeNames.STRING.value),
]

FIXTURES_DIR = Path(fixtures.__file__).parent


def fake_token_counts() -> LLMDocumentExtractionTokenCounts:
    return LLMDocumentExtractionTokenCounts(
        input_token_count=10,
        output_token_count=5,
        cached_input_token_count=2,
        thinking_token_count=0,
    )


# Internal person_id assigned to each external id in the fixture metadata (P1/P2
# are the two uploaded documents' people; P3's document was never uploaded).
_PERSON_ID_BY_EXTERNAL_ID = {"P1": 1001, "P2": 1002, "P3": 1003}
_PERSON_ID_A = _PERSON_ID_BY_EXTERNAL_ID["P1"]
_PERSON_ID_B = _PERSON_ID_BY_EXTERNAL_ID["P2"]

# The update datetimes the fixture metadata carries for each document; the
# composite entry->source map rows reference them so the entity-mentions view
# stitches each mention back to its source occurrence.
_DOC_A_UPDATE_DATETIME = datetime.datetime(
    2026, 1, 1, 10, 0, tzinfo=datetime.timezone.utc
)
_DOC_B_UPDATE_DATETIME = datetime.datetime(
    2026, 2, 1, 10, 0, tzinfo=datetime.timezone.utc
)

# Every module whose config loader defaults to the production config package and
# is reached by run_sandbox_extraction or the view-deploy path it drives. Each is
# redirected at the fake config module for the duration of a test.
_CONFIG_MODULE_ATTRS_TO_PATCH = [
    "recidiviz.documents.extraction.llm_extractor_config_collectors.default_config_module",
    "recidiviz.documents.extraction.models.llm_model_registry.default_config_module",
    "recidiviz.documents.extraction.models.llm_extractor_collection_config.default_config_module",
    "recidiviz.documents.extraction.models.reference_data.reference_data_registry.default_config_module",
    "recidiviz.documents.store.document_collection_config.default_config_module",
    "recidiviz.documents.store.document_collection_config_collectors.default_config_module",
]


# Distinct first-order extraction values per document, so every per-person
# row the run emits differs between the two people and a bug that crosses or
# collapses one person's data into the other's can't pass unnoticed.
_FIRST_ORDER_CONTENT_BY_DOC = {
    DOC_A: build_fake_extractor_result_content(
        primary_status="active",
        status_note="Currently active.",
        location="Kitchen",
        assignments=[
            build_fake_extractor_assignment_result_json(
                "Dish duty", "internal", 12.5, "hourly"
            ),
            build_fake_extractor_assignment_result_json(
                "Laundry", "internal", 9.0, "hourly"
            ),
        ],
    ),
    DOC_B: build_fake_extractor_result_content(
        primary_status="active",
        status_note="Active on the yard crew.",
        location="Cafeteria",
        assignments=[
            build_fake_extractor_assignment_result_json(
                "Yard crew", "external", 8.0, "monthly"
            ),
            build_fake_extractor_assignment_result_json(
                "Kitchen porter", "internal", 10.5, "hourly"
            ),
        ],
    ),
}

# The canned first-order success the fake LLM returns for each document,
# paired with the document text seeded into GCS. Validation checks a result's
# citations against its source document, so each result and its text are grounded
# together.
_GROUNDED_FIRST_ORDER_RESULT_BY_DOC = {
    document_contents_id: ground_citations_in_fake_source_text(
        wrap_in_result_key(content)
    )
    for document_contents_id, content in _FIRST_ORDER_CONTENT_BY_DOC.items()
}


def _person_external_id_address() -> BigQueryAddress:
    return queryable_address_for_normalized_entity(NormalizedStatePersonExternalId)


class RunSandboxExtractionTestBase(BigQueryEmulatorWithGCSTestCase):
    """Stands up the emulator + local Postgres environment run_sandbox_extraction
    needs and fakes the seams the emulator cannot execute (the Vertex client, the
    GCS->BQ CSV load transport, and the composite-generation materialization),
    exposing the seeding and assertion helpers the end-to-end tests build on."""

    wipe_emulator_data_on_teardown = False

    postgres_launch_result: OnDiskPostgresLaunchResult

    @classmethod
    def get_source_tables(cls) -> list[SourceTableCollection]:
        # Only the tables the run reads but never creates are seeded here: the
        # production document store the first-order input is read from, the raw
        # input the first-order generation query reads, and the person-external-id
        # the parsed views join. The run creates its own sandbox-prefixed result and
        # document-store tables, so pre-creating them would make its create step hit
        # the emulator's unsupported schema-update path.
        with patch_fake_entity_resolution_model_config_name():
            document_store_collections = collect_document_store_source_tables(
                fake_config
            )

        raw_input = SourceTableCollection(
            dataset_id=RAW_INPUT_NOTES_ADDRESS.dataset_id,
            update_config=SourceTableCollectionUpdateConfig.regenerable(),
            description="Raw input table the fake first-order generation query reads.",
        )
        raw_input.add_source_table(
            table_id=RAW_INPUT_NOTES_ADDRESS.table_id,
            schema_fields=RAW_INPUT_NOTES_SCHEMA,
        )

        person_external_id_address = _person_external_id_address()
        person_external_id = SourceTableCollection(
            dataset_id=person_external_id_address.dataset_id,
            update_config=SourceTableCollectionUpdateConfig.regenerable(),
            description="Person external ids the parsed views resolve person_id through.",
        )
        person_external_id.add_source_table(
            table_id=person_external_id_address.table_id,
            schema_fields=get_bq_schema_for_entity_table(
                normalized_entities, NormalizedStatePersonExternalId.get_table_id()
            ),
        )
        return [*document_store_collections, raw_input, person_external_id]

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

        self.enterContext(patch_fake_entity_resolution_model_config_name())
        self.enterContext(
            mock.patch.object(
                run_sandbox_extraction.GcsfsFactory, "build", return_value=self.fs
            )
        )
        self.enterContext(
            mock.patch.object(
                run_sandbox_extraction,
                "BigQueryClientImpl",
                return_value=self.bq_client,
            )
        )
        self.enterContext(
            mock.patch.object(
                run_sandbox_extraction,
                "VertexAISyncLLMClient",
                side_effect=self._fake_sync_client,
            )
        )
        # The requester label reads `git config user.name`, which is unset on CI.
        self.enterContext(
            mock.patch.object(
                run_sandbox_extraction,
                "get_normalized_git_username",
                return_value="test-user",
            )
        )
        self._point_config_resolution_at_fake_module()
        # The ER composite-document generation query emits an entry_source_map
        # ARRAY<STRUCT<...TIMESTAMP...>>, which the BQ emulator cannot round-trip
        # when written via CREATE TABLE AS SELECT (streamed rows work). Seed the
        # composite generation output for an ER collection with streamed rows
        # instead; a first-order collection runs the real materialization.
        # TODO(OBT-42814): drop this once the emulator supports the nested-timestamp
        # CTAS and let ER composite generation run for real here too.
        self._real_materialize = (
            # pylint: disable-next=protected-access
            NewDocumentDiscoverer._materialize_document_generation_output
        )
        self.enterContext(
            mock.patch.object(
                NewDocumentDiscoverer,
                "_materialize_document_generation_output",
                autospec=True,
                side_effect=self._materialize_document_generation_output,
            )
        )
        # Composite generation-output rows the seam streams per ER collection name,
        # and the clustering result the fake ER client returns per composite
        # document; both populated by a test before it runs the ER pass.
        self._composite_rows_by_collection: dict[str, list[dict[str, Any]]] = {}
        self._er_result_by_contents_id: dict[str, dict[str, Any]] = {}

    def tearDown(self) -> None:
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )
        self._clear_emulator_table_data()
        super().tearDown()

    def _point_config_resolution_at_fake_module(self) -> None:
        """Points every config loader that defaults to the production config package
        at the fake config module for the duration of the test.

        run_sandbox_extraction and the view-deploy path it drives both resolve
        extractor, document-collection, model-registry, and reference-data configs
        with no explicit module, so each of those loaders' default has to be
        redirected. The first-order config loader caches on its module argument, so
        clear it around the run to keep the fake configs from leaking into other
        tests.
        """
        for module_path in _CONFIG_MODULE_ATTRS_TO_PATCH:
            self.enterContext(mock.patch(module_path, fake_config))
        load_first_order_llm_extractor_configs.cache_clear()
        build_source_table_repository_for_collected_schemata.cache_clear()
        self.addCleanup(load_first_order_llm_extractor_configs.cache_clear)
        self.addCleanup(
            build_source_table_repository_for_collected_schemata.cache_clear
        )

    def _fake_sync_client(self, *, model_config: Any) -> FakeSyncLLMClient:
        return FakeSyncLLMClient(model_config=model_config, result_fn=self._result_fn)

    def _result_fn(
        self, request: LLMDocumentExtractionRequest
    ) -> LLMClientDocumentExtractionResult:
        """Returns the canned result for |request|, keyed on the document it extracts
        from: a composite document (its contents id is a seeded ER result) gets that
        clustering result, a first-order document its own grounded result."""
        result_json = (
            self._er_result_by_contents_id[request.document_contents_id]
            if request.document_contents_id in self._er_result_by_contents_id
            else copy.deepcopy(
                _GROUNDED_FIRST_ORDER_RESULT_BY_DOC[
                    request.document_contents_id
                ].result_json
            )
        )
        return LLMClientDocumentExtractionResult.from_success(
            document_contents_id=request.document_contents_id,
            result_json=result_json,
            token_counts=fake_token_counts(),
        )

    def _materialize_document_generation_output(
        self, discoverer: NewDocumentDiscoverer
    ) -> ProjectSpecificBigQueryAddress:
        config = discoverer.config
        if not isinstance(config, EntityResolutionDocumentCollectionConfig):
            return self._real_materialize(discoverer)

        address = config.temp_document_generation_output_table_address(
            discoverer.run_id
        )
        self.create_mock_table(
            address, schema=config.build_bq_document_generation_output_schema()
        )
        rows = self._composite_rows_by_collection.get(config.name, [])
        if rows:
            self.load_rows_into_table(address, rows)
        return address.to_project_specific_address(self.project_id)


# TODO(OBT-46375) Add more tests: partial failures, etc.
class RunSandboxExtractionTest(RunSandboxExtractionTestBase):
    """Drives run_sandbox_extraction end-to-end against the emulator and a local
    Postgres, asserting on the rows the deployed views emit."""

    def _args(
        self,
        *,
        document_limit: int | None = None,
    ) -> argparse.Namespace:
        return argparse.Namespace(
            state_code=STATE_CODE,
            collection=EXTRACTOR_COLLECTION_NAME,
            sandbox_prefix=SANDBOX_PREFIX,
            document_limit=document_limit,
            root_entity_ids=None,
            external_id_type=None,
            table_expiration_days=1,
            labels=[],
            pre_view_materialization_delay_minutes=0,
        )

    def _run(
        self, *, document_limit: int | None = None
    ) -> list[SandboxExtractionSummary]:
        # project_id() reads the metadata server, which is disabled under test; the
        # override points it at the emulator project the way main() does locally.
        with local_project_id_override(BQ_EMULATOR_PROJECT_ID):
            return run_sandbox_extraction.run_sandbox_extraction(
                self._args(document_limit=document_limit)
            )

    def _seed_person_external_ids(self) -> None:
        """Loads the person-external-id rows the parsed views resolve the fixture's
        external ids through, one internal person_id per fixture external id."""
        module_context = entities_module_context_for_module(normalized_entities)
        external_ids = [
            build_fake_extractor_person_external_id(
                external_id=external_id, id_type="US_XX_DOC", person_id=person_id
            )
            for external_id, person_id in _PERSON_ID_BY_EXTERNAL_ID.items()
        ]
        self.load_rows_into_table(
            _person_external_id_address(),
            [
                serialize_entity_into_json(external_id, module_context)
                for external_id in external_ids
            ],
        )

    def _seed_document_store_tables(self) -> None:
        """Loads the fake input collection's metadata and contents tables the
        eligible-document query reads, from the fixture CSVs, into their already-created
        production (un-prefixed) emulator tables."""
        config = get_fake_first_order_document_collection_config()
        self.load_fixture_into_existing_table(
            config.metadata_table_address(sandbox_dataset_prefix=None),
            FIXTURES_DIR / "fake_input_notes_metadata.csv",
            allow_comments=False,
        )
        self.load_fixture_into_existing_table(
            config.document_contents_table_address(sandbox_dataset_prefix=None),
            FIXTURES_DIR / "fake_input_notes_contents.csv",
            allow_comments=False,
        )

    def _seed_entity_resolution_composites(self) -> None:
        """Sets up the ER pass's inputs for every entity group: the composite
        generation-output rows the seam streams, and the clustering result the fake
        ER client returns for each composite.

        One composite per (group, person), carrying that person's own resolved entity
        values so the ER output differs person to person. The top-level location group
        renders one entry per person (source_array_index null); the array-sourced
        assignment and pay_rate groups render one entry per assignments-array element
        (the fake first-order result carries two). Each composite's mentions are
        clustered into a single entity, so every entry maps back to that entity.
        """
        people = [
            (
                _PERSON_ID_A,
                DOC_A,
                _DOC_A_UPDATE_DATETIME,
                {"location": "Kitchen"},
                {"assignment_name": "Dish duty", "assignment_type": "internal"},
                {"rate_amount": 12.5},
            ),
            (
                _PERSON_ID_B,
                DOC_B,
                _DOC_B_UPDATE_DATETIME,
                {"location": "Cafeteria"},
                {"assignment_name": "Yard crew", "assignment_type": "external"},
                {"rate_amount": 8.0},
            ),
        ]
        for (
            person_id,
            source_doc,
            source_datetime,
            location_fields,
            assignment_fields,
            pay_rate_fields,
        ) in people:
            self._add_composite_document(
                collection_name=FAKE_LOCATION_ER_COLLECTION_NAME,
                person_id=person_id,
                document_update_datetime=source_datetime,
                entries=[
                    build_fake_entry_source_map_entry(
                        entry_num=1,
                        source_document_contents_id=source_doc,
                        source_document_update_datetime=source_datetime,
                        source_array_index=None,
                    )
                ],
                entities=[
                    build_fake_entity_resolution_entity_result_json(
                        1, entry_nums=[1], **location_fields
                    )
                ],
            )
            self._add_composite_document(
                collection_name=FAKE_ASSIGNMENT_ER_COLLECTION_NAME,
                person_id=person_id,
                document_update_datetime=source_datetime,
                entries=[
                    build_fake_entry_source_map_entry(
                        entry_num=array_index + 1,
                        source_document_contents_id=source_doc,
                        source_document_update_datetime=source_datetime,
                        source_array_index=array_index,
                    )
                    for array_index in (0, 1)
                ],
                entities=[
                    build_fake_entity_resolution_entity_result_json(
                        1, entry_nums=[1, 2], **assignment_fields
                    )
                ],
            )
            self._add_composite_document(
                collection_name=FAKE_PAY_RATE_ER_COLLECTION_NAME,
                person_id=person_id,
                document_update_datetime=source_datetime,
                entries=[
                    build_fake_entry_source_map_entry(
                        entry_num=array_index + 1,
                        source_document_contents_id=source_doc,
                        source_document_update_datetime=source_datetime,
                        source_array_index=array_index,
                    )
                    for array_index in (0, 1)
                ],
                entities=[
                    build_fake_entity_resolution_entity_result_json(
                        1, entry_nums=[1, 2], **pay_rate_fields
                    )
                ],
            )

    def _add_composite_document(
        self,
        *,
        collection_name: str,
        person_id: int,
        document_update_datetime: datetime.datetime,
        entries: list[dict[str, Any]],
        entities: list[dict[str, Any]],
    ) -> None:
        document_contents_id = f"comp_{collection_name.lower()}_{person_id}"
        self._composite_rows_by_collection.setdefault(collection_name, []).append(
            build_fake_composite_generation_output_row(
                root_entity_id_column="person_id",
                root_entity_id=person_id,
                document_contents_id=document_contents_id,
                document_update_datetime=document_update_datetime,
                entries=entries,
            )
        )
        self._er_result_by_contents_id[
            document_contents_id
        ] = build_fake_entity_resolution_result_content(entities)

    def _seed_gcs(self, document_contents_id: str, text: str) -> None:
        self.fs.upload_from_string(
            path=gcs_path_for_document(
                project_id=self.project_id,
                state_code=STATE_CODE,
                collection_name=FAKE_INPUT_DOCUMENT_COLLECTION_NAME,
                document_contents_id=document_contents_id,
                sandbox_prefix=None,
            ),
            contents=text,
            content_type="text/plain",
        )

    def _assert_sandbox_table_matches_fixture(
        self,
        *,
        address: BigQueryAddress,
        fixture_name: str,
        columns_to_ignore: list[str] | None = None,
    ) -> None:
        """Asserts the full contents of the deployed sandbox table/view at |address|
        equal the CSV fixture named |fixture_name|, dropping any run-stamped
        |columns_to_ignore| whose values vary between runs."""
        self.compare_table_to_fixture(
            address=address,
            columns_to_ignore=columns_to_ignore or [],
            expected_output_fixture_path=FIXTURES_DIR
            / "run_sandbox_extraction"
            / fixture_name,
            expect_missing_fixtures_on_empty_results=False,
            create_expected=False,
            expect_unique_output_rows=True,
        )

    def _seed_gcs_for_documents(self) -> None:
        """Seeds each document's own grounded source text into GCS."""
        for (
            document_contents_id,
            grounded,
        ) in _GROUNDED_FIRST_ORDER_RESULT_BY_DOC.items():
            self._seed_gcs(document_contents_id, grounded.source_document_text)

    def test_first_order_and_entity_resolution_end_to_end(self) -> None:
        self._seed_document_store_tables()
        self._seed_person_external_ids()
        self._seed_entity_resolution_composites()
        self._seed_gcs_for_documents()

        summaries = self._run()

        # One first-order summary, then one per entity group's ER extraction; every
        # phase processed both root entities and succeeded.
        self.assertEqual(
            [
                (EXTRACTOR_COLLECTION_NAME, 2, 2),
                (FAKE_LOCATION_ER_COLLECTION_NAME, 2, 2),
                (FAKE_ASSIGNMENT_ER_COLLECTION_NAME, 2, 2),
                (FAKE_PAY_RATE_ER_COLLECTION_NAME, 2, 2),
            ],
            [(s.extractor_config_name, s.processed, s.succeeded) for s in summaries],
        )

        first_order_config = fake_first_order_extractor_config()
        collection = first_order_config.extractor_collection

        # The deployed first-order doc-level pre-resolution view carries one parsed row
        # per source document, resolved to person_id. The validation datetime is stamped
        # at run time, so it is excluded from the comparison.
        self._assert_sandbox_table_matches_fixture(
            # pylint: disable-next=protected-access
            address=LLMExtractorPreResolutionResultsViewBuilder._address_for_config(
                config=first_order_config, sandbox_dataset_prefix=SANDBOX_PREFIX
            ),
            fixture_name="pre_resolution/fake_extractor_collection.csv",
            columns_to_ignore=[VALIDATION_DATETIME_UTC_COLUMN_NAME],
        )

        # Each array-level pre-resolution view carries one parsed row per element of an
        # ARRAY_OF_STRUCT field an entity group is sourced from (the fake collection's is
        # `assignments`) — the same fields the deploy produces a pre-resolution view for.
        #
        # We assert against the view address, not its materialized table: these views
        # carry nested companion-metadata STRUCT columns, and the emulator cannot
        # round-trip that nested schema through the CREATE TABLE AS SELECT the deploy
        # materializes with, so the materialized table it produces here is unusable.
        # Reading the view re-runs the SELECT over the streamed result rows instead.
        # TODO(OBT-42814): assert against the materialized table once the emulator
        # supports the nested-schema CTAS.
        for array_field in collection.output_schema.array_of_struct_user_fields:
            if not collection.entity_groups_for_array_field(array_field):
                continue
            # pylint: disable-next=protected-access
            address = LLMExtractorPreResolutionArrayFieldResultsViewBuilder._address_for_array_field(
                config=first_order_config,
                array_field=array_field,
                sandbox_dataset_prefix=SANDBOX_PREFIX,
            )
            self._assert_sandbox_table_matches_fixture(
                address=address,
                fixture_name=f"pre_resolution/{address.table_id}.csv",
                columns_to_ignore=[VALIDATION_DATETIME_UTC_COLUMN_NAME],
            )

        for entity_group in collection.entity_groups:
            group = entity_group.name

            # The post-resolution entity-mentions view carries one row per resolved
            # mention, stitched back to its source occurrence via the entry->source
            # map — the assignment and pay_rate groups exercise the array-sourced path
            # (non-null source_array_index) the top-level location group does not.
            self._assert_sandbox_table_matches_fixture(
                address=LLMExtractorEntityMentionsViewBuilder.address_for_entity_group(
                    first_order_config=first_order_config,
                    entity_group=entity_group,
                    sandbox_dataset_prefix=SANDBOX_PREFIX,
                ),
                fixture_name=f"entity_mentions/{group}.csv",
            )

            # The entities view de-duplicates those mentions into one row per resolved
            # entity — the final ER output the whole run builds toward.
            self._assert_sandbox_table_matches_fixture(
                # pylint: disable-next=protected-access
                address=LLMExtractorEntitiesViewBuilder._address_for_entity_group(
                    first_order_config=first_order_config,
                    entity_group=entity_group,
                    sandbox_dataset_prefix=SANDBOX_PREFIX,
                ),
                fixture_name=f"entities/{group}.csv",
            )

            # The entry->source map table written back to the document store is the
            # join the mentions view stitches through; asserting it pins down that leg.
            self._assert_sandbox_table_matches_fixture(
                address=EntityResolutionEntrySourceMapBQTable.address(
                    state_code=first_order_config.state_code,
                    first_order_extractor_collection_name=collection.name,
                    entity_group_name=group,
                    sandbox_prefix=SANDBOX_PREFIX,
                ),
                fixture_name=f"entry_source_map/{group}.csv",
            )

    def test_document_limit_skips_entity_resolution(self) -> None:
        # A --document-limit run only makes sense for first-order extraction; the ER
        # pass is skipped because a truncated document set yields composites built
        # from partial mention sets. So the first-order summary comes back, no ER
        # summaries do, and no entity-mentions view is materialized.
        self._seed_document_store_tables()
        self._seed_person_external_ids()
        self._seed_gcs_for_documents()

        summaries = self._run(document_limit=1)

        self.assertEqual(
            [EXTRACTOR_COLLECTION_NAME], [s.extractor_config_name for s in summaries]
        )
        with SessionFactory.using_database(self.database_key) as session:
            self.assertEqual(1, session.query(schema.LLMExtractionJob).count())

    def test_no_eligible_documents_returns_empty(self) -> None:
        # No documents in the doc store, so the eligible-document query returns
        # nothing, no job is created, and the run exits with no summaries.
        self._seed_person_external_ids()

        summaries = self._run()

        self.assertEqual([], summaries)
        with SessionFactory.using_database(self.database_key) as session:
            self.assertEqual(0, session.query(schema.LLMExtractionJob).count())
