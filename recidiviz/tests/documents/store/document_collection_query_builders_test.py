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
"""Tests for document_collection_query_builders.py."""
import unittest
from pathlib import Path

import pandas as pd
from freezegun import freeze_time
from google.cloud.bigquery import SchemaField
from google.cloud.bigquery.enums import SqlTypeNames

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
)
from recidiviz.documents.store.document_collection_config_collectors import (
    collect_all_document_collection_configs,
    get_document_collection_config,
)
from recidiviz.documents.store.document_collection_query_builders import (
    DocumentCollectionDiffQueryBuilder,
    DocumentCollectionGenerationQueryBuilder,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.big_query.sqlglot_helpers import check_query_selects_output_columns
from recidiviz.tests.documents import fake_config as fake_config_module
from recidiviz.tests.documents.extraction.entity_resolution.entity_resolution_test_utils import (
    FAKE_ASSIGNMENT_ER_COLLECTION_NAME,
    patch_fake_entity_resolution_model_config_name,
)
from recidiviz.tests.documents.store.fixtures import document_diff


class TestBuildDocumentGenerationQuery(unittest.TestCase):
    def test_generation_query_output_matches_temp_table_schema(self) -> None:
        for state_code in StateCode:
            configs = collect_all_document_collection_configs(state_code)
            for config in configs.values():
                expected_columns = {
                    field.name
                    for field in config.build_bq_document_generation_output_schema()
                }
                query = DocumentCollectionGenerationQueryBuilder(
                    project_id="test-project",
                ).build_document_generation_query(
                    config=config,
                )
                try:
                    check_query_selects_output_columns(
                        query=query,
                        expected_output_columns=expected_columns,
                    )
                except ValueError as e:
                    raise ValueError(
                        f"Query output column mismatch for "
                        f"[{state_code.value}/{config.name}]"
                    ) from e


class TestBuildDocumentDiffQuery(BigQueryEmulatorTestCase):
    """Tests for build_document_diff_query."""

    def setUp(self) -> None:
        super().setUp()
        self.config = get_document_collection_config(
            StateCode.US_XX, "FAKE_CASE_NOTES", fake_config_module
        )
        self.metadata_address = self.config.metadata_table_address(
            sandbox_dataset_prefix=None
        ).to_project_specific_address(self.project_id)
        self.raw_table_address = BigQueryAddress(
            dataset_id="us_xx_raw_data",
            table_id="fake_notes",
        )
        self.generation_query_builder = DocumentCollectionGenerationQueryBuilder(
            project_id=self.project_id,
        )
        self.query_builder = DocumentCollectionDiffQueryBuilder(
            project_id=self.project_id,
        )
        self.fixture_dir = Path(document_diff.__file__).parent

    def _fixture_path(self, fixture_name: str) -> Path:
        return self.fixture_dir / f"{fixture_name}.csv"

    def _load_raw_table(self, fixture_path: Path) -> None:
        self.load_fixture_into_table(
            address=self.raw_table_address,
            schema=[
                SchemaField("person_id", SqlTypeNames.STRING),
                SchemaField("note_id", SqlTypeNames.STRING),
                SchemaField("note_type", SqlTypeNames.STRING),
                SchemaField("note_body", SqlTypeNames.STRING),
                SchemaField("created_at", SqlTypeNames.STRING),
            ],
            fixture_path=fixture_path,
            fixture_columns=None,
            allow_comments=False,
        )

    def _load_metadata(
        self,
        config: DocumentCollectionConfig,
        fixture_path: Path,
    ) -> None:
        self.load_fixture_into_table(
            address=self.metadata_address.to_project_agnostic_address(),
            schema=config.build_bq_metadata_schema(),
            fixture_path=fixture_path,
            fixture_columns=None,
            allow_comments=False,
        )

    def test_document_diff(self) -> None:
        """Fixture covers:
        - NOTE_1: same content and metadata in both, excluded from diff
        - NOTE_2: content differs between new and current (updated)
        - NOTE_3: exists in new but not current (added)
        - NOTE_5: exists in current but not new (deleted, NULL content)
        - NOTE_6: same content but note_type changed (metadata-only update)
        - NOTE_8: NULL document_text in source, filtered out of generation
          query so it never enters the diff
        """
        self._load_raw_table(self._fixture_path("fake_notes_latest"))
        self._load_metadata(self.config, self._fixture_path("fake_case_notes_metadata"))

        document_generation_output_address = (
            self.config.temp_document_generation_output_table_address(
                "test_run_id"
            ).to_project_specific_address(self.project_id)
        )
        with freeze_time("2026-03-15 12:00:00", tz_offset=0):
            generation_query = (
                self.generation_query_builder.build_document_generation_query(
                    self.config
                )
            )
            query = self.query_builder.build_document_diff_query(
                config=self.config,
                document_generation_output_address=document_generation_output_address,
            )
        self.bq_client.create_dataset_if_necessary(
            document_generation_output_address.dataset_id
        )
        self.bq_client.create_table_from_query(
            address=document_generation_output_address.to_project_agnostic_address(),
            query=generation_query,
            use_query_cache=False,
            overwrite=True,
        )
        results = self.query(query)

        self.compare_results_to_fixture(
            results=results,
            expected_output_fixture_path=self._fixture_path(
                "fake_case_notes_diff_output"
            ),
            expect_missing_fixtures_on_empty_results=False,
            create_expected=False,
            expect_unique_output_rows=False,
        )


class TestBuildDocumentDiffQueryEntityResolution(BigQueryEmulatorTestCase):
    """Tests the diff query over a materialized generation output for an
    entity-resolution composite collection, whose REPEATED RECORD
    entry_source_map column must survive the table read — including the
    deleted-documents branch's typed empty-array cast.

    The BQ emulator cannot round-trip ARRAY<STRUCT<TIMESTAMP>> columns through
    a CREATE TABLE AS SELECT write (and cannot stream them to the client at
    all), so this test seeds the generation-output table with streamed rows
    rather than running the real generation query, and reads the diff through
    wrapper queries (scalar columns + ARRAY_LENGTH, and UNNEST) instead of
    materializing it.
    TODO(OBT-42814): Once the emulator bug is fixed, materialize the
    generation output through the real generation query and the diff through a
    real temp-table write.
    """

    def setUp(self) -> None:
        super().setUp()
        self.enterContext(patch_fake_entity_resolution_model_config_name())
        self.er_collection = get_document_collection_config(
            StateCode.US_XX, FAKE_ASSIGNMENT_ER_COLLECTION_NAME, fake_config_module
        )
        self.generation_output_address = (
            self.er_collection.temp_document_generation_output_table_address(
                "run_1"
            ).to_project_specific_address(self.project_id)
        )
        self.create_mock_table(
            self.generation_output_address.to_project_agnostic_address(),
            schema=self.er_collection.build_bq_document_generation_output_schema(),
        )
        self.create_mock_table(
            self.er_collection.metadata_table_address(sandbox_dataset_prefix=None),
            schema=self.er_collection.build_bq_metadata_schema(),
        )

    def _diff_query(self) -> str:
        return DocumentCollectionDiffQueryBuilder(
            project_id=self.project_id
        ).build_document_diff_query(
            config=self.er_collection,
            document_generation_output_address=self.generation_output_address,
        )

    def test_er_document_diff(self) -> None:
        """Covers, for the assignment ER composite collection:
        - person 1001: composite exists and its stored metadata hash is stale
          (updated)
        - person 1002: composite exists with no metadata row (added)
        - person 1003: metadata row exists but no generation output (deleted,
          NULL contents and an empty entry_source_map)
        """
        self.load_rows_into_table(
            self.generation_output_address.to_project_agnostic_address(),
            [
                {
                    "person_id": 1001,
                    "document_contents_id": "composite_hash_1001_v2",
                    "document_text": "composite text for person 1001",
                    "document_update_datetime": "2026-01-15T00:00:00Z",
                    "entry_source_map": [
                        {
                            "entry_num": 1,
                            "source_document_contents_id": "doc_aaa",
                            "source_document_update_datetime": "2026-01-15T00:00:00Z",
                            "source_array_index": 0,
                        },
                        {
                            "entry_num": 2,
                            "source_document_contents_id": "doc_aaa",
                            "source_document_update_datetime": "2026-01-15T00:00:00Z",
                            "source_array_index": 1,
                        },
                    ],
                },
                {
                    "person_id": 1002,
                    "document_contents_id": "composite_hash_1002",
                    "document_text": "composite text for person 1002",
                    "document_update_datetime": "2026-01-20T00:00:00Z",
                    "entry_source_map": [
                        {
                            "entry_num": 1,
                            "source_document_contents_id": "doc_bbb",
                            "source_document_update_datetime": "2026-01-20T00:00:00Z",
                            "source_array_index": 0,
                        },
                    ],
                },
            ],
        )
        self.load_rows_into_table(
            self.er_collection.metadata_table_address(sandbox_dataset_prefix=None),
            [
                {
                    "person_id": 1001,
                    "document_contents_id": "composite_hash_1001_v1",
                    "document_update_datetime": "2026-01-01T00:00:00Z",
                    "row_create_datetime": "2026-01-02T00:00:00Z",
                },
                {
                    "person_id": 1003,
                    "document_contents_id": "composite_hash_1003",
                    "document_update_datetime": "2026-01-01T00:00:00Z",
                    "row_create_datetime": "2026-01-02T00:00:00Z",
                },
            ],
        )

        scalar_rows = self.query(
            "SELECT person_id, document_contents_id, document_text, "
            "document_update_datetime, ARRAY_LENGTH(entry_source_map) AS num_entries "
            f"FROM ({self._diff_query()}) ORDER BY person_id"
        ).to_dict("records")
        self.assertEqual(
            [
                {
                    "person_id": 1001,
                    "document_contents_id": "composite_hash_1001_v2",
                    "document_text": "composite text for person 1001",
                    "document_update_datetime": pd.Timestamp("2026-01-15T00:00:00Z"),
                    "num_entries": 2,
                },
                {
                    "person_id": 1002,
                    "document_contents_id": "composite_hash_1002",
                    "document_text": "composite text for person 1002",
                    "document_update_datetime": pd.Timestamp("2026-01-20T00:00:00Z"),
                    "num_entries": 1,
                },
                {
                    "person_id": 1003,
                    "document_contents_id": None,
                    "document_text": None,
                    "document_update_datetime": pd.NaT,
                    # The deleted stub's typed NULL-array cast reads as NULL
                    # in-query (ARRAY_LENGTH(NULL) is NULL); it only becomes a
                    # stored empty array when the diff is written to the temp
                    # table, which the emulator cannot round-trip for
                    # ARRAY<STRUCT<TIMESTAMP>> columns.
                    # TODO(OBT-42814): Assert against the stored (empty array)
                    # form once the emulator bug is fixed.
                    "num_entries": None,
                },
            ],
            scalar_rows,
        )

        map_rows = self.query(
            "SELECT t.person_id, e.entry_num, e.source_document_contents_id, "
            "e.source_document_update_datetime, e.source_array_index "
            f"FROM ({self._diff_query()}) t, UNNEST(t.entry_source_map) AS e "
            "ORDER BY t.person_id, e.entry_num"
        ).to_dict("records")
        self.assertEqual(
            [
                {
                    "person_id": 1001,
                    "entry_num": 1,
                    "source_document_contents_id": "doc_aaa",
                    "source_document_update_datetime": pd.Timestamp(
                        "2026-01-15T00:00:00Z"
                    ),
                    "source_array_index": 0,
                },
                {
                    "person_id": 1001,
                    "entry_num": 2,
                    "source_document_contents_id": "doc_aaa",
                    "source_document_update_datetime": pd.Timestamp(
                        "2026-01-15T00:00:00Z"
                    ),
                    "source_array_index": 1,
                },
                {
                    "person_id": 1002,
                    "entry_num": 1,
                    "source_document_contents_id": "doc_bbb",
                    "source_document_update_datetime": pd.Timestamp(
                        "2026-01-20T00:00:00Z"
                    ),
                    "source_array_index": 0,
                },
            ],
            map_rows,
        )
