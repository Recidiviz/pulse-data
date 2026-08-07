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
"""Tests for entity_resolution_entry_source_map_query_builder.py."""
from typing import Any

import pandas as pd

from recidiviz.big_query.big_query_address import ProjectSpecificBigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.entity_resolution.entity_resolution_document_collection_config import (
    EntityResolutionDocumentCollectionConfig,
)
from recidiviz.documents.extraction.entity_resolution.entity_resolution_entry_source_map_query_builder import (
    EntityResolutionEntrySourceMapQueryBuilder,
)
from recidiviz.documents.store.document_collection_config_collectors import (
    get_document_collection_config,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.documents import fake_config as fake_config_module
from recidiviz.tests.documents.extraction.entity_resolution.entity_resolution_test_utils import (
    FAKE_ASSIGNMENT_ER_COLLECTION_NAME,
    FAKE_LOCATION_ER_COLLECTION_NAME,
    patch_fake_entity_resolution_model_config_name,
)
from recidiviz.utils.types import assert_type


class TestBuildMapReplacementQuery(BigQueryEmulatorTestCase):
    """Tests for build_entry_source_map_query, run over a hand-seeded
    generation-output table.

    The generation-output tables are seeded with streamed rows rather than by
    running the real composite generation query: the BQ emulator cannot
    round-trip an ARRAY<STRUCT<TIMESTAMP>> column written by a CREATE TABLE AS
    SELECT, while streamed rows scan fine. The query's job — flattening the
    composite's entry_source_map into one row per entry, keyed by root entity —
    is unaffected by how the input table was populated.
    TODO(OBT-42814): Seed via the real generation query once the emulator bug
    is fixed.
    """

    def setUp(self) -> None:
        super().setUp()
        self.enterContext(patch_fake_entity_resolution_model_config_name())

    def _seed_generation_output(
        self, collection_name: str, rows: list[dict[str, Any]]
    ) -> tuple[
        EntityResolutionDocumentCollectionConfig, ProjectSpecificBigQueryAddress
    ]:
        """Creates |collection_name|'s generation-output temp table holding
        exactly |rows|, returning the collection and the table's address.
        """
        er_collection = assert_type(
            get_document_collection_config(
                StateCode.US_XX, collection_name, fake_config_module
            ),
            EntityResolutionDocumentCollectionConfig,
        )
        address = er_collection.temp_document_generation_output_table_address(
            "run_1"
        ).to_project_specific_address(self.project_id)
        self.create_mock_table(
            address.to_project_agnostic_address(),
            schema=er_collection.build_bq_document_generation_output_schema(),
        )
        self.load_rows_into_table(address.to_project_agnostic_address(), rows)
        return er_collection, address

    def _replacement_rows(
        self,
        er_collection: EntityResolutionDocumentCollectionConfig,
        address: ProjectSpecificBigQueryAddress,
    ) -> list[dict[str, Any]]:
        query = EntityResolutionEntrySourceMapQueryBuilder(
            project_id=self.project_id,
            config=er_collection,
        ).build_entry_source_map_query(address)
        return self.query(f"{query} ORDER BY person_id, entry_num").to_dict("records")

    def test_array_entity_group(self) -> None:
        """Each composite entry becomes one map row: person 1001's composite
        renders two entries from two elements of one note's array, person
        1002's renders one."""
        er_collection, address = self._seed_generation_output(
            FAKE_ASSIGNMENT_ER_COLLECTION_NAME,
            [
                {
                    "person_id": 1001,
                    "document_contents_id": "composite_hash_1001",
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

        self.assertEqual(
            [
                {
                    "person_id": 1001,
                    "document_contents_id": "composite_hash_1001",
                    "entry_num": 1,
                    "source_document_contents_id": "doc_aaa",
                    "source_document_update_datetime": pd.Timestamp(
                        "2026-01-15T00:00:00Z"
                    ),
                    "source_array_index": 0,
                },
                {
                    "person_id": 1001,
                    "document_contents_id": "composite_hash_1001",
                    "entry_num": 2,
                    "source_document_contents_id": "doc_aaa",
                    "source_document_update_datetime": pd.Timestamp(
                        "2026-01-15T00:00:00Z"
                    ),
                    "source_array_index": 1,
                },
                {
                    "person_id": 1002,
                    "document_contents_id": "composite_hash_1002",
                    "entry_num": 1,
                    "source_document_contents_id": "doc_bbb",
                    "source_document_update_datetime": pd.Timestamp(
                        "2026-01-20T00:00:00Z"
                    ),
                    "source_array_index": 0,
                },
            ],
            self._replacement_rows(er_collection, address),
        )

    def test_top_level_entity_group(self) -> None:
        """A top-level group's entries carry no array index: source_array_index
        is NULL on every row."""
        er_collection, address = self._seed_generation_output(
            FAKE_LOCATION_ER_COLLECTION_NAME,
            [
                {
                    "person_id": 1001,
                    "document_contents_id": "composite_hash_1001",
                    "document_text": "composite text for person 1001",
                    "document_update_datetime": "2026-01-15T00:00:00Z",
                    "entry_source_map": [
                        {
                            "entry_num": 1,
                            "source_document_contents_id": "doc_aaa",
                            "source_document_update_datetime": "2026-01-10T00:00:00Z",
                            "source_array_index": None,
                        },
                        {
                            "entry_num": 2,
                            "source_document_contents_id": "doc_ccc",
                            "source_document_update_datetime": "2026-01-15T00:00:00Z",
                            "source_array_index": None,
                        },
                    ],
                },
            ],
        )

        self.assertEqual(
            [
                {
                    "person_id": 1001,
                    "document_contents_id": "composite_hash_1001",
                    "entry_num": 1,
                    "source_document_contents_id": "doc_aaa",
                    "source_document_update_datetime": pd.Timestamp(
                        "2026-01-10T00:00:00Z"
                    ),
                    "source_array_index": None,
                },
                {
                    "person_id": 1001,
                    "document_contents_id": "composite_hash_1001",
                    "entry_num": 2,
                    "source_document_contents_id": "doc_ccc",
                    "source_document_update_datetime": pd.Timestamp(
                        "2026-01-15T00:00:00Z"
                    ),
                    "source_array_index": None,
                },
            ],
            self._replacement_rows(er_collection, address),
        )

    def test_shared_composite_hash_keeps_row_sets_distinct(self) -> None:
        """Two root entities whose identically-worded notes produce the same
        composite hash still get their own map rows: identically-worded notes
        at different times of the same day render one composite text (only the
        date appears) but two distinct maps."""
        er_collection, address = self._seed_generation_output(
            FAKE_LOCATION_ER_COLLECTION_NAME,
            [
                {
                    "person_id": 1001,
                    "document_contents_id": "shared_composite_hash",
                    "document_text": "identical composite text",
                    "document_update_datetime": "2026-01-15T09:00:00Z",
                    "entry_source_map": [
                        {
                            "entry_num": 1,
                            "source_document_contents_id": "doc_shared",
                            "source_document_update_datetime": "2026-01-15T09:00:00Z",
                            "source_array_index": None,
                        },
                    ],
                },
                {
                    "person_id": 1002,
                    "document_contents_id": "shared_composite_hash",
                    "document_text": "identical composite text",
                    "document_update_datetime": "2026-01-15T17:00:00Z",
                    "entry_source_map": [
                        {
                            "entry_num": 1,
                            "source_document_contents_id": "doc_shared",
                            "source_document_update_datetime": "2026-01-15T17:00:00Z",
                            "source_array_index": None,
                        },
                    ],
                },
            ],
        )

        self.assertEqual(
            [
                {
                    "person_id": 1001,
                    "document_contents_id": "shared_composite_hash",
                    "entry_num": 1,
                    "source_document_contents_id": "doc_shared",
                    "source_document_update_datetime": pd.Timestamp(
                        "2026-01-15T09:00:00Z"
                    ),
                    "source_array_index": None,
                },
                {
                    "person_id": 1002,
                    "document_contents_id": "shared_composite_hash",
                    "entry_num": 1,
                    "source_document_contents_id": "doc_shared",
                    "source_document_update_datetime": pd.Timestamp(
                        "2026-01-15T17:00:00Z"
                    ),
                    "source_array_index": None,
                },
            ],
            self._replacement_rows(er_collection, address),
        )

    def test_empty_generation_output(self) -> None:
        er_collection, address = self._seed_generation_output(
            FAKE_ASSIGNMENT_ER_COLLECTION_NAME, []
        )
        self.assertEqual([], self._replacement_rows(er_collection, address))
