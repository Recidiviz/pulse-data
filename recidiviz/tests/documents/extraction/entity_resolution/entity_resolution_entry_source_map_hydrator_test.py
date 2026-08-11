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
"""Tests for entity_resolution_entry_source_map_hydrator.py."""
import unittest
from typing import Any
from unittest.mock import MagicMock

import pandas as pd

from recidiviz.big_query.big_query_address import ProjectSpecificBigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.entity_resolution.entity_resolution_document_collection_config import (
    EntityResolutionDocumentCollectionConfig,
)
from recidiviz.documents.extraction.entity_resolution.entity_resolution_entry_source_map_hydrator import (
    EntityResolutionEntrySourceMapHydrator,
)
from recidiviz.documents.extraction.entity_resolution.entity_resolution_entry_source_map_table import (
    EntityResolutionEntrySourceMapBQTable,
)
from recidiviz.documents.store.document_collection_config_collectors import (
    get_document_collection_config,
)
from recidiviz.documents.store.document_collection_query_builders import (
    DocumentCollectionDiffQueryBuilder,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.documents import fake_config as fake_config_module
from recidiviz.tests.documents.extraction.entity_resolution.entity_resolution_test_utils import (
    FAKE_ASSIGNMENT_ER_COLLECTION_NAME,
    patch_fake_entity_resolution_model_config_name,
)
from recidiviz.utils.types import assert_type


def _fake_assignment_er_collection() -> EntityResolutionDocumentCollectionConfig:
    return assert_type(
        get_document_collection_config(
            StateCode.US_XX, FAKE_ASSIGNMENT_ER_COLLECTION_NAME, fake_config_module
        ),
        EntityResolutionDocumentCollectionConfig,
    )


def _generation_output_row(
    *,
    person_id: int,
    document_contents_id: str,
    entries: list[dict[str, Any]],
) -> dict[str, Any]:
    """Returns a generation-output table row whose composite text and update
    datetime carry fixed placeholder values the map never reads."""
    return {
        "person_id": person_id,
        "document_contents_id": document_contents_id,
        "document_text": f"composite text for person {person_id}",
        "document_update_datetime": "2026-01-15T00:00:00Z",
        "entry_source_map": entries,
    }


class TestEntrySourceMapHydratorWriteArgs(unittest.TestCase):
    """Pins the write call the hydrator makes: a write-truncate with the map
    table's managed schema and clustering."""

    def test_write_args(self) -> None:
        with patch_fake_entity_resolution_model_config_name():
            er_collection = _fake_assignment_er_collection()
        bq_client = MagicMock()

        EntityResolutionEntrySourceMapHydrator(
            project_id="recidiviz-testing",
            big_query_client=bq_client,
            config=er_collection,
        ).run(
            ProjectSpecificBigQueryAddress(
                project_id="recidiviz-testing",
                dataset_id="us_xx_document_store_temp",
                table_id="temp_document_generation_output_table",
            )
        )

        bq_client.create_table_from_query.assert_called_once()
        call_kwargs = bq_client.create_table_from_query.call_args.kwargs
        self.assertEqual(
            er_collection.entry_source_map_table_address, call_kwargs["address"]
        )
        self.assertTrue(call_kwargs["overwrite"])
        self.assertEqual(["person_id"], call_kwargs["clustering_fields"])
        self.assertEqual(
            EntityResolutionEntrySourceMapBQTable.schema(
                root_entity_id_type=er_collection.root_entity_id_type
            ),
            call_kwargs["output_schema"],
        )
        self.assertIn("temp_document_generation_output_table", call_kwargs["query"])


class TestEntrySourceMapHydrator(BigQueryEmulatorTestCase):
    """Tests the hydrator's real write path against the emulator.

    Generation-output tables are seeded with streamed rows rather than by
    running the real composite generation query: the BQ emulator cannot scan
    an ARRAY<STRUCT<TIMESTAMP>> column written by a CREATE TABLE AS SELECT
    (streamed rows work), which also rules out running full discovery for an
    ER collection here — the discovery-level wiring is pinned by mock tests in
    new_document_discovery_test.py instead.
    TODO(OBT-42814): Once the emulator bug is fixed, seed via the real
    generation query and add a true end-to-end ER discovery round-trip test.
    """

    def setUp(self) -> None:
        super().setUp()
        self.enterContext(patch_fake_entity_resolution_model_config_name())
        self.er_collection = _fake_assignment_er_collection()
        self.hydrator = EntityResolutionEntrySourceMapHydrator(
            project_id=self.project_id,
            big_query_client=self.bq_client,
            config=self.er_collection,
        )
        self.bq_client.create_dataset_if_necessary(
            self.er_collection.entry_source_map_table_address.dataset_id
        )

    def _seed_generation_output(
        self, run_id: str, rows: list[dict[str, Any]]
    ) -> ProjectSpecificBigQueryAddress:
        address = self.er_collection.temp_document_generation_output_table_address(
            run_id
        ).to_project_specific_address(self.project_id)
        self.create_mock_table(
            address.to_project_agnostic_address(),
            schema=self.er_collection.build_bq_document_generation_output_schema(),
        )
        self.load_rows_into_table(address.to_project_agnostic_address(), rows)
        return address

    def _map_rows(self) -> list[dict[str, Any]]:
        address = self.er_collection.entry_source_map_table_address.to_project_specific_address(
            self.project_id
        )
        return self.query(
            f"SELECT * FROM {address.format_address_for_query()} "
            "ORDER BY person_id, entry_num"
        ).to_dict("records")

    def test_hydrates_map_from_generation_output(self) -> None:
        """Person 1001's composite renders two entries from one note's array
        elements; person 1002's renders one. Each entry becomes one map row."""
        generation_output_address = self._seed_generation_output(
            "run_1",
            [
                _generation_output_row(
                    person_id=1001,
                    document_contents_id="composite_hash_1001",
                    entries=[
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
                ),
                _generation_output_row(
                    person_id=1002,
                    document_contents_id="composite_hash_1002",
                    entries=[
                        {
                            "entry_num": 1,
                            "source_document_contents_id": "doc_bbb",
                            "source_document_update_datetime": "2026-01-20T00:00:00Z",
                            "source_array_index": 0,
                        },
                    ],
                ),
            ],
        )

        self.hydrator.run(generation_output_address)

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
            self._map_rows(),
        )

    def test_stale_rows_removed_on_rehydration(self) -> None:
        """A root entity that leaves the generation output loses its map rows:
        the write is a true write-truncate, not an append or merge."""
        entries_1001 = [
            {
                "entry_num": 1,
                "source_document_contents_id": "doc_aaa",
                "source_document_update_datetime": "2026-01-15T00:00:00Z",
                "source_array_index": 0,
            }
        ]
        first_address = self._seed_generation_output(
            "run_1",
            [
                _generation_output_row(
                    person_id=1001,
                    document_contents_id="composite_hash_1001",
                    entries=entries_1001,
                ),
                _generation_output_row(
                    person_id=1003,
                    document_contents_id="composite_hash_1003",
                    entries=[
                        {
                            "entry_num": 1,
                            "source_document_contents_id": "doc_ccc",
                            "source_document_update_datetime": "2026-01-10T00:00:00Z",
                            "source_array_index": 0,
                        }
                    ],
                ),
            ],
        )
        self.hydrator.run(first_address)
        self.assertEqual([1001, 1003], [row["person_id"] for row in self._map_rows()])

        # Person 1003 leaves the generation output (e.g. their documents were
        # deleted in source data); rehydration drops their rows.
        second_address = self._seed_generation_output(
            "run_2",
            [
                _generation_output_row(
                    person_id=1001,
                    document_contents_id="composite_hash_1001",
                    entries=entries_1001,
                ),
            ],
        )
        self.hydrator.run(second_address)
        self.assertEqual([1001], [row["person_id"] for row in self._map_rows()])

    def test_rehydration_is_idempotent(self) -> None:
        """Re-running over unchanged generation output reproduces the identical
        map."""
        rows = [
            _generation_output_row(
                person_id=1001,
                document_contents_id="composite_hash_1001",
                entries=[
                    {
                        "entry_num": 1,
                        "source_document_contents_id": "doc_aaa",
                        "source_document_update_datetime": "2026-01-15T00:00:00Z",
                        "source_array_index": 0,
                    }
                ],
            )
        ]
        self.hydrator.run(self._seed_generation_output("run_1", rows))
        first_map = self._map_rows()
        self.hydrator.run(self._seed_generation_output("run_2", rows))
        self.assertEqual(first_map, self._map_rows())

    def test_index_shift_updates_map_without_document_change(self) -> None:
        """The ticket's founding case: a re-extraction stops emitting an
        all-null array element, shifting a mention's source_array_index without
        changing a byte of the rendered composite. The composite's hash — and
        therefore the document diff — see no change, and the map must update
        anyway.

        The two generation outputs share the same document_contents_id and
        text by construction (only the date of each occurrence is rendered, and
        the array index is never rendered); the diff-sees-nothing leg is
        asserted by running the real diff query against a metadata table
        holding the already-uploaded composite row.
        """
        composite_hash = "composite_hash_1001"
        v1_address = self._seed_generation_output(
            "run_1",
            [
                _generation_output_row(
                    person_id=1001,
                    document_contents_id=composite_hash,
                    # v1: the mention's element sits at index 1, behind an
                    # all-null element at index 0 that renders nothing.
                    entries=[
                        {
                            "entry_num": 1,
                            "source_document_contents_id": "doc_aaa",
                            "source_document_update_datetime": "2026-01-15T00:00:00Z",
                            "source_array_index": 1,
                        }
                    ],
                )
            ],
        )
        self.hydrator.run(v1_address)
        self.assertEqual([1], [row["source_array_index"] for row in self._map_rows()])

        # The composite was uploaded and recorded on the first run.
        self.create_mock_table(
            self.er_collection.metadata_table_address(sandbox_dataset_prefix=None),
            schema=self.er_collection.build_bq_metadata_schema(),
        )
        self.load_rows_into_table(
            self.er_collection.metadata_table_address(sandbox_dataset_prefix=None),
            [
                {
                    "person_id": 1001,
                    "document_contents_id": composite_hash,
                    "document_update_datetime": "2026-01-15T00:00:00Z",
                    "row_create_datetime": "2026-01-16T00:00:00Z",
                }
            ],
        )

        # v2: the all-null element is gone and the mention shifted to index 0;
        # the rendered text, and therefore the hash, are unchanged.
        v2_address = self._seed_generation_output(
            "run_2",
            [
                _generation_output_row(
                    person_id=1001,
                    document_contents_id=composite_hash,
                    entries=[
                        {
                            "entry_num": 1,
                            "source_document_contents_id": "doc_aaa",
                            "source_document_update_datetime": "2026-01-15T00:00:00Z",
                            "source_array_index": 0,
                        }
                    ],
                )
            ],
        )

        diff_query = DocumentCollectionDiffQueryBuilder(
            project_id=self.project_id
        ).build_document_diff_query(
            config=self.er_collection,
            document_generation_output_address=v2_address,
        )
        num_diff_rows = self.query(
            f"SELECT COUNT(*) AS num_rows FROM ({diff_query})"
        ).to_dict("records")[0]["num_rows"]
        self.assertEqual(0, num_diff_rows)

        self.hydrator.run(v2_address)
        self.assertEqual([0], [row["source_array_index"] for row in self._map_rows()])
