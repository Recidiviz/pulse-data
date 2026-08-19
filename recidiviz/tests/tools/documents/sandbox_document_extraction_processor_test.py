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
"""Tests for the sandbox_document_extraction_processor."""

import datetime
from unittest import mock

from recidiviz.documents.extraction.entity_resolution.entity_resolution_document_collection_config import (
    EntityResolutionDocumentCollectionConfig,
)
from recidiviz.documents.extraction.entity_resolution.entity_resolution_entry_source_map_table import (
    EntityResolutionEntrySourceMapBQTable,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
    PERSON_ID_COLUMN_NAME,
)
from recidiviz.documents.store.document_store_sandbox_context import (
    DocumentCollectionSandboxLocation,
    DocumentStoreSandboxContext,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.documents.extraction.entity_resolution.entity_resolution_test_utils import (
    fake_entity_resolution_extractor_config,
    fake_first_order_extractor_config,
    patch_fake_entity_resolution_model_config_name,
)
from recidiviz.tools.documents.sandbox_document_extraction_processor import (
    read_expected_entry_nums_by_document,
)
from recidiviz.utils.types import assert_type

_SANDBOX_PREFIX = "test_prefix"
_ASSIGNMENT_GROUP_NAME = "assignment"


class ReadExpectedEntryNumsByDocumentTest(BigQueryEmulatorTestCase):
    """Verifies the entry set each composite document's validation is checked
    against is read from the entry→source map table for an entity-resolution
    extractor, and skipped entirely for a first-order one."""

    def setUp(self) -> None:
        super().setUp()
        self.enterContext(patch_fake_entity_resolution_model_config_name())

    def _document_store_sandbox(
        self, config: LLMExtractorConfig
    ) -> DocumentStoreSandboxContext:
        return DocumentStoreSandboxContext(
            document_collection_locations={
                config.input_document_collection.name: DocumentCollectionSandboxLocation(
                    output_prefix=_SANDBOX_PREFIX, diff_read_prefix=_SANDBOX_PREFIX
                )
            },
            extractor_collection_read_prefixes={},
        )

    def test_reads_entry_set_per_composite_document(self) -> None:
        config = fake_entity_resolution_extractor_config(_ASSIGNMENT_GROUP_NAME)
        er_collection = assert_type(
            config.input_document_collection, EntityResolutionDocumentCollectionConfig
        )
        map_address = er_collection.entry_source_map_table_address(
            sandbox_dataset_prefix=_SANDBOX_PREFIX
        )
        self.create_mock_table(
            address=map_address,
            schema=EntityResolutionEntrySourceMapBQTable.schema(
                root_entity_id_type=er_collection.root_entity_id_type
            ),
        )
        # Two composite documents: person 1's spans entries {1, 2}, person 2's a
        # single entry {3}. source_array_index is null for a top-level group.
        self.load_rows_into_table(
            map_address,
            [
                {
                    PERSON_ID_COLUMN_NAME: person_id,
                    DOCUMENT_CONTENTS_ID_COLUMN_NAME: document_contents_id,
                    "entry_num": entry_num,
                    "source_document_contents_id": f"src_{entry_num}",
                    "source_document_update_datetime": datetime.datetime(
                        2024, 1, 1, tzinfo=datetime.UTC
                    ).isoformat(),
                    "source_array_index": None,
                }
                for person_id, document_contents_id, entry_num in (
                    (1, "COMPOSITE_A", 1),
                    (1, "COMPOSITE_A", 2),
                    (2, "COMPOSITE_B", 3),
                )
            ],
        )

        self.assertEqual(
            {"COMPOSITE_A": {1, 2}, "COMPOSITE_B": {3}},
            read_expected_entry_nums_by_document(
                config=config,
                document_store_sandbox=self._document_store_sandbox(config),
                bq_client=self.bq_client,
            ),
        )

    def test_reads_entry_set_from_production_when_no_sandbox(self) -> None:
        # With no sandbox document store, the map table is read from the production
        # (un-prefixed) document store.
        config = fake_entity_resolution_extractor_config(_ASSIGNMENT_GROUP_NAME)
        er_collection = assert_type(
            config.input_document_collection, EntityResolutionDocumentCollectionConfig
        )
        map_address = er_collection.entry_source_map_table_address(
            sandbox_dataset_prefix=None
        )
        self.create_mock_table(
            address=map_address,
            schema=EntityResolutionEntrySourceMapBQTable.schema(
                root_entity_id_type=er_collection.root_entity_id_type
            ),
        )
        self.load_rows_into_table(
            map_address,
            [
                {
                    PERSON_ID_COLUMN_NAME: 1,
                    DOCUMENT_CONTENTS_ID_COLUMN_NAME: "COMPOSITE_A",
                    "entry_num": 1,
                    "source_document_contents_id": "src_1",
                    "source_document_update_datetime": datetime.datetime(
                        2024, 1, 1, tzinfo=datetime.UTC
                    ).isoformat(),
                    "source_array_index": None,
                }
            ],
        )

        self.assertEqual(
            {"COMPOSITE_A": {1}},
            read_expected_entry_nums_by_document(
                config=config,
                document_store_sandbox=None,
                bq_client=self.bq_client,
            ),
        )

    def test_first_order_config_reads_nothing(self) -> None:
        # A first-order extractor has no numbered entries, so it returns None
        # without touching BigQuery.
        config = fake_first_order_extractor_config()
        with mock.patch.object(self.bq_client, "run_query_async") as run_query:
            self.assertIsNone(
                read_expected_entry_nums_by_document(
                    config=config,
                    document_store_sandbox=self._document_store_sandbox(config),
                    bq_client=self.bq_client,
                )
            )
        run_query.assert_not_called()
