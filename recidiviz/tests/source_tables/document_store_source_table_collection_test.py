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
"""Tests for collect_document_store_source_tables."""
import unittest

from more_itertools import one

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.common.constants.states_utils import find_state_codes_in_str
from recidiviz.documents.dataset_config import (
    document_store_metadata_dataset_for_region,
)
from recidiviz.documents.extraction.entity_resolution.entity_resolution_document_collection_config import (
    EntityResolutionDocumentCollectionConfig,
)
from recidiviz.documents.extraction.entity_resolution.entity_resolution_entry_source_map_table import (
    ENTRY_SOURCE_MAP_TABLE_ID_SUFFIX,
)
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    get_first_order_llm_extractor_config,
)
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
)
from recidiviz.documents.store.document_upload_status_table import (
    DocumentUploadStatusTable,
)
from recidiviz.source_tables.document_store_source_table_collection import (
    collect_document_store_source_tables,
    collect_document_store_source_tables_for_configs,
)
from recidiviz.source_tables.source_table_config import (
    DocumentStoreSourceTableLabel,
    SourceTableConfig,
    StateSpecificSourceTableLabel,
)
from recidiviz.tests.documents import fake_config


class CollectDocumentStoreSourceTablesTest(unittest.TestCase):
    """Tests for collect_document_store_source_tables."""

    def test_produces_expected_tables_for_fake_config(self) -> None:
        collections = collect_document_store_source_tables(config_module=fake_config)

        produced_addresses: set[BigQueryAddress] = set()
        for collection in collections:
            state_code = one(find_state_codes_in_str(collection.dataset_id))
            expected_labels = [
                StateSpecificSourceTableLabel(state_code=state_code),
                DocumentStoreSourceTableLabel(state_code=state_code),
            ]

            self.assertEqual(expected_labels, collection.labels)
            produced_addresses.update(
                table.address for table in collection.source_tables
            )

        expected_addresses = {
            BigQueryAddress.from_str(address_str)
            for address_str in [
                "us_xx_document_contents.fake_case_notes_document_contents",
                "us_xx_document_contents.fake_extractor_collection_assignment_entity_resolution_document_contents",
                "us_xx_document_contents.fake_extractor_collection_location_entity_resolution_document_contents",
                "us_xx_document_contents.fake_extractor_collection_pay_rate_entity_resolution_document_contents",
                "us_xx_document_contents.fake_input_notes_document_contents",
                "us_xx_document_contents.fake_person_id_notes_document_contents",
                "us_xx_document_contents.fake_staff_id_reports_document_contents",
                "us_xx_document_contents.fake_staff_reports_document_contents",
                "us_xx_document_store_metadata.document_upload_status",
                "us_xx_document_store_metadata.fake_case_notes",
                "us_xx_document_store_metadata.fake_extractor_collection_assignment_entity_resolution",
                "us_xx_document_store_metadata.fake_extractor_collection_assignment_entry_source_map",
                "us_xx_document_store_metadata.fake_extractor_collection_location_entity_resolution",
                "us_xx_document_store_metadata.fake_extractor_collection_location_entry_source_map",
                "us_xx_document_store_metadata.fake_extractor_collection_pay_rate_entity_resolution",
                "us_xx_document_store_metadata.fake_extractor_collection_pay_rate_entry_source_map",
                "us_xx_document_store_metadata.fake_input_notes",
                "us_xx_document_store_metadata.fake_person_id_notes",
                "us_xx_document_store_metadata.fake_staff_id_reports",
                "us_xx_document_store_metadata.fake_staff_reports",
                "us_yy_document_contents.fake_us_yy_notes_document_contents",
                "us_yy_document_store_metadata.document_upload_status",
                "us_yy_document_store_metadata.fake_us_yy_notes",
            ]
        }
        self.assertEqual(expected_addresses, produced_addresses)

    def _entry_source_map_tables(
        self, state_code: StateCode
    ) -> dict[str, SourceTableConfig]:
        """Returns the entry→source map tables produced for |state_code|, keyed by
        table id.
        """
        return {
            table.address.table_id: table
            for collection in collect_document_store_source_tables(
                config_module=fake_config
            )
            if collection.dataset_id
            == document_store_metadata_dataset_for_region(state_code)
            for table in collection.source_tables
            if table.address.table_id.endswith(ENTRY_SOURCE_MAP_TABLE_ID_SUFFIX)
        }

    def test_entity_resolution_map_tables_have_expected_schema_and_clustering(
        self,
    ) -> None:
        map_tables = self._entry_source_map_tables(StateCode.US_XX)

        self.assertEqual(
            {
                "fake_extractor_collection_assignment_entry_source_map",
                "fake_extractor_collection_location_entry_source_map",
                "fake_extractor_collection_pay_rate_entry_source_map",
            },
            set(map_tables),
        )
        for table_id, table in map_tables.items():
            with self.subTest(table_id=table_id):
                self.assertEqual(
                    [
                        "person_id",
                        "document_contents_id",
                        "entry_num",
                        "source_document_contents_id",
                        "source_document_update_datetime",
                        "source_array_index",
                    ],
                    [field.name for field in table.schema_fields],
                )
                # Clustered on the key the map is written and replaced by.
                self.assertEqual(["person_id"], table.clustering_fields)


class CollectDocumentStoreSourceTablesForConfigsTest(unittest.TestCase):
    """Tests for collect_document_store_source_tables_for_configs, which narrows to
    a given set of configs rather than scanning a whole module."""

    def _first_order_config(self) -> DocumentCollectionConfig:
        return get_first_order_llm_extractor_config(
            StateCode.US_XX, "FAKE_EXTRACTOR_COLLECTION", config_module=fake_config
        ).input_document_collection

    def _entity_resolution_config(self) -> EntityResolutionDocumentCollectionConfig:
        first_order = get_first_order_llm_extractor_config(
            StateCode.US_XX, "FAKE_EXTRACTOR_COLLECTION", config_module=fake_config
        )
        return EntityResolutionDocumentCollectionConfig(
            first_order_config=first_order,
            entity_group=first_order.extractor_collection.entity_groups[0],
        )

    def _produced_addresses(
        self, configs: list[DocumentCollectionConfig]
    ) -> set[BigQueryAddress]:
        return {
            table.address
            for collection in collect_document_store_source_tables_for_configs(configs)
            for table in collection.source_tables
        }

    def test_first_order_config_produces_only_its_own_tables(self) -> None:
        # Just the one collection's metadata + contents tables, plus the always-on
        # upload-status table — none of the module's other collections.
        self.assertEqual(
            {
                BigQueryAddress.from_str(
                    "us_xx_document_store_metadata.fake_input_notes"
                ),
                BigQueryAddress.from_str(
                    "us_xx_document_contents.fake_input_notes_document_contents"
                ),
                BigQueryAddress.from_str(
                    f"us_xx_document_store_metadata.{DocumentUploadStatusTable.table_id}"
                ),
            },
            self._produced_addresses([self._first_order_config()]),
        )

    def test_entity_resolution_config_additionally_emits_map_table(self) -> None:
        er_config = self._entity_resolution_config()
        produced = self._produced_addresses([er_config])

        map_addresses = {
            address
            for address in produced
            if address.table_id.endswith(ENTRY_SOURCE_MAP_TABLE_ID_SUFFIX)
        }
        self.assertEqual(
            {er_config.entry_source_map_table_address(sandbox_dataset_prefix=None)},
            map_addresses,
        )

    def test_empty_configs_produce_no_collections(self) -> None:
        self.assertEqual([], collect_document_store_source_tables_for_configs([]))
