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

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.source_tables.document_store_source_table_collection import (
    collect_document_store_source_tables,
)
from recidiviz.source_tables.source_table_config import (
    DocumentStoreSourceTableLabel,
    StateSpecificSourceTableLabel,
)
from recidiviz.tests.documents import fake_config


class CollectDocumentStoreSourceTablesTest(unittest.TestCase):
    """Tests for collect_document_store_source_tables."""

    def test_produces_expected_tables_for_fake_config(self) -> None:
        collections = collect_document_store_source_tables(config_module=fake_config)

        expected_labels = [
            StateSpecificSourceTableLabel(state_code=StateCode.US_XX),
            DocumentStoreSourceTableLabel(state_code=StateCode.US_XX),
        ]
        produced_addresses: set[BigQueryAddress] = set()
        for collection in collections:
            # Every document-store table is tagged as US_XX-specific and
            # document-store-owned.
            self.assertEqual(expected_labels, collection.labels)
            produced_addresses.update(
                table.address for table in collection.source_tables
            )

        expected_addresses = {
            BigQueryAddress.from_str(address_str)
            for address_str in [
                "us_xx_document_store_metadata.fake_input_notes",
                "us_xx_document_store_metadata.fake_extractor_collection_location_entity_resolution",
                "us_xx_document_store_metadata.fake_extractor_collection_assignment_entity_resolution",
                "us_xx_document_store_metadata.document_upload_status",
                "us_xx_document_contents.fake_input_notes_document_contents",
                "us_xx_document_contents.fake_extractor_collection_location_entity_resolution_document_contents",
                "us_xx_document_contents.fake_extractor_collection_assignment_entity_resolution_document_contents",
            ]
        }
        self.assertEqual(expected_addresses, produced_addresses)
