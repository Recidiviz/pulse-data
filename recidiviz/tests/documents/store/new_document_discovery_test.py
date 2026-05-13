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
"""Tests for new_document_discovery.py."""

import unittest
from unittest.mock import MagicMock, patch

from recidiviz.big_query.big_query_address import ProjectSpecificBigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
    collect_document_collection_configs,
)
from recidiviz.documents.store.new_document_discovery import NewDocumentDiscoverer
from recidiviz.tests.ingest.direct import fake_regions


class TestNewDocumentDiscovery(unittest.TestCase):
    """Tests for the NewDocumentDiscoverer class"""

    def setUp(self) -> None:
        self.bq_client = MagicMock()
        self.discovery = NewDocumentDiscoverer(
            state_code=StateCode.US_XX,
            project_id="recidiviz-testing",
            big_query_client=self.bq_client,
            job_id="test_job_id",
        )

        def mock_collect_document_collection_configs(
            state_code: StateCode,
        ) -> dict[str, DocumentCollectionConfig]:
            return collect_document_collection_configs(
                state_code, region_module=fake_regions
            )

        self.document_collection_patcher = patch(
            "recidiviz.documents.store.new_document_discovery.collect_document_collection_configs",
            side_effect=mock_collect_document_collection_configs,
        )
        self.collect_configs_mock = self.document_collection_patcher.start()

        self.configs = collect_document_collection_configs(
            StateCode.US_XX, region_module=fake_regions
        )
        self.config_names = list(self.configs.keys())

    def tearDown(self) -> None:
        self.document_collection_patcher.stop()

    def test_all_collections_empty_returns_no_results(self) -> None:
        mock_row_iterator = MagicMock()
        mock_row_iterator.total_rows = 0
        self.bq_client.create_table_from_query.return_value = mock_row_iterator

        result = self.discovery.run()

        self.assertEqual(result, [])
        # 2 create_table_from_query calls per collection (metadata + document)
        self.assertEqual(
            self.bq_client.create_table_from_query.call_count,
            2 * len(self.configs),
        )
        self.bq_client.run_query_async.assert_not_called()

    def test_creates_temp_tables_and_filters_by_metadata_updates(self) -> None:
        """Tests three categories of collections:
        - no_changes: 0 metadata updates → excluded from results
        - metadata_only: metadata updates but 0 new document contents → included in results
        - has_new_docs (remaining): metadata updates AND new documents → included in results
        """
        no_changes_collection = self.config_names[0]
        metadata_only_collection = self.config_names[1]

        def create_table_side_effect(
            address: ProjectSpecificBigQueryAddress, **_kwargs: object
        ) -> MagicMock:
            mock = MagicMock()
            if no_changes_collection in address.table_id:
                mock.total_rows = 0
            elif "temp_new_document_contents" in address.table_id:
                if metadata_only_collection in address.table_id:
                    mock.total_rows = 0
                else:
                    mock.total_rows = 6
            # Temp metadata updates table
            else:
                mock.total_rows = 10
            return mock

        self.bq_client.create_table_from_query.side_effect = create_table_side_effect

        result = self.discovery.run()

        self.assertEqual(
            self.bq_client.create_table_from_query.call_count,
            2 * len(self.configs),
        )

        collections_with_metadata_updates = len(self.configs) - 1
        self.assertEqual(len(result), collections_with_metadata_updates)
        result_collection_names = {r.collection_name for r in result}
        self.assertNotIn(no_changes_collection, result_collection_names)
        self.assertIn(metadata_only_collection, result_collection_names)
