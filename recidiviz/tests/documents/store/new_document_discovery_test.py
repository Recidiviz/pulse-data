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
from recidiviz.documents.store.new_document_discovery import (
    DEFAULT_NUM_BATCHES,
    DocumentBatchRange,
    NewDocumentDiscoverer,
    SingleCollectionDocumentDiscoveryResult,
    build_collection_new_document_batches,
    build_document_batches,
)
from recidiviz.tests.ingest.direct import fake_regions


class TestBuildBatches(unittest.TestCase):
    """Tests for the build_batches functions"""

    def test_build_collection_new_document_batches(self) -> None:
        addr = ProjectSpecificBigQueryAddress(
            project_id="recidiviz-testing", dataset_id="ds", table_id="t1"
        )
        batch_ranges = build_collection_new_document_batches(addr, 897587, 3)

        self.assertEqual(len(batch_ranges), 3)
        self.assertEqual(batch_ranges[0], DocumentBatchRange(addr, 0, 299196))
        self.assertEqual(batch_ranges[1], DocumentBatchRange(addr, 299196, 598392))
        self.assertEqual(batch_ranges[2], DocumentBatchRange(addr, 598392, 897587))

    def test_build_document_batches(self) -> None:
        configs = collect_document_collection_configs(
            StateCode.US_XX, region_module=fake_regions
        )
        config_list = list(configs.values())

        addr1 = ProjectSpecificBigQueryAddress(
            project_id="recidiviz-testing", dataset_id="ds", table_id="t1"
        )
        addr2 = ProjectSpecificBigQueryAddress(
            project_id="recidiviz-testing", dataset_id="ds", table_id="t2"
        )
        addr3 = ProjectSpecificBigQueryAddress(
            project_id="recidiviz-testing", dataset_id="ds", table_id="t3"
        )

        collection_results = [
            SingleCollectionDocumentDiscoveryResult(
                config=config_list[0],
                temp_document_metadata_updates_address=addr1,
                temp_new_document_contents_address=addr1,
                num_new_document_contents_rows=897587,
            ),
            SingleCollectionDocumentDiscoveryResult(
                config=config_list[1 % len(config_list)],
                temp_document_metadata_updates_address=addr2,
                temp_new_document_contents_address=addr2,
                num_new_document_contents_rows=105923,
            ),
            SingleCollectionDocumentDiscoveryResult(
                config=config_list[2 % len(config_list)],
                temp_document_metadata_updates_address=addr3,
                temp_new_document_contents_address=addr3,
                num_new_document_contents_rows=9768899,
            ),
        ]

        batches = build_document_batches(collection_results, 3)

        self.assertEqual(len(batches), 3)
        self.assertEqual(
            batches[0],
            [
                DocumentBatchRange(addr1, 0, 299196),
                DocumentBatchRange(addr2, 0, 35308),
                DocumentBatchRange(addr3, 0, 3256300),
            ],
        )
        self.assertEqual(
            batches[1],
            [
                DocumentBatchRange(addr1, 299196, 598392),
                DocumentBatchRange(addr2, 35308, 70616),
                DocumentBatchRange(addr3, 3256300, 6512600),
            ],
        )
        self.assertEqual(
            batches[2],
            [
                DocumentBatchRange(addr1, 598392, 897587),
                DocumentBatchRange(addr2, 70616, 105923),
                DocumentBatchRange(addr3, 6512600, 9768899),
            ],
        )

    def test_fewer_rows_than_batches(self) -> None:
        addr = ProjectSpecificBigQueryAddress(
            project_id="recidiviz-testing", dataset_id="ds", table_id="t1"
        )
        batch_ranges = build_collection_new_document_batches(addr, 2, 5)

        self.assertEqual(len(batch_ranges), 2)
        self.assertEqual(batch_ranges[0], DocumentBatchRange(addr, 0, 1))
        self.assertEqual(batch_ranges[1], DocumentBatchRange(addr, 1, 2))

    def test_empty_list(self) -> None:
        batches = build_document_batches([], 3)

        self.assertEqual(batches[0], [])
        self.assertEqual(batches[1], [])
        self.assertEqual(batches[2], [])


class TestNewDocumentDiscovery(unittest.TestCase):
    """Tests for the NewDocumentDiscoverer class"""

    def setUp(self) -> None:
        self.bq_client = MagicMock()
        self.discovery = NewDocumentDiscoverer(
            state_code=StateCode.US_XX,
            project_id="recidiviz-testing",
            big_query_client=self.bq_client,
            job_id="test_job_id",
            num_batches=3,
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
        self.num_collections = len(
            collect_document_collection_configs(
                StateCode.US_XX, region_module=fake_regions
            )
        )

    def tearDown(self) -> None:
        self.document_collection_patcher.stop()

    def test_all_collections_empty_returns_empty_batches(
        self,
    ) -> None:
        mock_row_iterator = MagicMock()
        mock_row_iterator.total_rows = 0
        self.bq_client.create_table_from_query.return_value = mock_row_iterator

        result = self.discovery.run()

        self.assertEqual(result.document_batches, [[], [], []])
        self.assertEqual(
            len(result.temp_document_metadata_updates_addresses), self.num_collections
        )
        # 2 create_table_from_query calls per collection (metadata + document)
        self.assertEqual(
            self.bq_client.create_table_from_query.call_count,
            2 * self.num_collections,
        )

    def test_creates_temp_tables_and_batches(
        self,
    ) -> None:
        configs = collect_document_collection_configs(
            StateCode.US_XX, region_module=fake_regions
        )
        config_names = list(configs.keys())
        empty_collection_name = config_names[0]

        def create_table_side_effect(
            address: ProjectSpecificBigQueryAddress, **_kwargs: object
        ) -> MagicMock:
            mock = MagicMock()
            # The "new document contents" temp tables contain the row count
            # that determines batching; metadata tables are ignored.
            if "temp_new_document_contents" in address.table_id:
                if empty_collection_name in address.table_id:
                    mock.total_rows = 0
                else:
                    mock.total_rows = 6
            else:
                mock.total_rows = 0
            return mock

        non_empty_table_count = self.num_collections - 1
        self.bq_client.create_table_from_query.side_effect = create_table_side_effect

        result = self.discovery.run()

        self.assertEqual(
            self.bq_client.create_table_from_query.call_count,
            2 * self.num_collections,
        )

        num_batches = min(non_empty_table_count, DEFAULT_NUM_BATCHES)
        self.assertEqual(len(result.document_batches), num_batches)
        for batch in result.document_batches:
            self.assertEqual(len(batch), non_empty_table_count)

        self.assertEqual(
            len(result.temp_document_metadata_updates_addresses), self.num_collections
        )

        first_batch = result.document_batches[0]
        for batch_range in first_batch:
            self.assertEqual(batch_range.start_sequence_num_inclusive, 0)

        last_batch = result.document_batches[-1]
        for batch_range in last_batch:
            self.assertEqual(batch_range.end_sequence_num_exclusive, 6)
