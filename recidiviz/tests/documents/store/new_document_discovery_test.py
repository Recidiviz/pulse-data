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
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_UPLOAD_BATCH_NUM_COLUMN_NAME,
)
from recidiviz.documents.store.document_store_types import (
    DocumentUploadBatch,
    SingleCollectionDocumentDiscoveryResult,
)
from recidiviz.documents.store.new_document_discovery import (
    NewDocumentDiscoverer,
    build_document_batches,
)
from recidiviz.tests.ingest.direct import fake_regions


class TestBuildBatches(unittest.TestCase):
    """Tests for the build_batches functions"""

    def setUp(self) -> None:
        self.addr = ProjectSpecificBigQueryAddress(
            project_id="recidiviz-testing", dataset_id="ds", table_id="t1"
        )

    def _make_upload_batch(
        self, batch_number: int, name: str = "test_collection"
    ) -> DocumentUploadBatch:
        return DocumentUploadBatch(
            collection_name=name,
            temp_new_document_contents_table_address=self.addr,
            batch_number=batch_number,
        )

    def _make_collection_doc_discovery_result(
        self,
        config: DocumentCollectionConfig,
        num_new_document_contents_rows: int,
    ) -> SingleCollectionDocumentDiscoveryResult:
        return SingleCollectionDocumentDiscoveryResult(
            config=config,
            temp_document_metadata_updates_address=self.addr,
            temp_new_document_contents_address=self.addr,
            num_new_document_contents_rows=num_new_document_contents_rows,
            num_document_metadata_updates_rows=num_new_document_contents_rows,
        )

    def _mock_bq_client_with_batch_numbers(self, batch_numbers: list[int]) -> MagicMock:
        bq_client = MagicMock()
        mock_job = MagicMock()
        mock_job.result.return_value = [
            {DOCUMENT_UPLOAD_BATCH_NUM_COLUMN_NAME: n} for n in batch_numbers
        ]
        bq_client.run_query_async.return_value = mock_job
        return bq_client

    def test_build_document_batches(self) -> None:
        configs = collect_document_collection_configs(
            StateCode.US_XX, region_module=fake_regions
        )
        config_list = list(configs.values())

        name1 = config_list[0].name
        name2 = config_list[1 % len(config_list)].name
        name3 = config_list[2 % len(config_list)].name

        collection_results = [
            self._make_collection_doc_discovery_result(
                config=config_list[0],
                num_new_document_contents_rows=897587,
            ),
            self._make_collection_doc_discovery_result(
                config=config_list[1 % len(config_list)],
                num_new_document_contents_rows=105923,
            ),
            self._make_collection_doc_discovery_result(
                config=config_list[2 % len(config_list)],
                num_new_document_contents_rows=9768899,
            ),
            # Should be ignored since it has zero new document contents rows.
            self._make_collection_doc_discovery_result(
                config=config_list[3 % len(config_list)],
                num_new_document_contents_rows=0,
            ),
        ]

        bq_client = self._mock_bq_client_with_batch_numbers([0, 1])
        batches = build_document_batches(
            collection_results, num_upload_task_instances=2, big_query_client=bq_client
        )

        self.assertEqual(len(batches), 2)
        self.assertEqual(
            batches[0],
            [
                self._make_upload_batch(batch_number=0, name=name1),
                self._make_upload_batch(batch_number=0, name=name2),
                self._make_upload_batch(batch_number=0, name=name3),
            ],
        )
        self.assertEqual(
            batches[1],
            [
                self._make_upload_batch(batch_number=1, name=name1),
                self._make_upload_batch(batch_number=1, name=name2),
                self._make_upload_batch(batch_number=1, name=name3),
            ],
        )

    def test_empty_list(self) -> None:
        bq_client = MagicMock()
        batches = build_document_batches(
            collection_results=[],
            num_upload_task_instances=3,
            big_query_client=bq_client,
        )

        self.assertEqual(batches[0], [])
        self.assertEqual(batches[1], [])
        self.assertEqual(batches[2], [])
        bq_client.run_query_async.assert_not_called()


class TestNewDocumentDiscovery(unittest.TestCase):
    """Tests for the NewDocumentDiscoverer class"""

    def setUp(self) -> None:
        self.bq_client = MagicMock()
        self.discovery = NewDocumentDiscoverer(
            state_code=StateCode.US_XX,
            project_id="recidiviz-testing",
            big_query_client=self.bq_client,
            job_id="test_job_id",
            upload_task_instance_count=3,
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

    def test_all_collections_empty_returns_empty_batches(self) -> None:
        mock_row_iterator = MagicMock()
        mock_row_iterator.total_rows = 0
        self.bq_client.create_table_from_query.return_value = mock_row_iterator

        result = self.discovery.run()

        self.assertEqual(result.document_batches, [[], [], []])
        # Don't include collections with zero new metadata rows in collection_results
        self.assertEqual(result.collection_results, [])
        # 2 create_table_from_query calls per collection (metadata + document)
        self.assertEqual(
            self.bq_client.create_table_from_query.call_count,
            2 * len(self.configs),
        )
        # No collections have new metadata rows, so build_document_batches
        # should not issue any batch number queries.
        self.bq_client.run_query_async.assert_not_called()

    def test_creates_temp_tables_and_batches(self) -> None:
        """Tests three categories of collections:
        - no_changes: 0 metadata updates → excluded from collection_results entirely
        - metadata_only: metadata updates but 0 new document contents → included
            in collection_results but produces no document batches
        - has_new_docs (remaining): metadata updates AND new documents → included
            in collection_results and produces document batches
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

        mock_batch_numbers_job = MagicMock()
        mock_batch_numbers_job.result.return_value = [
            {DOCUMENT_UPLOAD_BATCH_NUM_COLUMN_NAME: 0}
        ]
        self.bq_client.run_query_async.return_value = mock_batch_numbers_job

        self.bq_client.create_table_from_query.side_effect = create_table_side_effect

        result = self.discovery.run()

        self.assertEqual(
            self.bq_client.create_table_from_query.call_count,
            2 * len(self.configs),
        )

        collections_with_metadata_updates = len(self.configs) - 1
        collections_with_new_docs = len(self.configs) - 2

        # run_query_async called once per collection with new docs
        self.assertEqual(
            self.bq_client.run_query_async.call_count,
            collections_with_new_docs,
        )

        self.assertEqual(
            len(result.collection_results), collections_with_metadata_updates
        )
        # The no_changes collection should be excluded
        result_collection_names = {r.config.name for r in result.collection_results}
        self.assertNotIn(no_changes_collection, result_collection_names)
        self.assertIn(metadata_only_collection, result_collection_names)

        # Each non-empty collection has 1 batch, distributed round-robin
        # across 3 task instances.
        self.assertEqual(len(result.document_batches), 3)
        total_upload_batches = sum(len(b) for b in result.document_batches)
        self.assertEqual(total_upload_batches, collections_with_new_docs)

        all_upload_batches = [b for task in result.document_batches for b in task]
        for upload_batch in all_upload_batches:
            self.assertEqual(upload_batch.batch_number, 0)
