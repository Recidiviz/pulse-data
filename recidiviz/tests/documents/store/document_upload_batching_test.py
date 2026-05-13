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
"""Tests for document_upload_batching.py."""

import unittest
from unittest.mock import MagicMock

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
from recidiviz.documents.store.document_upload_batching import build_document_batches
from recidiviz.tests.ingest.direct import fake_regions


class TestBuildDocumentBatches(unittest.TestCase):
    """Tests for build_document_batches."""

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
            state_code=config.state_code,
            collection_name=config.name,
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
