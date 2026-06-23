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
from recidiviz.tests.documents.store import config as fake_config_module


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

    def test_build_document_batches_round_robin(self) -> None:
        config = next(
            iter(
                collect_document_collection_configs(
                    StateCode.US_XX, config_module=fake_config_module
                ).values()
            )
        )
        collection_result = self._make_collection_doc_discovery_result(
            config=config,
            num_new_document_contents_rows=897587,
        )

        # 5 batches across 2 task instances → instance 0 gets batches [0, 2, 4],
        # instance 1 gets batches [1, 3].
        bq_client = self._mock_bq_client_with_batch_numbers([0, 1, 2, 3, 4])
        batches = build_document_batches(
            collection_result=collection_result,
            num_upload_task_instances=2,
            big_query_client=bq_client,
        )

        self.assertEqual(
            batches,
            [
                [
                    self._make_upload_batch(batch_number=0, name=config.name),
                    self._make_upload_batch(batch_number=2, name=config.name),
                    self._make_upload_batch(batch_number=4, name=config.name),
                ],
                [
                    self._make_upload_batch(batch_number=1, name=config.name),
                    self._make_upload_batch(batch_number=3, name=config.name),
                ],
            ],
        )

    def test_zero_new_document_contents_returns_empty_batches(self) -> None:
        config = next(
            iter(
                collect_document_collection_configs(
                    StateCode.US_XX, config_module=fake_config_module
                ).values()
            )
        )
        collection_result = self._make_collection_doc_discovery_result(
            config=config,
            num_new_document_contents_rows=0,
        )
        bq_client = MagicMock()

        batches = build_document_batches(
            collection_result=collection_result,
            num_upload_task_instances=3,
            big_query_client=bq_client,
        )

        self.assertEqual(batches, [[], [], []])
        bq_client.run_query_async.assert_not_called()
