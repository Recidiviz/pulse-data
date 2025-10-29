#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Tests for the Workflows Firestore ETL Delegate."""
from datetime import datetime, timezone
from typing import List, Optional, Tuple
from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock, patch

import mock.mock
from freezegun import freeze_time
from google.api_core.exceptions import AlreadyExists
from google.cloud.firestore_admin_v1 import CreateIndexRequest

from recidiviz.common.constants.states import StateCode
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.workflows.etl.workflows_etl_delegate import (
    MAX_FIRESTORE_RECORDS_PER_BATCH,
    WorkflowsFirestoreETLDelegate,
)


class TestETLDelegate(WorkflowsFirestoreETLDelegate):
    COLLECTION_BY_FILENAME = {"test_export.json": "testOpportunity"}

    def get_supported_files(self) -> List[str]:
        return ["test_export.json"]

    def transform_row(self, row: str) -> Tuple[str, dict]:
        return row, {"data": row}


class FakeBatchWriter(IsolatedAsyncioTestCase):
    def __init__(
        self,
        verify_batch_size: bool = False,
        verify_timestamp: Optional[datetime] = None,
    ) -> None:
        super().__init__()
        self.doc_count = 0
        self.verify_batch_size = verify_batch_size
        self.verify_timestamp = verify_timestamp

    def set(self, doc_id: str, doc: dict) -> None:  # pylint: disable=unused-argument
        self.doc_count += 1
        if self.verify_batch_size:
            self.assertGreaterEqual(MAX_FIRESTORE_RECORDS_PER_BATCH, self.doc_count)
        if self.verify_timestamp is not None:
            self.assertEqual(doc["__loadedAt"], self.verify_timestamp)

    async def commit(self) -> None:
        if self.verify_batch_size:
            self.assertGreaterEqual(MAX_FIRESTORE_RECORDS_PER_BATCH, self.doc_count)
        self.doc_count = 0

    def __len__(self) -> int:
        return self.doc_count


class FakeFileStream:
    def __init__(self, total_values: int) -> None:
        self.total_values = total_values
        self.values_written = 0

    def readline(self) -> Optional[str]:
        if self.values_written >= self.total_values:
            return None

        self.values_written += 1
        return str(self.values_written)


# Because we are testing an abstract class, we need to subclass it within this test,
# which ultimately means we have to patch the individual client methods instead of the
# entire class (because our test instance already has a reference to the real class,
# which also references google.cloud at instantiation, which is why we have to patch that too)
@patch("google.cloud.firestore_v1.AsyncClient")
@patch("google.cloud.firestore_admin_v1.FirestoreAdminClient")
@patch("google.cloud.firestore_v1.Client")
@patch("recidiviz.firestore.firestore_client.FirestoreClientImpl.get_collection")
@patch("recidiviz.firestore.firestore_client.FirestoreClientImpl.async_batch")
@patch("recidiviz.firestore.firestore_client.FirestoreClientImpl.delete_old_documents")
@patch(
    "recidiviz.workflows.etl.workflows_etl_delegate.WorkflowsETLDelegate.get_file_stream"
)
class WorkflowsFirestoreEtlDelegateTest(IsolatedAsyncioTestCase):
    """Tests for the Firestore ETL Delegate."""

    async def test_run_etl_respects_batching(
        self,
        mock_get_file_stream: mock.MagicMock,
        _mock_delete_old_documents: mock.MagicMock,
        mock_batch_writer: mock.MagicMock,
        mock_get_collection: mock.MagicMock,
        _mock_firestore_client: mock.MagicMock,
        _mock_firestore_admin_client: mock.MagicMock,
        _mock_firestore_async_client: mock.MagicMock,
    ) -> None:
        """Tests that the ETL Delegate respects the max batch size for writing to Firestore."""
        mock_batch_writer.side_effect = lambda: FakeBatchWriter(verify_batch_size=True)
        mock_get_file_stream.return_value = [FakeFileStream(3000)]
        with local_project_id_override("test-project"):
            delegate = TestETLDelegate(StateCode.US_XX)
            await delegate.run_etl("test_export.json")

        mock_get_collection.assert_called_once_with("testOpportunity")

    async def test_run_etl_timestamp(
        self,
        mock_get_file_stream: mock.MagicMock,
        _mock_delete_old_documents: mock.MagicMock,
        mock_batch_writer: mock.MagicMock,
        _mock_get_collection: mock.MagicMock,
        _mock_firestore_client: mock.MagicMock,
        _mock_firestore_admin_client: mock.MagicMock,
        _mock_firestore_async_client: mock.MagicMock,
    ) -> None:
        """Tests that the ETL Delegate adds timestamp to each loaded record."""
        mock_now = datetime(2022, 5, 1, tzinfo=timezone.utc)

        mock_batch_writer.side_effect = lambda: FakeBatchWriter(
            verify_timestamp=mock_now
        )
        mock_get_file_stream.return_value = [FakeFileStream(2)]
        with local_project_id_override("test-project"):
            delegate = TestETLDelegate(StateCode.US_XX)
            with freeze_time(mock_now):
                await delegate.run_etl("test_export.json")

    async def test_run_etl_delete_outdated(
        self,
        _mock_get_file_stream: mock.MagicMock,
        mock_delete_old_documents: mock.MagicMock,
        mock_batch_writer: mock.MagicMock,
        _mock_get_collection: mock.MagicMock,
        _mock_firestore_client: mock.MagicMock,
        _mock_firestore_admin_client: mock.MagicMock,
        _mock_firestore_async_client: mock.MagicMock,
    ) -> None:
        """Tests that the ETL Delegate deletes records with outdated timestamps"""

        mock_now = datetime(2022, 5, 1, tzinfo=timezone.utc)
        # need a function here to create a new batchwriter instance
        # pylint: disable-next=unnecessary-lambda
        mock_batch_writer.side_effect = lambda: FakeBatchWriter()

        with local_project_id_override("test-project"):
            delegate = TestETLDelegate(StateCode.US_XX)
            with freeze_time(mock_now):
                await delegate.run_etl("test_export.json")

        mock_delete_old_documents.assert_called_once_with(
            "testOpportunity", "US_XX", "__loadedAt", mock_now
        )

    async def test_run_etl_transform_error(
        self,
        mock_get_file_stream: mock.MagicMock,
        _mock_delete_old_documents: mock.MagicMock,
        mock_batch_writer: mock.MagicMock,
        _mock_get_collection: mock.MagicMock,
        _mock_firestore_client: mock.MagicMock,
        _mock_firestore_admin_client: mock.MagicMock,
        _mock_firestore_async_client: mock.MagicMock,
    ) -> None:
        """Tests that the ETL Delegate logs an error when transform_row raises Exception"""

        # need a function here to create a new batchwriter instance
        # pylint: disable-next=unnecessary-lambda
        mock_batch_writer.side_effect = lambda: FakeBatchWriter()

        mock_now = datetime(2022, 5, 1, tzinfo=timezone.utc)
        mock_get_file_stream.return_value = [FakeFileStream(2)]

        with local_project_id_override("test-project"):
            delegate = TestETLDelegate(StateCode.US_XX)
            with freeze_time(mock_now):
                with mock.patch.object(
                    TestETLDelegate, "transform_row"
                ) as mock_transform:
                    with patch("logging.Logger.error") as mock_logger:
                        mock_transform.side_effect = [
                            (123, {"personExternalId": 123}),
                            Exception,
                        ]
                        await delegate.run_etl("test_export.json")
                        mock_logger.assert_called_once()
                        assert mock_transform.call_count == 2

    @patch(
        "recidiviz.firestore.firestore_client.FirestoreClientImpl.index_exists_for_collection"
    )
    @patch("recidiviz.firestore.firestore_client.FirestoreClientImpl.create_index")
    async def test_run_etl_creates_index_if_needed(
        self,
        mock_create_index: mock.MagicMock,
        mock_index_exists_for_collection: mock.MagicMock,
        _mock_get_file_stream: mock.MagicMock,
        _mock_delete_old_documents: mock.MagicMock,
        mock_batch_writer: mock.MagicMock,
        _mock_get_collection: mock.MagicMock,
        _mock_firestore_client: mock.MagicMock,
        _mock_firestore_admin_client: mock.MagicMock,
        _mock_firestore_async_client: mock.MagicMock,
    ) -> None:
        """Tests that the ETL Delegate calls create_index if one does not exist yet."""
        # need a function here to create a new batchwriter instance
        # pylint: disable-next=unnecessary-lambda
        mock_batch_writer.side_effect = lambda: FakeBatchWriter()

        with local_project_id_override("test-project"):
            mock_index_exists_for_collection.return_value = False
            delegate = TestETLDelegate(StateCode.US_XX)
            await delegate.run_etl("test_export.json")
            mock_create_index.assert_called_once()
            call_args = mock_create_index.mock_calls[0].args

            self.assertEqual("testOpportunity", call_args[0])
            self.assertEqual(
                CreateIndexRequest(
                    index={
                        "name": "testOpportunity",
                        "query_scope": "COLLECTION",
                        "fields": [
                            {"field_path": "stateCode", "order": "ASCENDING"},
                            {"field_path": "__loadedAt", "order": "ASCENDING"},
                        ],
                    }
                ),
                call_args[1],
            )

    @patch(
        "recidiviz.firestore.firestore_client.FirestoreClientImpl.index_exists_for_collection"
    )
    @patch("recidiviz.firestore.firestore_client.FirestoreClientImpl.create_index")
    async def test_run_etl_does_not_create_index_if_exists(
        self,
        mock_create_index: mock.MagicMock,
        mock_index_exists_for_collection: mock.MagicMock,
        _mock_get_file_stream: mock.MagicMock,
        _mock_delete_old_documents: mock.MagicMock,
        mock_batch_writer: mock.MagicMock,
        _mock_get_collection: mock.MagicMock,
        _mock_firestore_client: mock.MagicMock,
        _mock_firestore_admin_client: mock.MagicMock,
        _mock_firestore_async_client: mock.MagicMock,
    ) -> None:
        """Tests that the ETL Delegate does not call create_index if one already exists."""
        # need a function here to create a new batchwriter instance
        # pylint: disable-next=unnecessary-lambda
        mock_batch_writer.side_effect = lambda: FakeBatchWriter()

        with local_project_id_override("test-project"):
            mock_index_exists_for_collection.return_value = True
            delegate = TestETLDelegate(StateCode.US_XX)
            await delegate.run_etl("test_export.json")
            mock_create_index.assert_not_called()

    @patch(
        "recidiviz.firestore.firestore_client.FirestoreClientImpl.index_exists_for_collection"
    )
    @patch("recidiviz.firestore.firestore_client.FirestoreClientImpl.create_index")
    async def test_run_etl_does_not_create_index_on_exists_exception(
        self,
        mock_create_index: mock.MagicMock,
        mock_index_exists_for_collection: mock.MagicMock,
        _mock_get_file_stream: mock.MagicMock,
        _mock_delete_old_documents: mock.MagicMock,
        mock_batch_writer: mock.MagicMock,
        _mock_get_collection: mock.MagicMock,
        _mock_firestore_client: mock.MagicMock,
        _mock_firestore_admin_client: mock.MagicMock,
        _mock_firestore_async_client: mock.MagicMock,
    ) -> None:
        """Tests that the ETL Delegate logs when creating an index fails on the AlreadyExists exception."""
        # need a function here to create a new batchwriter instance
        # pylint: disable-next=unnecessary-lambda
        mock_batch_writer.side_effect = lambda: FakeBatchWriter()

        with local_project_id_override("test-project"):
            mock_index_exists_for_collection.return_value = False
            mock_create_index.side_effect = AlreadyExists("Index already exists.")
            with self.assertLogs(level="INFO") as log:
                delegate = TestETLDelegate(StateCode.US_XX)
                await delegate.run_etl("test_export.json")
                self.assertTrue("Index already exists." in str(log.output))

    async def test_run_etl_sanitize_doc_id(
        self,
        mock_get_file_stream: mock.MagicMock,
        _mock_delete_old_documents: mock.MagicMock,
        mock_batch_writer: mock.MagicMock,
        mock_get_collection: mock.MagicMock,
        _mock_firestore_client: mock.MagicMock,
        _mock_firestore_admin_client: mock.MagicMock,
        _mock_firestore_async_client: mock.MagicMock,
    ) -> None:
        """Tests that the ETL Delegate sanitizes document ids"""
        # need a function here to create a new batchwriter instance
        # pylint: disable-next=unnecessary-lambda
        mock_batch_writer.side_effect = lambda: FakeBatchWriter()
        mock_get_file_stream.return_value = [FakeFileStream(1)]
        mock_collection = MagicMock()
        mock_get_collection.return_value = mock_collection
        raw_row_id = "FUNKY ID//Slash!!!"
        document_id = "us_xx_funky_id--slash"
        with local_project_id_override("test-project"), patch.object(
            TestETLDelegate, "transform_row"
        ) as mock_transform:
            mock_transform.return_value = (raw_row_id, {})
            delegate = TestETLDelegate(StateCode.US_XX)
            await delegate.run_etl("test_export.json")
            mock_collection.document.assert_called_once_with(document_id)
