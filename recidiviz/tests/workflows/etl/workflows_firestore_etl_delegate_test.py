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
from typing import Optional, Tuple
from unittest import TestCase
from unittest.mock import patch

import mock.mock
from freezegun import freeze_time

from recidiviz.utils.metadata import local_project_id_override
from recidiviz.workflows.etl.workflows_etl_delegate import (
    MAX_FIRESTORE_RECORDS_PER_BATCH,
    WorkflowsFirestoreETLDelegate,
)


class TestETLDelegate(WorkflowsFirestoreETLDelegate):
    EXPORT_FILENAME = "test_export.json"
    _COLLECTION_NAME_BASE = "test_collection"
    STATE_CODE = "US_XX"

    def transform_row(self, row: str) -> Tuple[str, dict]:
        return row, {"data": row}


class FakeBatchWriter(TestCase):
    def __init__(
        self, verify_batch_size: bool = False, verify_timestamp: datetime = None
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

    def commit(self) -> None:
        if self.verify_batch_size:
            self.assertGreaterEqual(MAX_FIRESTORE_RECORDS_PER_BATCH, self.doc_count)
        self.doc_count = 0


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
@patch("google.cloud.firestore.Client")
@patch("recidiviz.firestore.firestore_client.FirestoreClientImpl.get_collection")
@patch("recidiviz.firestore.firestore_client.FirestoreClientImpl.batch")
@patch("recidiviz.firestore.firestore_client.FirestoreClientImpl.delete_old_documents")
@patch(
    "recidiviz.workflows.etl.workflows_etl_delegate.WorkflowsETLDelegate.get_file_stream"
)
class WorkflowsFirestoreEtlDelegateTest(TestCase):
    """Tests for the Firestore ETL Delegate."""

    def test_run_etl_respects_batching(
        self,
        mock_get_file_stream: mock.MagicMock,
        mock_delete_old_documents: mock.MagicMock,  # pylint: disable=unused-argument
        mock_batch_writer: mock.MagicMock,
        mock_get_collection: mock.MagicMock,
        mock_firestore_client: mock.MagicMock,  # pylint: disable=unused-argument
    ) -> None:
        """Tests that the ETL Delegate respects the max batch size for writing to Firestore."""
        mock_batch_writer.return_value = FakeBatchWriter(verify_batch_size=True)
        mock_get_file_stream.return_value = [FakeFileStream(3000)]
        with local_project_id_override("test-project"):
            delegate = TestETLDelegate()
            delegate.run_etl()

        mock_get_collection.assert_called_once_with("test_collection")

    def test_run_etl_timestamp(
        self,
        mock_get_file_stream: mock.MagicMock,
        mock_delete_old_documents: mock.MagicMock,  # pylint: disable=unused-argument
        mock_batch_writer: mock.MagicMock,
        mock_get_collection: mock.MagicMock,  # pylint: disable=unused-argument
        mock_firestore_client: mock.MagicMock,  # pylint: disable=unused-argument
    ) -> None:
        """Tests that the ETL Delegate adds timestamp to each loaded record."""
        mock_now = datetime(2022, 5, 1, tzinfo=timezone.utc)

        mock_batch_writer.return_value = FakeBatchWriter(verify_timestamp=mock_now)
        mock_get_file_stream.return_value = [FakeFileStream(2)]
        with local_project_id_override("test-project"):
            delegate = TestETLDelegate()
            with freeze_time(mock_now):
                delegate.run_etl()

    def test_run_etl_delete_outdated(
        self,
        mock_get_file_stream: mock.MagicMock,  # pylint: disable=unused-argument
        mock_delete_old_documents: mock.MagicMock,
        mock_batch_writer: mock.MagicMock,  # pylint: disable=unused-argument
        mock_get_collection: mock.MagicMock,  # pylint: disable=unused-argument
        mock_firestore_client: mock.MagicMock,  # pylint: disable=unused-argument
    ) -> None:
        """Tests that the ETL Delegate deletes records with outdated timestamps"""
        mock_now = datetime(2022, 5, 1, tzinfo=timezone.utc)

        with local_project_id_override("test-project"):
            delegate = TestETLDelegate()
            with freeze_time(mock_now):
                delegate.run_etl()

        mock_delete_old_documents.assert_called_once_with(
            "test_collection", "__loadedAt", mock_now
        )
