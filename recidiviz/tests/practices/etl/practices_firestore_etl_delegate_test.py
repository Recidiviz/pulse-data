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
"""Tests for the Firestore ETL Delegate."""
from typing import Optional, Tuple
from unittest import TestCase
from unittest.mock import patch

import mock.mock

from recidiviz.practices.etl.practices_etl_delegate import (
    MAX_FIRESTORE_RECORDS_PER_BATCH,
    PracticesFirestoreETLDelegate,
)
from recidiviz.utils.metadata import local_project_id_override


class TestETLDelegate(PracticesFirestoreETLDelegate):
    EXPORT_FILENAME = "test_export.json"
    COLLECTION_NAME = "test_collection"
    STATE_CODE = "US_XX"

    def transform_row(self, row: str) -> Tuple[str, dict]:
        return row, {"data": row}


class FakeBatchWriter(TestCase):
    def __init__(self) -> None:
        super().__init__()
        self.doc_count = 0

    def set(self, doc_id: str, doc: dict) -> None:  # pylint: disable=unused-argument
        self.doc_count += 1
        self.assertGreaterEqual(MAX_FIRESTORE_RECORDS_PER_BATCH, self.doc_count)

    def commit(self) -> None:
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


class PracticesFirestoreEtlDelegateTest(TestCase):
    """Tests for the Firestore ETL Delegate."""

    @patch("google.cloud.firestore.Client")
    @patch("recidiviz.firestore.firestore_client.FirestoreClientImpl.get_collection")
    @patch("recidiviz.firestore.firestore_client.FirestoreClientImpl.batch")
    @patch("recidiviz.firestore.firestore_client.FirestoreClientImpl.delete_collection")
    @patch(
        "recidiviz.practices.etl.practices_etl_delegate.PracticesETLDelegate.get_file_stream"
    )
    def test_run_etl_respects_batching(
        self,
        mock_get_file_stream: mock.MagicMock,
        mock_delete_collection: mock.MagicMock,
        mock_batch_writer: mock.MagicMock,
        mock_get_collection: mock.MagicMock,
        mock_firestore_client: mock.MagicMock,  # pylint: disable=unused-argument
    ) -> None:
        """Tests that the ETL Delegate respects the max batch size for writing to Firestore."""
        # mock_firestore_client.batch.return_value = FakeBatchWriter()
        mock_batch_writer.return_value = FakeBatchWriter()
        mock_get_file_stream.return_value = [FakeFileStream(3000)]
        with local_project_id_override("test-project"):
            delegate = TestETLDelegate()
            delegate.run_etl()

        mock_delete_collection.assert_called_once_with("test_collection")
        mock_get_collection.assert_called_once_with("test_collection")
