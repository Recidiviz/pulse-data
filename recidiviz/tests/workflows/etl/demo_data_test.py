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
"""Tests for the Firestore Demo ETL Delegate."""
from typing import Dict, List, Optional, Tuple
from unittest import TestCase
from unittest.mock import mock_open, patch

import mock.mock

from recidiviz.utils.metadata import local_project_id_override
from recidiviz.workflows.etl.demo_data import load_all_demo_data, load_demo_fixture
from recidiviz.workflows.etl.workflows_etl_delegate import WorkflowsFirestoreETLDelegate


class TestETLDelegate(WorkflowsFirestoreETLDelegate):
    @property
    def COLLECTION_BY_FILENAME(self) -> Dict[str, str]:
        return {"test_export.json": "test_collection"}

    def get_supported_files(self, state_code: str) -> List[str]:
        return ["test_export.json"]

    def transform_row(self, row: str) -> Tuple[str, dict]:
        return row, {"data": row}


class FakeFileStream:
    def __init__(self, total_values: int) -> None:
        self.total_values = total_values
        self.values_written = 0

    def readline(self) -> Optional[str]:
        if self.values_written >= self.total_values:
            return None

        self.values_written += 1
        return str(self.values_written)


@patch("google.cloud.firestore.Client")
@patch("recidiviz.firestore.firestore_client.FirestoreClientImpl.get_collection")
@patch("recidiviz.firestore.firestore_client.FirestoreClientImpl.batch")
@patch("recidiviz.firestore.firestore_client.FirestoreClientImpl.delete_old_documents")
class WorkflowsDemoDataTest(TestCase):
    """Tests for the Firestore demo ETL feature."""

    @patch("recidiviz.workflows.etl.demo_data.open", mock_open())
    def test_uses_demo_collection(
        self,
        _mock_delete_old_documents: mock.MagicMock,
        _mock_batch_method: mock.MagicMock,
        mock_get_collection: mock.MagicMock,
        _mock_firestore_client: mock.MagicMock,
    ) -> None:
        """Tests that the ETL Delegate uses collection with a demo prefix"""

        with local_project_id_override("test-project"):
            load_demo_fixture(TestETLDelegate, "US_TN", "test_export.json")

        mock_get_collection.assert_called_once_with("DEMO_test_collection")

    def test_fixtures(
        self,
        _mock_delete_old_documents: mock.MagicMock,
        mock_batch_method: mock.MagicMock,
        _mock_get_collection: mock.MagicMock,
        _mock_firestore_client: mock.MagicMock,
    ) -> None:
        """Verifies that the delegates used for demo data can process their fixtures successfully"""

        mock_batch_writer = mock.MagicMock()
        mock_batch_method.return_value = mock_batch_writer

        with local_project_id_override("test-project"):
            # the main thing we are testing here is that this does not raise an exception;
            # not a 100% guarantee that the demo data will be OK but should at least catch
            # the nastiest surprises (like there are new expected fields that haven't been
            #  added to the fixture)
            load_all_demo_data()

        # sanity check that all the pipelines ran: there are three fixtures,
        # we expect one commit for each since they are within the batch limit
        self.assertEqual(len(mock_batch_writer.commit.call_args_list), 3)
