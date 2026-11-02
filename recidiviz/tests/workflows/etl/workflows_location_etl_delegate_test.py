#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests the ability for LocationRecordETLDelegate to parse json rows."""
import json
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any
from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock, patch

from freezegun import freeze_time
from google.cloud.firestore_v1.async_batch import AsyncWriteBatch

from recidiviz.common.constants.states import StateCode
from recidiviz.tests.workflows.etl.workflows_firestore_etl_delegate_test import (
    FakeFileStream,
)
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.workflows.etl.workflows_location_etl_delegate import (
    WorkflowsLocationETLDelegate,
)

LOCATION_FILENAME = "location_record.json"

TEST_DATA: dict[str, Any] = {
    "id": "U2",
    "location_id": "U2",
    "state_code": "US_XX",
    "name": "Unit 2",
    "system": "INCARCERATION",
    "id_type": "unitId",
}

EXPECTED_DOCUMENT: dict[str, Any] = {
    "id": "U2",
    "locationId": "U2",
    "stateCode": "US_XX",
    "name": "Unit 2",
    "system": "INCARCERATION",
    "idType": "unitId",
}


class WorkflowsLocationETLDelegateTest(IsolatedAsyncioTestCase):
    """
    Test class for the WorkflowsLocationETLDelegate
    """

    def test_supports_filename(self) -> None:
        """Test that the location file is supported for any state"""
        delegate = WorkflowsLocationETLDelegate(StateCode.US_ND)
        self.assertTrue(delegate.supports_file("location_record.json"))

        delegate = WorkflowsLocationETLDelegate(StateCode.US_TN)
        self.assertTrue(delegate.supports_file("location_record.json"))

        delegate = WorkflowsLocationETLDelegate(StateCode.US_WW)
        self.assertTrue(delegate.supports_file("location_record.json"))

        delegate = WorkflowsLocationETLDelegate(StateCode.US_ND)
        self.assertFalse(delegate.supports_file("not_location_record.json"))

    def test_transform_row(self) -> None:
        """
        Test that the transform_row method correctly parses the json
        """
        delegate = WorkflowsLocationETLDelegate(StateCode.US_XX)
        doc_id, row = delegate.transform_row(json.dumps(TEST_DATA), LOCATION_FILENAME)
        self.assertEqual("U2", doc_id)
        self.assertEqual(EXPECTED_DOCUMENT, row)

    def test_transform_row_with_special_characters_in_id(self) -> None:
        """
        Test that transform_row method leaves an apostrophe and a slash in the doc id
        since WorkflowsFirestoreETLDelegate sanitizes them before uploading
        """
        delegate = WorkflowsLocationETLDelegate(StateCode.US_XX)
        data = deepcopy(TEST_DATA)
        data["id"] = "WOMEN'S FACILITY 1/2"
        expected = deepcopy(EXPECTED_DOCUMENT)
        expected["id"] = "WOMEN'S FACILITY 1/2"

        doc_id, row = delegate.transform_row(json.dumps(data), LOCATION_FILENAME)
        self.assertEqual("WOMEN'S FACILITY 1/2", doc_id)
        self.assertEqual(expected, row)

    @patch("google.cloud.firestore_v1.AsyncClient")
    @patch("google.cloud.firestore_admin_v1.FirestoreAdminClient")
    @patch("google.cloud.firestore_v1.Client")
    @patch("recidiviz.firestore.firestore_client.FirestoreClientImpl.get_collection")
    @patch("recidiviz.firestore.firestore_client.FirestoreClientImpl.async_batch")
    @patch(
        "recidiviz.firestore.firestore_client.FirestoreClientImpl.delete_old_documents"
    )
    @patch(
        "recidiviz.workflows.etl.workflows_etl_delegate.WorkflowsETLDelegate.get_file_stream"
    )
    async def test_run_etl_imports_with_document_id(
        self,
        mock_get_file_stream: MagicMock,
        _mock_delete_old_documents: MagicMock,
        mock_batch_writer: MagicMock,
        mock_get_collection: MagicMock,
        _mock_firestore_client: MagicMock,
        _mock_firestore_admin_client: MagicMock,
        _mock_firestore_async_client: MagicMock,
    ) -> None:
        """Tests that the ETL Delegate for Location imports the collection with the document ID."""
        mock_batch_set = MagicMock(AsyncWriteBatch)
        mock_batch_writer.return_value = mock_batch_set
        mock_get_file_stream.return_value = [FakeFileStream(1)]
        mock_collection = MagicMock()
        mock_get_collection.return_value = mock_collection
        mock_document_ref = MagicMock()
        mock_collection.document.return_value = mock_document_ref
        mock_now = datetime(2022, 5, 1, tzinfo=timezone.utc)
        document_id = "us_tn_facility_1"
        with local_project_id_override("test-project"):
            with freeze_time(mock_now):
                with patch.object(
                    WorkflowsLocationETLDelegate, "transform_row"
                ) as mock_transform:
                    mock_transform.return_value = (
                        "facility_1",
                        {"id": "facility 1", "stateCode": "US_XX"},
                    )
                    delegate = WorkflowsLocationETLDelegate(StateCode.US_TN)
                    await delegate.run_etl("location_record.json")
                    mock_collection.document.assert_called_once_with(document_id)
                    mock_batch_set.set.assert_called_once_with(
                        mock_document_ref,
                        {
                            "id": "facility 1",
                            "stateCode": "US_XX",
                            "__loadedAt": mock_now,
                        },
                    )
