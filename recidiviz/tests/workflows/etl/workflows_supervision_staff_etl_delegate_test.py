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
"""Tests the ability for SupervisionStaffRecordEtlDelegate to parse json rows."""

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
from recidiviz.workflows.etl.workflows_supervision_staff_etl_delegate import (
    WorkflowsSupervisionStaffETLDelegate,
)

SUPERVISION_STAFF_FILENAME = "supervision_staff_record.json"

TEST_DATA: dict[str, Any] = {
    "id": "100",
    "staff_external_id": "100",
    "state_code": "US_XX",
    "email": "fakename@testdomain.com",
    "district": "District 1",
    "given_names": "JOEY",
    "surname": "JOE-JOE JR. III",
    "role_subtype": "SUPERVISION_OFFICER",
    "supervisor_external_id": "SUPER123",
    "pseudonymized_id": "p100",
}

EXPECTED_DOCUMENT: dict[str, Any] = {
    "id": "100",
    "staffExternalId": "100",
    "stateCode": "US_XX",
    "email": "fakename@testdomain.com",
    "district": "District 1",
    "givenNames": "Joey",
    "surname": "Joe-Joe Jr. III",
    "roleSubtype": "SUPERVISION_OFFICER",
    "supervisorExternalId": "SUPER123",
    "pseudonymizedId": "p100",
}


class WorkflowsSupervisionStaffETLDelegateTest(IsolatedAsyncioTestCase):
    """
    Test class for the WorkflowsSupervisionStaffETLDelegate
    """

    def test_supports_filename(self) -> None:
        """Test that the supervision_staff_record file is supported for any state"""
        delegate = WorkflowsSupervisionStaffETLDelegate(StateCode.US_ND)
        self.assertTrue(delegate.supports_file("supervision_staff_record.json"))

        delegate = WorkflowsSupervisionStaffETLDelegate(StateCode.US_TN)
        self.assertTrue(delegate.supports_file("supervision_staff_record.json"))

        delegate = WorkflowsSupervisionStaffETLDelegate(StateCode.US_WW)
        self.assertTrue(delegate.supports_file("supervision_staff_record.json"))

        delegate = WorkflowsSupervisionStaffETLDelegate(StateCode.US_ND)
        self.assertFalse(delegate.supports_file("not_supervision_staff_record.json"))

    def test_transform_row(self) -> None:
        """
        Test that the transform_row method correctly parses the json
        """
        delegate = WorkflowsSupervisionStaffETLDelegate(StateCode.US_XX)
        doc_id, row = delegate.transform_row(TEST_DATA, SUPERVISION_STAFF_FILENAME)
        self.assertEqual("100", doc_id)
        self.assertEqual(EXPECTED_DOCUMENT, row)

    def test_transform_row_missing_fields(self) -> None:
        """
        Test that transform_row method omits email, supervisorExternalId, roleSubtype
        when they are missing
        """
        delegate = WorkflowsSupervisionStaffETLDelegate(StateCode.US_XX)
        data = deepcopy(TEST_DATA)
        del data["email"]
        del data["role_subtype"]
        del data["supervisor_external_id"]
        expected = deepcopy(EXPECTED_DOCUMENT)
        del expected["email"]
        del expected["roleSubtype"]
        del expected["supervisorExternalId"]

        doc_id, row = delegate.transform_row(data, SUPERVISION_STAFF_FILENAME)
        self.assertEqual("100", doc_id)
        self.assertEqual(expected, row)

    def test_transform_row_with_missing_names(self) -> None:
        """
        Test that transform_row method defaults the names to empty strings when they are
        missing
        """
        delegate = WorkflowsSupervisionStaffETLDelegate(StateCode.US_XX)
        data = deepcopy(TEST_DATA)
        del data["given_names"]
        del data["surname"]
        expected = deepcopy(EXPECTED_DOCUMENT)
        expected["givenNames"] = ""
        expected["surname"] = ""

        doc_id, row = delegate.transform_row(data, SUPERVISION_STAFF_FILENAME)
        self.assertEqual("100", doc_id)
        self.assertEqual(expected, row)

    def test_transform_row_with_us_ix_state_specific_data(self) -> None:
        """
        Test that transform_row method parses the state_specific_data JSON string and
        remaps a US_IX state code within it to US_ID
        """
        delegate = WorkflowsSupervisionStaffETLDelegate(StateCode.US_XX)
        data = deepcopy(TEST_DATA)
        data["state_specific_data"] = {
            "state_code": "US_IX",
            "some_field": "some_value",
        }
        expected = deepcopy(EXPECTED_DOCUMENT)
        expected["stateSpecificData"] = {
            "stateCode": "US_ID",
            "someField": "some_value",
        }

        doc_id, row = delegate.transform_row(data, SUPERVISION_STAFF_FILENAME)
        self.assertEqual("100", doc_id)
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
        """Tests that the ETL Delegate for Staff imports the collection with the document ID."""
        mock_batch_set = MagicMock(AsyncWriteBatch)
        mock_batch_writer.return_value = mock_batch_set
        mock_get_file_stream.return_value = [FakeFileStream(1)]
        mock_collection = MagicMock()
        mock_get_collection.return_value = mock_collection
        mock_document_ref = MagicMock()
        mock_collection.document.return_value = mock_document_ref
        mock_now = datetime(2022, 5, 1, tzinfo=timezone.utc)
        document_id = "us_tn_123"
        with local_project_id_override("test-project"):
            with freeze_time(mock_now):
                with patch.object(
                    WorkflowsSupervisionStaffETLDelegate, "transform_row"
                ) as mock_transform:
                    mock_transform.return_value = (
                        123,
                        {"personExternalId": 123, "stateCode": "US_XX"},
                    )
                    delegate = WorkflowsSupervisionStaffETLDelegate(StateCode.US_TN)
                    await delegate.run_etl("supervision_staff_record.json")
                    mock_collection.document.assert_called_once_with(document_id)
                    mock_batch_set.set.assert_called_once_with(
                        mock_document_ref,
                        {
                            "personExternalId": 123,
                            "stateCode": "US_XX",
                            "__loadedAt": mock_now,
                        },
                    )
