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
"""Tests the ability for StaffRecordEtlDelegate to parse json rows."""
import os
from datetime import datetime, timezone
from unittest import TestCase
from unittest.mock import MagicMock, patch

from freezegun import freeze_time

from recidiviz.tests.workflows.etl.workflows_firestore_etl_delegate_test import (
    FakeFileStream,
)
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.workflows.etl.workflows_staff_etl_delegate import (
    WorkflowsStaffETLDelegate,
)


class WorkflowsStaffETLDelegateTest(TestCase):
    """
    Test class for the WorkflowsStaffETLDelegate
    """

    def test_supports_filename(self) -> None:
        """Test that the staff file is supported for any state"""
        delegate = WorkflowsStaffETLDelegate()

        self.assertTrue(delegate.supports_file("US_ND", "staff_record.json"))
        self.assertTrue(delegate.supports_file("US_TN", "staff_record.json"))
        self.assertTrue(
            delegate.supports_file("LITERALLY_ANYTHING", "staff_record.json")
        )
        self.assertFalse(delegate.supports_file("US_ND", "not_staff_record.json"))

    def test_transform_row(self) -> None:
        """
        Test that the transform_row method correctly parses the json
        """
        delegate = WorkflowsStaffETLDelegate()

        path_to_fixture = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "fixtures", "staff_record.json"
        )
        with open(path_to_fixture, "r", encoding="utf-8") as fp:
            fixture = fp.readline()

            doc_id, row = delegate.transform_row(fixture)
            self.assertEqual(doc_id, "100")
            self.assertEqual(
                row,
                {
                    "id": "100",
                    "stateCode": "US_XX",
                    "name": "Joey Joe-Joe Jr. III",
                    "email": "jjjj3@xx.gov",
                    "hasCaseload": True,
                    "district": "District 1",
                },
            )

            fixture = fp.readline()

            doc_id, row = delegate.transform_row(fixture)
            self.assertEqual(doc_id, "101")
            self.assertEqual(
                row,
                {
                    "id": "101",
                    "stateCode": "US_XX",
                    "name": "Sally S. Slithers",
                    "email": "sal.sli@xx.gov",
                    "hasCaseload": False,
                    "district": "District 2",
                },
            )

            fixture = fp.readline()

            doc_id, row = delegate.transform_row(fixture)
            self.assertEqual(doc_id, "102")
            self.assertEqual(
                row,
                {
                    "id": "102",
                    "stateCode": "US_XX",
                    "name": "Foghorn Leghorn",
                    "email": None,
                    "hasCaseload": True,
                    "district": "District 3",
                },
            )

    @patch("google.cloud.firestore.Client")
    @patch("recidiviz.firestore.firestore_client.FirestoreClientImpl.get_collection")
    @patch("recidiviz.firestore.firestore_client.FirestoreClientImpl.batch")
    @patch(
        "recidiviz.firestore.firestore_client.FirestoreClientImpl.delete_old_documents"
    )
    @patch(
        "recidiviz.workflows.etl.workflows_etl_delegate.WorkflowsETLDelegate.get_file_stream"
    )
    def test_run_etl_imports_with_document_id(
        self,
        mock_get_file_stream: MagicMock,
        _mock_delete_old_documents: MagicMock,
        mock_batch_writer: MagicMock,
        mock_get_collection: MagicMock,
        _mock_firestore_client: MagicMock,
    ) -> None:
        """Tests that the ETL Delegate for Staff imports the collection with the document ID."""
        mock_batch_set = MagicMock()
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
                    WorkflowsStaffETLDelegate, "transform_row"
                ) as mock_transform:
                    mock_transform.return_value = (123, {"personExternalId": 123})
                    delegate = WorkflowsStaffETLDelegate()
                    delegate.run_etl("US_TN", "staff_record.json")
                    mock_collection.document.assert_called_once_with(document_id)
                    mock_batch_set.set.assert_called_once_with(
                        mock_document_ref,
                        {
                            "personExternalId": 123,
                            "__loadedAt": mock_now,
                        },
                    )
