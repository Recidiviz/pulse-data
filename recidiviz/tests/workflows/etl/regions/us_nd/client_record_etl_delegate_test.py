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
"""Tests the ability for US_ND ClientRecordEtlDelegate to parse json rows."""
import os
from datetime import datetime, timezone
from unittest import TestCase
from unittest.mock import MagicMock, patch

from freezegun import freeze_time

from recidiviz.tests.workflows.etl.workflows_firestore_etl_delegate_test import (
    FakeFileStream,
)
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.workflows.etl.regions.us_nd.client_record_etl_delegate import (
    ClientRecordETLDelegate,
)


class ClientRecordEtlDelegateTest(TestCase):
    """
    Test class for the ClientRecordEtlDelegate
    """

    def test_transform_row(self) -> None:
        """
        Test that the transform_row method correctly parses the json
        """
        self.maxDiff = None
        delegate = ClientRecordETLDelegate()

        path_to_fixture = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "fixtures",
            "client_record.json",
        )
        with open(path_to_fixture, "r", encoding="utf-8") as fp:
            # first row is comprehensive
            fixture = fp.readline()
            doc_id, row = delegate.transform_row(fixture)
            self.assertEqual(doc_id, "200")
            self.assertEqual(
                row,
                {
                    "address": "123 Fake st., Faketown, ND 12345",
                    "earlyTerminationEligible": True,
                    "expirationDate": "2022-02-28",
                    "officerId": "100",
                    "personExternalId": "200",
                    "pseudonymizedId": "p200",
                    "personName": {
                        "givenNames": "Matilda",
                        "middleNames": "",
                        "nameSuffix": "",
                        "surname": "Mouse-House",
                    },
                    "phoneNumber": "8889997777",
                    "stateCode": "US_XX",
                    "supervisionLevel": "MEDIUM",
                    "supervisionLevelStart": "2020-03-10",
                    "supervisionType": "PROBATION",
                    "supervisionStartDate": "2021-03-04",
                },
            )

            # second row has none of the nullable fields
            fixture = fp.readline()
            doc_id, row = delegate.transform_row(fixture)
            self.assertEqual(doc_id, "201")
            self.assertEqual(
                row,
                {
                    "personExternalId": "201",
                    "pseudonymizedId": "p201",
                    "stateCode": "US_XX",
                    "personName": {
                        "givenNames": "Harry",
                        "middleNames": "Henry",
                        "nameSuffix": "",
                        "surname": "Houdini IV",
                    },
                    "officerId": "102",
                    "supervisionType": "PAROLE",
                },
            )

            # third row has almost-eligible data
            fixture = fp.readline()
            doc_id, row = delegate.transform_row(fixture)
            self.assertEqual(doc_id, "202")
            self.assertEqual(
                row,
                {
                    "personExternalId": "202",
                    "pseudonymizedId": "p202",
                    "personName": {
                        "givenNames": "Third",
                        "middleNames": "Persons",
                        "nameSuffix": "",
                        "surname": "Realname",
                    },
                    "address": "456 Fake st., Faketown, ND 12345",
                    "expirationDate": "2022-02-28",
                    "officerId": "100",
                    "phoneNumber": "8889997777",
                    "stateCode": "US_XX",
                    "supervisionLevel": "MAXIMUM",
                    "supervisionLevelStart": "2020-03-10",
                    "supervisionType": "DUAL",
                    "supervisionStartDate": "2021-03-04",
                    "earlyTerminationEligible": True,
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
        mock_delete_old_documents: MagicMock,  # pylint: disable=unused-argument
        mock_batch_writer: MagicMock,
        mock_get_collection: MagicMock,
        mock_firestore_client: MagicMock,  # pylint: disable=unused-argument
    ) -> None:
        """Tests that the ETL Delegate for Clients imports the collection with the document ID."""
        mock_batch_set = MagicMock()
        mock_batch_writer.return_value = mock_batch_set
        mock_get_file_stream.return_value = [FakeFileStream(1)]
        mock_collection = MagicMock()
        mock_get_collection.return_value = mock_collection
        mock_document_ref = MagicMock()
        mock_collection.document.return_value = mock_document_ref
        mock_now = datetime(2022, 5, 1, tzinfo=timezone.utc)
        document_id = "us_nd_123"
        with local_project_id_override("test-project"):
            with freeze_time(mock_now):
                with patch.object(
                    ClientRecordETLDelegate, "transform_row"
                ) as mock_transform:
                    mock_transform.return_value = (123, {"personExternalId": 123})
                    delegate = ClientRecordETLDelegate()
                    delegate.run_etl()
                    mock_collection.document.assert_called_once_with(document_id)
                    mock_batch_set.set.assert_called_once_with(
                        mock_document_ref,
                        {
                            "personExternalId": 123,
                            "__loadedAt": mock_now,
                        },
                    )
