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
"""Tests the ability for WorkflowsClientETLDelegate to parse json rows."""
import os
from datetime import datetime, timezone
from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock, patch

from freezegun import freeze_time
from google.cloud.firestore_v1.async_batch import AsyncWriteBatch

from recidiviz.common.constants.states import StateCode
from recidiviz.tests.workflows.etl.workflows_firestore_etl_delegate_test import (
    FakeFileStream,
)
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.workflows.etl.workflows_client_etl_delegate import (
    WorkflowsClientETLDelegate,
)


class WorkflowsClientETLDelegateTest(IsolatedAsyncioTestCase):
    """
    Test class for the WorkflowsClientETLDelegate
    """

    def test_supports_filename(self) -> None:
        """Test that the client file is supported for any state"""
        delegate = WorkflowsClientETLDelegate(StateCode.US_ND)
        self.assertTrue(delegate.supports_file("client_record.json"))

        delegate = WorkflowsClientETLDelegate(StateCode.US_TN)
        self.assertTrue(delegate.supports_file("client_record.json"))

        delegate = WorkflowsClientETLDelegate(StateCode.US_WW)
        self.assertTrue(delegate.supports_file("client_record.json"))

        delegate = WorkflowsClientETLDelegate(StateCode.US_ND)
        self.assertFalse(delegate.supports_file("not_client_record.json"))

    def test_transform_row(self) -> None:
        """
        Test that the transform_row method correctly parses the json
        """
        path_to_fixture = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "fixtures",
            "client_record.json",
        )
        with open(path_to_fixture, "r", encoding="utf-8") as fp:
            fixture = fp.readline()

            delegate = WorkflowsClientETLDelegate(StateCode.US_TN)

            doc_id, row = delegate.transform_row(fixture)
            # US_TN first row has board conditions
            self.assertEqual(doc_id, "200")
            self.assertEqual(
                {
                    "address": "123 Fake st., Faketown, TN 12345",
                    "allEligibleOpportunities": ["compliantReporting"],
                    "currentBalance": 45.1,
                    "expirationDate": "2022-02-28",
                    "lastPaymentAmount": 10.25,
                    "lastPaymentDate": "2021-12-20",
                    "officerId": "100",
                    "personExternalId": "200",
                    "pseudonymizedId": "p200",
                    "displayId": "d200",
                    "personName": {
                        "givenNames": "Matilda",
                        "middleNames": "",
                        "nameSuffix": "",
                        "surname": "Mouse-House",
                    },
                    "phoneNumber": "8889997777",
                    "specialConditions": "SPECIAL",
                    "stateCode": "US_TN",
                    "supervisionLevel": "MEDIUM",
                    "supervisionLevelStart": "2020-03-10",
                    "supervisionType": "Probation",
                    "boardConditions": [
                        {
                            "condition": "CT",
                            "conditionDescription": "COMPLETE THERAPEUTIC COMMUNITY",
                        },
                        {
                            "condition": "CW",
                            "conditionDescription": "COMMUNITY SERVICE REFERRAL",
                        },
                    ],
                    "supervisionStartDate": "2021-03-04",
                    "district": "DISTRICT 0",
                    "milestones": [
                        {
                            "type": "BIRTHDAY_THIS_MONTH",
                            "text": "Birthday this month (February 17)",
                        },
                        {
                            "type": "MONTHS_WITHOUT_VIOLATION",
                            "text": "12 months without a violation",
                        },
                    ],
                    "emailAddress": "matilda@fake.net",
                    "currentEmployers": [
                        {
                            "name": "Mouse House",
                            "address": "123 Mousey Way",
                            "startDate": "2021-04-04",
                        },
                        {
                            "name": "Denny's",
                            "address": "456 Cookie Crumble Drive",
                            "startDate": "2022-03-04",
                        },
                    ],
                },
                row,
            )

            # US_TN second row has none of the nullable fields
            fixture = fp.readline()
            doc_id, row = delegate.transform_row(fixture)
            self.assertEqual(doc_id, "201")
            self.assertEqual(
                {
                    "personExternalId": "201",
                    "pseudonymizedId": "p201",
                    "displayId": "d201",
                    "stateCode": "US_TN",
                    "personName": {
                        "givenNames": "Harry",
                        "middleNames": "Henry",
                        "nameSuffix": "",
                        "surname": "Houdini IV",
                    },
                    "officerId": "102",
                    "currentBalance": 282,
                    "district": "DISTRICT X",
                    "supervisionType": "ISC",
                    "specialConditions": "NULL",
                    "boardConditions": [],
                    "emailAddress": "harry@fake.net",
                    "currentEmployers": [
                        {
                            "name": "The Camera Store",
                            "address": "496 Fakers Ave",
                        }
                    ],
                },
                row,
            )

            # US_TN third row has supervision downgrade eligible and milestones
            fixture = fp.readline()
            doc_id, row = delegate.transform_row(fixture)
            self.assertEqual(doc_id, "202")
            self.assertEqual(
                {
                    "personExternalId": "202",
                    "pseudonymizedId": "p202",
                    "displayId": "d202",
                    "personName": {
                        "givenNames": "Third",
                        "middleNames": "Persons",
                        "nameSuffix": "",
                        "surname": "Realname",
                    },
                    "address": "456 Fake st., Faketown, TN 12345",
                    "currentBalance": 45.1,
                    "expirationDate": "2022-02-28",
                    "lastPaymentAmount": 10.25,
                    "lastPaymentDate": "2021-12-20",
                    "officerId": "100",
                    "phoneNumber": "8889997777",
                    "specialConditions": "SPECIAL",
                    "stateCode": "US_TN",
                    "supervisionLevel": "MEDIUM",
                    "supervisionLevelStart": "2020-03-10",
                    "supervisionType": "Probation",
                    "boardConditions": [
                        {
                            "condition": "CT",
                            "conditionDescription": "COMPLETE THERAPEUTIC COMMUNITY",
                        },
                        {
                            "condition": "CW",
                            "conditionDescription": "COMMUNITY SERVICE REFERRAL",
                        },
                    ],
                    "supervisionStartDate": "2021-03-04",
                    "district": "DISTRICT 0",
                    "allEligibleOpportunities": ["supervisionLevelDowngrade"],
                    "milestones": [
                        {
                            "type": "MONTHS_WITHOUT_VIOLATION",
                            "text": "12 months without a violation",
                        },
                        {
                            "type": "MONTHS_ON_+SUPERVISION",
                            "text": "23 months on supervision",
                        },
                    ],
                    "emailAddress": "third@realname.net",
                },
                row,
            )

            # US_ND row
            delegate = WorkflowsClientETLDelegate(StateCode.US_ND)
            fixture = fp.readline()
            doc_id, row = delegate.transform_row(fixture)
            self.assertEqual(doc_id, "203")
            self.assertEqual(
                {
                    "personExternalId": "203",
                    "pseudonymizedId": "p203",
                    "displayId": "d203",
                    "stateCode": "US_ND",
                    "address": "456 Fake st., Faketown, ND 12345",
                    "phoneNumber": "8889997777",
                    "expirationDate": "2022-02-28",
                    "personName": {
                        "givenNames": "Fourth",
                        "middleNames": "Persons",
                        "nameSuffix": "",
                        "surname": "Realname",
                    },
                    "officerId": "100",
                    "supervisionType": "DUAL",
                    "supervisionLevel": "MEDIUM",
                    "supervisionLevelStart": "2020-03-10",
                    "supervisionStartDate": "2021-03-04",
                    "allEligibleOpportunities": ["earlyTermination"],
                    "metadata": {
                        "stateCode": "US_ND",
                    },
                },
                row,
            )

            # US_ID row
            delegate = WorkflowsClientETLDelegate(StateCode.US_ID)
            fixture = fp.readline()
            doc_id, row = delegate.transform_row(fixture)
            self.assertEqual(doc_id, "204")
            self.assertEqual(
                {
                    "personExternalId": "204",
                    "pseudonymizedId": "p204",
                    "displayId": "d204",
                    "stateCode": "US_ID",
                    "address": "456 Fake st., Faketown, ID 12345",
                    "expirationDate": "2022-02-28",
                    "phoneNumber": "8889997777",
                    "personName": {
                        "givenNames": "Fifth",
                        "middleNames": "Persons",
                        "nameSuffix": "",
                        "surname": "Realname",
                    },
                    "officerId": "100",
                    "supervisionType": "PROBATION",
                    "supervisionLevel": "MEDIUM",
                    "supervisionLevelStart": "2020-03-10",
                    "supervisionStartDate": "2021-03-04",
                    "allEligibleOpportunities": ["earnedDischarge"],
                    "metadata": {
                        "stateCode": "US_ID",
                        "notStateCode": "US_IX",
                    },
                },
                row,
            )

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
        """Tests that the ETL Delegate for Client imports the collection with the document ID."""
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
                    WorkflowsClientETLDelegate, "transform_row"
                ) as mock_transform:
                    mock_transform.return_value = (123, {"personExternalId": 123})
                    delegate = WorkflowsClientETLDelegate(StateCode.US_TN)
                    await delegate.run_etl("client_record.json")
                    mock_collection.document.assert_called_once_with(document_id)
                    mock_batch_set.set.assert_called_once_with(
                        mock_document_ref,
                        {
                            "personExternalId": 123,
                            "__loadedAt": mock_now,
                        },
                    )
