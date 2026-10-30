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
from recidiviz.workflows.etl.workflows_client_etl_delegate import (
    WorkflowsClientETLDelegate,
)

CLIENT_FILENAME = "client_record.json"

TEST_DATA: dict[str, Any] = {
    "person_external_id": "200",
    "pseudonymized_id": "p200",
    "display_id": "d200",
    "state_code": "US_TN",
    "person_name": json.dumps(
        {
            "given_names": "MATILDA",
            "middle_names": "",
            "name_suffix": "",
            "surname": "MOUSE-HOUSE",
        }
    ),
    "officer_id": "100",
    "supervision_type": "TN PROBATIONER",
    "supervision_level": "MEDIUM",
    "supervision_level_start": "2020-03-10",
    "all_eligible_opportunities": ["usTnCompliantReporting2025Policy"],
    "all_ineligible_opportunities": ["usTnExpiration"],
    "current_balance": 45.10,
    "special_conditions": "SPECIAL",
    "address": "123 Fake st., Faketown, Tn 12345",
    "phone_number": "8889997777",
    "expiration_date": "2022-02-28",
    "last_payment_date": "2021-12-20",
    "last_payment_amount": 10.25,
    "board_conditions": [
        {"condition": "CT", "condition_description": "COMPLETE THERAPEUTIC COMMUNITY"},
        {"condition": "CW", "condition_description": "COMMUNITY SERVICE REFERRAL"},
    ],
    "supervision_start_date": "2021-03-04",
    "district": "DISTRICT 0",
    "milestones": [
        {"type": "BIRTHDAY_THIS_MONTH", "text": "Birthday this month (February 17)"},
        {"type": "MONTHS_WITHOUT_VIOLATION", "text": "12 months without a violation"},
    ],
    "email_address": "matilda@testdomain.com",
    "current_employers": [
        {
            "name": "Mouse House",
            "address": "123 Mousey Way",
            "start_date": "2021-04-04",
        },
        {
            "name": "Denny's",
            "address": "456 Cookie Crumble Drive",
            "start_date": "2022-03-04",
        },
    ],
}

EXPECTED_DOCUMENT: dict[str, Any] = {
    "personExternalId": "200",
    "pseudonymizedId": "p200",
    "displayId": "d200",
    "stateCode": "US_TN",
    "personName": {
        "givenNames": "Matilda",
        "middleNames": "",
        "nameSuffix": "",
        "surname": "Mouse-House",
    },
    "officerId": "100",
    "supervisionType": "Probation",
    "supervisionLevel": "MEDIUM",
    "supervisionLevelStart": "2020-03-10",
    "allEligibleOpportunities": ["usTnCompliantReporting2025Policy"],
    "allIneligibleOpportunities": ["usTnExpiration"],
    "currentBalance": 45.1,
    "specialConditions": "SPECIAL",
    "address": "123 Fake st., Faketown, TN 12345",
    "phoneNumber": "8889997777",
    "expirationDate": "2022-02-28",
    "lastPaymentDate": "2021-12-20",
    "lastPaymentAmount": 10.25,
    "boardConditions": [
        {"condition": "CT", "conditionDescription": "COMPLETE THERAPEUTIC COMMUNITY"},
        {"condition": "CW", "conditionDescription": "COMMUNITY SERVICE REFERRAL"},
    ],
    "supervisionStartDate": "2021-03-04",
    "district": "DISTRICT 0",
    "milestones": [
        {"type": "BIRTHDAY_THIS_MONTH", "text": "Birthday this month (February 17)"},
        {"type": "MONTHS_WITHOUT_VIOLATION", "text": "12 months without a violation"},
    ],
    "emailAddress": "matilda@testdomain.com",
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
}


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
        delegate = WorkflowsClientETLDelegate(StateCode.US_TN)
        doc_id, row = delegate.transform_row(json.dumps(TEST_DATA), CLIENT_FILENAME)
        self.assertEqual("200", doc_id)
        self.assertEqual(EXPECTED_DOCUMENT, row)

    def test_transform_row_without_optional_fields(self) -> None:
        """
        Test that transform_row method omits optional fields that are missing rather
        than defaulting them
        """
        delegate = WorkflowsClientETLDelegate(StateCode.US_TN)
        data = deepcopy(TEST_DATA)
        del data["supervision_level"]
        del data["all_eligible_opportunities"]
        del data["all_ineligible_opportunities"]
        del data["address"]
        del data["phone_number"]
        del data["expiration_date"]
        del data["last_payment_date"]
        del data["last_payment_amount"]
        del data["supervision_start_date"]
        del data["milestones"]

        expected = deepcopy(EXPECTED_DOCUMENT)
        del expected["supervisionLevel"]
        del expected["allEligibleOpportunities"]
        del expected["allIneligibleOpportunities"]
        del expected["address"]
        del expected["phoneNumber"]
        del expected["expirationDate"]
        del expected["lastPaymentDate"]
        del expected["lastPaymentAmount"]
        del expected["supervisionStartDate"]
        del expected["milestones"]

        doc_id, row = delegate.transform_row(json.dumps(data), CLIENT_FILENAME)
        self.assertEqual("200", doc_id)
        self.assertEqual(expected, row)

    def test_transform_row_with_supervision_level_downgrade(self) -> None:
        """
        Test that transform_row method correctly parses json when the row has a
        supervision downgrade eligible
        """
        delegate = WorkflowsClientETLDelegate(StateCode.US_TN)
        data = deepcopy(TEST_DATA)
        data["all_eligible_opportunities"] = ["supervisionLevelDowngrade"]
        expected = deepcopy(EXPECTED_DOCUMENT)
        expected["allEligibleOpportunities"] = ["supervisionLevelDowngrade"]

        doc_id, row = delegate.transform_row(json.dumps(data), CLIENT_FILENAME)
        self.assertEqual("200", doc_id)
        self.assertEqual(expected, row)

    def test_transform_row_for_non_us_tn_state(self) -> None:
        """
        Test that transform_row method for US_ND correctly parses the json
        """
        delegate = WorkflowsClientETLDelegate(StateCode.US_ND)
        data = deepcopy(TEST_DATA)
        data["state_code"] = "US_ND"
        data["supervision_type"] = "DUAL"
        data["address"] = "456 Fake st., Faketown, ND 12345"
        expected = deepcopy(EXPECTED_DOCUMENT)
        expected["stateCode"] = "US_ND"
        expected["supervisionType"] = "DUAL"
        expected["address"] = "456 Fake st., Faketown, ND 12345"

        doc_id, row = delegate.transform_row(json.dumps(data), CLIENT_FILENAME)
        self.assertEqual("200", doc_id)
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
                    mock_transform.return_value = (
                        123,
                        {"personExternalId": 123, "stateCode": "US_XX"},
                    )
                    delegate = WorkflowsClientETLDelegate(StateCode.US_TN)
                    await delegate.run_etl("client_record.json")
                    mock_collection.document.assert_called_once_with(document_id)
                    mock_batch_set.set.assert_called_once_with(
                        mock_document_ref,
                        {
                            "personExternalId": 123,
                            "stateCode": "US_XX",
                            "__loadedAt": mock_now,
                        },
                    )
