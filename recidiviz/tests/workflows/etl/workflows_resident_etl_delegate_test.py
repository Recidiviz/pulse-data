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
"""Tests the ability for WorkflowsResidentETLDelegate to parse json rows."""
import json
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any
from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock, patch

from freezegun import freeze_time
from google.cloud.firestore_v1.async_batch import AsyncWriteBatch
from mock.mock import call

from recidiviz.common.constants.states import StateCode
from recidiviz.tests.workflows.etl.workflows_firestore_etl_delegate_test import (
    FakeFileStream,
)
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.workflows.etl.workflows_etl_delegate import WorkflowsETLDelegate
from recidiviz.workflows.etl.workflows_resident_etl_delegate import (
    WorkflowsResidentETLDelegate,
)

RESIDENT_FILENAME = "resident_record.json"

TEST_DATA: dict[str, Any] = {
    "person_external_id": "300",
    "pseudonymized_id": "p300",
    "display_id": "d300",
    "state_code": "US_TN",
    "person_name": json.dumps(
        {
            "given_names": "STASSI",
            "middle_names": "FAKE",
            "name_suffix": "",
            "surname": "SCHROEDER",
        }
    ),
    "custody_level": "MEDIUM",
    "officer_id": "100",
    "facility_id": "ABC",
    "unit_id": "ABC 123",
    "facility_unit_id": "ABC-_-ABC 123",
    "admission_date": "2023-01-01",
    "release_date": "2027-03-28",
    "all_eligible_opportunities": ["usTnCustodyLevelDowngrade"],
    "all_ineligible_opportunities": ["usTnAnnualReclassification2026Policy"],
    "gender": "FEMALE",
    "metadata": json.dumps({"state_code": "US_TN"}),
}

EXPECTED_DOCUMENT: dict[str, Any] = {
    "pseudonymizedId": "p300",
    "personExternalId": "300",
    "displayId": "d300",
    "stateCode": "US_TN",
    "personName": {
        "givenNames": "Stassi",
        "middleNames": "Fake",
        "nameSuffix": "",
        "surname": "Schroeder",
    },
    "custodyLevel": "MEDIUM",
    "portionServedNeeded": None,
    "officerId": "100",
    "facilityId": "ABC",
    "unitId": "ABC 123",
    "facilityUnitId": "ABC-_-ABC 123",
    "gender": "FEMALE",
    "admissionDate": "2023-01-01",
    "releaseDate": "2027-03-28",
    "metadata": {"stateCode": "US_TN"},
    "sccpEligibilityDate": None,
    "allEligibleOpportunities": ["usTnCustodyLevelDowngrade"],
    "allIneligibleOpportunities": ["usTnAnnualReclassification2026Policy"],
    "usTnFacilityAdmissionDate": None,
    "usMePortionNeededEligibleDate": None,
}


class WorkflowsResidentETLDelegateTest(IsolatedAsyncioTestCase):
    """
    Test class for the WorkflowsResidentETLDelegate
    """

    def test_supports_filename(self) -> None:
        """Test that the resident_record file is supported"""
        self.assertTrue(
            WorkflowsResidentETLDelegate.supports_file(
                StateCode.US_XX, "resident_record.json"
            )
        )
        self.assertFalse(
            WorkflowsResidentETLDelegate.supports_file(
                StateCode.US_XX, "not_resident_record.json"
            )
        )

    def test_collection_name(self) -> None:
        """Test that the correct collection_name is set after initialization"""
        delegate = WorkflowsResidentETLDelegate(StateCode.US_TN, RESIDENT_FILENAME)
        self.assertEqual(delegate.collection_name, "residents")

    def test_transform_row(self) -> None:
        """
        Test that the transform_row method correctly parses the json
        """
        delegate = WorkflowsResidentETLDelegate(StateCode.US_MO, RESIDENT_FILENAME)
        doc_id, row = delegate.transform_row(TEST_DATA)
        self.assertEqual("300", doc_id)
        self.assertEqual(EXPECTED_DOCUMENT, row)

    def test_transform_row_for_us_mo_resident(self) -> None:
        """
        Test that transform_row method correctly parses the json for US_MO resident with
        RestrictiveHousingOpp and metadata blob
        """
        delegate = WorkflowsResidentETLDelegate(StateCode.US_MO, RESIDENT_FILENAME)
        data = deepcopy(TEST_DATA)
        data["state_code"] = "US_MO"
        del data["custody_level"]
        data["all_eligible_opportunities"] = [
            "usMoOverdueRestrictiveHousingReviewHearing"
        ]
        data["all_ineligible_opportunities"] = ["usMoWorkRelease"]
        data["metadata"] = json.dumps(
            {
                "top_level_key": {
                    "first_nested_key": "don't_reformat_me",
                    "second_nested_key": 17,
                },
                "another_top_level_key": "jackalope",
            }
        )

        expected = deepcopy(EXPECTED_DOCUMENT)
        expected["stateCode"] = "US_MO"
        expected["custodyLevel"] = None
        expected["allEligibleOpportunities"] = [
            "usMoOverdueRestrictiveHousingReviewHearing"
        ]
        expected["allIneligibleOpportunities"] = ["usMoWorkRelease"]
        expected["metadata"] = {
            "topLevelKey": {
                "firstNestedKey": "don't_reformat_me",
                "secondNestedKey": 17,
            },
            "anotherTopLevelKey": "jackalope",
        }

        doc_id, row = delegate.transform_row(data)
        self.assertEqual("300", doc_id)
        self.assertEqual(expected, row)

    def test_transform_row_with_missing_gender(self) -> None:
        """Test that the transform_row method handles missing gender gracefully."""
        delegate = WorkflowsResidentETLDelegate(StateCode.US_MO, RESIDENT_FILENAME)
        data = deepcopy(TEST_DATA)
        del data["gender"]
        expected = deepcopy(EXPECTED_DOCUMENT)
        expected["gender"] = None

        doc_id, row = delegate.transform_row(data)
        self.assertEqual("300", doc_id)
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
        _mock_firestore_resident: MagicMock,
        _mock_firestore_admin_resident: MagicMock,
        _mock_firestore_async_client: MagicMock,
    ) -> None:
        """Tests that the ETL Delegate for Resident imports the collection with the document ID."""
        mock_batch_set = MagicMock(AsyncWriteBatch)
        mock_batch_writer.return_value = mock_batch_set
        mock_get_file_stream.return_value = [FakeFileStream(1)]
        mock_collection = MagicMock()
        mock_get_collection.return_value = mock_collection
        mock_document_ref = MagicMock()
        mock_collection.document.return_value = mock_document_ref
        mock_now = datetime(2022, 5, 1, tzinfo=timezone.utc)
        document_id = "us_tn_123"
        with local_project_id_override("test-project"), freeze_time(
            mock_now
        ), patch.object(
            WorkflowsResidentETLDelegate, "transform_row"
        ) as mock_transform:
            mock_transform.return_value = (
                123,
                {"personExternalId": 123, "stateCode": "US_XX"},
            )
            with patch.object(
                WorkflowsETLDelegate, "pseudonymize_record"
            ) as mock_pseudonymize:
                mock_pseudonymize.return_value = (
                    {"personExternalId": 123, "stateCode": "US_XX"},
                )
                delegate = WorkflowsResidentETLDelegate(
                    StateCode.US_TN, RESIDENT_FILENAME
                )
                await delegate.run_etl()
                mock_collection.document.assert_has_calls(
                    [call(document_id), call(document_id)]
                )
                mock_batch_set.set.assert_has_calls(
                    [
                        call(
                            mock_document_ref,
                            {
                                "personExternalId": 123,
                                "stateCode": "US_XX",
                                "__loadedAt": mock_now,
                            },
                        ),
                        call(
                            mock_document_ref,
                            {
                                "personExternalId": 123,
                                "stateCode": "US_XX",
                                "__loadedAt": mock_now,
                            },
                        ),
                    ]
                )
