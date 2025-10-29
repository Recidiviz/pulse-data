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
from recidiviz.workflows.etl.workflows_resident_etl_delegate import (
    WorkflowsResidentETLDelegate,
)


class WorkflowsResidentETLDelegateTest(IsolatedAsyncioTestCase):
    """
    Test class for the WorkflowsResidentETLDelegate
    """

    def test_supports_filename(self) -> None:
        """Test that the resident file is supported for any state"""
        delegate = WorkflowsResidentETLDelegate(StateCode.US_ND)
        self.assertTrue(delegate.supports_file("resident_record.json"))

        delegate = WorkflowsResidentETLDelegate(StateCode.US_TN)
        self.assertTrue(delegate.supports_file("resident_record.json"))

        delegate = WorkflowsResidentETLDelegate(StateCode.US_ID)
        self.assertTrue(delegate.supports_file("resident_record.json"))

        delegate = WorkflowsResidentETLDelegate(StateCode.US_WW)
        self.assertTrue(delegate.supports_file("resident_record.json"))

        delegate = WorkflowsResidentETLDelegate(StateCode.US_ND)
        self.assertFalse(delegate.supports_file("not_resident_record.json"))

    def test_transform_row(self) -> None:
        """
        Test that the transform_row method correctly parses the json
        """
        path_to_fixture = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "fixtures",
            "resident_record.json",
        )

        with open(path_to_fixture, "r", encoding="utf-8") as fp:
            fixture = fp.readline()
            delegate = WorkflowsResidentETLDelegate(StateCode.US_MO)

            doc_id, row = delegate.transform_row(fixture)
            # First row US_MO Resident with RestrictiveHousingOpp
            # and metadata blob
            self.assertEqual(doc_id, "300")
            self.assertEqual(
                {
                    "pseudonymizedId": "p300",
                    "personExternalId": "300",
                    "displayId": "d300",
                    "stateCode": "US_MO",
                    "personName": {
                        "givenNames": "Stassi",
                        "middleNames": "Fake",
                        "nameSuffix": "",
                        "surname": "Schroeder",
                    },
                    "custodyLevel": None,
                    "portionServedNeeded": None,
                    "officerId": "100",
                    "facilityId": "ABC",
                    "unitId": "ABC 123",
                    "facilityUnitId": "ABC-_-ABC 123",
                    "gender": "FEMALE",
                    "admissionDate": "2023-01-01",
                    "releaseDate": "2027-03-28",
                    "metadata": {
                        "topLevelKey": {
                            "firstNestedKey": "don't_reformat_me",
                            "secondNestedKey": 17,
                        },
                        "anotherTopLevelKey": "jackalope",
                    },
                    "sccpEligibilityDate": None,
                    "allEligibleOpportunities": [
                        "usMoOverdueRestrictiveHousingReviewHearing"
                    ],
                    "usTnFacilityAdmissionDate": None,
                    "usMePortionNeededEligibleDate": None,
                },
                row,
            )

            fixture = fp.readline()
            delegate = WorkflowsResidentETLDelegate(StateCode.US_ME)

            doc_id, row = delegate.transform_row(fixture)
            # Second row US_ME Resident with all Opp
            self.assertEqual(doc_id, "301")
            self.assertEqual(
                {
                    "pseudonymizedId": "p301",
                    "personExternalId": "301",
                    "displayId": "d301",
                    "stateCode": "US_ME",
                    "personName": {
                        "givenNames": "Kristen",
                        "middleNames": "Fake",
                        "nameSuffix": "",
                        "surname": "Doute",
                    },
                    "custodyLevel": "MINIMUM",
                    "portionServedNeeded": "1/2",
                    "officerId": "100",
                    "facilityId": "FAKE CORRECTIONAL FACILITY",
                    "unitId": "UNIT 1",
                    "facilityUnitId": "FAKE CORRECTIONAL FACILITY-_-UNIT 1",
                    "gender": "FEMALE",
                    "admissionDate": "2023-01-01",
                    "releaseDate": "2024-03-28",
                    "metadata": {},
                    "sccpEligibilityDate": "2023-08-14",
                    "usMePortionNeededEligibleDate": "2023-08-14",
                    "allEligibleOpportunities": [
                        "usMeSCCP",
                        "usMeWorkRelease",
                        "usMeFurloughRelease",
                    ],
                    "usTnFacilityAdmissionDate": None,
                },
                row,
            )

            fixture = fp.readline()
            delegate = WorkflowsResidentETLDelegate(StateCode.US_ME)

            doc_id, row = delegate.transform_row(fixture)
            # Third row US_ME Resident 2/3
            self.assertEqual(doc_id, "302")
            self.assertEqual(
                {
                    "pseudonymizedId": "p302",
                    "personExternalId": "302",
                    "displayId": "d302",
                    "stateCode": "US_ME",
                    "personName": {
                        "givenNames": "Jax",
                        "middleNames": "Fake",
                        "nameSuffix": "",
                        "surname": "Taylor",
                    },
                    "custodyLevel": "COMMUNITY",
                    "portionServedNeeded": "2/3",
                    "officerId": "100",
                    "facilityId": "FAKE VIEW CORRECTIONAL FACILITY",
                    "unitId": "UNIT 1",
                    "facilityUnitId": "FAKE VIEW CORRECTIONAL FACILITY-_-UNIT 1",
                    "gender": "MALE",
                    "admissionDate": "2020-07-11",
                    "releaseDate": "2026-01-23",
                    "metadata": {},
                    "sccpEligibilityDate": "2023-03-17",
                    "usMePortionNeededEligibleDate": "2024-03-20",
                    "allEligibleOpportunities": [
                        "usMeWorkRelease",
                        "usMeFurloughRelease",
                    ],
                    "usTnFacilityAdmissionDate": None,
                },
                row,
            )

            fixture = fp.readline()
            delegate = WorkflowsResidentETLDelegate(StateCode.US_TN)

            doc_id, row = delegate.transform_row(fixture)
            # Fourth row US_TN
            self.assertEqual(doc_id, "303")
            self.assertEqual(
                {
                    "pseudonymizedId": "p303",
                    "personExternalId": "303",
                    "displayId": "d303",
                    "stateCode": "US_TN",
                    "personName": {
                        "givenNames": "James",
                        "middleNames": "Fake",
                        "nameSuffix": "",
                        "surname": "Kennedy",
                    },
                    "custodyLevel": "MEDIUM",
                    "portionServedNeeded": None,
                    "officerId": "100",
                    "facilityId": "FACILITY NAME",
                    "unitId": None,
                    "facilityUnitId": "FACILITY NAME-_-",
                    "gender": "UNKNOWN",
                    "admissionDate": "2023-05-01",
                    "releaseDate": "2024-05-01",
                    "metadata": {
                        "stateCode": "US_TN",
                    },
                    "sccpEligibilityDate": None,
                    "allEligibleOpportunities": ["usTnCustodyLevelDowngrade"],
                    "usTnFacilityAdmissionDate": None,
                    "usMePortionNeededEligibleDate": None,
                },
                row,
            )

            fixture = fp.readline()
            delegate = WorkflowsResidentETLDelegate(StateCode.US_ID)

            doc_id, row = delegate.transform_row(fixture)
            # Fifth row US_ID resident with metadata blob
            # ETL detegate should update the state code in the blob to US_ID
            self.assertEqual(doc_id, "304")
            self.assertEqual(
                {
                    "pseudonymizedId": "p304",
                    "personExternalId": "304",
                    "displayId": "d304",
                    "stateCode": "US_ID",
                    "personName": {
                        "givenNames": "Harlan",
                        "middleNames": "Faux",
                        "nameSuffix": "",
                        "surname": "Martian",
                    },
                    "custodyLevel": None,
                    "portionServedNeeded": None,
                    "officerId": "100",
                    "facilityId": "ABC",
                    "unitId": "ABC 123",
                    "facilityUnitId": "ABC-_-ABC 123",
                    "gender": "FEMALE",
                    "admissionDate": "2023-01-01",
                    "releaseDate": "2027-03-28",
                    "metadata": {
                        "stateCode": "US_ID",
                        "anotherTopLevelKey": "US_IX",
                    },
                    "sccpEligibilityDate": None,
                    "allEligibleOpportunities": [
                        "usMoOverdueRestrictiveHousingReviewHearing"
                    ],
                    "usTnFacilityAdmissionDate": None,
                    "usMePortionNeededEligibleDate": None,
                },
                row,
            )

    def test_transform_row_with_missing_gender(self) -> None:
        """Test that the transform_row method handles missing gender gracefully."""
        # Create test data with missing gender
        test_data = {
            "person_external_id": "TEST123",
            "pseudonymized_id": "pTEST123",
            "display_id": "dTEST123",
            "state_code": "US_TN",
            "person_name": '{"given_names":"TEST","middle_names":"","name_suffix":"","surname":"USER"}',
            "officer_id": "OFFICER123",
            "facility_id": "FACILITY123",
            "unit_id": "UNIT123",
            "facility_unit_id": "FACILITY123-_-UNIT123",
            "admission_date": "2023-01-01",
            "release_date": "2024-01-01",
            "all_eligible_opportunities": [],
            # Explicitly omitting gender field to simulate null/missing value
        }

        delegate = WorkflowsResidentETLDelegate(StateCode.US_TN)
        doc_id, row = delegate.transform_row(json.dumps(test_data))

        self.assertEqual(doc_id, "TEST123")
        self.assertIsNone(row["gender"])

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
            mock_transform.return_value = (123, {"personExternalId": 123})
            delegate = WorkflowsResidentETLDelegate(StateCode.US_TN)
            await delegate.run_etl("resident_record.json")
            mock_collection.document.assert_called_once_with(document_id)
            mock_batch_set.set.assert_called_once_with(
                mock_document_ref,
                {
                    "personExternalId": 123,
                    "__loadedAt": mock_now,
                },
            )
