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
"""Tests for the Workflows ETL delegate."""
from copy import deepcopy
from typing import Any
from unittest import TestCase

from recidiviz.common.constants.states import StateCode
from recidiviz.workflows.etl.workflows_opportunity_etl_delegate import (
    WorkflowsOpportunityETLDelegate,
)
from recidiviz.workflows.types import WorkflowsSystemType

# A real (source file -> opportunity type) pairing from WORKFLOWS_OPPORTUNITY_CONFIGS,
# used to exercise the config-driven opportunity_type lookup. The test_opportunity_type
# _matches_config test guards this pairing against config drift.
TEST_OPPORTUNITY_FILENAME = (
    "us_nd_complete_discharge_early_from_supervision_record.json"
)
TEST_OPPORTUNITY_TYPE = "earlyTermination"
TEST_COLLECTION_NAME = "earlyTerminationReferrals"

TEST_DATA: dict[str, Any] = {
    "external_id": "123",
    "state_code": "US_ND",
    "form_information_crime_names": [
        "Class (A) Misdemeanor",
        "Class (A) Misdemeanor",
    ],
    "reasons": [
        {
            "criteria_name": "SUPERVISION_EARLY_DISCHARGE_DATE_WITHIN_30_DAYS",
            "reason": {"eligible_date": "2022-11-11"},
        },
        {
            "criteria_name": "US_ND_IMPLIED_VALID_EARLY_TERMINATION_SENTENCE_TYPE",
            "reason": {"supervision_type": "DEFERRED"},
        },
        {
            "criteria_name": "US_ND_IMPLIED_VALID_EARLY_TERMINATION_SUPERVISION_LEVEL",
            "reason": {"supervision_level": "MEDIUM"},
        },
        {
            "criteria_name": "US_ND_NOT_IN_ACTIVE_REVOCATION_STATUS",
            "reason": {"revocation_date": None},
        },
    ],
    "ineligible_criteria": [
        "SUPERVISION_EARLY_DISCHARGE_DATE_WITHIN_30_DAYS",
        "US_ND_NOT_IN_ACTIVE_REVOCATION_STATUS",
    ],
    "case_notes": [
        {
            "criteria": "criteria A",
            "note_body": "body1",
            "note_title": "title1",
            "event_date": "2011-03-04",
        },
        {
            "criteria": "criteria A",
            "note_body": "body2",
            "note_title": "title2",
            "event_date": "2018-08-12",
        },
        {
            "criteria": "criteria B",
            "note_body": "body3",
            "note_title": "title3",
            "event_date": "2016-06-19",
        },
    ],
    "metadata_multiple_sentences": True,
    "metadata_out_of_state": False,
    "random_field_with_metadata": False,
    "opportunity_id": "id001",
    "opportunity_pseudonymized_id": "anon001",
    "is_eligible": False,
    "is_almost_eligible": True,
}

EXPECTED_DOCUMENT: dict[str, Any] = {
    "opportunityType": TEST_OPPORTUNITY_TYPE,
    "externalId": "123",
    "stateCode": "US_ND",
    "formInformation": {
        "crimeNames": ["Class (A) Misdemeanor", "Class (A) Misdemeanor"]
    },
    "eligibleCriteria": {
        "usNdImpliedValidEarlyTerminationSentenceType": {"supervisionType": "DEFERRED"},
        "usNdImpliedValidEarlyTerminationSupervisionLevel": {
            "supervisionLevel": "MEDIUM"
        },
    },
    "ineligibleCriteria": {
        "supervisionEarlyDischargeDateWithin30Days": {"eligibleDate": "2022-11-11"},
        "usNdNotInActiveRevocationStatus": {"revocationDate": None},
    },
    "metadata": {
        "outOfState": False,
        "multipleSentences": True,
    },
    "caseNotes": {
        "criteria A": [
            {"noteTitle": "title1", "noteBody": "body1", "eventDate": "2011-03-04"},
            {"noteTitle": "title2", "noteBody": "body2", "eventDate": "2018-08-12"},
        ],
        "criteria B": [
            {"noteTitle": "title3", "noteBody": "body3", "eventDate": "2016-06-19"}
        ],
    },
    "randomFieldWithMetadata": False,
    "opportunityId": "id001",
    "opportunityPseudonymizedId": "anon001",
    "isEligible": False,
    "isAlmostEligible": True,
}


class TestWorkflowsETLDelegate(TestCase):
    """Tests for the Workflows ETL delegate."""

    def test_supports_file(self) -> None:
        """Test that expected state codes and files are supported"""
        self.assertTrue(
            WorkflowsOpportunityETLDelegate.supports_file(
                StateCode.US_ID,
                "us_ix_complete_discharge_early_from_supervision_request_record.json",
            )
        )

        self.assertTrue(
            WorkflowsOpportunityETLDelegate.supports_file(
                StateCode.US_ND,
                "us_nd_complete_discharge_early_from_supervision_record.json",
            )
        )

        self.assertFalse(
            WorkflowsOpportunityETLDelegate.supports_file(
                StateCode.US_TN,
                "us_nd_complete_discharge_early_from_supervision_record.json",
            )
        )

    def test_collection_name(self) -> None:
        """Test that the correct collection_name is set after initialization"""
        delegate = WorkflowsOpportunityETLDelegate(
            StateCode.US_TN, TEST_OPPORTUNITY_FILENAME
        )
        self.assertEqual(delegate.collection_name, TEST_COLLECTION_NAME)

    def test_opportunity_type(self) -> None:
        """Test that the correct opportunity_type is set after initialization"""
        delegate = WorkflowsOpportunityETLDelegate(
            StateCode.US_TN, TEST_OPPORTUNITY_FILENAME
        )
        self.assertEqual(delegate.opportunity_type, TEST_OPPORTUNITY_TYPE)

    def test_system_type_matches_config(self) -> None:
        delegate = WorkflowsOpportunityETLDelegate(
            StateCode.US_ND, TEST_OPPORTUNITY_FILENAME
        )
        self.assertEqual(
            WorkflowsSystemType.SUPERVISION,
            delegate.SYSTEM_TYPE_BY_FILENAME[TEST_OPPORTUNITY_FILENAME],
        )

    def test_transform_row(self) -> None:
        """Test that transform_row returns a tuple with id and document."""
        delegate = WorkflowsOpportunityETLDelegate(
            StateCode.US_ND, TEST_OPPORTUNITY_FILENAME
        )
        result = delegate.transform_row(TEST_DATA, TEST_OPPORTUNITY_FILENAME)
        self.assertEqual(("123_id001", EXPECTED_DOCUMENT), result)

    def test_build_document(self) -> None:
        """Test that the build_document method renames the keys correctly."""
        delegate = WorkflowsOpportunityETLDelegate(
            StateCode.US_ND, TEST_OPPORTUNITY_FILENAME
        )
        new_document = delegate.build_document(TEST_DATA, TEST_OPPORTUNITY_TYPE)
        self.assertDictEqual(
            EXPECTED_DOCUMENT,
            new_document,
        )

    def test_transform_row_with_prefixed_state_keys(self) -> None:
        """Test that transform_row replaces state prefixes on specified criteria."""
        delegate = WorkflowsOpportunityETLDelegate(
            StateCode.US_XX, TEST_OPPORTUNITY_FILENAME
        )
        data = deepcopy(TEST_DATA)
        data["reasons"] = [
            {
                "criteria_name": "US_XX_SUPERVISION_LEVEL_HIGHER_THAN_ASSESSMENT_LEVEL",
                "reason": {"some_value": "some_data"},
            },
        ]
        data["ineligible_criteria"] = []
        expected = deepcopy(EXPECTED_DOCUMENT)
        expected["eligibleCriteria"] = {
            "supervisionLevelHigherThanAssessmentLevel": {"someValue": "some_data"}
        }
        expected["ineligibleCriteria"] = {}

        result = delegate.transform_row(data, TEST_OPPORTUNITY_FILENAME)
        self.assertEqual(("123_id001", expected), result)

    def test_transform_row_with_ix_prefixed_state_keys(self) -> None:
        """
        Test that transform_row replaces state prefixes on specified US_IX criteria.
        """
        delegate = WorkflowsOpportunityETLDelegate(
            StateCode.US_ID, TEST_OPPORTUNITY_FILENAME
        )
        data = deepcopy(TEST_DATA)
        data["state"] = "US_ID"
        data["reasons"] = [
            {
                "criteria_name": "US_IX_SUPERVISION_LEVEL_HIGHER_THAN_ASSESSMENT_LEVEL",
                "reason": {"some_value": "some_data"},
            },
        ]
        data["ineligible_criteria"] = []
        expected = deepcopy(EXPECTED_DOCUMENT)
        expected["state"] = "US_ID"
        expected["eligibleCriteria"] = {
            "supervisionLevelHigherThanAssessmentLevel": {"someValue": "some_data"}
        }
        expected["ineligibleCriteria"] = {}

        result = delegate.transform_row(data, TEST_OPPORTUNITY_FILENAME)
        self.assertEqual(("123_id001", expected), result)

    def test_transform_row_with_ix_non_prefixed_state_keys(self) -> None:
        """Test that transform_row replaces IX with ID for the ATLAS migration."""
        delegate = WorkflowsOpportunityETLDelegate(
            StateCode.US_ID, TEST_OPPORTUNITY_FILENAME
        )
        data = deepcopy(TEST_DATA)
        data["state"] = "US_ID"
        data["reasons"] = [
            {
                "criteria_name": "US_IX_LSIR_LEVEL_LOW_MODERATE_FOR_X_DAYS",
                "reason": {"some_value": "some_data"},
            },
            {
                "criteria_name": "US_ID_INCOME_VERIFIED_WITHIN_3_MONTHS",
                "reason": {"some_value": "some_data"},
            },
        ]
        data["ineligible_criteria"] = ["US_IX_INCOME_VERIFIED_WITHIN_3_MONTHS"]
        expected = deepcopy(EXPECTED_DOCUMENT)
        expected["state"] = "US_ID"
        expected["eligibleCriteria"] = {
            "usIdLsirLevelLowModerateForXDays": {"someValue": "some_data"}
        }
        expected["ineligibleCriteria"] = {
            "usIdIncomeVerifiedWithin3Months": {"someValue": "some_data"}
        }

        result = delegate.transform_row(data, TEST_OPPORTUNITY_FILENAME)
        self.assertEqual(("123_id001", expected), result)

    def test_transform_with_empty_ineligible_criteria_field(self) -> None:
        """Tests that the delegate correctly processes the document when `ineligible_criteria` is set to `[]`."""
        delegate = WorkflowsOpportunityETLDelegate(
            StateCode.US_ND, TEST_OPPORTUNITY_FILENAME
        )
        data = deepcopy(TEST_DATA)
        data["ineligible_criteria"] = []
        expected = deepcopy(EXPECTED_DOCUMENT)
        expected["eligibleCriteria"].update(expected["ineligibleCriteria"])
        expected["ineligibleCriteria"] = {}
        new_document = delegate.build_document(data, TEST_OPPORTUNITY_TYPE)
        self.assertEqual(expected, new_document)

    def test_transform_without_ineligible_criteria_field(self) -> None:
        """Tests that the delegate can process a document without the `ineligible_criteria` field."""
        delegate = WorkflowsOpportunityETLDelegate(
            StateCode.US_ND, TEST_OPPORTUNITY_FILENAME
        )
        data = deepcopy(TEST_DATA)
        del data["ineligible_criteria"]
        expected = deepcopy(EXPECTED_DOCUMENT)
        expected["eligibleCriteria"].update(expected["ineligibleCriteria"])
        expected["ineligibleCriteria"] = {}
        new_document = delegate.build_document(data, TEST_OPPORTUNITY_TYPE)
        self.assertEqual(expected, new_document)

    def test_transform_with_null_case_notes(self) -> None:
        """Tests that the delegate can process a document with a null `case_notes` field."""
        delegate = WorkflowsOpportunityETLDelegate(
            StateCode.US_ND, TEST_OPPORTUNITY_FILENAME
        )
        data = deepcopy(TEST_DATA)
        data["case_notes"] = None
        expected = deepcopy(EXPECTED_DOCUMENT)
        expected["caseNotes"] = {}
        new_document = delegate.build_document(data, TEST_OPPORTUNITY_TYPE)
        self.assertEqual(expected, new_document)

    def test_build_document_with_scope_fields(self) -> None:
        delegate = WorkflowsOpportunityETLDelegate(
            StateCode.US_ND, TEST_OPPORTUNITY_FILENAME
        )
        data = deepcopy(TEST_DATA)
        data["officer_id"] = "OFFICER_A12"
        data["location_id"] = "District 4"
        expected = deepcopy(EXPECTED_DOCUMENT)
        expected["officerId"] = "OFFICER_A12"
        expected["locationId"] = "District 4"
        new_document = delegate.build_document(data, TEST_OPPORTUNITY_TYPE)
        self.assertEqual(expected, new_document)

    def test_transform_with_nested_criteria(self) -> None:
        """Tests that the delegate can process a document where the "reason" in a reasons blob is itself, a reasons blob."""
        delegate = WorkflowsOpportunityETLDelegate(
            StateCode.US_ND, TEST_OPPORTUNITY_FILENAME
        )
        data = deepcopy(TEST_DATA)
        data["state"] = "US_TN"
        data["reasons"] = [
            {
                "criteria_name": "US_TN_FINES_FEES_ELIGIBLE",
                "reason": [
                    {
                        "criteria_name": "HAS_FINES_FEES_BALANCE_BELOW_500",
                        "reason": {"amount_owed": 100},
                    },
                    {
                        "criteria_name": "HAS_PAYMENTS_3_CONSECUTIVE_MONTHS",
                        "reason": {
                            "amount_owed": 100,
                            "consecutive_monthly_payments": 3,
                        },
                    },
                ],
            },
        ]
        data["ineligible_criteria"] = []
        expected = deepcopy(EXPECTED_DOCUMENT)
        expected["state"] = "US_TN"
        expected["eligibleCriteria"] = {
            "usTnFinesFeesEligible": {
                "hasFinesFeesBalanceBelow500": {"amountOwed": 100},
                "hasPayments3ConsecutiveMonths": {
                    "amountOwed": 100,
                    "consecutiveMonthlyPayments": 3,
                },
            },
        }
        expected["ineligibleCriteria"] = {}

        result = delegate.transform_row(data, TEST_OPPORTUNITY_FILENAME)
        self.assertEqual(("123_id001", expected), result)

    def test_transform_with_missing_external_id(self) -> None:
        """Tests that the delegate outputs None for a document with no external id."""
        delegate = WorkflowsOpportunityETLDelegate(
            StateCode.US_TN, TEST_OPPORTUNITY_FILENAME
        )
        data = deepcopy(TEST_DATA)
        del data["external_id"]
        result = delegate.transform_row(data, TEST_OPPORTUNITY_FILENAME)
        self.assertEqual((None, None), result)

    def test_transform_with_null_external_id(self) -> None:
        """Tests that the delegate outputs None for a document with no external id."""
        delegate = WorkflowsOpportunityETLDelegate(
            StateCode.US_ND, TEST_OPPORTUNITY_FILENAME
        )
        data = deepcopy(TEST_DATA)
        del data["external_id"]
        result = delegate.transform_row(data, TEST_OPPORTUNITY_FILENAME)
        self.assertEqual((None, None), result)

    def test_transform_with_eligibility_flags_already_set(self) -> None:
        """Tests that the delegate does not override eligibility flags in input data."""
        delegate = WorkflowsOpportunityETLDelegate(
            StateCode.US_ND, TEST_OPPORTUNITY_FILENAME
        )
        data = deepcopy(TEST_DATA)
        data["is_eligible"] = False
        data["is_almost_eligible"] = False

        expected = deepcopy(EXPECTED_DOCUMENT)
        # this would have been inferred true if not for the fields we just added to data
        expected["isAlmostEligible"] = False
        new_document = delegate.build_document(data, TEST_OPPORTUNITY_TYPE)
        self.assertEqual(expected, new_document)
