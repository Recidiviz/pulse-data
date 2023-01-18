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
import json
from copy import deepcopy
from unittest import TestCase

from recidiviz.common.constants.states import StateCode
from recidiviz.workflows.etl.workflows_opportunity_etl_delegate import (
    CONFIG_BY_STATE,
    WorkflowsOpportunityETLDelegate,
)

TEST_DATA = {
    "external_id": "123",
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
}

EXPECTED_DOCUMENT = {
    "externalId": "123",
    "formInformation": {
        "crimeNames": ["Class (A) Misdemeanor", "Class (A) Misdemeanor"]
    },
    "criteria": {
        "supervisionEarlyDischargeDateWithin30Days": {"eligibleDate": "2022-11-11"},
        "usNdImpliedValidEarlyTerminationSentenceType": {"supervisionType": "DEFERRED"},
        "usNdImpliedValidEarlyTerminationSupervisionLevel": {
            "supervisionLevel": "MEDIUM"
        },
        "usNdNotInActiveRevocationStatus": {"revocationDate": None},
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
}

TEST_DATA_WITH_PREFIX_TO_STRIP = {
    "external_id": "456",
    "reasons": [
        {
            "criteria_name": "US_XX_SUPERVISION_LEVEL_HIGHER_THAN_ASSESSMENT_LEVEL",
            "reason": {"some_value": "some_data"},
        },
    ],
}

TEST_DATA_FOR_IX_WITH_PREFIX_TO_STRIP = {
    "external_id": "456",
    "reasons": [
        {
            "criteria_name": "US_IX_SUPERVISION_LEVEL_HIGHER_THAN_ASSESSMENT_LEVEL",
            "reason": {"some_value": "some_data"},
        },
    ],
}

EXPECTED_DOCUMENT_WITH_PREFIX_STRIPPED = {
    "externalId": "456",
    "criteria": {
        "supervisionLevelHigherThanAssessmentLevel": {"someValue": "some_data"}
    },
    "eligibleCriteria": {
        "supervisionLevelHigherThanAssessmentLevel": {"someValue": "some_data"}
    },
    "formInformation": {},
    "ineligibleCriteria": {},
    "metadata": {},
    "caseNotes": {},
}

# Test data specific to IX for the ATLAS migration
TEST_DATA_FOR_IX_WITHOUT_PREFIX_TO_STRIP = {
    "external_id": "456",
    "reasons": [
        {
            "criteria_name": "US_IX_LSIR_LEVEL_LOW_MODERATE_FOR_X_DAYS",
            "reason": {"some_value": "some_data"},
        },
        {
            "criteria_name": "US_ID_INCOME_VERIFIED_WITHIN_3_MONTHS",
            "reason": {"some_value": "some_data"},
        },
    ],
    "ineligible_criteria": ["US_IX_INCOME_VERIFIED_WITHIN_3_MONTHS"],
}

EXPECTED_IX_DOCUMENT_WITHOUT_PREFIX_STRIPPED = {
    "externalId": "456",
    "criteria": {
        "usIdLsirLevelLowModerateForXDays": {"someValue": "some_data"},
        "usIdIncomeVerifiedWithin3Months": {"someValue": "some_data"},
    },
    "eligibleCriteria": {
        "usIdLsirLevelLowModerateForXDays": {"someValue": "some_data"}
    },
    "formInformation": {},
    "ineligibleCriteria": {
        "usIdIncomeVerifiedWithin3Months": {"someValue": "some_data"}
    },
    "metadata": {},
    "caseNotes": {},
}


class TestWorkflowsETLDelegate(TestCase):
    """Tests for the Workflows ETL delegate."""

    def test_supports_file(self) -> None:
        """Test that expected state codes and files are supported"""
        delegate = WorkflowsOpportunityETLDelegate(StateCode.US_ID)
        self.assertTrue(
            delegate.supports_file(
                "us_ix_complete_discharge_early_from_supervision_request_record.json"
            )
        )

        delegate = WorkflowsOpportunityETLDelegate(StateCode.US_ND)
        self.assertTrue(
            delegate.supports_file(
                "us_nd_complete_discharge_early_from_supervision_record.json"
            )
        )

        delegate = WorkflowsOpportunityETLDelegate(StateCode.US_TN)
        self.assertFalse(
            delegate.supports_file(
                "us_nd_complete_discharge_early_from_supervision_record.json"
            )
        )

    def test_transform_row(self) -> None:
        """Test that transform_row returns a tuple with id and document."""
        delegate = WorkflowsOpportunityETLDelegate(StateCode.US_ND)
        result = delegate.transform_row(json.dumps(TEST_DATA))
        self.assertEqual(("123", EXPECTED_DOCUMENT), result)

    def test_build_document(self) -> None:
        """Test that the build_document method renames the keys correctly."""
        delegate = WorkflowsOpportunityETLDelegate(StateCode.US_ND)
        new_document = delegate.build_document(TEST_DATA)
        self.assertEqual(
            EXPECTED_DOCUMENT,
            new_document,
        )

    def test_transform_row_with_prefixed_state_keys(self) -> None:
        """Test that transform_row replaces state prefixes on specified criteria."""
        delegate = WorkflowsOpportunityETLDelegate(StateCode.US_XX)
        result = delegate.transform_row(json.dumps(TEST_DATA_WITH_PREFIX_TO_STRIP))
        self.assertEqual(("456", EXPECTED_DOCUMENT_WITH_PREFIX_STRIPPED), result)

    def test_transform_row_with_ix_prefixed_state_keys(self) -> None:
        """Test that transform_row replaces state prefixes on specified criteria."""
        delegate = WorkflowsOpportunityETLDelegate(StateCode.US_ID)
        result = delegate.transform_row(
            json.dumps(TEST_DATA_FOR_IX_WITH_PREFIX_TO_STRIP)
        )
        self.assertEqual(("456", EXPECTED_DOCUMENT_WITH_PREFIX_STRIPPED), result)

    def test_transform_row_with_ix_non_prefixed_state_keys(self) -> None:
        """Test that transform_row replaces IX with ID for the ATLAS migration."""
        delegate = WorkflowsOpportunityETLDelegate(StateCode.US_ID)
        result = delegate.transform_row(
            json.dumps(TEST_DATA_FOR_IX_WITHOUT_PREFIX_TO_STRIP)
        )
        self.assertEqual(("456", EXPECTED_IX_DOCUMENT_WITHOUT_PREFIX_STRIPPED), result)

    def test_transform_with_empty_ineligible_criteria_field(self) -> None:
        """Tests that the delegate correctly processes the document when `ineligible_criteria` is set to `[]`."""
        delegate = WorkflowsOpportunityETLDelegate(StateCode.US_ND)
        data = deepcopy(TEST_DATA)
        data["ineligible_criteria"] = []
        expected = deepcopy(EXPECTED_DOCUMENT)
        expected["ineligibleCriteria"] = {}  # type: ignore
        expected["eligibleCriteria"] = expected["criteria"]  # type: ignore
        new_document = delegate.build_document(data)
        self.assertEqual(
            expected,
            new_document,
        )

    def test_transform_without_ineligible_criteria_field(self) -> None:
        """Tests that the delegate can process a document without the `ineligible_criteria` field."""
        delegate = WorkflowsOpportunityETLDelegate(StateCode.US_ND)
        data = deepcopy(TEST_DATA)
        del data["ineligible_criteria"]
        expected = deepcopy(EXPECTED_DOCUMENT)
        expected["ineligibleCriteria"] = {}  # type: ignore
        expected["eligibleCriteria"] = expected["criteria"]  # type: ignore
        new_document = delegate.build_document(data)
        self.assertEqual(
            expected,
            new_document,
        )


class TestWorkflowsETLConfig(TestCase):
    """Checks constraints on the ETL config"""

    def test_source_filename_format(self) -> None:
        """Tests for common mistakes one can make when configuring filenames"""
        for configs in CONFIG_BY_STATE.values():
            for config in configs:
                # filename must have json extension
                self.assertEqual(".json", config.source_filename[-5:])

                # filename (unlike corresponding view) must not have materialized suffix
                self.assertNotRegex(config.source_filename, r"_materialized.json$")
