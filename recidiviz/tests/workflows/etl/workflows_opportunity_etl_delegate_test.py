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


class TestWorkflowsETLDelegate(TestCase):
    """Tests for the Workflows ETL delegate."""

    def test_supports_file(self) -> None:
        """Test that expected state codes and files are supported"""
        delegate = WorkflowsOpportunityETLDelegate()
        self.assertTrue(
            delegate.supports_file(
                "US_ID",
                "us_id_complete_discharge_early_from_supervision_request_record.json",
            )
        )

        self.assertTrue(
            delegate.supports_file(
                "US_ND", "us_nd_complete_discharge_early_from_supervision_record.json"
            )
        )

        self.assertFalse(
            delegate.supports_file(
                "US_TN", "us_nd_complete_discharge_early_from_supervision_record.json"
            )
        )

    def test_transform_row(self) -> None:
        """Test that transform_row returns a tuple with id and document."""
        delegate = WorkflowsOpportunityETLDelegate()
        result = delegate.transform_row(json.dumps(TEST_DATA))
        self.assertEqual(("123", EXPECTED_DOCUMENT), result)

    def test_build_document(self) -> None:
        """Test that the build_document method renames the keys correctly."""
        delegate = WorkflowsOpportunityETLDelegate()
        new_document = delegate.build_document(TEST_DATA)
        self.assertEqual(
            EXPECTED_DOCUMENT,
            new_document,
        )

    def test_transform_with_empty_ineligible_criteria_field(self) -> None:
        """Tests that the delegate correctly processes the document when `ineligible_criteria` is set to `[]`."""
        delegate = WorkflowsOpportunityETLDelegate()
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
        delegate = WorkflowsOpportunityETLDelegate()
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
