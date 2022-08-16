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
from unittest import TestCase

from recidiviz.workflows.etl.workflows_opportunity_etl_delegate import (
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
    "metadata_multiple_sentences": True,
    "metadata_out_of_state": False,
    "random_field_with_metadata": False,
}

EXPECTED_DOCUMENT = {
    "externalId": "123",
    "formInformation": {
        "crimeNames": ["Class (A) Misdemeanor", "Class (A) Misdemeanor"]
    },
    "reasons": [
        {
            "criteriaName": "SUPERVISION_EARLY_DISCHARGE_DATE_WITHIN_30_DAYS",
            "reason": {"eligibleDate": "2022-11-11"},
        },
        {
            "criteriaName": "US_ND_IMPLIED_VALID_EARLY_TERMINATION_SENTENCE_TYPE",
            "reason": {"supervisionType": "DEFERRED"},
        },
        {
            "criteriaName": "US_ND_IMPLIED_VALID_EARLY_TERMINATION_SUPERVISION_LEVEL",
            "reason": {"supervisionLevel": "MEDIUM"},
        },
        {
            "criteriaName": "US_ND_NOT_IN_ACTIVE_REVOCATION_STATUS",
            "reason": {"revocationDate": None},
        },
    ],
    "metadata": {
        "outOfState": False,
        "multipleSentences": True,
    },
    "randomFieldWithMetadata": False,
}


class TestWorkflowsETLDelegate(TestCase):
    """Tests for the Workflows ETL delegate."""

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
