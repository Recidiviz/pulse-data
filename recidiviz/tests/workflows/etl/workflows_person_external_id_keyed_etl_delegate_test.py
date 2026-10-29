# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Tests for WorkflowsPersonExternalIdKeyedETLDelegate."""

import unittest

from recidiviz.common.constants.states import StateCode
from recidiviz.workflows.etl.workflows_person_external_id_keyed_etl_delegate import (
    WorkflowsPersonExternalIdKeyedETLDelegate,
)


class _FakeRecordETLDelegate(WorkflowsPersonExternalIdKeyedETLDelegate):
    COLLECTION_BY_FILENAME = {"fake_record.json": "fakeRecords"}

    def get_supported_files(self) -> list[str]:
        return ["fake_record.json"]


class WorkflowsPersonExternalIdKeyedETLDelegateTest(unittest.TestCase):
    """Tests for WorkflowsPersonExternalIdKeyedETLDelegate."""

    def test_transform_row_keys_on_person_external_id_and_camel_cases_the_record(
        self,
    ) -> None:
        """The person_external_id keys the document id, matching the resident's own
        document, and is left out of the document body."""
        delegate = _FakeRecordETLDelegate(StateCode.US_XX)
        row = (
            '{"state_code": "US_XX", "person_external_id": "A100", '
            '"pseudonymized_id": "p100", '
            '"move_to_nici_is_eligible": false, '
            '"move_to_nici_eligible_date": null, '
            '"move_to_nici_reasons": [{"criteria_name": "US_XX_NOT_IN_NICI", '
            '"reason": {"facility_start_date": "2026-05-01"}}], '
            '"move_to_nici_ineligible_criteria": ["US_XX_NOT_IN_NICI"]}'
        )

        doc_id, document = delegate.transform_row(row, "fake_record.json")

        self.assertEqual("A100", doc_id)
        self.assertEqual(
            {
                "stateCode": "US_XX",
                "pseudonymizedId": "p100",
                "moveToNiciIsEligible": False,
                "moveToNiciEligibleDate": None,
                "moveToNiciReasons": [
                    {
                        "criteriaName": "US_XX_NOT_IN_NICI",
                        "reason": {"facilityStartDate": "2026-05-01"},
                    }
                ],
                "moveToNiciIneligibleCriteria": ["US_XX_NOT_IN_NICI"],
            },
            document,
        )
