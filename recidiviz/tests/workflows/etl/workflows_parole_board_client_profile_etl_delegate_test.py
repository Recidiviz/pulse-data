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
"""Tests for WorkflowsParoleBoardClientProfileETLDelegate."""
import unittest

from recidiviz.common.constants.states import StateCode
from recidiviz.workflows.etl.workflows_parole_board_client_profile_etl_delegate import (
    WorkflowsParoleBoardClientProfileETLDelegate,
)


class WorkflowsParoleBoardClientProfileETLDelegateTest(unittest.TestCase):
    """Tests for WorkflowsParoleBoardClientProfileETLDelegate."""

    def test_supports_only_the_profile_record_file(self) -> None:
        delegate = WorkflowsParoleBoardClientProfileETLDelegate(StateCode.US_XX)
        self.assertTrue(
            delegate.supports_file("parole_board_client_profile_record.json")
        )
        self.assertFalse(delegate.supports_file("resident_record.json"))

    def test_writes_to_the_parole_board_client_profiles_collection(self) -> None:
        delegate = WorkflowsParoleBoardClientProfileETLDelegate(StateCode.US_XX)
        self.assertEqual(
            {"parole_board_client_profile_record.json": "paroleBoardClientProfiles"},
            delegate.COLLECTION_BY_FILENAME,
        )

    def test_transform_row_keys_on_person_external_id_and_camel_cases_the_profile(
        self,
    ) -> None:
        """The person_external_id keys the document id, matching the resident's own
        document, and is left out of the document body."""
        delegate = WorkflowsParoleBoardClientProfileETLDelegate(StateCode.US_XX)
        row = (
            '{"state_code": "US_XX", "person_external_id": "A100", '
            '"pseudonymized_id": "p100", '
            '"demographics": {"resident_dob": "1980-02-03", "case_manager": "Ana Ruiz"}, '
            '"parole_hearings": [{"hearing_status": "SCHEDULED", '
            '"hearing_date": "2026-11-04"}], '
            '"active_sentences": []}'
        )

        doc_id, document = delegate.transform_row(
            row, "parole_board_client_profile_record.json"
        )

        self.assertEqual("A100", doc_id)
        self.assertEqual(
            {
                "stateCode": "US_XX",
                "pseudonymizedId": "p100",
                "demographics": {
                    "residentDob": "1980-02-03",
                    "caseManager": "Ana Ruiz",
                },
                "paroleHearings": [
                    {"hearingStatus": "SCHEDULED", "hearingDate": "2026-11-04"}
                ],
                "activeSentences": [],
            },
            document,
        )
