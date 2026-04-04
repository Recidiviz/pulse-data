# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Tests for WorkflowsUsTnWriteTEPENoteToTomisRequest format_request method."""
import datetime
import json
from typing import Generator
from unittest import TestCase

import freezegun
import pytest
from flask import Flask
from mock import MagicMock, patch

from recidiviz.case_triage.workflows.constants import WorkflowsUsTnVotersRightsCode
from recidiviz.case_triage.workflows.writeback.us_tn_contact_note import (
    WorkflowsUsTnWriteTEPENoteToTomisRequest,
)

PERSON_EXTERNAL_ID = "123"
STAFF_ID = "456"


@pytest.fixture(autouse=True)
def app_context() -> Generator[None, None, None]:
    test_app = Flask("test_workflows_authorization")
    with test_app.app_context():
        yield


class TestWorkflowsUsTnWriteTEPENoteToTomisRequest(TestCase):
    """Test class for the TEPE note request formatter"""

    @patch("recidiviz.case_triage.workflows.writeback.us_tn_contact_note.get_secret")
    @patch(
        "recidiviz.case_triage.workflows.writeback.us_tn_contact_note.in_gcp_production"
    )
    @freezegun.freeze_time("2000-12-30")
    def test_workflows_us_tn_write_tepe_note_to_tomis_request_format_request(
        self, mock_in_prod: MagicMock, mock_secret: MagicMock
    ) -> None:
        mock_in_prod.return_value = False
        mock_secret.return_value = "test-id"
        request = WorkflowsUsTnWriteTEPENoteToTomisRequest(
            offender_id=PERSON_EXTERNAL_ID,
            staff_id=STAFF_ID,
            contact_note_date_time=datetime.datetime.now().isoformat(),
            contact_sequence_number=1,
            comments=["line 1", "line 2"],
        )

        actual = request.format_request()

        expected = json.dumps(
            {
                "ContactNoteDateTime": "2000-12-30T00:00:00",
                "OffenderId": "test-id",
                "StaffId": "test-id",
                "ContactTypeCode1": "TEPE",
                "ContactSequenceNumber": 1,
                "Comment1": "line 1",
                "Comment2": "line 2",
            }
        )

        self.assertEqual(actual, expected)

    @patch("recidiviz.case_triage.workflows.writeback.us_tn_contact_note.get_secret")
    @patch(
        "recidiviz.case_triage.workflows.writeback.us_tn_contact_note.in_gcp_production"
    )
    @freezegun.freeze_time("2000-12-30")
    def test_workflows_us_tn_write_tepe_note_to_tomis_request_format_request_recidiviz(
        self, mock_in_prod: MagicMock, mock_secret: MagicMock
    ) -> None:
        mock_in_prod.return_value = True
        mock_secret.return_value = "test-id"
        request = WorkflowsUsTnWriteTEPENoteToTomisRequest(
            offender_id=PERSON_EXTERNAL_ID,
            staff_id="RECIDIVIZ",
            contact_note_date_time=datetime.datetime.now().isoformat(),
            contact_sequence_number=1,
            comments=["line 1", "line 2"],
        )

        actual = request.format_request()

        expected = json.dumps(
            {
                "ContactNoteDateTime": "2000-12-30T00:00:00",
                "OffenderId": "test-id",
                "StaffId": "test-id",
                "ContactTypeCode1": "TEPE",
                "ContactSequenceNumber": 1,
                "Comment1": "line 1",
                "Comment2": "line 2",
            }
        )

        self.assertEqual(actual, expected)

    @patch(
        "recidiviz.case_triage.workflows.writeback.us_tn_contact_note.in_gcp_production"
    )
    @freezegun.freeze_time("2000-12-30")
    def test_workflows_us_tn_write_tepe_note_to_tomis_request_format_request_vrc(
        self, mock_in_prod: MagicMock
    ) -> None:
        mock_in_prod.return_value = True
        request = WorkflowsUsTnWriteTEPENoteToTomisRequest(
            offender_id=PERSON_EXTERNAL_ID,
            staff_id=STAFF_ID,
            contact_note_date_time=datetime.datetime.now().isoformat(),
            contact_sequence_number=1,
            comments=["line 1", "line 2"],
            voters_rights_code=WorkflowsUsTnVotersRightsCode.VRRE,
        )

        actual = json.loads(request.format_request())

        expected = {
            "ContactNoteDateTime": "2000-12-30T00:00:00",
            "OffenderId": PERSON_EXTERNAL_ID,
            "StaffId": STAFF_ID,
            "ContactTypeCode1": "TEPE",
            "ContactTypeCode2": "VRRE",
            "ContactSequenceNumber": 1,
            "Comment1": "line 1",
            "Comment2": "line 2",
        }

        self.assertEqual(actual, expected)
