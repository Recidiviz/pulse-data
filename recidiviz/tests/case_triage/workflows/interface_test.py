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
"""Implements test for interface used to support external requests in Workflows"""
import datetime
import json
from typing import Generator
from unittest import TestCase

import freezegun
import pytest
import responses
from flask import Flask
from mock import MagicMock, patch

from recidiviz.case_triage.workflows.constants import WorkflowsUsTnVotersRightsCode
from recidiviz.case_triage.workflows.interface import (
    WorkflowsUsTnExternalRequestInterface,
    WorkflowsUsTnWriteTEPENoteToTomisRequest,
)

PERSON_EXTERNAL_ID = "123"
USER_ID = "456"
CONTACT_NOTE_DATE_TIME = datetime.datetime.now().isoformat()


@pytest.fixture(autouse=True)
def app_context() -> Generator[None, None, None]:
    test_app = Flask("test_workflows_authorization")
    with test_app.app_context():
        yield


class TestWorkflowsInterface(TestCase):
    """Test class for making external requests in Workflows"""

    def setUp(self) -> None:
        self.fake_url = "http://fake-url.com"

    @patch("recidiviz.case_triage.workflows.interface.get_secret")
    @patch("recidiviz.case_triage.workflows.interface.FirestoreClientImpl")
    def test_insert_contact_note_success(
        self, mock_client: MagicMock, mock_get_secret: MagicMock
    ) -> None:
        response_json = {"status": "OK"}
        mock_get_secret.return_value = self.fake_url
        mock_client = mock_client.return_value
        with responses.RequestsMock(assert_all_requests_are_fired=True) as rsps:
            rsps.add(responses.PUT, self.fake_url, json=response_json)
            WorkflowsUsTnExternalRequestInterface().insert_tepe_contact_note(
                PERSON_EXTERNAL_ID,
                USER_ID,
                CONTACT_NOTE_DATE_TIME,
                {1: [], 2: []},
                "VRRE",
            )
            # update_document is called 5 times: page 1 in progress, page 1 success, page 2 in progress, page 2 success,
            # and entire note status success
            # TODO(#2938): Explore more granular testing for firestore calls
            self.assertEqual(mock_client.update_document.call_count, 5)

    @patch("recidiviz.case_triage.workflows.interface.get_secret")
    @patch("recidiviz.case_triage.workflows.interface.FirestoreClientImpl")
    def test_insert_contact_note_exception_raised_during_write(
        self, mock_client: MagicMock, mock_get_secret: MagicMock
    ) -> None:
        mock_get_secret.return_value = self.fake_url
        mock_client = mock_client.return_value
        with responses.RequestsMock(assert_all_requests_are_fired=True) as rsps:
            rsps.add(responses.PUT, self.fake_url, body=ConnectionRefusedError())
            with self.assertRaises(ConnectionRefusedError):
                WorkflowsUsTnExternalRequestInterface().insert_tepe_contact_note(
                    PERSON_EXTERNAL_ID, USER_ID, CONTACT_NOTE_DATE_TIME, {1: []}
                )
            self.assertEqual(mock_client.update_document.call_count, 3)

    @patch("requests.put")
    @patch("recidiviz.case_triage.workflows.interface.get_secret")
    @patch("recidiviz.case_triage.workflows.interface.FirestoreClientImpl")
    def test_insert_contact_note_no_secret(
        self, mock_client: MagicMock, mock_get_secret: MagicMock, mock_put: MagicMock
    ) -> None:
        mock_get_secret.return_value = None
        mock_client = mock_client.return_value
        with self.assertRaises(Exception):
            WorkflowsUsTnExternalRequestInterface().insert_tepe_contact_note(
                PERSON_EXTERNAL_ID, USER_ID, CONTACT_NOTE_DATE_TIME, {}
            )

        mock_put.assert_not_called()
        mock_client.update_document.assert_called_once()

    @patch("recidiviz.case_triage.workflows.interface.get_secret")
    @patch("recidiviz.case_triage.workflows.interface.FirestoreClientImpl")
    @patch("recidiviz.case_triage.workflows.interface.in_gcp_production")
    def test_insert_contact_note_prod_and_recidiviz(
        self,
        mock_in_prod: MagicMock,
        mock_client: MagicMock,
        mock_get_secret: MagicMock,
    ) -> None:
        response_json = {"status": "OK"}
        mock_get_secret.return_value = self.fake_url
        mock_client = mock_client.return_value
        mock_in_prod.return_value = True
        with responses.RequestsMock(assert_all_requests_are_fired=True) as rsps:
            rsps.add(responses.PUT, self.fake_url, json=response_json)
            WorkflowsUsTnExternalRequestInterface().insert_tepe_contact_note(
                PERSON_EXTERNAL_ID, "RECIDIVIZ", CONTACT_NOTE_DATE_TIME, {1: [], 2: []}
            )

            # Check third call to get_secret gets test url
            mock_get_secret.mock_calls[2].assert_called_with(
                "workflows_us_tn_insert_contact_note_test_url"
            )
            self.assertEqual(mock_client.update_document.call_count, 5)

    @patch("recidiviz.case_triage.workflows.interface.get_secret")
    @patch("recidiviz.case_triage.workflows.interface.in_gcp_production")
    @freezegun.freeze_time("2000-12-30")
    def test_workflows_us_tn_write_tepe_note_to_tomis_request_format_request(
        self, mock_in_prod: MagicMock, mock_secret: MagicMock
    ) -> None:
        mock_in_prod.return_value = False
        mock_secret.return_value = "test-id"
        request = WorkflowsUsTnWriteTEPENoteToTomisRequest(
            offender_id=PERSON_EXTERNAL_ID,
            user_id=USER_ID,
            contact_note_date_time=datetime.datetime.now().isoformat(),
            contact_sequence_number=1,
            comments=["line 1", "line 2"],
        )

        actual = request.format_request()

        expected = json.dumps(
            {
                "ContactNoteDateTime": "2000-12-30T00:00:00",
                "OffenderId": "test-id",
                "UserId": "test-id",
                "ContactTypeCode1": "TEPE",
                "ContactSequenceNumber": 1,
                "Comment1": "line 1",
                "Comment2": "line 2",
            }
        )

        self.assertEqual(actual, expected)

    @patch("recidiviz.case_triage.workflows.interface.get_secret")
    @patch("recidiviz.case_triage.workflows.interface.in_gcp_production")
    @freezegun.freeze_time("2000-12-30")
    def test_workflows_us_tn_write_tepe_note_to_tomis_request_format_request_recidiviz(
        self, mock_in_prod: MagicMock, mock_secret: MagicMock
    ) -> None:
        mock_in_prod.return_value = True
        mock_secret.return_value = "test-id"
        request = WorkflowsUsTnWriteTEPENoteToTomisRequest(
            offender_id=PERSON_EXTERNAL_ID,
            user_id="RECIDIVIZ",
            contact_note_date_time=datetime.datetime.now().isoformat(),
            contact_sequence_number=1,
            comments=["line 1", "line 2"],
        )

        actual = request.format_request()

        expected = json.dumps(
            {
                "ContactNoteDateTime": "2000-12-30T00:00:00",
                "OffenderId": "test-id",
                "UserId": "test-id",
                "ContactTypeCode1": "TEPE",
                "ContactSequenceNumber": 1,
                "Comment1": "line 1",
                "Comment2": "line 2",
            }
        )

        self.assertEqual(actual, expected)

    @patch("recidiviz.case_triage.workflows.interface.in_gcp_production")
    @freezegun.freeze_time("2000-12-30")
    def test_workflows_us_tn_write_tepe_note_to_tomis_request_format_request_vrc(
        self, mock_in_prod: MagicMock
    ) -> None:
        mock_in_prod.return_value = True
        request = WorkflowsUsTnWriteTEPENoteToTomisRequest(
            offender_id=PERSON_EXTERNAL_ID,
            user_id=USER_ID,
            contact_note_date_time=datetime.datetime.now().isoformat(),
            contact_sequence_number=1,
            comments=["line 1", "line 2"],
            voters_rights_code=WorkflowsUsTnVotersRightsCode.VRRE,
        )

        actual = json.loads(request.format_request())

        expected = {
            "ContactNoteDateTime": "2000-12-30T00:00:00",
            "OffenderId": PERSON_EXTERNAL_ID,
            "UserId": USER_ID,
            "ContactTypeCode1": "TEPE",
            "ContactTypeCode2": "VRRE",
            "ContactSequenceNumber": 1,
            "Comment1": "line 1",
            "Comment2": "line 2",
        }

        self.assertEqual(actual, expected)
