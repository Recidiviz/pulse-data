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
"""Tests for the US_TN contact note writeback."""
import datetime
from typing import Generator
from unittest import TestCase

import pytest
import responses
from flask import Flask
from mock import MagicMock, patch

from recidiviz.case_triage.workflows.constants import ExternalSystemRequestStatus
from recidiviz.case_triage.workflows.writeback.us_tn_contact_note import (
    UsTnContactNoteRequestData,
    UsTnContactNoteStatusTracker,
    UsTnContactNoteWritebackExecutor,
)

PERSON_EXTERNAL_ID = "123"
STAFF_ID = "456"
CONTACT_NOTE_DATE_TIME = datetime.datetime.now().isoformat()

MODULE = "recidiviz.case_triage.workflows.writeback.us_tn_contact_note"


@pytest.fixture(autouse=True)
def app_context() -> Generator[None, None, None]:
    test_app = Flask("test_us_tn_writeback")
    with test_app.app_context():
        yield


class TestUsTnContactNoteWritebackExecutor(TestCase):
    """Tests for UsTnContactNoteWritebackExecutor."""

    def setUp(self) -> None:
        self.fake_url = "http://fake-url.com"

    @patch(f"{MODULE}.get_secret")
    @patch(f"{MODULE}.FirestoreClientImpl")
    def test_execute_success(
        self,
        mock_client: MagicMock,
        mock_get_secret: MagicMock,
    ) -> None:
        response_json = {"status": "OK"}
        mock_get_secret.return_value = self.fake_url
        mock_client = mock_client.return_value
        with responses.RequestsMock(assert_all_requests_are_fired=True) as rsps:
            rsps.add(responses.PUT, self.fake_url, json=response_json)
            writeback = UsTnContactNoteWritebackExecutor(PERSON_EXTERNAL_ID)
            writeback.execute(
                UsTnContactNoteRequestData(
                    person_external_id=PERSON_EXTERNAL_ID,
                    staff_id=STAFF_ID,
                    contact_note_date_time=CONTACT_NOTE_DATE_TIME,
                    contact_note={1: [], 2: []},
                    voters_rights_code="VRRE",
                )
            )
            # update_document called 4 times: page 1 in_progress, page 1 success,
            # page 2 in_progress, page 2 success
            self.assertEqual(mock_client.update_document.call_count, 4)

    @patch(f"{MODULE}.get_secret")
    @patch(f"{MODULE}.FirestoreClientImpl")
    def test_execute_exception_raised_during_write(
        self,
        mock_client: MagicMock,
        mock_get_secret: MagicMock,
    ) -> None:
        mock_get_secret.return_value = self.fake_url
        mock_client = mock_client.return_value
        with responses.RequestsMock(assert_all_requests_are_fired=True) as rsps:
            rsps.add(responses.PUT, self.fake_url, body=ConnectionRefusedError())
            with self.assertRaises(ConnectionRefusedError):
                writeback = UsTnContactNoteWritebackExecutor(PERSON_EXTERNAL_ID)
                writeback.execute(
                    UsTnContactNoteRequestData(
                        person_external_id=PERSON_EXTERNAL_ID,
                        staff_id=STAFF_ID,
                        contact_note_date_time=CONTACT_NOTE_DATE_TIME,
                        contact_note={1: []},
                        voters_rights_code=None,
                    )
                )
            # page 1 in_progress, page 1 failure
            self.assertEqual(mock_client.update_document.call_count, 2)

    @patch("requests.put")
    @patch(f"{MODULE}.get_secret")
    @patch(f"{MODULE}.FirestoreClientImpl")
    def test_execute_no_secret(
        self, mock_client: MagicMock, mock_get_secret: MagicMock, mock_put: MagicMock
    ) -> None:
        mock_get_secret.return_value = None
        mock_client = mock_client.return_value
        with self.assertRaises(EnvironmentError):
            writeback = UsTnContactNoteWritebackExecutor(PERSON_EXTERNAL_ID)
            writeback.execute(
                UsTnContactNoteRequestData(
                    person_external_id=PERSON_EXTERNAL_ID,
                    staff_id=STAFF_ID,
                    contact_note_date_time=CONTACT_NOTE_DATE_TIME,
                    contact_note={},
                    voters_rights_code=None,
                )
            )

        mock_put.assert_not_called()

    @patch(f"{MODULE}.get_secret")
    @patch(f"{MODULE}.FirestoreClientImpl")
    @patch(f"{MODULE}.in_gcp_production")
    def test_execute_prod_and_recidiviz(
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
            writeback = UsTnContactNoteWritebackExecutor(PERSON_EXTERNAL_ID)
            writeback.execute(
                UsTnContactNoteRequestData(
                    person_external_id=PERSON_EXTERNAL_ID,
                    staff_id="RECIDIVIZ",
                    contact_note_date_time=CONTACT_NOTE_DATE_TIME,
                    contact_note={1: [], 2: []},
                    voters_rights_code=None,
                )
            )

            # Check third call to get_secret gets test url
            mock_get_secret.mock_calls[2].assert_called_with(
                "workflows_us_tn_insert_contact_note_test_url"
            )
            self.assertEqual(mock_client.update_document.call_count, 4)


class TestUsTnContactNoteParseData(TestCase):
    """Tests parse data functionality in UsTnContactNoteWritebackExecutor"""

    def test_parse_data(self) -> None:
        raw = {
            "person_external_id": PERSON_EXTERNAL_ID,
            "staff_id": STAFF_ID,
            "contact_note_date_time": CONTACT_NOTE_DATE_TIME,
            "contact_note": {1: ["line 1"]},
            "voters_rights_code": "VRRE",
        }
        result = UsTnContactNoteWritebackExecutor.parse_request_data(raw)
        self.assertEqual(
            result,
            UsTnContactNoteRequestData(
                person_external_id=PERSON_EXTERNAL_ID,
                staff_id=STAFF_ID,
                contact_note_date_time=CONTACT_NOTE_DATE_TIME,
                contact_note={1: ["line 1"]},
                voters_rights_code="VRRE",
            ),
        )

    def test_parse_data_optional_voters_rights_code(self) -> None:
        raw = {
            "person_external_id": PERSON_EXTERNAL_ID,
            "staff_id": STAFF_ID,
            "contact_note_date_time": CONTACT_NOTE_DATE_TIME,
            "contact_note": {1: []},
        }
        result = UsTnContactNoteWritebackExecutor.parse_request_data(raw)
        self.assertIsNone(result.voters_rights_code)

    def test_parse_data_missing_field(self) -> None:
        with self.assertRaises(KeyError):
            UsTnContactNoteWritebackExecutor.parse_request_data(
                {"person_external_id": PERSON_EXTERNAL_ID, "staff_id": STAFF_ID}
            )


class TestUsTnContactNoteStatusTracker(TestCase):
    def setUp(self) -> None:
        self.mock_firestore = MagicMock()
        self.mock_firestore.timestamp_key = "serverTimestamp"
        self.tracker = UsTnContactNoteStatusTracker("123", self.mock_firestore)
        self.expected_path = (
            "clientUpdatesV2/us_tn_123/clientOpportunityUpdates/usTnExpiration"
        )

    def test_set_status_updates_firestore(self) -> None:
        self.tracker.set_status(ExternalSystemRequestStatus.IN_PROGRESS)

        self.mock_firestore.update_document.assert_called_once()
        call_args = self.mock_firestore.update_document.call_args
        self.assertEqual(call_args[0][0], self.expected_path)
        self.assertEqual(
            call_args[0][1]["contactNote.status"],
            ExternalSystemRequestStatus.IN_PROGRESS.value,
        )

    def test_set_page_status_updates_firestore(self) -> None:
        self.tracker.set_page_status(1, ExternalSystemRequestStatus.SUCCESS)

        self.mock_firestore.update_document.assert_called_once()
        call_args = self.mock_firestore.update_document.call_args
        self.assertEqual(call_args[0][0], self.expected_path)
        self.assertEqual(
            call_args[0][1]["contactNote.noteStatus.1"],
            ExternalSystemRequestStatus.SUCCESS.value,
        )
