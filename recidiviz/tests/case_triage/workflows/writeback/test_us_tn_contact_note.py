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
import json
from typing import Generator
from unittest import TestCase

import freezegun
import pytest
import responses
from flask import Flask
from mock import MagicMock, patch
from pydantic import ValidationError
from pydantic_core import TzInfo

from recidiviz.case_triage.workflows.constants import (
    ExternalSystemRequestStatus,
    WorkflowsUsTnVotersRightsCode,
)
from recidiviz.case_triage.workflows.writeback.us_tn_contact_note import (
    TomisContactNoteRequest,
    UsTnContactNoteRequestData,
    UsTnContactNoteStatusTracker,
    UsTnContactNoteWritebackExecutor,
)

PERSON_EXTERNAL_ID = "123"
STAFF_ID = "456"
CONTACT_NOTE_DATE_TIME = datetime.datetime.now()

MODULE = "recidiviz.case_triage.workflows.writeback.us_tn_contact_note"
TRANSPORT_MODULE = "recidiviz.case_triage.workflows.writeback.transports.rest"


@pytest.fixture(autouse=True)
def app_context() -> Generator[None, None, None]:
    test_app = Flask("test_us_tn_writeback")
    with test_app.app_context():
        yield


class TestUsTnContactNoteWritebackExecutor(TestCase):
    """Tests for UsTnContactNoteWritebackExecutor."""

    def setUp(self) -> None:
        self.fake_url = "http://fake-url.com"

    @patch(f"{MODULE}.FirestoreClientImpl")
    def test_to_cloud_task_payload(self, _mock_client: MagicMock) -> None:
        request_data = UsTnContactNoteRequestData(
            person_external_id=PERSON_EXTERNAL_ID,
            staff_id=STAFF_ID,
            contact_note_date_time=CONTACT_NOTE_DATE_TIME,
            contact_note={1: ["line 1", "line 2"], 2: ["line 3"]},
            voters_rights_code=WorkflowsUsTnVotersRightsCode.VRRE,
            should_queue_task=True,
        )
        executor = UsTnContactNoteWritebackExecutor(request_data)

        self.assertEqual(
            executor.to_cloud_task_payload(),
            {
                "should_queue_task": False,
                "person_external_id": PERSON_EXTERNAL_ID,
                "staff_id": STAFF_ID,
                "contact_note_date_time": CONTACT_NOTE_DATE_TIME.isoformat(),
                "contact_note": {"1": ["line 1", "line 2"], "2": ["line 3"]},
                "voters_rights_code": "VRRE",
            },
        )

    @patch(f"{MODULE}.get_secret")
    @patch(f"{TRANSPORT_MODULE}.get_secret")
    @patch(f"{MODULE}.FirestoreClientImpl")
    def test_execute_success(
        self,
        mock_client: MagicMock,
        mock_get_secret: MagicMock,
        mock_interface_get_secret: MagicMock,
    ) -> None:
        mock_interface_get_secret.return_value = "test-id"
        response_json = {"status": "OK"}
        mock_get_secret.return_value = self.fake_url
        mock_client = mock_client.return_value
        with responses.RequestsMock(assert_all_requests_are_fired=True) as rsps:
            rsps.add(responses.PUT, self.fake_url, json=response_json)
            request_data = UsTnContactNoteRequestData(
                person_external_id=PERSON_EXTERNAL_ID,
                staff_id=STAFF_ID,
                contact_note_date_time=CONTACT_NOTE_DATE_TIME,
                contact_note={1: [], 2: []},
                voters_rights_code=WorkflowsUsTnVotersRightsCode.VRRE,
            )
            writeback = UsTnContactNoteWritebackExecutor(request_data)
            writeback.execute()
            # update_document called 4 times: page 1 in_progress, page 1 success,
            # page 2 in_progress, page 2 success
            self.assertEqual(mock_client.update_document.call_count, 4)

    @patch(f"{MODULE}.get_secret")
    @patch(f"{TRANSPORT_MODULE}.get_secret")
    @patch(f"{MODULE}.FirestoreClientImpl")
    def test_execute_exception_raised_during_write(
        self,
        mock_client: MagicMock,
        mock_get_secret: MagicMock,
        mock_interface_get_secret: MagicMock,
    ) -> None:
        mock_interface_get_secret.return_value = "test-id"
        mock_get_secret.return_value = self.fake_url
        mock_client = mock_client.return_value
        with responses.RequestsMock(assert_all_requests_are_fired=True) as rsps:
            rsps.add(responses.PUT, self.fake_url, body=ConnectionRefusedError())
            with self.assertRaises(ConnectionRefusedError):
                request_data = UsTnContactNoteRequestData(
                    person_external_id=PERSON_EXTERNAL_ID,
                    staff_id=STAFF_ID,
                    contact_note_date_time=CONTACT_NOTE_DATE_TIME,
                    contact_note={1: []},
                    voters_rights_code=None,
                )
                writeback = UsTnContactNoteWritebackExecutor(request_data)
                writeback.execute()
            # page 1 in_progress, page 1 failure
            self.assertEqual(mock_client.update_document.call_count, 2)

    @patch("requests.put")
    @patch(f"{TRANSPORT_MODULE}.get_secret")
    @patch(f"{MODULE}.FirestoreClientImpl")
    def test_execute_no_secret(
        self, mock_client: MagicMock, mock_get_secret: MagicMock, mock_put: MagicMock
    ) -> None:
        mock_get_secret.return_value = None
        mock_client = mock_client.return_value
        with self.assertRaises(EnvironmentError):
            request_data = UsTnContactNoteRequestData(
                person_external_id=PERSON_EXTERNAL_ID,
                staff_id=STAFF_ID,
                contact_note_date_time=CONTACT_NOTE_DATE_TIME,
                contact_note={1: []},
                voters_rights_code=None,
            )
            writeback = UsTnContactNoteWritebackExecutor(request_data)
            writeback.execute()

        mock_put.assert_not_called()

    @patch(f"{MODULE}.get_secret")
    @patch(f"{TRANSPORT_MODULE}.get_secret")
    @patch(f"{MODULE}.FirestoreClientImpl")
    @patch(f"{MODULE}.in_gcp_production")
    def test_execute_prod_and_recidiviz(
        self,
        mock_in_prod: MagicMock,
        mock_client: MagicMock,
        mock_get_secret: MagicMock,
        mock_interface_get_secret: MagicMock,
    ) -> None:
        mock_interface_get_secret.return_value = "test-id"
        response_json = {"status": "OK"}
        mock_get_secret.return_value = self.fake_url
        mock_client = mock_client.return_value
        mock_in_prod.return_value = True
        with responses.RequestsMock(assert_all_requests_are_fired=True) as rsps:
            rsps.add(responses.PUT, self.fake_url, json=response_json)
            request_data = UsTnContactNoteRequestData(
                person_external_id=PERSON_EXTERNAL_ID,
                staff_id="RECIDIVIZ",
                contact_note_date_time=CONTACT_NOTE_DATE_TIME,
                contact_note={1: [], 2: []},
                voters_rights_code=None,
            )
            writeback = UsTnContactNoteWritebackExecutor(request_data)
            writeback.execute()

            # When use_test_url=True, transport fetches test URL secret first
            mock_get_secret.assert_any_call(
                "workflows_us_tn_insert_contact_note_test_url"
            )
            self.assertEqual(mock_client.update_document.call_count, 4)


class TestUsTnContactNoteRequestData(TestCase):
    """Tests for UsTnContactNoteRequestData pydantic model."""

    def test_valid_camel_case(self) -> None:
        data = UsTnContactNoteRequestData.model_validate(
            {
                "personExternalId": PERSON_EXTERNAL_ID,
                "staffId": STAFF_ID,
                "contactNoteDateTime": "2000-12-30T00:00:00",
                "contactNote": {"1": ["line 1", "line 2"]},
                "votersRightsCode": "VRRE",
            }
        )
        self.assertEqual(data.person_external_id, PERSON_EXTERNAL_ID)
        self.assertEqual(data.staff_id, STAFF_ID)

    def test_valid_snake_case(self) -> None:
        data = UsTnContactNoteRequestData.model_validate(
            {
                "person_external_id": PERSON_EXTERNAL_ID,
                "staff_id": STAFF_ID,
                "contact_note_date_time": "2000-12-30T00:00:00",
                "contact_note": {"1": ["line 1", "line 2"]},
                "voters_rights_code": "VRRE",
            }
        )
        self.assertEqual(data.person_external_id, PERSON_EXTERNAL_ID)

    def test_optional_voters_rights_code(self) -> None:
        data = UsTnContactNoteRequestData.model_validate(
            {
                "personExternalId": PERSON_EXTERNAL_ID,
                "staffId": STAFF_ID,
                "contactNoteDateTime": "2000-12-30T00:00:00",
                "contactNote": {"1": ["line 1"]},
            }
        )
        self.assertIsNone(data.voters_rights_code)

    def test_valid_iso_date_time(self) -> None:
        data = UsTnContactNoteRequestData.model_validate(
            {
                "person_external_id": PERSON_EXTERNAL_ID,
                "staff_id": STAFF_ID,
                "contact_note_date_time": "2026-04-05T12:34:56.789Z",
                "contact_note": {"1": ["line 1", "line 2"]},
                "voters_rights_code": "VRRE",
            }
        )
        self.assertEqual(
            data.contact_note_date_time,
            datetime.datetime(2026, 4, 5, 12, 34, 56, 789000, tzinfo=TzInfo()),
        )

    def test_missing_field(self) -> None:
        with self.assertRaises(ValidationError):
            UsTnContactNoteRequestData.model_validate(
                {"personExternalId": PERSON_EXTERNAL_ID, "staffId": STAFF_ID}
            )

    def test_invalid_voters_rights_code(self) -> None:
        with self.assertRaises(ValidationError):
            UsTnContactNoteRequestData.model_validate(
                {
                    "personExternalId": PERSON_EXTERNAL_ID,
                    "staffId": STAFF_ID,
                    "contactNoteDateTime": "2000-12-30T00:00:00",
                    "contactNote": {"1": ["line 1"]},
                    "votersRightsCode": "VVVV",
                }
            )

    def test_empty_contact_note(self) -> None:
        with self.assertRaises(ValidationError):
            UsTnContactNoteRequestData.model_validate(
                {
                    "personExternalId": PERSON_EXTERNAL_ID,
                    "staffId": STAFF_ID,
                    "contactNoteDateTime": "2000-12-30T00:00:00",
                    "contactNote": {},
                }
            )

    def test_invalid_date_format(self) -> None:
        with self.assertRaises(ValidationError):
            UsTnContactNoteRequestData.model_validate(
                {
                    "personExternalId": PERSON_EXTERNAL_ID,
                    "staffId": STAFF_ID,
                    "contactNoteDateTime": "15/01/2024 10:00",
                    "contactNote": {"1": ["line 1", "line 2"]},
                    "votersRightsCode": "VRRE",
                }
            )

    def test_too_many_lines(self) -> None:
        with self.assertRaises(ValidationError):
            UsTnContactNoteRequestData.model_validate(
                {
                    "personExternalId": PERSON_EXTERNAL_ID,
                    "staffId": STAFF_ID,
                    "contactNoteDateTime": "2000-12-30T00:00:00",
                    "contactNote": {
                        "1": [f"line {i}" for i in range(11)],
                    },
                }
            )

    def test_invalid_page_number(self) -> None:
        with self.assertRaises(ValidationError):
            UsTnContactNoteRequestData.model_validate(
                {
                    "personExternalId": PERSON_EXTERNAL_ID,
                    "staffId": STAFF_ID,
                    "contactNoteDateTime": "2000-12-30T00:00:00",
                    "contactNote": {"11": ["line 1"]},
                }
            )


class TestTomisContactNoteRequestToJson(TestCase):
    """Tests for TomisContactNoteRequest.to_json() serialization."""

    @freezegun.freeze_time("2000-12-30")
    def test_basic_request(self) -> None:
        request = TomisContactNoteRequest(
            offender_id=PERSON_EXTERNAL_ID,
            staff_id=STAFF_ID,
            contact_note_date_time=datetime.datetime.now(),
            page_number=1,
            comments=["line 1", "line 2"],
        )

        actual = json.loads(request.to_json())

        self.assertEqual(
            actual,
            {
                "ContactNoteDateTime": "2000-12-30T00:00:00",
                "OffenderId": PERSON_EXTERNAL_ID,
                "StaffId": STAFF_ID,
                "ContactTypeCode1": "TEPE",
                "ContactSequenceNumber": 1,
                "Comment1": "line 1",
                "Comment2": "line 2",
            },
        )

    @freezegun.freeze_time("2000-12-30")
    def test_with_voters_rights_code(self) -> None:
        request = TomisContactNoteRequest(
            offender_id=PERSON_EXTERNAL_ID,
            staff_id=STAFF_ID,
            contact_note_date_time=datetime.datetime.now(),
            page_number=1,
            comments=["line 1", "line 2"],
            voters_rights_code=WorkflowsUsTnVotersRightsCode.VRRE,
        )

        actual = json.loads(request.to_json())

        self.assertEqual(actual["ContactTypeCode2"], "VRRE")
        self.assertEqual(actual["ContactTypeCode1"], "TEPE")

    @freezegun.freeze_time("2000-12-30")
    def test_no_voters_rights_code_omits_field(self) -> None:
        request = TomisContactNoteRequest(
            offender_id=PERSON_EXTERNAL_ID,
            staff_id=STAFF_ID,
            contact_note_date_time=datetime.datetime.now(),
            page_number=1,
            comments=["line 1"],
        )

        actual = json.loads(request.to_json())

        self.assertNotIn("ContactTypeCode2", actual)


class TestTomisContactNoteRequestFromRequestData(TestCase):
    """Tests for TomisContactNoteRequest.from_request_data() ID substitution."""

    @patch(f"{MODULE}.get_secret")
    @patch(f"{MODULE}.in_gcp_production")
    def test_non_production_uses_test_ids(
        self, mock_in_prod: MagicMock, mock_secret: MagicMock
    ) -> None:
        mock_in_prod.return_value = False
        mock_secret.return_value = "test-id"
        request_data = UsTnContactNoteRequestData(
            person_external_id=PERSON_EXTERNAL_ID,
            staff_id=STAFF_ID,
            contact_note_date_time=CONTACT_NOTE_DATE_TIME,
            contact_note={1: ["line 1"]},
        )

        result = TomisContactNoteRequest.from_request_data(
            request_data, page_number=1, comments=["line 1"]
        )

        self.assertEqual(result.offender_id, "test-id")
        self.assertEqual(result.staff_id, "test-id")

    @patch(f"{MODULE}.get_secret")
    @patch(f"{MODULE}.in_gcp_production")
    def test_production_recidiviz_staff_uses_test_ids(
        self, mock_in_prod: MagicMock, mock_secret: MagicMock
    ) -> None:
        mock_in_prod.return_value = True
        mock_secret.return_value = "test-id"
        request_data = UsTnContactNoteRequestData(
            person_external_id=PERSON_EXTERNAL_ID,
            staff_id="RECIDIVIZ",
            contact_note_date_time=CONTACT_NOTE_DATE_TIME,
            contact_note={1: ["line 1"]},
        )

        result = TomisContactNoteRequest.from_request_data(
            request_data, page_number=1, comments=["line 1"]
        )

        self.assertEqual(result.offender_id, "test-id")
        self.assertEqual(result.staff_id, "test-id")

    @patch(f"{MODULE}.in_gcp_production")
    def test_production_real_staff_uses_real_ids(self, mock_in_prod: MagicMock) -> None:
        mock_in_prod.return_value = True
        request_data = UsTnContactNoteRequestData(
            person_external_id=PERSON_EXTERNAL_ID,
            staff_id=STAFF_ID,
            contact_note_date_time=CONTACT_NOTE_DATE_TIME,
            contact_note={1: ["line 1"]},
        )

        result = TomisContactNoteRequest.from_request_data(
            request_data, page_number=1, comments=["line 1"]
        )

        self.assertEqual(result.offender_id, PERSON_EXTERNAL_ID)
        self.assertEqual(result.staff_id, STAFF_ID)

    @patch(f"{MODULE}.get_secret")
    @patch(f"{MODULE}.in_gcp_production")
    def test_missing_test_secrets_raises(
        self, mock_in_prod: MagicMock, mock_secret: MagicMock
    ) -> None:
        mock_in_prod.return_value = False
        mock_secret.return_value = None
        request_data = UsTnContactNoteRequestData(
            person_external_id=PERSON_EXTERNAL_ID,
            staff_id=STAFF_ID,
            contact_note_date_time=CONTACT_NOTE_DATE_TIME,
            contact_note={1: ["line 1"]},
        )

        with self.assertRaises(ValueError):
            TomisContactNoteRequest.from_request_data(
                request_data, page_number=1, comments=["line 1"]
            )


class TestUsTnContactNoteStatusTracker(TestCase):
    """Tests for UsTnContactNoteStatusTracker"""

    def test_set_status_updates_firestore(self) -> None:
        mock_firestore = MagicMock()
        mock_firestore.timestamp_key = "serverTimestamp"
        tracker = UsTnContactNoteStatusTracker("123", mock_firestore)
        tracker.set_status(ExternalSystemRequestStatus.IN_PROGRESS)

        mock_firestore.update_document.assert_called_once()
        call_args = mock_firestore.update_document.call_args
        self.assertEqual(
            call_args[0][0],
            "clientUpdatesV2/us_tn_123/clientOpportunityUpdates/usTnExpiration",
        )
        self.assertEqual(
            call_args[0][1]["contactNote.status"],
            ExternalSystemRequestStatus.IN_PROGRESS.value,
        )

    def test_set_page_status_updates_firestore(self) -> None:
        mock_firestore = MagicMock()
        mock_firestore.timestamp_key = "serverTimestamp"
        tracker = UsTnContactNoteStatusTracker("123", mock_firestore)

        tracker.set_page_status(1, ExternalSystemRequestStatus.SUCCESS)

        mock_firestore.update_document.assert_called_once()
        call_args = mock_firestore.update_document.call_args
        self.assertEqual(
            call_args[0][0],
            "clientUpdatesV2/us_tn_123/clientOpportunityUpdates/usTnExpiration",
        )
        self.assertEqual(
            call_args[0][1]["contactNote.noteStatus.1"],
            ExternalSystemRequestStatus.SUCCESS.value,
        )
