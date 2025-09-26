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
"""Implement tests for Workflows APIs"""
import datetime
import os
from http import HTTPStatus
from typing import Callable, Dict, Optional
from unittest import TestCase, mock
from unittest.mock import MagicMock, call, patch

import responses
from flask import Flask
from flask.testing import FlaskClient
from freezegun import freeze_time

from recidiviz.case_triage.error_handlers import register_error_handlers
from recidiviz.case_triage.workflows.constants import ExternalSystemRequestStatus
from recidiviz.case_triage.workflows.workflows_authorization import (
    on_successful_authorization,
    on_successful_authorization_recidiviz_only,
)
from recidiviz.case_triage.workflows.workflows_routes import (
    OPT_OUT_MESSAGE,
    create_workflows_api_blueprint,
)
from recidiviz.common.common_utils import convert_nested_dictionary_keys
from recidiviz.common.str_field_utils import snake_to_camel
from recidiviz.utils.types import assert_type
from recidiviz.workflows.types import (
    FullOpportunityInfo,
    OpportunityConfig,
    WorkflowsSystemType,
)

PERSON_EXTERNAL_ID = "123"
STAFF_ID = "456"
STAFF_EMAIL = "fake email address"
USER_HASH = "xyz+&123_="


class WorkflowsBlueprintTestCase(TestCase):
    """Base class for Workflows flask Tests"""

    mock_authorization_handler: MagicMock
    test_app: Flask
    test_client: FlaskClient

    def setUp(self) -> None:
        self.mock_authorization_handler = MagicMock()

        self.auth_patcher = mock.patch(
            f"{create_workflows_api_blueprint.__module__}.build_authorization_handler",
            return_value=self.mock_authorization_handler,
        )

        self.auth_patcher.start()

        self.test_app = Flask(__name__)
        register_error_handlers(self.test_app)
        self.test_app.register_blueprint(
            create_workflows_api_blueprint(), url_prefix="/workflows"
        )
        self.test_app.secret_key = "NOT A SECRET"
        self.test_client = self.test_app.test_client()

    def tearDown(self) -> None:
        self.auth_patcher.stop()

    @staticmethod
    def auth_side_effect(
        state_code: str,
        email_address: str = "test_user@somestate.gov",
        allowed_states: Optional[list[str]] = None,
        successful_authorization_handler: Callable = on_successful_authorization,
    ) -> Callable:
        if allowed_states is None:
            allowed_states = []

        return lambda: successful_authorization_handler(
            {
                f"{os.environ['AUTH0_CLAIM_NAMESPACE']}/app_metadata": {
                    "stateCode": f"{state_code.lower()}",
                    "allowedStates": allowed_states,
                },
                f"{os.environ['AUTH0_CLAIM_NAMESPACE']}/email_address": email_address,
            }
        )


class TestWorkflowsRoutes(WorkflowsBlueprintTestCase):
    """Implements tests for the Workflows routes."""

    def setUp(self) -> None:
        super().setUp()

        self.allowed_twilio_patch = mock.patch(
            "recidiviz.case_triage.workflows.workflows_routes.allowed_twilio_dev_recipient",
            return_value=True,
        )

        self.allowed_twilio_patch.start()

        self.old_auth_claim_namespace = os.environ.get("AUTH0_CLAIM_NAMESPACE", None)
        os.environ["AUTH0_CLAIM_NAMESPACE"] = "https://recidiviz-test"
        self.fake_url = "http://fake-url.com"

    def tearDown(self) -> None:
        self.allowed_twilio_patch.stop()
        super().tearDown()

    @patch("recidiviz.case_triage.workflows.workflows_routes.get_secret")
    def test_proxy_defaults(self, mock_get_secret: MagicMock) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "recidiviz",
            "test_user@recidiviz.org",
            ["US_TN"],
            on_successful_authorization_recidiviz_only,
        )

        mock_get_secret.return_value = self.fake_url
        proxy_response = "test response"
        timeout = 360  # default defined in api_schemas.py

        with responses.RequestsMock(
            assert_all_requests_are_fired=True
        ) as rsps, self.assertLogs(level="INFO") as log:
            rsps.add(
                responses.GET,
                self.fake_url,
                body=proxy_response,
                match=[responses.matchers.request_kwargs_matcher({"timeout": timeout})],
            )
            response = self.test_client.post(
                "/workflows/proxy",
                json={"url_secret": "foo", "method": "GET"},
            )
            self.assertEqual(response.get_data(as_text=True), proxy_response)
            self.assertIn(
                f"INFO:root:Workflows proxy: [test_user@recidiviz.org] is sending a [GET] request to url_secret [foo] with value [{self.fake_url}]",
                log.output,
            )

    @patch("recidiviz.case_triage.workflows.workflows_routes.get_secret")
    def test_proxy(self, mock_get_secret: MagicMock) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "recidiviz",
            "test_user@recidiviz.org",
            ["US_TN"],
            on_successful_authorization_recidiviz_only,
        )

        mock_get_secret.return_value = self.fake_url
        proxy_response = "test response"
        timeout = 99
        headers = {"header_key": "header_value"}

        with responses.RequestsMock(assert_all_requests_are_fired=True) as rsps:
            rsps.add(
                responses.PUT,
                self.fake_url,
                body=proxy_response,
                match=[
                    responses.matchers.request_kwargs_matcher({"timeout": timeout}),
                    responses.matchers.header_matcher(headers),
                ],
            )
            response = self.test_client.post(
                "/workflows/proxy",
                json={
                    "url_secret": "foo",
                    "method": "PUT",
                    "json": {"key": "value"},
                    "headers": headers,
                    "timeout": timeout,
                },
            )
            self.assertEqual(response.get_data(as_text=True), proxy_response)

    @patch("recidiviz.case_triage.workflows.workflows_routes.get_secret")
    def test_proxy_missing_secret(self, mock_get_secret: MagicMock) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "recidiviz",
            "test_user@recidiviz.org",
            ["US_TN"],
            on_successful_authorization_recidiviz_only,
        )

        mock_get_secret.return_value = None

        response = self.test_client.post(
            "/workflows/proxy",
            json={"url_secret": "foo", "method": "GET"},
        )
        self.assertEqual(response.status_code, 404, response.get_data(as_text=True))

    def test_proxy_not_authorized(self) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_id",
            "test_user@recidiviz.org",
            [],
            on_successful_authorization_recidiviz_only,
        )

        response = self.test_client.post(
            "/workflows/proxy",
            json={"url_secret": "foo", "method": "GET"},
        )
        self.assertEqual(
            response.status_code,
            HTTPStatus.UNAUTHORIZED,
            response.get_data(as_text=True),
        )

    def test_init(self) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "recidiviz", allowed_states=["US_TN"]
        )

        response = self.test_client.get(
            "/workflows/US_TN/init", headers={"Origin": "http://localhost:3000"}
        )
        response_json = assert_type(response.get_json(), dict)
        self.assertEqual(response.status_code, HTTPStatus.OK, response_json)

    def test_init_not_authorized(self) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_id")

        response = self.test_client.get("/workflows/US_TN/init")
        self.assertEqual(response.status_code, HTTPStatus.UNAUTHORIZED)

    def test_insert_tepe_contact_note_passthrough(self) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "recidiviz", allowed_states=["US_TN"]
        )

        response = self.test_client.post(
            "/workflows/external_request/US_TN/insert_tepe_contact_note",
            headers={"Origin": "http://localhost:3000"},
            json={},
        )
        response_json = assert_type(response.get_json(), dict)
        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST, response_json)

    def test_insert_tepe_contact_note_not_authorized(self) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_id")

        response = self.test_client.post(
            "/workflows/external_request/US_TN/insert_tepe_contact_note", json={}
        )
        self.assertEqual(response.status_code, HTTPStatus.UNAUTHORIZED)

    @patch("recidiviz.case_triage.workflows.workflows_routes.FirestoreClientImpl")
    @patch(
        "recidiviz.case_triage.workflows.workflows_routes.SingleCloudTaskQueueManager"
    )
    @freeze_time("2023-01-01 01:23:45")
    def test_insert_tepe_contact_note_success(
        self,
        mock_task_manager: MagicMock,
        mock_firestore: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_tn")

        request_body = {
            "personExternalId": PERSON_EXTERNAL_ID,
            "staffId": STAFF_ID,
            "contactNoteDateTime": str(datetime.datetime.now()),
            "contactNote": {1: ["Line 1", "Line 2"]},
        }

        with self.test_app.test_request_context():
            response = self.test_client.post(
                "/workflows/external_request/US_TN/insert_tepe_contact_note",
                headers={"Origin": "http://localhost:3000"},
                json=request_body,
            )

        expected_task_body = {
            "person_external_id": PERSON_EXTERNAL_ID,
            "staff_id": STAFF_ID,
            "contact_note_date_time": "2023-01-01T01:23:45",
            "contact_note": {1: ["Line 1", "Line 2"]},
        }
        mock_task_manager.return_value.create_task.assert_called_once()
        self.assertEqual(
            mock_task_manager.return_value.create_task.call_args.kwargs["body"],
            expected_task_body,
        )
        mock_firestore.return_value.update_document.assert_called_once()
        self.assertEqual(response.status_code, HTTPStatus.OK)

    @patch(
        "recidiviz.case_triage.workflows.workflows_routes.WorkflowsUsTnExternalRequestInterface."
        "insert_tepe_contact_note"
    )
    @patch("recidiviz.case_triage.workflows.workflows_routes.FirestoreClientImpl")
    @patch(
        "recidiviz.case_triage.workflows.workflows_routes.SingleCloudTaskQueueManager"
    )
    @freeze_time("2023-01-01 01:23:45")
    def test_insert_tepe_contact_note_no_queue_success(
        self,
        mock_task_manager: MagicMock,
        mock_firestore: MagicMock,
        mock_insert: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_tn")

        request_body = {
            "personExternalId": PERSON_EXTERNAL_ID,
            "staffId": STAFF_ID,
            "contactNoteDateTime": str(datetime.datetime.now()),
            "contactNote": {1: ["Line 1", "Line 2"]},
            "shouldQueueTask": False,
        }

        with self.test_app.test_request_context():
            response = self.test_client.post(
                "/workflows/external_request/US_TN/insert_tepe_contact_note",
                headers={"Origin": "http://localhost:3000"},
                json=request_body,
            )

        expected_body = {
            "person_external_id": PERSON_EXTERNAL_ID,
            "staff_id": STAFF_ID,
            "contact_note_date_time": "2023-01-01T01:23:45",
            "contact_note": {1: ["Line 1", "Line 2"]},
        }
        mock_task_manager.return_value.create_task.assert_not_called()
        self.assertEqual(
            mock_insert.call_args.kwargs,
            expected_body,
        )
        mock_firestore.return_value.update_document.assert_called_once()
        self.assertEqual(response.status_code, HTTPStatus.OK)

    @patch("recidiviz.case_triage.workflows.workflows_routes.FirestoreClientImpl")
    @patch(
        "recidiviz.case_triage.workflows.workflows_routes.SingleCloudTaskQueueManager"
    )
    def test_insert_tepe_contact_note_exception(
        self,
        mock_task_manager: MagicMock,
        mock_firestore: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_tn")

        request_body = {
            "personExternalId": PERSON_EXTERNAL_ID,
            "staffId": STAFF_ID,
            "contactNoteDateTime": str(datetime.datetime.now()),
            "contactNote": {1: ["Line 1", "Line 2"]},
        }

        mock_task_manager.return_value.create_task.side_effect = Exception

        with self.test_app.test_request_context():
            response = self.test_client.post(
                "/workflows/external_request/US_TN/insert_tepe_contact_note",
                headers={"Origin": "http://localhost:3000"},
                json=request_body,
            )

        mock_task_manager.return_value.create_task.assert_called_once()
        mock_firestore.return_value.update_document.assert_called_once()
        self.assertEqual(response.status_code, HTTPStatus.INTERNAL_SERVER_ERROR)

    @patch("recidiviz.case_triage.workflows.workflows_routes.FirestoreClientImpl")
    @patch(
        "recidiviz.case_triage.workflows.workflows_routes.SingleCloudTaskQueueManager"
    )
    def test_insert_tepe_contact_note_missing_param(
        self,
        mock_task_manager: MagicMock,
        mock_firestore: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_tn")

        # No contactNoteDateTime
        request_body = {
            "personExternalId": PERSON_EXTERNAL_ID,
            "staffId": STAFF_ID,
            "contactNote": {1: ["Line 1", "Line 2"]},
        }

        mock_task_manager.return_value.create_task.side_effect = Exception

        with self.test_app.test_request_context():
            response = self.test_client.post(
                "/workflows/external_request/US_TN/insert_tepe_contact_note",
                headers={"Origin": "http://localhost:3000"},
                json=request_body,
            )

        mock_task_manager.return_value.create_task.assert_not_called()
        mock_firestore.return_value.update_document.assert_not_called()
        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)

    def test_handle_insert_tepe_contact_note_state_not_enabled(self) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_tn")

        request_body = {
            "person_external_id": PERSON_EXTERNAL_ID,
            "staff_id": STAFF_ID,
            "contact_note": {1: ["Line 1", "Line 2"]},
            "contact_note_date_time": str(datetime.datetime.now()),
        }

        with self.test_app.test_request_context():
            response = self.test_client.post(
                "/workflows/external_request/US_ID/handle_insert_tepe_contact_note",
                json=request_body,
            )
        self.assertEqual(response.status_code, HTTPStatus.UNAUTHORIZED)

    def test_handle_insert_tepe_contact_note_missing_param(self) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_tn")

        request_body = {
            "person_external_id": PERSON_EXTERNAL_ID,
            "staff_id": STAFF_ID,
            "contact_note": {1: ["Line 1", "Line 2"]},
        }
        with self.test_app.test_request_context():
            response = self.test_client.post(
                "/workflows/external_request/US_TN/handle_insert_tepe_contact_note",
                json=request_body,
                headers={
                    "Origin": "http://localhost:3000",
                    "User-Agent": "Google-Cloud-Tasks",
                },
            )
        self.assertEqual(response.status_code, HTTPStatus.INTERNAL_SERVER_ERROR)

    @patch(
        "recidiviz.case_triage.workflows.workflows_routes.WorkflowsUsTnExternalRequestInterface."
        "insert_tepe_contact_note"
    )
    @freeze_time("2023-01-01 01:23:45")
    def test_handle_insert_tepe_contact_note_success(
        self, mock_insert: MagicMock
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_tn")

        request_body = {
            "person_external_id": PERSON_EXTERNAL_ID,
            "staff_id": STAFF_ID,
            "contact_note": {1: ["Line 1", "Line 2"]},
            "contact_note_date_time": str(datetime.datetime.now()),
        }
        with self.test_app.test_request_context():
            mock_insert.return_value = None
            response = self.test_client.post(
                "/workflows/external_request/US_TN/handle_insert_tepe_contact_note",
                headers={
                    "Origin": "http://localhost:3000",
                    "User-Agent": "Google-Cloud-Tasks",
                },
                json=request_body,
            )

        expected_body = {
            "person_external_id": PERSON_EXTERNAL_ID,
            "staff_id": STAFF_ID,
            "contact_note_date_time": "2023-01-01T01:23:45",
            "contact_note": {1: ["Line 1", "Line 2"]},
        }
        self.assertEqual(
            mock_insert.call_args.kwargs,
            expected_body,
        )
        self.assertEqual(response.status_code, HTTPStatus.OK)

    @patch(
        "recidiviz.case_triage.workflows.workflows_routes.WorkflowsUsTnExternalRequestInterface."
        "insert_tepe_contact_note"
    )
    def test_handle_insert_tepe_contact_note_exception(
        self, mock_insert: MagicMock
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_tn")

        request_body = {
            "person_external_id": PERSON_EXTERNAL_ID,
            "staff_id": STAFF_ID,
            "contact_note": {1: ["Line 1", "Line 2"]},
            "contact_note_date_time": str(datetime.datetime.now()),
        }
        with self.test_app.test_request_context():
            mock_insert.side_effect = Exception
            response = self.test_client.post(
                "/workflows/external_request/US_TN/handle_insert_tepe_contact_note",
                json=request_body,
                headers={
                    "Origin": "http://localhost:3000",
                    "User-Agent": "Google-Cloud-Tasks",
                },
            )
        self.assertEqual(response.status_code, HTTPStatus.INTERNAL_SERVER_ERROR)

    @patch("uuid.uuid4")
    @patch("recidiviz.case_triage.workflows.workflows_routes.FirestoreClientImpl")
    @patch(
        "recidiviz.case_triage.workflows.workflows_routes.SingleCloudTaskQueueManager"
    )
    @freeze_time("2023-01-01 01:23:45")
    def test_enqueue_sms_request_success(
        self,
        mock_task_manager: MagicMock,
        mock_firestore: MagicMock,
        mock_uuid: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_ca", STAFF_EMAIL
        )
        mock_uuid.return_value = "MY UUID"

        request_body = {
            "recipientExternalId": PERSON_EXTERNAL_ID,
            "senderId": STAFF_EMAIL,
            "message": "Hello, is it me you're looking for?",
            "recipientPhoneNumber": "5153338822",
            "userHash": USER_HASH,
        }

        with self.test_app.test_request_context():
            response = self.test_client.post(
                "/workflows/external_request/US_CA/enqueue_sms_request",
                headers={"Origin": "http://localhost:3000"},
                json=request_body,
            )

        expected_task_body = {
            "recipient": f"+1{request_body['recipientPhoneNumber']}",
            "message": request_body["message"],
            "client_firestore_id": "us_ca_123",
            "recipient_external_id": PERSON_EXTERNAL_ID,
        }
        mock_task_manager.return_value.create_task.assert_called_once()
        self.assertEqual(
            mock_task_manager.return_value.create_task.call_args.kwargs["body"],
            expected_task_body,
        )
        mock_firestore.return_value.set_document.assert_called_once_with(
            "clientUpdatesV2/us_ca_123/milestonesMessages/milestones_01_2023",
            {
                "updated": {
                    "date": datetime.datetime.now(datetime.timezone.utc),
                    "by": STAFF_EMAIL,
                },
                "status": "IN_PROGRESS",
                "mid": "MY UUID",
                "sentBy": STAFF_EMAIL,
            },
            merge=True,
        )
        self.assertEqual(HTTPStatus.OK, response.status_code)

    def test_enqueue_sms_request_failure_unauthorized_state(
        self,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_tn")

        request_body = {
            "recipientExternalId": PERSON_EXTERNAL_ID,
            "senderId": STAFF_EMAIL,
            "message": "I can see it in your eyes",
            "recipientPhoneNumber": "5153338822",
            "userHash": USER_HASH,
        }

        with self.test_app.test_request_context():
            response = self.test_client.post(
                "/workflows/external_request/US_TN/enqueue_sms_request",
                headers={"Origin": "http://localhost:3000"},
                json=request_body,
            )

        self.assertEqual(HTTPStatus.UNAUTHORIZED, response.status_code)

    @patch("recidiviz.case_triage.workflows.workflows_routes.FirestoreClientImpl")
    @patch(
        "recidiviz.case_triage.workflows.workflows_routes.SingleCloudTaskQueueManager"
    )
    @freeze_time("2023-01-01 01:23:45")
    def test_enqueue_sms_request_create_task_failure(
        self,
        mock_task_manager: MagicMock,
        mock_firestore: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_ca", STAFF_EMAIL
        )
        mock_task_manager.return_value.create_task.side_effect = Exception

        request_body = {
            "recipientExternalId": PERSON_EXTERNAL_ID,
            "senderId": STAFF_EMAIL,
            "message": "I can see it in your smile",
            "recipientPhoneNumber": "5153338822",
            "userHash": USER_HASH,
        }

        with self.test_app.test_request_context():
            response = self.test_client.post(
                "/workflows/external_request/US_CA/enqueue_sms_request",
                headers={"Origin": "http://localhost:3000"},
                json=request_body,
            )

        mock_task_manager.return_value.create_task.assert_called_once()
        mock_firestore.return_value.set_document.assert_called_once_with(
            "clientUpdatesV2/us_ca_123/milestonesMessages/milestones_01_2023",
            {
                "status": "FAILURE",
                "updated": {
                    "date": datetime.datetime.now(datetime.timezone.utc),
                    "by": STAFF_EMAIL,
                },
            },
            merge=True,
        )
        self.assertEqual(HTTPStatus.INTERNAL_SERVER_ERROR, response.status_code)

    @patch(
        "recidiviz.case_triage.workflows.workflows_routes.allowed_twilio_dev_recipient"
    )
    def test_enqueue_sms_request_failure_not_allowed_number(
        self,
        mock_allowed_recipient: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_ca", STAFF_EMAIL
        )
        mock_allowed_recipient.return_value = False

        request_body = {
            "recipientExternalId": PERSON_EXTERNAL_ID,
            "senderId": STAFF_EMAIL,
            "message": "You're all I've ever wanted and my arms are open wide",
            "recipientPhoneNumber": "5153338822",
            "userHash": USER_HASH,
        }

        with self.test_app.test_request_context():
            response = self.test_client.post(
                "/workflows/external_request/US_CA/enqueue_sms_request",
                headers={"Origin": "http://localhost:3000"},
                json=request_body,
            )

        self.assertEqual(HTTPStatus.UNAUTHORIZED, response.status_code)

    def test_enqueue_sms_request_verifies_sender_id(
        self,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_ca", "NOT STAFF"
        )

        request_body = {
            "recipientExternalId": PERSON_EXTERNAL_ID,
            "senderId": STAFF_EMAIL,
            "message": "It's me, a very real staff member",
            "recipientPhoneNumber": "5153338822",
            "userHash": USER_HASH,
        }

        with self.test_app.test_request_context():
            response = self.test_client.post(
                "/workflows/external_request/US_CA/enqueue_sms_request",
                headers={"Origin": "http://localhost:3000"},
                json=request_body,
            )

        self.assertEqual(HTTPStatus.UNAUTHORIZED, response.status_code)

    @freeze_time("2023-01-01 01:23:45")
    @patch("twilio.rest.api.v2010.account.message.MessageList.create")
    @patch("recidiviz.case_triage.workflows.workflows_routes.FirestoreClientImpl")
    @patch("recidiviz.case_triage.workflows.workflows_routes.get_secret")
    def test_send_sms_request_valid(
        self,
        mock_get_secret: MagicMock,
        mock_firestore: MagicMock,
        mock_twilio_messages: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_ca")

        response = self.test_client.post(
            "/workflows/external_request/us_ca/send_sms_request",
            headers={"Origin": "http://localhost:3000"},
            json={
                "message": "Message!",
                "recipient": "+12223334444",
                "recipient_external_id": PERSON_EXTERNAL_ID,
                "client_firestore_id": "us_ca_123",
            },
        )

        assert_type(response.get_json(), dict)
        self.assertEqual(HTTPStatus.OK, response.status_code)
        mock_twilio_messages.assert_has_calls(
            [
                call(
                    body=f"Message!\n\n{OPT_OUT_MESSAGE}",
                    messaging_service_sid=mock_get_secret(),
                    to="+12223334444",
                    status_callback="http://localhost:5000/workflows/webhook/twilio_status",
                ),
            ]
        )
        mock_get_secret.assert_called()
        mock_firestore.return_value.set_document.assert_called()

    @patch("recidiviz.case_triage.workflows.workflows_routes.FirestoreClientImpl")
    @patch("twilio.rest.api.v2010.account.message.MessageList.create")
    @patch("recidiviz.case_triage.workflows.workflows_routes.get_secret")
    @freeze_time("2023-01-01 01:23:45")
    def test_send_sms_request_valid_from_recidiviz_user(
        self,
        mock_get_secret: MagicMock,
        mock_twilio_messages: MagicMock,
        mock_firestore: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "recidiviz", allowed_states=["US_CA"]
        )

        response = self.test_client.post(
            "/workflows/external_request/us_ca/send_sms_request",
            headers={"Origin": "http://localhost:3000"},
            json={
                "message": "Message!",
                "recipient": "+12223334444",
                "recipient_external_id": PERSON_EXTERNAL_ID,
                "client_firestore_id": "us_ca_123",
            },
        )
        assert_type(response.get_json(), dict)
        self.assertEqual(HTTPStatus.OK, response.status_code)
        mock_firestore.return_value.set_document.assert_called()
        mock_twilio_messages.assert_called()
        mock_get_secret.assert_called()

    def test_send_sms_request_unauthorized_from_recidiviz_user(self) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "recidiviz", allowed_states=["US_TN"]
        )

        response = self.test_client.post(
            "/workflows/external_request/us_ca/send_sms_request",
            headers={"Origin": "http://localhost:3000"},
            json={
                "message": "Message!",
                "recipient": "+12223334444",
                "recipient_external_id": PERSON_EXTERNAL_ID,
                "client_firestore_id": "us_ca_123",
            },
        )
        assert_type(response.get_json(), dict)
        self.assertEqual(HTTPStatus.UNAUTHORIZED, response.status_code)

    def test_send_sms_request_invalid_state_code(self) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("CA")

        response = self.test_client.post(
            "/workflows/external_request/us_tn/send_sms_request",
            headers={"Origin": "http://localhost:3000"},
            json={
                "message": "Message!",
                "recipient": "+12223334444",
                "recipient_external_id": PERSON_EXTERNAL_ID,
                "client_firestore_id": "us_ca_123",
            },
        )
        assert_type(response.get_json(), dict)
        self.assertEqual(HTTPStatus.UNAUTHORIZED, response.status_code)

    def test_send_sms_request_mismatched_state_code(self) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_tn")

        response = self.test_client.post(
            "/workflows/external_request/us_ca/send_sms_request",
            headers={"Origin": "http://localhost:3000"},
            json={
                "message": "Message!",
                "recipient": "+12223334444",
                "recipient_external_id": PERSON_EXTERNAL_ID,
                "client_firestore_id": "us_ca_123",
            },
        )
        assert_type(response.get_json(), dict)
        self.assertEqual(HTTPStatus.UNAUTHORIZED, response.status_code)

    @patch("recidiviz.case_triage.workflows.workflows_routes.FirestoreClientImpl")
    @patch("twilio.rest.api.v2010.account.message.MessageList.create")
    @patch("recidiviz.case_triage.workflows.workflows_routes.get_secret")
    @freeze_time("2023-01-01 01:23:45")
    def test_send_sms_request_twilio_failure(
        self,
        mock_get_secret: MagicMock,
        mock_twilio_messages: MagicMock,
        mock_firestore: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_ca")

        mock_twilio_messages.side_effect = Exception("Twilio API error")

        response = self.test_client.post(
            "/workflows/external_request/us_ca/send_sms_request",
            headers={"Origin": "http://localhost:3000"},
            json={
                "message": "Message!",
                "recipient": "+12223334444",
                "recipient_external_id": PERSON_EXTERNAL_ID,
                "client_firestore_id": "us_ca_123",
            },
        )
        assert_type(response.get_json(), dict)
        self.assertEqual(
            HTTPStatus.INTERNAL_SERVER_ERROR,
            response.status_code,
        )
        mock_firestore.return_value.set_document.assert_called_once_with(
            "clientUpdatesV2/us_ca_123/milestonesMessages/milestones_01_2023",
            {
                "status": "FAILURE",
                "updated": {
                    "date": datetime.datetime.now(datetime.timezone.utc),
                    "by": "RECIDIVIZ",
                },
                "errors": ["Twilio API error"],
            },
            merge=True,
        )
        mock_twilio_messages.assert_called()
        mock_get_secret.assert_called()

    @patch(
        "recidiviz.case_triage.workflows.workflows_routes.allowed_twilio_dev_recipient"
    )
    def test_send_sms_request_can_only_send_to_allowed_recipients(
        self,
        mock_allowed_recipient: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_ca")
        mock_allowed_recipient.return_value = False

        response = self.test_client.post(
            "/workflows/external_request/us_ca/send_sms_request",
            headers={"Origin": "http://localhost:3000"},
            json={
                "message": "Message!",
                "recipient": "+12223334444",
                "recipient_external_id": PERSON_EXTERNAL_ID,
                "client_firestore_id": "us_ca_123",
            },
        )
        assert_type(response.get_json(), dict)
        self.assertEqual(HTTPStatus.UNAUTHORIZED, response.status_code)

    @freeze_time("2023-01-01 01:23:45")
    @patch("recidiviz.case_triage.workflows.workflows_routes.FirestoreClientImpl")
    @patch("recidiviz.case_triage.helpers.TwilioValidator.validate")
    @patch("recidiviz.case_triage.workflows.workflows_routes.WorkflowsSegmentClient")
    def test_twilio_status_delivered(
        self,
        _mock_segment_client: MagicMock,
        mock_twilio_validator: MagicMock,
        mock_firestore: MagicMock,
    ) -> None:
        account_sid = "XYZ"
        doc = MagicMock()
        doc.to_dict.return_value = {"message_sid": "ABC"}
        doc.reference.path = (
            "clientUpdatesV2/us_ca_flinstone/milestonesMessages/milestones_01_2023"
        )
        mock_firestore.return_value.get_collection_group.return_value.where.return_value.get.return_value = [
            doc
        ]
        mock_twilio_validator.return_value = True

        response = self.test_client.post(
            "/workflows/webhook/twilio_status",
            headers={
                "Origin": "http://localhost:5000",
                "X-Twilio-Signature": "1234567a",
            },
            data={
                "MessageSid": "ABC",
                "MessageStatus": "delivered",
                "AccountSid": account_sid,
            },
        )
        self.assertEqual(HTTPStatus.NO_CONTENT, response.status_code)
        mock_firestore.return_value.set_document.assert_called_once_with(
            "clientUpdatesV2/us_ca_flinstone/milestonesMessages/milestones_01_2023",
            {
                "status": "SUCCESS",
                "lastUpdated": datetime.datetime.now(datetime.timezone.utc),
                "rawStatus": "delivered",
            },
            merge=True,
        )

    @freeze_time("2023-01-01 01:23:45")
    @patch("recidiviz.case_triage.workflows.workflows_routes.FirestoreClientImpl")
    @patch("recidiviz.case_triage.helpers.TwilioValidator.validate")
    @patch("recidiviz.case_triage.workflows.workflows_routes.WorkflowsSegmentClient")
    def test_twilio_status_failed(
        self,
        _mock_segment_client: MagicMock,
        mock_twilio_validator: MagicMock,
        mock_firestore: MagicMock,
    ) -> None:
        account_sid = "XYZ"
        doc = MagicMock()
        doc.to_dict.return_value = {"message_sid": "ABC"}
        doc.reference.path = (
            "clientUpdatesV2/us_ca_flinstone/milestonesMessages/milestones_01_2023"
        )
        mock_firestore.return_value.get_collection_group.return_value.where.return_value.get.return_value = [
            doc
        ]
        mock_twilio_validator.return_value = True

        response = self.test_client.post(
            "/workflows/webhook/twilio_status",
            headers={
                "Origin": "http://localhost:5000",
                "X-Twilio-Signature": "1234567a",
            },
            data={
                "MessageSid": "ABC",
                "MessageStatus": "failed",
                "AccountSid": account_sid,
                "ErrorCode": "30004",
            },
        )
        self.assertEqual(HTTPStatus.NO_CONTENT, response.status_code)
        mock_firestore.return_value.set_document.assert_called_once_with(
            "clientUpdatesV2/us_ca_flinstone/milestonesMessages/milestones_01_2023",
            {
                "status": "FAILURE",
                "errors": [
                    "The message could not be delivered. This recipient can't receive messages. Consider congratulating the client in person or through some other way."
                ],
                "lastUpdated": datetime.datetime.now(datetime.timezone.utc),
                "rawStatus": "failed",
                "errorCode": "30004",
            },
            merge=True,
        )

    @freeze_time("2023-01-01 01:23:45")
    @patch("recidiviz.case_triage.workflows.workflows_routes.FirestoreClientImpl")
    @patch("recidiviz.case_triage.helpers.TwilioValidator.validate")
    @patch("recidiviz.case_triage.workflows.workflows_routes.WorkflowsSegmentClient")
    def test_twilio_status_segment_update_opt_out(
        self,
        mock_segment_client: MagicMock,
        mock_twilio_validator: MagicMock,
        mock_firestore: MagicMock,
    ) -> None:
        doc = MagicMock()
        doc.to_dict.return_value = {"recipient": "5555555555", "userHash": "test-123"}
        doc.reference.path = (
            "clientUpdatesV2/us_ca_flinstone/milestonesMessages/milestones_01_2023"
        )
        mock_firestore.return_value.get_collection_group.return_value.where.return_value.get.return_value = [
            doc
        ]
        mock_twilio_validator.return_value = True

        opt_out_type = "STOP"
        response = self.test_client.post(
            "/workflows/webhook/twilio_incoming_message",
            headers={
                "Origin": "http://localhost:5000",
                "X-Twilio-Signature": "1234567a",
            },
            data={
                "From": "+15555555555",
                "OptOutType": opt_out_type,
            },
        )
        self.assertEqual(HTTPStatus.NO_CONTENT, response.status_code)
        mock_firestore.return_value.set_document.assert_called_once_with(
            "clientUpdatesV2/us_ca_flinstone/milestonesMessages/milestones_01_2023",
            {
                "lastUpdated": datetime.datetime.now(datetime.timezone.utc),
                "optOutType": opt_out_type,
            },
            merge=True,
        )

        mock_segment_client.return_value.track_milestones_message_opt_out.assert_called_with(
            user_hash="test-123", opt_out_type=opt_out_type
        )

    @freeze_time("2023-01-01 01:23:45")
    @patch("recidiviz.case_triage.workflows.workflows_routes.FirestoreClientImpl")
    @patch("recidiviz.case_triage.helpers.TwilioValidator.validate")
    @patch("recidiviz.case_triage.workflows.workflows_routes.WorkflowsSegmentClient")
    def test_twilio_status_segment_update_opt_out_changed(
        self,
        mock_segment_client: MagicMock,
        mock_twilio_validator: MagicMock,
        mock_firestore: MagicMock,
    ) -> None:
        doc = MagicMock()
        doc.to_dict.return_value = {
            "recipient": "5555555555",
            "userHash": "test-123",
            "optOutType": "START",
        }
        doc.reference.path = (
            "clientUpdatesV2/us_ca_flinstone/milestonesMessages/milestones_01_2023"
        )
        mock_firestore.return_value.get_collection_group.return_value.where.return_value.get.return_value = [
            doc
        ]
        mock_twilio_validator.return_value = True

        opt_out_type = "STOP"
        response = self.test_client.post(
            "/workflows/webhook/twilio_incoming_message",
            headers={
                "Origin": "http://localhost:5000",
                "X-Twilio-Signature": "1234567a",
            },
            data={
                "From": "+15555555555",
                "OptOutType": opt_out_type,
            },
        )
        self.assertEqual(HTTPStatus.NO_CONTENT, response.status_code)
        mock_firestore.return_value.set_document.assert_called_once_with(
            "clientUpdatesV2/us_ca_flinstone/milestonesMessages/milestones_01_2023",
            {
                "lastUpdated": datetime.datetime.now(datetime.timezone.utc),
                "optOutType": opt_out_type,
            },
            merge=True,
        )

        mock_segment_client.return_value.track_milestones_message_opt_out.assert_called_with(
            user_hash="test-123", opt_out_type=opt_out_type
        )

    @freeze_time("2023-01-01 01:23:45")
    @patch("recidiviz.case_triage.workflows.workflows_routes.FirestoreClientImpl")
    @patch("recidiviz.case_triage.helpers.TwilioValidator.validate")
    @patch("recidiviz.case_triage.workflows.workflows_routes.WorkflowsSegmentClient")
    def test_twilio_status_segment_update_opt_out_recipient_not_found(
        self,
        mock_segment_client: MagicMock,
        mock_twilio_validator: MagicMock,
        mock_firestore: MagicMock,
    ) -> None:
        mock_firestore.return_value.get_collection_group.return_value.where.return_value.get.return_value = (
            []
        )
        mock_twilio_validator.return_value = True

        opt_out_type = "STOP"
        response = self.test_client.post(
            "/workflows/webhook/twilio_incoming_message",
            headers={
                "Origin": "http://localhost:5000",
                "X-Twilio-Signature": "1234567a",
            },
            data={
                "From": "+12222222222",
                "OptOutType": opt_out_type,
            },
        )
        self.assertEqual(HTTPStatus.NO_CONTENT, response.status_code)
        mock_firestore.return_value.set_document.assert_not_called()
        mock_segment_client.return_value.track_milestones_message_opt_out.assert_not_called()

    @freeze_time("2023-01-01 01:23:45")
    @patch("recidiviz.case_triage.workflows.workflows_routes.FirestoreClientImpl")
    @patch("recidiviz.case_triage.helpers.TwilioValidator.validate")
    @patch("recidiviz.case_triage.workflows.workflows_routes.WorkflowsSegmentClient")
    def test_twilio_status_segment_unchanged_opt_out(
        self,
        mock_segment_client: MagicMock,
        mock_twilio_validator: MagicMock,
        mock_firestore: MagicMock,
    ) -> None:
        opt_out_type = "STOP"
        account_sid = "XYZ"
        doc = MagicMock()
        doc.to_dict.return_value = {
            "message_sid": "ABC",
            "userHash": "test-123",
            "optOutType": opt_out_type,
        }
        doc.reference.path = (
            "clientUpdatesV2/us_ca_flinstone/milestonesMessages/milestones_01_2023"
        )
        mock_firestore.return_value.get_collection_group.return_value.where.return_value.get.return_value = [
            doc
        ]
        mock_twilio_validator.return_value = True

        self.test_client.post(
            "/workflows/webhook/twilio_status",
            headers={
                "Origin": "http://localhost:5000",
                "X-Twilio-Signature": "1234567a",
            },
            data={
                "MessageSid": "ABC",
                "MessageStatus": "failed",
                "AccountSid": account_sid,
                "ErrorCode": "21610",
                "OptOutType": opt_out_type,
            },
        )
        mock_segment_client.return_value.track_milestones_message_opt_out.assert_not_called()

    @freeze_time("2023-01-01 01:23:45")
    @patch("recidiviz.case_triage.workflows.workflows_routes.FirestoreClientImpl")
    @patch("recidiviz.case_triage.helpers.TwilioValidator.validate")
    @patch("recidiviz.case_triage.workflows.workflows_routes.WorkflowsSegmentClient")
    def test_twilio_status_failed_with_exception(
        self,
        _mock_segment_client: MagicMock,
        mock_twilio_validator: MagicMock,
        mock_firestore: MagicMock,
    ) -> None:
        account_sid = "XYZ"
        error_code = "30001"
        doc = MagicMock()
        doc.to_dict.return_value = {"message_sid": "ABC"}
        doc.reference.path = (
            "clientUpdatesV2/us_ca_flinstone/milestonesMessages/milestones_01_2023"
        )
        mock_firestore.return_value.get_collection_group.return_value.where.return_value.get.return_value = [
            doc
        ]
        mock_twilio_validator.return_value = True

        response = self.test_client.post(
            "/workflows/webhook/twilio_status",
            headers={
                "Origin": "http://localhost:5000",
                "X-Twilio-Signature": "1234567a",
            },
            data={
                "MessageSid": "ABC",
                "MessageStatus": "failed",
                "AccountSid": account_sid,
                "ErrorCode": error_code,
            },
        )

        mock_firestore.return_value.set_document.assert_called_once_with(
            "clientUpdatesV2/us_ca_flinstone/milestonesMessages/milestones_01_2023",
            {
                "status": "FAILURE",
                "errors": [
                    "The message could not be delivered at this time. Consider congratulating the client in person or through some other way."
                ],
                "lastUpdated": datetime.datetime.now(datetime.timezone.utc),
                "rawStatus": "failed",
                "errorCode": error_code,
            },
            merge=True,
        )
        self.assertEqual(HTTPStatus.INTERNAL_SERVER_ERROR, response.status_code)
        self.assertEqual(
            b"Critical Twilio account error [30001] for message_sid [ABC]",
            response.data,
        )

    @freeze_time("2023-01-01 01:23:45")
    @patch("recidiviz.case_triage.workflows.workflows_routes.FirestoreClientImpl")
    @patch("recidiviz.case_triage.helpers.TwilioValidator.validate")
    @patch("recidiviz.case_triage.workflows.workflows_routes.WorkflowsSegmentClient")
    def test_twilio_status_sending(
        self,
        _mock_segment_client: MagicMock,
        mock_twilio_validator: MagicMock,
        mock_firestore: MagicMock,
    ) -> None:
        account_sid = "XYZ"
        doc = MagicMock()
        doc.to_dict.return_value = {"message_sid": "ABC"}
        doc.reference.path = (
            "clientUpdatesV2/us_ca_flinstone/milestonesMessages/milestones_01_2023"
        )
        mock_firestore.return_value.get_collection_group.return_value.where.return_value.get.return_value = [
            doc
        ]
        mock_twilio_validator.return_value = True

        response = self.test_client.post(
            "/workflows/webhook/twilio_status",
            headers={
                "Origin": "http://localhost:5000",
                "X-Twilio-Signature": "1234567a",
            },
            data={
                "MessageSid": "ABC",
                "MessageStatus": "sending",
                "AccountSid": account_sid,
            },
        )
        self.assertEqual(HTTPStatus.NO_CONTENT, response.status_code)
        mock_firestore.return_value.set_document.assert_called_once_with(
            "clientUpdatesV2/us_ca_flinstone/milestonesMessages/milestones_01_2023",
            {
                "status": "IN_PROGRESS",
                "lastUpdated": datetime.datetime.now(datetime.timezone.utc),
                "rawStatus": "sending",
            },
            merge=True,
        )

    @patch("recidiviz.case_triage.workflows.workflows_routes.FirestoreClientImpl")
    @patch("recidiviz.case_triage.helpers.TwilioValidator.validate")
    @patch("recidiviz.case_triage.workflows.workflows_routes.WorkflowsSegmentClient")
    def test_handle_twilio_status_segment_tracks_new_success_status(
        self,
        mock_segment_client: MagicMock,
        mock_twilio_validator: MagicMock,
        mock_firestore: MagicMock,
    ) -> None:
        account_sid = "XYZ"
        doc = MagicMock()
        doc.to_dict.return_value = {"message_sid": "ABC", "userHash": "test-123"}
        doc.reference.path = (
            "clientUpdatesV2/us_ca_flinstone/milestonesMessages/01_2023"
        )
        mock_firestore.return_value.get_collection_group.return_value.where.return_value.get.return_value = [
            doc
        ]
        mock_twilio_validator.return_value = True

        self.test_client.post(
            "/workflows/webhook/twilio_status",
            headers={
                "Origin": "http://localhost:5000",
                "X-Twilio-Signature": "1234567a",
            },
            data={
                "MessageSid": "ABC",
                "MessageStatus": "delivered",
                "AccountSid": account_sid,
            },
        )
        mock_segment_client.return_value.track_milestones_message_status.assert_called_with(
            user_hash="test-123",
            twilioRawStatus="delivered",
            status="SUCCESS",
            error_code=None,
            error_message=None,
        )

    @patch("recidiviz.case_triage.workflows.workflows_routes.FirestoreClientImpl")
    @patch("recidiviz.case_triage.helpers.TwilioValidator.validate")
    @patch("recidiviz.case_triage.workflows.workflows_routes.WorkflowsSegmentClient")
    def test_handle_twilio_status_segment_tracks_new_failed_status(
        self,
        mock_segment_client: MagicMock,
        mock_twilio_validator: MagicMock,
        mock_firestore: MagicMock,
    ) -> None:
        account_sid = "XYZ"
        doc = MagicMock()
        doc.to_dict.return_value = {"message_sid": "ABC", "userHash": "test-123"}
        doc.reference.path = (
            "clientUpdatesV2/us_ca_flinstone/milestonesMessages/01_2023"
        )
        mock_firestore.return_value.get_collection_group.return_value.where.return_value.get.return_value = [
            doc
        ]
        mock_twilio_validator.return_value = True

        self.test_client.post(
            "/workflows/webhook/twilio_status",
            headers={
                "Origin": "http://localhost:5000",
                "X-Twilio-Signature": "1234567a",
            },
            data={
                "MessageSid": "ABC",
                "MessageStatus": "undelivered",
                "AccountSid": account_sid,
                "ErrorCode": "30005",
            },
        )
        mock_segment_client.return_value.track_milestones_message_status.assert_called_with(
            user_hash="test-123",
            twilioRawStatus="undelivered",
            status="FAILURE",
            error_code="30005",
            error_message="The message could not be delivered. The mobile number entered is unknown or may no longer exist. Please update to a valid number and re-send.",
        )

    @patch("recidiviz.case_triage.workflows.workflows_routes.FirestoreClientImpl")
    @patch("recidiviz.case_triage.helpers.TwilioValidator.validate")
    @patch("recidiviz.case_triage.workflows.workflows_routes.WorkflowsSegmentClient")
    def test_handle_twilio_status_segment_unchanged_status(
        self,
        mock_segment_client: MagicMock,
        mock_twilio_validator: MagicMock,
        mock_firestore: MagicMock,
    ) -> None:
        account_sid = "XYZ"
        doc = MagicMock()
        doc.to_dict.return_value = {
            "rawStatus": "delivered",
            "message_sid": "ABC",
            "userHash": "test-123",
        }
        doc.reference.path = (
            "clientUpdatesV2/us_ca_flinstone/milestonesMessages/01_2023"
        )
        mock_firestore.return_value.get_collection_group.return_value.where.return_value.get.return_value = [
            doc
        ]
        mock_twilio_validator.return_value = True

        self.test_client.post(
            "/workflows/webhook/twilio_status",
            headers={
                "Origin": "http://localhost:5000",
                "X-Twilio-Signature": "1234567a",
            },
            data={
                "MessageSid": "ABC",
                "MessageStatus": "delivered",
                "AccountSid": account_sid,
            },
        )
        mock_segment_client.return_value.track_milestones_message_status.assert_not_called()

    @patch(
        "recidiviz.case_triage.workflows.workflows_routes.WorkflowsUsNdExternalRequestInterface",
        autospec=True,
    )
    @patch(
        "recidiviz.case_triage.workflows.workflows_routes.SingleCloudTaskQueueManager"
    )
    def test_update_docstars_early_termination_date_success(
        self,
        mock_task_manager: MagicMock,
        mock_interface: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_nd", "foo@nd.gov"
        )

        request_body = {
            "personExternalId": "1234",
            "userEmail": "foo@nd.gov",
            "earlyTerminationDate": "2024-01-01",
            "justificationReasons": [{"code": "FOO", "description": "Code foo."}],
        }

        with self.test_app.test_request_context():
            response = self.test_client.post(
                "/workflows/external_request/US_ND/update_docstars_early_termination_date",
                headers={"Origin": "http://localhost:3000"},
                json=request_body,
            )

        expected_task_body = {
            "person_external_id": 1234,
            "user_email": "foo@nd.gov",
            "early_termination_date": "2024-01-01",
            "justification_reasons": [{"code": "FOO", "description": "Code foo."}],
        }
        mock_task_manager.return_value.create_task.assert_called_once()
        self.assertEqual(
            mock_task_manager.return_value.create_task.call_args.kwargs["body"],
            expected_task_body,
        )
        mock_interface.return_value.update_early_termination_date.assert_not_called()
        mock_interface.return_value.set_firestore_early_termination_status.assert_called_with(
            ExternalSystemRequestStatus.IN_PROGRESS
        )
        self.assertEqual(response.status_code, HTTPStatus.OK)

    @patch(
        "recidiviz.case_triage.workflows.workflows_routes.WorkflowsUsNdExternalRequestInterface",
        autospec=True,
    )
    @patch(
        "recidiviz.case_triage.workflows.workflows_routes.SingleCloudTaskQueueManager"
    )
    def test_update_docstars_early_termination_date_no_queue_success(
        self,
        mock_task_manager: MagicMock,
        mock_interface: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_nd", "foo@nd.gov"
        )

        request_body = {
            "personExternalId": "1234",
            "userEmail": "foo@nd.gov",
            "earlyTerminationDate": "2024-01-01",
            "justificationReasons": [{"code": "FOO", "description": "Code foo."}],
            "shouldQueueTask": False,
        }

        with self.test_app.test_request_context():
            response = self.test_client.post(
                "/workflows/external_request/US_ND/update_docstars_early_termination_date",
                headers={"Origin": "http://localhost:3000"},
                json=request_body,
            )

        expected_body = {
            "user_email": "foo@nd.gov",
            "early_termination_date": "2024-01-01",
            "justification_reasons": [{"code": "FOO", "description": "Code foo."}],
        }
        mock_task_manager.return_value.create_task.assert_not_called()
        mock_interface.return_value.update_early_termination_date.assert_called_once_with(
            **expected_body,
        )
        mock_interface.return_value.set_firestore_early_termination_status.assert_called_with(
            ExternalSystemRequestStatus.SUCCESS
        )
        self.assertEqual(response.status_code, HTTPStatus.OK)

    @patch(
        "recidiviz.case_triage.workflows.workflows_routes.WorkflowsUsNdExternalRequestInterface",
        autospec=True,
    )
    @patch(
        "recidiviz.case_triage.workflows.workflows_routes.SingleCloudTaskQueueManager"
    )
    def test_update_docstars_early_termination_date_no_queue_api_failure(
        self,
        mock_task_manager: MagicMock,
        mock_interface: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_nd", "foo@nd.gov"
        )

        mock_interface.return_value.update_early_termination_date.side_effect = (
            Exception("It broke!")
        )

        request_body = {
            "personExternalId": "1234",
            "userEmail": "foo@nd.gov",
            "earlyTerminationDate": "2024-01-01",
            "justificationReasons": [{"code": "FOO", "description": "Code foo."}],
            "shouldQueueTask": False,
        }

        with self.test_app.test_request_context():
            response = self.test_client.post(
                "/workflows/external_request/US_ND/update_docstars_early_termination_date",
                headers={"Origin": "http://localhost:3000"},
                json=request_body,
            )

        expected_body = {
            "user_email": "foo@nd.gov",
            "early_termination_date": "2024-01-01",
            "justification_reasons": [{"code": "FOO", "description": "Code foo."}],
        }
        mock_task_manager.return_value.create_task.assert_not_called()
        mock_interface.return_value.update_early_termination_date.assert_called_once_with(
            **expected_body,
        )
        mock_interface.return_value.set_firestore_early_termination_status.assert_called_with(
            ExternalSystemRequestStatus.FAILURE
        )
        self.assertEqual(response.status_code, HTTPStatus.INTERNAL_SERVER_ERROR)

    @patch(
        "recidiviz.case_triage.workflows.workflows_routes.WorkflowsUsNdExternalRequestInterface",
        autospec=True,
    )
    @patch(
        "recidiviz.case_triage.workflows.workflows_routes.SingleCloudTaskQueueManager"
    )
    def test_update_docstars_early_termination_queue_failure(
        self,
        mock_task_manager: MagicMock,
        mock_interface: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_nd", "foo@nd.gov"
        )

        mock_task_manager.return_value.create_task.side_effect = Exception("It broke!")

        request_body = {
            "personExternalId": "1234",
            "userEmail": "foo@nd.gov",
            "earlyTerminationDate": "2024-01-01",
            "justificationReasons": [{"code": "FOO", "description": "Code foo."}],
        }

        with self.test_app.test_request_context():
            response = self.test_client.post(
                "/workflows/external_request/US_ND/update_docstars_early_termination_date",
                headers={"Origin": "http://localhost:3000"},
                json=request_body,
            )

        expected_task_body = {
            "person_external_id": 1234,
            "user_email": "foo@nd.gov",
            "early_termination_date": "2024-01-01",
            "justification_reasons": [{"code": "FOO", "description": "Code foo."}],
        }
        mock_task_manager.return_value.create_task.assert_called_once()
        self.assertEqual(
            mock_task_manager.return_value.create_task.call_args.kwargs["body"],
            expected_task_body,
        )
        mock_interface.return_value.update_early_termination_date.assert_not_called()
        mock_interface.return_value.set_firestore_early_termination_status.assert_called_with(
            ExternalSystemRequestStatus.FAILURE
        )
        self.assertEqual(response.status_code, HTTPStatus.INTERNAL_SERVER_ERROR)

    @patch(
        "recidiviz.case_triage.workflows.workflows_routes.WorkflowsUsNdExternalRequestInterface",
        autospec=True,
    )
    @patch(
        "recidiviz.case_triage.workflows.workflows_routes.SingleCloudTaskQueueManager"
    )
    def test_update_docstars_early_termination_date_bad_state(
        self,
        mock_task_manager: MagicMock,
        mock_interface: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_tn", "foo@tn.gov"
        )

        request_body = {
            "personExternalId": "1234",
            "userEmail": "foo@tn.gov",
            "earlyTerminationDate": "2024-01-01",
            "justificationReasons": [{"code": "FOO", "description": "Code foo."}],
        }

        with self.test_app.test_request_context():
            response = self.test_client.post(
                "/workflows/external_request/US_TN/update_docstars_early_termination_date",
                headers={"Origin": "http://localhost:3000"},
                json=request_body,
            )

        mock_task_manager.return_value.create_task.assert_not_called()
        mock_interface.return_value.update_early_termination_date.assert_not_called()
        mock_interface.return_value.set_firestore_early_termination_status.assert_not_called()
        self.assertEqual(response.status_code, HTTPStatus.UNAUTHORIZED)

    @patch(
        "recidiviz.case_triage.workflows.workflows_routes.WorkflowsUsNdExternalRequestInterface",
        autospec=True,
    )
    @patch(
        "recidiviz.case_triage.workflows.workflows_routes.SingleCloudTaskQueueManager"
    )
    def test_update_docstars_early_termination_date_mismatched_email(
        self,
        mock_task_manager: MagicMock,
        mock_interface: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_nd", "foo@nd.gov"
        )

        request_body = {
            "personExternalId": "1234",
            "userEmail": "bar@tn.gov",
            "earlyTerminationDate": "2024-01-01",
            "justificationReasons": [{"code": "FOO", "description": "Code foo."}],
        }

        with self.test_app.test_request_context():
            response = self.test_client.post(
                "/workflows/external_request/US_TN/update_docstars_early_termination_date",
                headers={"Origin": "http://localhost:3000"},
                json=request_body,
            )

        mock_task_manager.return_value.create_task.assert_not_called()
        mock_interface.return_value.update_early_termination_date.assert_not_called()
        mock_interface.return_value.set_firestore_early_termination_status.assert_not_called()
        self.assertEqual(response.status_code, HTTPStatus.UNAUTHORIZED)

    @patch(
        "recidiviz.case_triage.workflows.workflows_routes.WorkflowsUsNdExternalRequestInterface",
        autospec=True,
    )
    @patch(
        "recidiviz.case_triage.workflows.workflows_routes.SingleCloudTaskQueueManager"
    )
    def test_update_docstars_early_termination_date_missing_field(
        self,
        mock_task_manager: MagicMock,
        mock_interface: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_nd", "foo@nd.gov"
        )

        request_body = {
            "personExternalId": "1234",
            "userEmail": "foo@nd.gov",
            # missing earlyTerminationDate
            "justificationReasons": [{"code": "FOO", "description": "Code foo."}],
        }

        with self.test_app.test_request_context():
            response = self.test_client.post(
                "/workflows/external_request/US_ND/update_docstars_early_termination_date",
                headers={"Origin": "http://localhost:3000"},
                json=request_body,
            )

        mock_task_manager.return_value.create_task.assert_not_called()
        mock_interface.return_value.update_early_termination_date.assert_not_called()
        mock_interface.return_value.set_firestore_early_termination_status.assert_not_called()
        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)

    @patch(
        "recidiviz.case_triage.workflows.workflows_routes.WorkflowsUsNdExternalRequestInterface",
        autospec=True,
    )
    def test_handle_update_docstars_early_termination_date_success(
        self,
        mock_interface: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_nd", "foo@nd.gov"
        )

        request_body = {
            "person_external_id": 1234,
            "user_email": "foo@nd.gov",
            "early_termination_date": "2024-01-01",
            "justification_reasons": [{"code": "FOO", "description": "Code foo."}],
        }

        with self.test_app.test_request_context():
            response = self.test_client.post(
                "/workflows/external_request/US_ND/handle_update_docstars_early_termination_date",
                headers={
                    "Origin": "http://localhost:3000",
                    "User-Agent": "Google-Cloud-Tasks",
                },
                json=request_body,
            )

        expected_body = {
            "user_email": "foo@nd.gov",
            "early_termination_date": "2024-01-01",
            "justification_reasons": [{"code": "FOO", "description": "Code foo."}],
        }

        mock_interface.return_value.update_early_termination_date.assert_called_once_with(
            **expected_body
        )
        mock_interface.return_value.set_firestore_early_termination_status.assert_called_once_with(
            ExternalSystemRequestStatus.SUCCESS
        )
        self.assertEqual(response.status_code, HTTPStatus.OK)

    @patch(
        "recidiviz.case_triage.workflows.workflows_routes.WorkflowsUsNdExternalRequestInterface",
        autospec=True,
    )
    def test_handle_update_docstars_early_termination_date_api_failure(
        self,
        mock_interface: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_nd", "foo@nd.gov"
        )

        mock_interface.return_value.update_early_termination_date.side_effect = (
            Exception("It broke!")
        )

        request_body = {
            "person_external_id": 1234,
            "user_email": "foo@nd.gov",
            "early_termination_date": "2024-01-01",
            "justification_reasons": [{"code": "FOO", "description": "Code foo."}],
        }

        with self.test_app.test_request_context():
            response = self.test_client.post(
                "/workflows/external_request/US_ND/handle_update_docstars_early_termination_date",
                headers={
                    "Origin": "http://localhost:3000",
                    "User-Agent": "Google-Cloud-Tasks",
                },
                json=request_body,
            )

        expected_body = {
            "user_email": "foo@nd.gov",
            "early_termination_date": "2024-01-01",
            "justification_reasons": [{"code": "FOO", "description": "Code foo."}],
        }

        mock_interface.return_value.update_early_termination_date.assert_called_once_with(
            **expected_body
        )
        mock_interface.return_value.set_firestore_early_termination_status.assert_called_once_with(
            ExternalSystemRequestStatus.FAILURE
        )
        self.assertEqual(response.status_code, HTTPStatus.INTERNAL_SERVER_ERROR)

    @patch(
        "recidiviz.case_triage.workflows.workflows_routes.WorkflowsUsNdExternalRequestInterface",
        autospec=True,
    )
    def test_handle_update_docstars_early_termination_date_missing_pei(
        self,
        mock_interface: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_nd", "foo@nd.gov"
        )

        request_body = {
            "user_email": "foo@nd.gov",
            "early_termination_date": "2024-01-01",
            "justification_reasons": [{"code": "FOO", "description": "Code foo."}],
        }

        with self.test_app.test_request_context():
            response = self.test_client.post(
                "/workflows/external_request/US_ND/handle_update_docstars_early_termination_date",
                headers={
                    "Origin": "http://localhost:3000",
                    "User-Agent": "Google-Cloud-Tasks",
                },
                json=request_body,
            )

        mock_interface.return_value.update_early_termination_date.assert_not_called()
        mock_interface.return_value.set_firestore_early_termination_status.assert_not_called()
        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)

    @patch(
        "recidiviz.case_triage.workflows.workflows_routes.WorkflowsUsNdExternalRequestInterface",
        autospec=True,
    )
    def test_handle_update_docstars_early_termination_date_missing_field(
        self,
        mock_interface: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_nd", "foo@nd.gov"
        )

        request_body = {
            "person_external_id": 1234,
            # missing user_email
            "early_termination_date": "2024-01-01",
            "justification_reasons": [{"code": "FOO", "description": "Code foo."}],
        }

        with self.test_app.test_request_context():
            response = self.test_client.post(
                "/workflows/external_request/US_ND/handle_update_docstars_early_termination_date",
                headers={
                    "Origin": "http://localhost:3000",
                    "User-Agent": "Google-Cloud-Tasks",
                },
                json=request_body,
            )

        mock_interface.return_value.update_early_termination_date.assert_not_called()
        mock_interface.return_value.set_firestore_early_termination_status.assert_called_once_with(
            ExternalSystemRequestStatus.FAILURE
        )
        self.assertEqual(response.status_code, HTTPStatus.INTERNAL_SERVER_ERROR)

    @patch("recidiviz.case_triage.workflows.workflows_routes.WorkflowsQuerier")
    def test_workflows_config_response(self, mock_workflows_querier: MagicMock) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_id", "foo@id.gov"
        )

        mock_workflows_querier.return_value.get_enabled_opportunities.return_value = [
            FullOpportunityInfo(
                state_code="US_ID",
                opportunity_type="oppType",
                system_type=WorkflowsSystemType.SUPERVISION,
                url_section="urlSection",
                firestore_collection="firestoreCollection",
                homepage_position=1,
                # These keys are internal and should not end up in the final config
                experiment_id="1234",
                last_updated_by="Kenneth Ojibe",
                last_updated_at=datetime.datetime(2024, 11, 26),
                completion_event="NEW_YEARS",
            )
        ]

        fake_config = OpportunityConfig(
            state_code="US_ID",
            opportunity_type="oppType",
            display_name="Opportunity",
            methodology_url="example.com",
            initial_header="header",
            dynamic_eligibility_text="dynamic text[|s]",
            call_to_action="action",
            subheading="the subheading",
            snooze={"default_snooze_days": 12, "max_snooze_days": 90},
            is_alert=False,
            priority="NORMAL",
            denial_text=None,
            eligible_criteria_copy=[
                {
                    "key": "criteria",
                    "text": "baz",
                    "tooltip": "fill this:{{opportunity.client.goop}}",
                }
            ],
            ineligible_criteria_copy=[],
            denial_reasons=[{"key": "DENY", "text": "Denied"}],
            sidebar_components=["someComponent", "someOtherComponent"],
            eligibility_date_text=None,
            hide_denial_revert=True,
            tooltip_eligibility_text="eligible",
            tab_groups=None,
            compare_by=[
                {
                    "field": "eligibilityDate",
                    "sort_direction": "asc",
                    "undefined_behavior": "undefinedFirst",
                }
            ],
            notifications=[],
            zero_grants_tooltip="example tooltip",
            denied_tab_title="Marked Ineligible",
            denial_adjective="Ineligible",
            denial_noun="Ineligibility",
            supports_submitted=True,
            submitted_tab_title="Submitted",
            empty_tab_copy=[],
            tab_preface_copy=[],
            subcategory_headings=[],
            subcategory_orderings=[],
            mark_submitted_options_by_tab=[],
            oms_criteria_header="Validated by data from OMS",
            non_oms_criteria_header="Requirements to check",
            non_oms_criteria=[{}],
            highlight_cases_on_homepage=False,
            highlighted_case_cta_copy="Opportunity name",
            overdue_opportunity_callout_copy="overdue for opportunity",
            snooze_companion_opportunity_types=["usNdOppType1", "usNdOppType2"],
            case_notes_title=None,
        )

        mock_workflows_querier.return_value.get_top_config_for_opportunity_types.return_value = {
            "oppType": fake_config
        }

        with self.test_app.test_request_context():
            response = self.test_client.get(
                "/workflows/US_ID/opportunities",
                headers={"Origin": "http://localhost:3000"},
            )

        expected_config = {
            "firestoreCollection": "firestoreCollection",
            "homepagePosition": 1,
            "systemType": "SUPERVISION",
            "urlSection": "urlSection",
            **convert_nested_dictionary_keys(fake_config.__dict__, snake_to_camel),
        }

        expected_config.pop("opportunityType")

        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.assertEqual(
            response.get_json(),
            {"enabledConfigs": {"oppType": expected_config}},
        )

    @patch(
        "recidiviz.case_triage.workflows.workflows_routes.SendGridClientWrapper",
        autospec=True,
    )
    def test_handle_email_user_success(
        self,
        mock_sendgrid_client: MagicMock,
    ) -> None:
        user_email = "foo@example.com"
        subject = "test subject"
        body = "test body"

        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_tx", "foo@example.com"
        )

        request_body = {
            "user_email": user_email,
            "email_subject": subject,
            "email_body": body,
        }

        with self.test_app.test_request_context():
            response = self.test_client.post(
                "/workflows/external_request/US_TX/email_user",
                headers={"Origin": "http://localhost:3000"},
                json=request_body,
            )

        expected_send_message_args = {
            "to_email": user_email,
            "from_email": "no-reply@recidiviz.org",
            "from_email_name": "Recidiviz",
            "subject": subject,
            "html_content": body,
            "reply_to_email": "feedback@recidiviz.org",
            "reply_to_name": "Recidiviz Support",
            "disable_unsubscribe": True,
        }
        mock_sendgrid_client.return_value.send_message.assert_called_once()
        self.assertEqual(
            mock_sendgrid_client.return_value.send_message.call_args.kwargs,
            expected_send_message_args,
        )
        self.assertEqual(response.status_code, HTTPStatus.OK)

    @patch(
        "recidiviz.case_triage.workflows.workflows_routes.SendGridClientWrapper",
        autospec=True,
    )
    def test_handle_email_user_unauthorized_state(
        self,
        mock_sendgrid_client: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_az", "foo@example.com"
        )

        request_body = {
            "user_email": "foo@example.com",
            "email_subject": "test subject",
            "email_body": "test body",
        }

        with self.test_app.test_request_context():
            response = self.test_client.post(
                "/workflows/external_request/US_AZ/email_user",
                headers={"Origin": "http://localhost:3000"},
                json=request_body,
            )

        mock_sendgrid_client.return_value.send_message.assert_not_called()
        self.assertEqual(response.status_code, HTTPStatus.UNAUTHORIZED)

    @patch(
        "recidiviz.case_triage.workflows.workflows_routes.SendGridClientWrapper",
        autospec=True,
    )
    def test_handle_email_user_mismatched_email(
        self,
        mock_sendgrid_client: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_tx", "foo@example.com"
        )

        request_body = {
            # We shouldn't be allowed to email anyone else.
            "user_email": "other-email@example.com",
            "email_subject": "test subject",
            "email_body": "test body",
        }

        with self.test_app.test_request_context():
            response = self.test_client.post(
                "/workflows/external_request/US_TX/email_user",
                headers={"Origin": "http://localhost:3000"},
                json=request_body,
            )

        mock_sendgrid_client.return_value.send_message.assert_not_called()
        self.assertEqual(response.status_code, HTTPStatus.UNAUTHORIZED)

    @patch(
        "recidiviz.case_triage.workflows.workflows_routes.SendGridClientWrapper",
        autospec=True,
    )
    def test_handle_email_user_missing_field(
        self,
        mock_sendgrid_client: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            "us_tx", "foo@example.com"
        )

        request_body = {
            "user_email": "foo@example.com",
            "email_subject": "test subject",
            # Missing email_body
        }

        with self.test_app.test_request_context():
            response = self.test_client.post(
                "/workflows/external_request/US_TX/email_user",
                headers={"Origin": "http://localhost:3000"},
                json=request_body,
            )

        mock_sendgrid_client.return_value.send_message.assert_not_called()
        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)

    def test_workflows_config_disabled_state(self) -> None:
        with self.test_app.test_request_context():
            response = self.test_client.get(
                "/workflows/US_RI/opportunities",
                headers={"Origin": "http://localhost:3000"},
            )

        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.assertEqual({"enabledConfigs": {}}, response.get_json())  # type: ignore


def make_cors_test(
    path: str,
    request_origin: str,
    request_method: str = "OPTIONS",
    expected_headers: Optional[Dict[str, str]] = None,
    expected_status: int = 200,
) -> Callable:
    def inner(self: WorkflowsBlueprintTestCase) -> None:
        response = self.test_client.open(
            method=request_method,
            path=path,
            headers={"Origin": request_origin},
        )

        self.assertEqual(expected_status, response.status_code, response.get_json())

        for header, value in (expected_headers or {}).items():
            self.assertEqual(value, response.headers[header])

    return inner


class TestWorkflowsCORS(WorkflowsBlueprintTestCase):
    """Tests various CORS scenarios"""

    test_localhost_is_allowed = make_cors_test(
        path="/workflows/external_request/US_TN/insert_tepe_contact_note",
        request_origin="http://localhost:3000",
        expected_headers={
            "Access-Control-Allow-Origin": "http://localhost:3000",
            "Access-Control-Allow-Headers": "authorization, sentry-trace, x-csrf-token, content-type, baggage",
            "Access-Control-Max-Age": "7200",
            "Vary": "Origin",
        },
    )

    test_handler_localhost_is_allowed = make_cors_test(
        path="/workflows/external_request/US_TN/handle_insert_tepe_contact_note",
        request_origin="http://localhost:3000",
        expected_headers={
            "Access-Control-Allow-Origin": "http://localhost:3000",
            "Access-Control-Allow-Headers": "authorization, sentry-trace, x-csrf-token, content-type, baggage",
            "Access-Control-Max-Age": "7200",
            "Vary": "Origin",
        },
    )

    test_staging_is_allowed = make_cors_test(
        path="/workflows/external_request/US_TN/insert_tepe_contact_note",
        request_origin="https://dashboard-staging.recidiviz.org",
        expected_headers={
            "Access-Control-Allow-Origin": "https://dashboard-staging.recidiviz.org",
            "Access-Control-Allow-Headers": "authorization, sentry-trace, x-csrf-token, content-type, baggage",
            "Vary": "Origin",
        },
    )

    test_prod_is_allowed = make_cors_test(
        path="/workflows/external_request/US_TN/insert_tepe_contact_note",
        request_origin="https://dashboard.recidiviz.org",
        expected_headers={
            "Access-Control-Allow-Origin": "https://dashboard.recidiviz.org",
            "Access-Control-Allow-Headers": "authorization, sentry-trace, x-csrf-token, content-type, baggage",
            "Vary": "Origin",
        },
    )

    test_preview_apps_are_allowed = make_cors_test(
        path="/workflows/external_request/US_TN/insert_tepe_contact_note",
        request_origin="https://recidiviz-dashboard-stag-e1108--preview-999a999.web.app",
        expected_headers={
            "Access-Control-Allow-Origin": "https://recidiviz-dashboard-stag-e1108--preview-999a999.web.app",
            "Access-Control-Allow-Headers": "authorization, sentry-trace, x-csrf-token, content-type, baggage",
            "Vary": "Origin",
        },
    )

    test_spoof_preview_is_disallowed = make_cors_test(
        path="/workflows/external_request/US_TN/handle_insert_tepe_contact_note",
        request_origin="https://recidiviz-dashboard-stag-e1108--officer-7tjx0jmi.fake.web.app",
        expected_status=HTTPStatus.FORBIDDEN,
    )

    test_spoof_preview_2_is_disallowed = make_cors_test(
        path="/workflows/external_request/US_TN/handle_insert_tepe_contact_note",
        request_origin="https://preview.hacked.com/web.app",
        expected_status=HTTPStatus.FORBIDDEN,
    )
