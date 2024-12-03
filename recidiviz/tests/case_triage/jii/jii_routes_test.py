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
"""Implement tests for the JII API"""
import datetime
import os
from http import HTTPStatus
from typing import Callable, Optional
from unittest import TestCase, mock
from unittest.mock import MagicMock, patch

from flask import Flask
from flask.testing import FlaskClient
from freezegun import freeze_time
from google.cloud.firestore_v1 import ArrayUnion

from recidiviz.case_triage.error_handlers import register_error_handlers
from recidiviz.case_triage.jii.jii_texts_routes import create_jii_api_blueprint
from recidiviz.case_triage.workflows.workflows_authorization import (
    on_successful_authorization,
)


class JIIBlueprintTestCase(TestCase):
    """Base class for JII flask Tests"""

    mock_authorization_handler: MagicMock
    test_app: Flask
    test_client: FlaskClient

    def setUp(self) -> None:
        self.mock_authorization_handler = MagicMock()

        self.auth_patcher = mock.patch(
            f"{create_jii_api_blueprint.__module__}.build_authorization_handler",
            return_value=self.mock_authorization_handler,
        )

        self.auth_patcher.start()

        self.test_app = Flask(__name__)
        register_error_handlers(self.test_app)
        self.test_app.register_blueprint(create_jii_api_blueprint(), url_prefix="/jii")
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


class TestJIIRoutes(JIIBlueprintTestCase):
    """Implements tests for the JII routes."""

    def setUp(self) -> None:
        super().setUp()

        self.old_auth_claim_namespace = os.environ.get("AUTH0_CLAIM_NAMESPACE", None)
        os.environ["AUTH0_CLAIM_NAMESPACE"] = "https://recidiviz-test"
        self.fake_url = "http://fake-url.com"

    def tearDown(self) -> None:
        super().tearDown()

    @freeze_time("2023-01-01 01:23:45")
    @patch("recidiviz.case_triage.jii.jii_texts_routes.FirestoreClientImpl")
    @patch("recidiviz.case_triage.helpers.TwilioValidator.validate")
    def test_twilio_status_delivered(
        self,
        mock_twilio_validator: MagicMock,
        mock_firestore: MagicMock,
    ) -> None:
        account_sid = "XYZ"
        doc = MagicMock()
        doc.to_dict.return_value = {"message_sid": "ABC"}
        doc.reference.path = "twilio_messages/us_id_999999999/lsu_eligibility_messages/eligibility_01_2023"
        mock_firestore.return_value.get_collection_group.return_value.where.return_value.stream.return_value = [
            doc
        ]
        mock_twilio_validator.return_value = True

        response = self.test_client.post(
            "/jii/webhook/twilio_status",
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
            "twilio_messages/us_id_999999999/lsu_eligibility_messages/eligibility_01_2023",
            {
                "status": "SUCCESS",
                "status_last_updated": datetime.datetime.now(datetime.timezone.utc),
                "raw_status": "delivered",
            },
            merge=True,
        )

    @freeze_time("2023-01-01 01:23:45")
    @patch("recidiviz.case_triage.jii.jii_texts_routes.FirestoreClientImpl")
    @patch("recidiviz.case_triage.helpers.TwilioValidator.validate")
    def test_twilio_status_failed(
        self,
        mock_twilio_validator: MagicMock,
        mock_firestore: MagicMock,
    ) -> None:
        account_sid = "XYZ"
        doc = MagicMock()
        doc.to_dict.return_value = {"message_sid": "ABC"}
        doc.reference.path = (
            "twilio_messages/999999999/lsu_eligibility_messages/eligibility_01_2023"
        )
        mock_firestore.return_value.get_collection_group.return_value.where.return_value.stream.return_value = [
            doc
        ]
        mock_twilio_validator.return_value = True

        response = self.test_client.post(
            "/jii/webhook/twilio_status",
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
            "twilio_messages/999999999/lsu_eligibility_messages/eligibility_01_2023",
            {
                "status": "FAILURE",
                "errors": ["Message blocked"],
                "status_last_updated": datetime.datetime.now(datetime.timezone.utc),
                "raw_status": "failed",
                "error_code": "30004",
            },
            merge=True,
        )

    @freeze_time("2023-01-01 01:23:45")
    @patch("recidiviz.case_triage.jii.jii_texts_routes.FirestoreClientImpl")
    @patch("recidiviz.case_triage.helpers.TwilioValidator.validate")
    def test_twilio_status_failed_with_exception(
        self,
        mock_twilio_validator: MagicMock,
        mock_firestore: MagicMock,
    ) -> None:
        account_sid = "XYZ"
        error_code = "30001"
        doc = MagicMock()
        doc.to_dict.return_value = {"message_sid": "ABC"}
        doc.reference.path = (
            "twilio_messages/999999999/lsu_eligibility_messages/eligibility_01_2023"
        )
        mock_firestore.return_value.get_collection_group.return_value.where.return_value.stream.return_value = [
            doc
        ]
        mock_twilio_validator.return_value = True

        response = self.test_client.post(
            "/jii/webhook/twilio_status",
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
            "twilio_messages/999999999/lsu_eligibility_messages/eligibility_01_2023",
            {
                "status": "FAILURE",
                "errors": ["Queue overflow"],
                "status_last_updated": datetime.datetime.now(datetime.timezone.utc),
                "raw_status": "failed",
                "error_code": error_code,
            },
            merge=True,
        )
        self.assertEqual(HTTPStatus.INTERNAL_SERVER_ERROR, response.status_code)
        self.assertEqual(
            b"Critical Twilio account error [30001] for message_sid [ABC]",
            response.data,
        )

    @freeze_time("2023-01-01 01:23:45")
    @patch("recidiviz.case_triage.jii.jii_texts_routes.FirestoreClientImpl")
    @patch("recidiviz.case_triage.helpers.TwilioValidator.validate")
    def test_twilio_status_sending(
        self,
        mock_twilio_validator: MagicMock,
        mock_firestore: MagicMock,
    ) -> None:
        account_sid = "XYZ"
        doc = MagicMock()
        doc.to_dict.return_value = {"message_sid": "ABC"}
        doc.reference.path = (
            "twilio_messages/999999999/lsu_eligibility_messages/eligibility_01_2023"
        )
        mock_firestore.return_value.get_collection_group.return_value.where.return_value.stream.return_value = [
            doc
        ]
        mock_twilio_validator.return_value = True

        response = self.test_client.post(
            "/jii/webhook/twilio_status",
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
            "twilio_messages/999999999/lsu_eligibility_messages/eligibility_01_2023",
            {
                "status": "IN_PROGRESS",
                "status_last_updated": datetime.datetime.now(datetime.timezone.utc),
                "raw_status": "sending",
            },
            merge=True,
        )

    @freeze_time("2023-01-01 01:23:45")
    @patch("recidiviz.case_triage.jii.jii_texts_routes.FirestoreClientImpl")
    @patch("recidiviz.case_triage.helpers.TwilioValidator.validate")
    def test_incoming_message_update_opt_out(
        self,
        mock_twilio_validator: MagicMock,
        mock_firestore: MagicMock,
    ) -> None:
        doc = MagicMock()
        doc.to_dict.return_value = {"phone_numbers": ["5555555555"]}
        doc.reference.path = "twilio_messages/999999999"
        mock_firestore.return_value.get_collection.return_value.where.return_value.stream.return_value = [
            doc
        ]
        mock_twilio_validator.return_value = True

        opt_out_type = "STOP"
        response = self.test_client.post(
            "/jii/webhook/twilio_incoming_message",
            headers={
                "Origin": "http://localhost:5000",
                "X-Twilio-Signature": "1234567a",
            },
            data={
                "From": "+15555555555",
                "OptOutType": opt_out_type,
                "Body": opt_out_type,
            },
        )
        self.assertEqual(HTTPStatus.NO_CONTENT, response.status_code)
        mock_firestore.return_value.update_document.assert_called_once_with(
            "twilio_messages/999999999",
            {
                "responses": ArrayUnion(
                    [
                        {
                            "response": opt_out_type,
                            "response_date": datetime.datetime.now(
                                datetime.timezone.utc
                            ),
                        }
                    ]
                )
            },
        )
        mock_firestore.return_value.set_document.assert_called_once_with(
            "twilio_messages/999999999",
            {
                "last_opt_out_update": datetime.datetime.now(datetime.timezone.utc),
                "opt_out_type": opt_out_type,
            },
            merge=True,
        )

    @freeze_time("2023-01-01 01:23:45")
    @patch("recidiviz.case_triage.jii.jii_texts_routes.FirestoreClientImpl")
    @patch("recidiviz.case_triage.helpers.TwilioValidator.validate")
    def test_incoming_message_update_opt_out_changed(
        self,
        mock_twilio_validator: MagicMock,
        mock_firestore: MagicMock,
    ) -> None:
        doc = MagicMock()
        doc.to_dict.return_value = {
            "phone_numbers": ["5555555555"],
            "opt_out_type": "START",
        }
        doc.reference.path = "twilio_messages/999999999"
        mock_firestore.return_value.get_collection.return_value.where.return_value.stream.return_value = [
            doc
        ]
        mock_twilio_validator.return_value = True

        opt_out_type = "STOP"
        response = self.test_client.post(
            "/jii/webhook/twilio_incoming_message",
            headers={
                "Origin": "http://localhost:5000",
                "X-Twilio-Signature": "1234567a",
            },
            data={
                "From": "+15555555555",
                "OptOutType": opt_out_type,
                "Body": opt_out_type,
            },
        )
        self.assertEqual(HTTPStatus.NO_CONTENT, response.status_code)
        mock_firestore.return_value.update_document.assert_called_once_with(
            "twilio_messages/999999999",
            {
                "responses": ArrayUnion(
                    [
                        {
                            "response": opt_out_type,
                            "response_date": datetime.datetime.now(
                                datetime.timezone.utc
                            ),
                        }
                    ]
                )
            },
        )
        mock_firestore.return_value.set_document.assert_called_once_with(
            "twilio_messages/999999999",
            {
                "last_opt_out_update": datetime.datetime.now(datetime.timezone.utc),
                "opt_out_type": opt_out_type,
            },
            merge=True,
        )

    @freeze_time("2023-01-01 01:23:45")
    @patch("recidiviz.case_triage.jii.jii_texts_routes.FirestoreClientImpl")
    @patch("recidiviz.case_triage.helpers.TwilioValidator.validate")
    def test_incoming_message_update_opt_out_recipient_not_found(
        self,
        mock_twilio_validator: MagicMock,
        mock_firestore: MagicMock,
    ) -> None:
        mock_firestore.return_value.get_collection.return_value.where.return_value.stream.return_value = (
            []
        )
        mock_twilio_validator.return_value = True

        opt_out_type = "STOP"
        response = self.test_client.post(
            "/jii/webhook/twilio_incoming_message",
            headers={
                "Origin": "http://localhost:5000",
                "X-Twilio-Signature": "1234567a",
            },
            data={
                "From": "+12222222222",
                "OptOutType": opt_out_type,
                "Body": opt_out_type,
            },
        )
        self.assertEqual(HTTPStatus.NO_CONTENT, response.status_code)
        mock_firestore.return_value.update_document.assert_not_called()
        mock_firestore.return_value.set_document.assert_called_once_with(
            "unknown_phone_number_replies/2222222222",
            {
                "responses": ArrayUnion(
                    [
                        {
                            "response": opt_out_type,
                            "response_date": datetime.datetime.now(
                                datetime.timezone.utc
                            ),
                        },
                    ]
                ),
            },
            merge=True,
        )
