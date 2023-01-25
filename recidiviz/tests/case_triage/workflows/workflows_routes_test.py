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
from typing import Callable
from unittest import TestCase, mock
from unittest.mock import MagicMock, patch

from flask import Flask
from flask.testing import FlaskClient

from recidiviz.case_triage.error_handlers import register_error_handlers
from recidiviz.case_triage.workflows.workflows_authorization import (
    on_successful_authorization,
)
from recidiviz.case_triage.workflows.workflows_routes import (
    create_workflows_api_blueprint,
)
from recidiviz.utils.types import assert_type

PERSON_EXTERNAL_ID = "123"
USER_ID = "456"
CONTACT_NOTE_DATE_TIME = datetime.datetime.now()


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
    def auth_side_effect(state_code: str) -> Callable:
        return lambda: on_successful_authorization(
            {
                f"{os.environ['AUTH0_CLAIM_NAMESPACE']}/app_metadata": {
                    "state_code": f"{state_code.lower()}"
                }
            }
        )


class TestWorkflowsRoutes(WorkflowsBlueprintTestCase):
    """Implements tests for the Workflows routes."""

    def setUp(self) -> None:
        super().setUp()

        self.old_auth_claim_namespace = os.environ.get("AUTH0_CLAIM_NAMESPACE", None)
        os.environ["AUTH0_CLAIM_NAMESPACE"] = "https://recidiviz-test"

    def tearDown(self) -> None:
        super().tearDown()

    def test_init(self) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("recidiviz")

        response = self.test_client.get("/workflows/US_TN/init")
        response_json = assert_type(response.get_json(), dict)
        self.assertEqual(response.status_code, HTTPStatus.OK, response_json)

    def test_init_not_authorized(self) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_id")

        response = self.test_client.get("/workflows/US_TN/init")
        self.assertEqual(response.status_code, HTTPStatus.UNAUTHORIZED)

    def test_insert_tepe_contact_note_passthrough(self) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("recidiviz")

        response = self.test_client.post(
            "/workflows/external_request/US_TN/insert_tepe_contact_note", json={}
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
    def test_insert_tepe_contact_note_success(
        self,
        mock_task_manager: MagicMock,
        mock_firestore: MagicMock,
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_tn")

        request_body = {
            "personExternalId": PERSON_EXTERNAL_ID,
            "userId": USER_ID,
            "contactNoteDateTime": str(datetime.datetime.now()),
            "contactNote": {1: ["Line 1", "Line 2"]},
        }

        with self.test_app.test_request_context():
            response = self.test_client.post(
                "/workflows/external_request/US_TN/insert_tepe_contact_note",
                json=request_body,
            )

        mock_task_manager.return_value.create_task.assert_called_once()
        mock_firestore.return_value.set_document.assert_called_once()
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
            "userId": USER_ID,
            "contactNoteDateTime": str(datetime.datetime.now()),
            "contactNote": {1: ["Line 1", "Line 2"]},
        }

        mock_task_manager.return_value.create_task.side_effect = Exception

        with self.test_app.test_request_context():
            response = self.test_client.post(
                "/workflows/external_request/US_TN/insert_tepe_contact_note",
                json=request_body,
            )

        mock_task_manager.return_value.create_task.assert_called_once()
        mock_firestore.return_value.set_document.assert_called_once()
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
            "userId": USER_ID,
            "contactNote": {1: ["Line 1", "Line 2"]},
        }

        mock_task_manager.return_value.create_task.side_effect = Exception

        with self.test_app.test_request_context():
            response = self.test_client.post(
                "/workflows/external_request/US_TN/insert_tepe_contact_note",
                json=request_body,
            )

        mock_task_manager.return_value.create_task.assert_not_called()
        mock_firestore.return_value.update_document.assert_not_called()
        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)

    def test_handle_insert_tepe_contact_note_state_not_enabled(self) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_tn")

        request_body = {
            "person_external_id": PERSON_EXTERNAL_ID,
            "user_id": USER_ID,
            "contact_note": {1: ["Line 1", "Line 2"]},
            "contact_note_date_time": str(datetime.datetime.now()),
        }

        with self.test_app.test_request_context():
            response = self.test_client.post(
                "/workflows/external_request/US_ID/handle_insert_tepe_contact_note",
                json=request_body,
            )
        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)

    def test_handle_insert_tepe_contact_note_missing_param(self) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_tn")

        request_body = {
            "person_external_id": PERSON_EXTERNAL_ID,
            "user_id": USER_ID,
            "contact_note": {1: ["Line 1", "Line 2"]},
        }
        with self.test_app.test_request_context():
            response = self.test_client.post(
                "/workflows/external_request/US_TN/handle_insert_tepe_contact_note",
                json=request_body,
            )
        self.assertEqual(response.status_code, HTTPStatus.INTERNAL_SERVER_ERROR)

    @patch(
        "recidiviz.case_triage.workflows.workflows_routes.WorkflowsUsTnExternalRequestInterface."
        "insert_tepe_contact_note"
    )
    def test_handle_insert_tepe_contact_note_success(
        self, mock_insert: MagicMock
    ) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect("us_tn")

        request_body = {
            "person_external_id": PERSON_EXTERNAL_ID,
            "user_id": USER_ID,
            "contact_note": {1: ["Line 1", "Line 2"]},
            "contact_note_date_time": str(datetime.datetime.now()),
        }
        with self.test_app.test_request_context():
            mock_insert.return_value = None
            response = self.test_client.post(
                "/workflows/external_request/US_TN/handle_insert_tepe_contact_note",
                json=request_body,
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
            "user_id": USER_ID,
            "contact_note": {1: ["Line 1", "Line 2"]},
            "contact_note_date_time": str(datetime.datetime.now()),
        }
        with self.test_app.test_request_context():
            mock_insert.side_effect = Exception
            response = self.test_client.post(
                "/workflows/external_request/US_TN/handle_insert_tepe_contact_note",
                json=request_body,
            )
        self.assertEqual(response.status_code, HTTPStatus.INTERNAL_SERVER_ERROR)
