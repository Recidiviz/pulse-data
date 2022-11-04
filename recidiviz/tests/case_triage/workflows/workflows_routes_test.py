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
import os
from http import HTTPStatus
from unittest import TestCase, mock
from unittest.mock import MagicMock, patch

from flask import Flask, jsonify, make_response
from flask.testing import FlaskClient

from recidiviz.case_triage.error_handlers import register_error_handlers
from recidiviz.case_triage.workflows.workflows_authorization import (
    on_successful_authorization,
)
from recidiviz.case_triage.workflows.workflows_routes import (
    create_workflows_api_blueprint,
)
from recidiviz.utils.types import assert_type


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


class TestWorkflowsRoutes(WorkflowsBlueprintTestCase):
    """Implements tests for the Workflows routes."""

    def setUp(self) -> None:
        super().setUp()

        self.old_auth_claim_namespace = os.environ.get("AUTH0_CLAIM_NAMESPACE", None)
        os.environ["AUTH0_CLAIM_NAMESPACE"] = "https://recidiviz-test"

    def tearDown(self) -> None:
        super().tearDown()

    def test_init(self) -> None:
        self.mock_authorization_handler.side_effect = (
            lambda: on_successful_authorization(
                {
                    f"{os.environ['AUTH0_CLAIM_NAMESPACE']}/app_metadata": {
                        "state_code": "recidiviz"
                    }
                }
            )
        )

        response = self.test_client.get("/workflows/US_TN/init")
        response_json = assert_type(response.get_json(), dict)
        self.assertEqual(response.status_code, HTTPStatus.OK, response_json)

    def test_init_not_authorized(self) -> None:
        self.mock_authorization_handler.side_effect = (
            lambda: on_successful_authorization(
                {
                    f"{os.environ['AUTH0_CLAIM_NAMESPACE']}/app_metadata": {
                        "state_code": "us_tn"
                    }
                }
            )
        )

        response = self.test_client.get("/workflows/US_TN/init")
        self.assertEqual(response.status_code, HTTPStatus.UNAUTHORIZED)

    def test_insert_tomis_contact_note_passthrough(self) -> None:
        self.mock_authorization_handler.side_effect = (
            lambda: on_successful_authorization(
                {
                    f"{os.environ['AUTH0_CLAIM_NAMESPACE']}/app_metadata": {
                        "state_code": "recidiviz"
                    }
                }
            )
        )

        response = self.test_client.post(
            "/workflows/external_request/US_TN/insert_contact_note", json={}
        )
        response_json = assert_type(response.get_json(), dict)
        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST, response_json)

    def test_insert_tomis_contact_note_not_authorized(self) -> None:
        self.mock_authorization_handler.side_effect = (
            lambda: on_successful_authorization(
                {
                    f"{os.environ['AUTH0_CLAIM_NAMESPACE']}/app_metadata": {
                        "state_code": "us_tn"
                    }
                }
            )
        )

        response = self.test_client.post(
            "/workflows/external_request/US_TN/insert_contact_note", json={}
        )
        self.assertEqual(response.status_code, HTTPStatus.UNAUTHORIZED)

    @patch(
        "recidiviz.case_triage.workflows.workflows_routes.WorkflowsExternalRequestInterface.insert_contact_note"
    )
    def test_insert_tomis_contact_note_success(self, mock_insert: MagicMock) -> None:
        self.mock_authorization_handler.side_effect = (
            lambda: on_successful_authorization(
                {
                    f"{os.environ['AUTH0_CLAIM_NAMESPACE']}/app_metadata": {
                        "state_code": "recidiviz"
                    }
                }
            )
        )

        with self.test_app.test_request_context(
            path="/workflows/external_request/US_TN/insert_contact_note"
        ):
            mock_insert.return_value = make_response(jsonify(), HTTPStatus.OK)
            response = self.test_client.post(
                "/workflows/external_request/US_TN/insert_contact_note",
                json={"isTest": True, "env": "staging"},
            )

        self.assertEqual(response.status_code, HTTPStatus.OK)
