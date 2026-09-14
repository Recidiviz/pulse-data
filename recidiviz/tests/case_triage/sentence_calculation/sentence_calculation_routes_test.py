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
"""Implements tests for the Sentence Calculation routes."""
import os
from http import HTTPStatus
from typing import Callable, Optional
from unittest import TestCase, mock
from unittest.mock import MagicMock

from flask import Flask
from flask.testing import FlaskClient

from recidiviz.case_triage.error_handlers import register_error_handlers
from recidiviz.case_triage.sentence_calculation.sentence_calculation_authorization import (
    on_successful_authorization,
)
from recidiviz.case_triage.sentence_calculation.sentence_calculation_routes import (
    create_sentence_calculation_api_blueprint,
)

TEST_ORIGIN = "http://localhost:3000"


class SentenceCalculationBlueprintTestCase(TestCase):
    """Base class for Sentence Calculation flask tests."""

    mock_authorization_handler: MagicMock
    test_app: Flask
    test_client: FlaskClient

    def setUp(self) -> None:
        self.mock_authorization_handler = MagicMock()

        self.auth_patcher = mock.patch(
            f"{create_sentence_calculation_api_blueprint.__module__}.build_authorization_handler",
            return_value=self.mock_authorization_handler,
        )
        self.auth_patcher.start()

        self.test_app = Flask(__name__)
        register_error_handlers(self.test_app)
        self.test_app.register_blueprint(
            create_sentence_calculation_api_blueprint(),
            url_prefix="/sentence_calculation",
        )
        self.test_client = self.test_app.test_client()

        self.old_auth_claim_namespace = os.environ.get("AUTH0_CLAIM_NAMESPACE", None)
        os.environ["AUTH0_CLAIM_NAMESPACE"] = "https://recidiviz-test"

    def tearDown(self) -> None:
        self.auth_patcher.stop()

        if self.old_auth_claim_namespace:
            os.environ["AUTH0_CLAIM_NAMESPACE"] = self.old_auth_claim_namespace
        else:
            os.unsetenv("AUTH0_CLAIM_NAMESPACE")

    @staticmethod
    def auth_side_effect(
        state_code: str,
        allowed_states: Optional[list[str]] = None,
    ) -> Callable:
        if allowed_states is None:
            allowed_states = []

        return lambda: on_successful_authorization(
            {
                f"{os.environ['AUTH0_CLAIM_NAMESPACE']}/app_metadata": {
                    "stateCode": state_code.lower(),
                    "allowedStates": allowed_states,
                },
            }
        )


class TestSentenceCalculationRoutes(SentenceCalculationBlueprintTestCase):
    """Implements tests for the Sentence Calculation routes."""

    def test_echo_state_user_success(self) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="US_NV",
        )

        response = self.test_client.post(
            "/sentence_calculation/US_NV/echo",
            json={"value": "hello"},
            headers={"Origin": TEST_ORIGIN},
        )

        self.assertEqual(HTTPStatus.OK, response.status_code)
        self.assertEqual({"echo": "hello", "stateCode": "US_NV"}, response.get_json())

    def test_echo_recidiviz_user_with_allowed_state_success(self) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="recidiviz",
            allowed_states=["US_NV"],
        )

        response = self.test_client.post(
            "/sentence_calculation/US_NV/echo",
            json={"value": "hello"},
            headers={"Origin": TEST_ORIGIN},
        )

        self.assertEqual(HTTPStatus.OK, response.status_code)
        self.assertEqual({"echo": "hello", "stateCode": "US_NV"}, response.get_json())

    def test_echo_missing_value_defaults_to_empty_string(self) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="US_NV",
        )

        response = self.test_client.post(
            "/sentence_calculation/US_NV/echo",
            json={},
            headers={"Origin": TEST_ORIGIN},
        )

        self.assertEqual(HTTPStatus.OK, response.status_code)
        self.assertEqual({"echo": "", "stateCode": "US_NV"}, response.get_json())

    def test_echo_recidiviz_user_without_allowed_state_rejected(self) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="recidiviz",
            allowed_states=["US_CO"],
        )

        response = self.test_client.post(
            "/sentence_calculation/US_NV/echo",
            json={"value": "hello"},
            headers={"Origin": TEST_ORIGIN},
        )

        self.assertEqual(HTTPStatus.UNAUTHORIZED, response.status_code)
        self.assertEqual(
            "recidiviz_user_not_authorized",
            (response.get_json() or {}).get("code"),
        )

    def test_echo_wrong_state_user_rejected(self) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="US_CO",
        )

        response = self.test_client.post(
            "/sentence_calculation/US_NV/echo",
            json={"value": "hello"},
            headers={"Origin": TEST_ORIGIN},
        )

        self.assertEqual(HTTPStatus.UNAUTHORIZED, response.status_code)
        self.assertEqual("not_authorized", (response.get_json() or {}).get("code"))

    def test_echo_non_enabled_state_rejected(self) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="US_CO",
        )

        response = self.test_client.post(
            "/sentence_calculation/US_CO/echo",
            json={"value": "hello"},
            headers={"Origin": TEST_ORIGIN},
        )

        self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
        self.assertEqual("state_not_enabled", (response.get_json() or {}).get("code"))

    def test_echo_invalid_state_code_rejected(self) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="US_NV",
        )

        response = self.test_client.post(
            "/sentence_calculation/NOTASTATE/echo",
            json={"value": "hello"},
            headers={"Origin": TEST_ORIGIN},
        )

        self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
        self.assertEqual(
            "valid_state_required", (response.get_json() or {}).get("code")
        )

    def test_cors_rejected_for_disallowed_origin(self) -> None:
        self.mock_authorization_handler.side_effect = self.auth_side_effect(
            state_code="US_NV",
        )

        response = self.test_client.post(
            "/sentence_calculation/US_NV/echo",
            json={"value": "hello"},
            headers={"Origin": "http://evil.example.com"},
        )

        self.assertEqual(HTTPStatus.FORBIDDEN, response.status_code)
