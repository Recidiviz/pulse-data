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
"""Tests for recidiviz/case_triage/helpers.py"""
from unittest import TestCase, mock
from unittest.mock import MagicMock

from flask import Flask, g
from flask.ctx import RequestContext
from google.auth.exceptions import GoogleAuthError

from recidiviz.case_triage import helpers
from recidiviz.utils.auth.auth0 import AuthorizationError

_REQUEST_PATH = "/workflows/external_request/US_CA/send_sms_request"
_BASE_URL = "https://case-triage.example.com"
_VALID_CLAIMS = {
    "email_verified": True,
    "email": helpers.cloud_run_metadata.service_account_email,
}


class CloudTaskAuthValidatorTestCase(TestCase):
    """Base class that mocks in_gcp and the OIDC verifier, and provides a real
    Flask request context so request.headers/request.base_url behave
    normally."""

    def setUp(self) -> None:
        self.handle_authorization = MagicMock()
        self.handle_recidiviz_only_authorization = MagicMock()

        self.in_gcp_patcher = mock.patch(f"{helpers.__name__}.in_gcp")
        self.mock_in_gcp = self.in_gcp_patcher.start()

        self.verify_token_patcher = mock.patch(
            f"{helpers.__name__}.id_token.verify_oauth2_token"
        )
        self.mock_verify_token = self.verify_token_patcher.start()

        self.app = Flask(__name__)

    def tearDown(self) -> None:
        self.in_gcp_patcher.stop()
        self.verify_token_patcher.stop()

    def request_context(self, headers: dict[str, str] | None = None) -> RequestContext:
        return self.app.test_request_context(
            _REQUEST_PATH, base_url=_BASE_URL, headers=headers or {}
        )


class TestValidateCloudTaskRequest(CloudTaskAuthValidatorTestCase):
    """Tests for validate_cloud_task_request, used by routes that are Cloud
    Tasks targets only and are never called directly by the frontend."""

    def test_skips_check_outside_gcp(self) -> None:
        self.mock_in_gcp.return_value = False

        with self.request_context():
            helpers.validate_cloud_task_request(
                self.handle_authorization, self.handle_recidiviz_only_authorization
            )

        self.mock_verify_token.assert_not_called()
        self.handle_authorization.assert_not_called()
        self.handle_recidiviz_only_authorization.assert_not_called()

    def test_accepts_valid_token_from_expected_service_account(self) -> None:
        self.mock_in_gcp.return_value = True
        self.mock_verify_token.return_value = _VALID_CLAIMS

        with self.request_context(headers={"Authorization": "Bearer valid-token"}):
            helpers.validate_cloud_task_request(
                self.handle_authorization, self.handle_recidiviz_only_authorization
            )

        self.mock_verify_token.assert_called_once_with(
            "valid-token", mock.ANY, audience=f"{_BASE_URL}{_REQUEST_PATH}"
        )

    def test_rejects_missing_token(self) -> None:
        self.mock_in_gcp.return_value = True

        with self.request_context():
            with self.assertRaisesRegex(
                AuthorizationError, r"^Missing or invalid Cloud Tasks OIDC token$"
            ):
                helpers.validate_cloud_task_request(
                    self.handle_authorization,
                    self.handle_recidiviz_only_authorization,
                )

    def test_rejects_token_signed_for_a_different_service_account(self) -> None:
        self.mock_in_gcp.return_value = True
        self.mock_verify_token.return_value = {
            "email_verified": True,
            "email": "someone-else@another-project.iam.gserviceaccount.com",
        }

        with self.request_context(headers={"Authorization": "Bearer valid-token"}):
            with self.assertRaisesRegex(
                AuthorizationError, r"^Missing or invalid Cloud Tasks OIDC token$"
            ):
                helpers.validate_cloud_task_request(
                    self.handle_authorization,
                    self.handle_recidiviz_only_authorization,
                )

    def test_rejects_unverified_email(self) -> None:
        self.mock_in_gcp.return_value = True
        self.mock_verify_token.return_value = {
            "email_verified": False,
            "email": helpers.cloud_run_metadata.service_account_email,
        }

        with self.request_context(headers={"Authorization": "Bearer valid-token"}):
            with self.assertRaisesRegex(
                AuthorizationError, r"^Missing or invalid Cloud Tasks OIDC token$"
            ):
                helpers.validate_cloud_task_request(
                    self.handle_authorization,
                    self.handle_recidiviz_only_authorization,
                )

    def test_rejects_token_that_fails_signature_verification(self) -> None:
        self.mock_in_gcp.return_value = True
        self.mock_verify_token.side_effect = GoogleAuthError("bad signature")

        with self.request_context(headers={"Authorization": "Bearer garbage"}):
            with self.assertRaisesRegex(
                AuthorizationError, r"^Missing or invalid Cloud Tasks OIDC token$"
            ):
                helpers.validate_cloud_task_request(
                    self.handle_authorization,
                    self.handle_recidiviz_only_authorization,
                )


class TestValidateCloudTaskOrDashboardAuth(CloudTaskAuthValidatorTestCase):
    """Tests for validate_cloud_task_or_dashboard_auth, used by routes that
    are called directly by the frontend and are also their own Cloud Tasks
    target."""

    def test_valid_oidc_token_skips_dashboard_auth(self) -> None:
        self.mock_in_gcp.return_value = True
        self.mock_verify_token.return_value = _VALID_CLAIMS

        with self.request_context(headers={"Authorization": "Bearer valid-token"}):
            helpers.validate_cloud_task_or_dashboard_auth(
                self.handle_authorization, self.handle_recidiviz_only_authorization
            )
            self.assertTrue(g.is_cloud_task_request)

        self.handle_authorization.assert_not_called()

    def test_dashboard_auth0_token_falls_back_to_dashboard_auth(self) -> None:
        self.mock_in_gcp.return_value = True
        # An Auth0 JWT is not a Google-signed token, so verification fails.
        self.mock_verify_token.side_effect = ValueError("Invalid token signature")

        with self.request_context(
            headers={"Authorization": "Bearer a-dashboard-auth0-jwt"}
        ):
            helpers.validate_cloud_task_or_dashboard_auth(
                self.handle_authorization, self.handle_recidiviz_only_authorization
            )
            self.assertFalse(g.is_cloud_task_request)

        self.handle_authorization.assert_called_once()

    def test_outside_gcp_always_falls_back_to_dashboard_auth(self) -> None:
        self.mock_in_gcp.return_value = False

        with self.request_context():
            helpers.validate_cloud_task_or_dashboard_auth(
                self.handle_authorization, self.handle_recidiviz_only_authorization
            )
            self.assertFalse(g.is_cloud_task_request)

        self.mock_verify_token.assert_not_called()
        self.handle_authorization.assert_called_once()

    def test_no_token_falls_back_to_dashboard_auth(self) -> None:
        self.mock_in_gcp.return_value = True

        with self.request_context():
            helpers.validate_cloud_task_or_dashboard_auth(
                self.handle_authorization, self.handle_recidiviz_only_authorization
            )
            self.assertFalse(g.is_cloud_task_request)

        self.mock_verify_token.assert_not_called()
        self.handle_authorization.assert_called_once()
