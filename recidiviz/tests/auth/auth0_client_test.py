# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests for the wrapper class Auth0Client"""
from unittest import TestCase
from unittest.mock import patch

from recidiviz.auth.auth0_client import Auth0AppMetadata, Auth0Client


class Auth0ClientTest(TestCase):
    """Tests for the wrapper class Auth0Client"""

    def setUp(self) -> None:
        self.client_patcher = patch("recidiviz.auth.auth0_client.Auth0")
        self.get_token_patcher = patch("recidiviz.auth.auth0_client.GetToken")
        self.secrets_patcher = patch("recidiviz.auth.auth0_client.secrets")
        self.mock_client = self.client_patcher.start().return_value
        self.mock_get_token = self.get_token_patcher.start()
        self.mock_secrets = self.secrets_patcher.start()
        self.secrets = {
            "auth0_api_domain": "fake_api_domain",
            "auth0_api_client_id": "fake client id",
            "auth0_api_client_secret": "fake client secret",
        }

        self.mock_secrets.get_secret.side_effect = self.secrets.get
        self.auth0_client = Auth0Client()

    def tearDown(self) -> None:
        self.client_patcher.stop()
        self.get_token_patcher.stop()
        self.secrets_patcher.stop()

    def test_access_token(self) -> None:
        self.mock_get_token.assert_called_once_with("fake_api_domain")
        self.mock_get_token.return_value.client_credentials.assert_called_once_with(
            client_id="fake client id",
            client_secret="fake client secret",
            audience=f"https://{self.secrets['auth0_api_domain']}/api/v2/",
        )
        self.mock_get_token.return_value.client_credentials.return_value = {
            "access_token": "API access token"
        }
        self.assertEqual(self.auth0_client.access_token, "API access token")

    def test_get_all_users_by_email_addresses(self) -> None:
        email_addresses = ["one@test.gov", "two@test.gov"]
        expected_users = [
            {"user_id": "1", "email": "one@test.gov"},
            {"user_id": "2", "email": "two@test.gov"},
        ]
        list_response = {"total": "1", "start": "0", "users": expected_users}

        self.mock_client.users.list.return_value = list_response

        returned_users = self.auth0_client.get_all_users_by_email_addresses(
            email_addresses
        )

        self.mock_client.users.list.assert_called_with(
            per_page=25,
            fields=["user_id", "email", "app_metadata"],
            q='email: "one@test.gov" or email: "two@test.gov"',
        )
        self.assertEqual(returned_users, expected_users)

    def test_update_user_app_metadata(self) -> None:
        app_metadata: Auth0AppMetadata = {
            "allowed_supervision_location_ids": ["12", "AP"],
            "allowed_supervision_location_level": "level_1_supervision_location",
            "can_access_leadership_dashboard": False,
            "can_access_case_triage": False,
            "routes": None,
        }
        self.auth0_client.update_user_app_metadata(
            user_id="1", app_metadata=app_metadata
        )
        self.mock_client.users.update.assert_called_once_with(
            id="1", body={"app_metadata": app_metadata}
        )
