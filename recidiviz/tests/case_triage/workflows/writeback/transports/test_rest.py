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
"""Tests for the REST transport."""
import base64
from unittest import TestCase
from unittest.mock import MagicMock, patch

import requests
import responses

from recidiviz.case_triage.workflows.writeback.transports.rest import (
    BasicAuth,
    RestTransport,
    RestTransportConfig,
    TokenHeaderAuth,
)

MODULE = "recidiviz.case_triage.workflows.writeback.transports.rest"

FAKE_URL = "http://fake-url.com"
FAKE_CREDENTIAL = "my-secret-key"

BASIC_CONFIG = RestTransportConfig(
    system_name="TEST_SYSTEM",
    url_secret="test_url",
    credential_secret="test_key",
    test_url_secret="test_url_test",
    auth_strategy=BasicAuth(),
)

TOKEN_CONFIG = RestTransportConfig(
    system_name="TEST_SYSTEM",
    url_secret="test_url",
    credential_secret="test_key",
    test_url_secret="test_url_test",
    auth_strategy=TokenHeaderAuth("X-Api-Key"),
)


class TestBasicAuth(TestCase):
    def test_headers(self) -> None:
        auth = BasicAuth()
        result = auth.headers("my-key")
        expected = base64.b64encode(b"my-key").decode()
        self.assertEqual(result, {"Authorization": f"Basic {expected}"})


class TestTokenHeaderAuth(TestCase):
    def test_headers(self) -> None:
        auth = TokenHeaderAuth("Recidiviz-Credential-Token")
        result = auth.headers("my-key")
        self.assertEqual(result, {"Recidiviz-Credential-Token": "my-key"})


class TestRestTransport(TestCase):
    "Tests for RestTransport"

    @patch(f"{MODULE}.get_secret")
    def test_missing_secrets_raises(self, mock_get_secret: MagicMock) -> None:
        mock_get_secret.return_value = None
        with self.assertRaises(EnvironmentError):
            RestTransport(BASIC_CONFIG, use_test_url=False)

    @patch(f"{MODULE}.get_secret")
    def test_use_test_url(self, mock_get_secret: MagicMock) -> None:
        mock_get_secret.return_value = FAKE_URL
        RestTransport(BASIC_CONFIG, use_test_url=True)
        # First call should be for test_url_secret, second for credential_secret
        mock_get_secret.assert_any_call("test_url_test")
        mock_get_secret.assert_any_call("test_key")

    @patch(f"{MODULE}.get_secret")
    def test_use_production_url(self, mock_get_secret: MagicMock) -> None:
        mock_get_secret.return_value = FAKE_URL
        RestTransport(BASIC_CONFIG, use_test_url=False)
        mock_get_secret.assert_any_call("test_url")
        mock_get_secret.assert_any_call("test_key")

    @patch(f"{MODULE}.get_secret")
    def test_send_success(self, mock_get_secret: MagicMock) -> None:
        mock_get_secret.return_value = FAKE_URL
        transport = RestTransport(BASIC_CONFIG, use_test_url=False)
        with responses.RequestsMock(assert_all_requests_are_fired=True) as rsps:
            rsps.add(responses.PUT, FAKE_URL, json={"status": "OK"})
            transport.send('{"key": "value"}')

    @patch(f"{MODULE}.get_secret")
    def test_send_http_error(self, mock_get_secret: MagicMock) -> None:
        mock_get_secret.return_value = FAKE_URL
        transport = RestTransport(BASIC_CONFIG, use_test_url=False)
        with responses.RequestsMock(assert_all_requests_are_fired=True) as rsps:
            rsps.add(responses.PUT, FAKE_URL, status=500)
            with self.assertRaises(requests.exceptions.HTTPError):
                transport.send('{"key": "value"}')

    @patch(f"{MODULE}.get_secret")
    def test_send_connection_error(self, mock_get_secret: MagicMock) -> None:
        mock_get_secret.return_value = FAKE_URL
        transport = RestTransport(BASIC_CONFIG, use_test_url=False)
        with responses.RequestsMock(assert_all_requests_are_fired=True) as rsps:
            rsps.add(responses.PUT, FAKE_URL, body=ConnectionRefusedError())
            with self.assertRaises(ConnectionRefusedError):
                transport.send('{"key": "value"}')

    @patch(f"{MODULE}.get_secret")
    def test_send_with_token_auth(self, mock_get_secret: MagicMock) -> None:
        mock_get_secret.return_value = FAKE_URL
        transport = RestTransport(TOKEN_CONFIG, use_test_url=False)
        with responses.RequestsMock(assert_all_requests_are_fired=True) as rsps:
            rsps.add(responses.PUT, FAKE_URL, json={"status": "OK"})
            transport.send('{"key": "value"}')
            # Verify the custom header was sent
            self.assertEqual(rsps.calls[0].request.headers["X-Api-Key"], FAKE_URL)

    @patch(f"{MODULE}.get_secret")
    def test_send_with_log_context(self, mock_get_secret: MagicMock) -> None:
        mock_get_secret.return_value = FAKE_URL
        transport = RestTransport(BASIC_CONFIG, use_test_url=False)
        with responses.RequestsMock(assert_all_requests_are_fired=True) as rsps:
            rsps.add(responses.PUT, FAKE_URL, json={"status": "OK"})
            # Should not raise — log_context is just for logging
            transport.send('{"key": "value"}', log_context="page 3")
