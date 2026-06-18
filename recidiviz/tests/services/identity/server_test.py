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
"""Tests for the Identity Service Flask app."""
from http import HTTPStatus
from unittest import TestCase

from recidiviz.services.identity.server import app
from recidiviz.tests.services.identity.test_utils import (
    DEFAULT_MAPPING,
    MAPPED_SERVICE_ACCOUNT,
    STRANGER_SERVICE_ACCOUNT,
    mock_iap_environment,
)

IAP_HEADERS = {"x-goog-iap-jwt-assertion": "anything"}


class IdentityServiceServerTest(TestCase):
    """Tests for the Identity Service Flask app, including IAP auth wiring."""

    def setUp(self) -> None:
        self.app = app
        self.client = self.app.test_client()

    def test_health_with_mapped_caller_returns_200(self) -> None:
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=MAPPED_SERVICE_ACCOUNT
        ):
            response = self.client.get("/health", headers=IAP_HEADERS)
            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.assertEqual(b"", response.data)

    def test_health_without_iap_header_returns_401(self) -> None:
        with mock_iap_environment():
            response = self.client.get("/health")
            self.assertEqual(HTTPStatus.UNAUTHORIZED, response.status_code)

    def test_health_with_invalid_jwt_returns_401(self) -> None:
        with mock_iap_environment(invalid_jwt=True):
            response = self.client.get("/health", headers=IAP_HEADERS)
            self.assertEqual(HTTPStatus.UNAUTHORIZED, response.status_code)

    def test_health_with_unmapped_caller_returns_403(self) -> None:
        with mock_iap_environment(authenticated_as=STRANGER_SERVICE_ACCOUNT):
            response = self.client.get("/health", headers=IAP_HEADERS)
            self.assertEqual(HTTPStatus.FORBIDDEN, response.status_code)

    def test_health_in_development_returns_200(self) -> None:
        # In development the JWT check is bypassed and the dev caller default is
        # applied, so a request with no IAP header still succeeds.
        with mock_iap_environment(in_development=True):
            response = self.client.get("/health")
            self.assertEqual(HTTPStatus.OK, response.status_code)

    def test_security_headers_applied(self) -> None:
        with mock_iap_environment(
            mapping=DEFAULT_MAPPING, authenticated_as=MAPPED_SERVICE_ACCOUNT
        ):
            response = self.client.get("/health", headers=IAP_HEADERS)
            self.assertEqual("DENY", response.headers["X-Frame-Options"])
            self.assertEqual("nosniff", response.headers["X-Content-Type-Options"])
            self.assertEqual(
                "frame-ancestors 'none'",
                response.headers["Content-Security-Policy"],
            )
