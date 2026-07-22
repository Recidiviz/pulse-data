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
"""Tests for the Edovo WIF bearer-token verifier."""
import uuid
from http import HTTPStatus
from typing import Any
from unittest import TestCase
from unittest.mock import MagicMock, patch

import requests

from recidiviz.case_triage.edovo import wif_verifier
from recidiviz.case_triage.edovo.wif_verifier import verify_bearer_token
from recidiviz.utils.auth.auth0 import AuthorizationError
from recidiviz.utils.flask_exception import FlaskException

MODULE = "recidiviz.case_triage.edovo.wif_verifier"

# Neutral fake project ID (not a real Recidiviz project) so the constructed
# service-account email doesn't trip DLP detectors; the verifier only compares
# it as a string, so the value is immaterial to what these tests exercise.
_PROJECT_ID = "test-project"
_EXPECTED_EMAIL = f"edovo-wif@{_PROJECT_ID}.iam.gserviceaccount.com"
_EXPECTED_UNIQUE_ID = "102362795548081388936"


def _header() -> str:
    """A Bearer header with a unique token, so tests never share cache entries."""
    return f"Bearer token-{uuid.uuid4()}"


def _tokeninfo_response(status_code: int, body: dict[str, Any]) -> MagicMock:
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = body
    return response


def _valid_body(**overrides: Any) -> dict[str, Any]:
    """tokeninfo body for a default-scoped (cloud-platform only) Edovo token:
    aud/azp present, NO email — the shape the google-auth WIF default mints."""
    body: dict[str, Any] = {
        "aud": _EXPECTED_UNIQUE_ID,
        "azp": _EXPECTED_UNIQUE_ID,
        "scope": "https://www.googleapis.com/auth/cloud-platform",
        "expires_in": "3600",
    }
    body.update(overrides)
    return body


class TestVerifyBearerToken(TestCase):
    """Tests for verifying Edovo's federated GCP access token."""

    def setUp(self) -> None:
        self.project_patcher = patch(f"{MODULE}.project_id", return_value=_PROJECT_ID)
        self.project_patcher.start()
        self.env_patcher = patch.dict(
            "os.environ", {"EDOVO_WIF_SA_UNIQUE_ID": _EXPECTED_UNIQUE_ID}
        )
        self.env_patcher.start()
        self.get_patcher = patch(f"{MODULE}.requests.get")
        self.mock_get = self.get_patcher.start()
        wif_verifier._positive_cache.clear()  # pylint: disable=protected-access

    def tearDown(self) -> None:
        self.project_patcher.stop()
        self.env_patcher.stop()
        self.get_patcher.stop()

    def test_default_scoped_token_without_email_passes(self) -> None:
        # The google-auth WIF default (cloud-platform only) yields no `email`
        # from tokeninfo; verification must not depend on it.
        self.mock_get.return_value = _tokeninfo_response(200, _valid_body())
        verify_bearer_token(_header())  # does not raise

    def test_email_scoped_token_passes(self) -> None:
        # e.g. a gcloud-minted token, which silently adds userinfo.email.
        self.mock_get.return_value = _tokeninfo_response(
            200,
            _valid_body(
                scope="https://www.googleapis.com/auth/userinfo.email",
                email=_EXPECTED_EMAIL,
                email_verified="true",
            ),
        )
        verify_bearer_token(_header())  # does not raise

    def test_verifies_via_tokeninfo_with_the_access_token(self) -> None:
        self.mock_get.return_value = _tokeninfo_response(200, _valid_body())
        verify_bearer_token("Bearer abc123")
        _, kwargs = self.mock_get.call_args
        self.assertEqual(kwargs["params"], {"access_token": "abc123"})

    def test_rejected_token_is_401(self) -> None:
        # tokeninfo returns 400 for an invalid or expired token.
        self.mock_get.return_value = _tokeninfo_response(
            400, {"error": "invalid_token"}
        )
        with self.assertRaises(AuthorizationError) as cm:
            verify_bearer_token(_header())
        self.assertEqual(cm.exception.status_code, HTTPStatus.UNAUTHORIZED)
        self.assertEqual(cm.exception.code, "invalid_bearer_token")

    def test_wrong_aud_is_403(self) -> None:
        self.mock_get.return_value = _tokeninfo_response(
            200, _valid_body(aud="999999999999999999999", azp="999999999999999999999")
        )
        with self.assertRaises(FlaskException) as cm:
            verify_bearer_token(_header())
        self.assertEqual(cm.exception.status_code, HTTPStatus.FORBIDDEN)
        self.assertEqual(cm.exception.code, "wrong_identity")

    def test_missing_aud_is_403(self) -> None:
        body = _valid_body()
        del body["aud"]
        del body["azp"]
        self.mock_get.return_value = _tokeninfo_response(200, body)
        with self.assertRaises(FlaskException) as cm:
            verify_bearer_token(_header())
        self.assertEqual(cm.exception.status_code, HTTPStatus.FORBIDDEN)
        self.assertEqual(cm.exception.code, "wrong_identity")

    def test_mismatched_azp_is_403_even_with_matching_aud(self) -> None:
        self.mock_get.return_value = _tokeninfo_response(
            200, _valid_body(azp="999999999999999999999")
        )
        with self.assertRaises(FlaskException) as cm:
            verify_bearer_token(_header())
        self.assertEqual(cm.exception.status_code, HTTPStatus.FORBIDDEN)

    def test_mismatched_email_is_403_even_with_matching_aud(self) -> None:
        # Defense in depth: email is optional, but when present it must match.
        self.mock_get.return_value = _tokeninfo_response(
            200,
            _valid_body(email=f"someone-else@{_PROJECT_ID}.iam.gserviceaccount.com"),
        )
        with self.assertRaises(FlaskException) as cm:
            verify_bearer_token(_header())
        self.assertEqual(cm.exception.status_code, HTTPStatus.FORBIDDEN)
        self.assertEqual(cm.exception.code, "wrong_identity")

    def test_google_unreachable_is_500(self) -> None:
        self.mock_get.side_effect = requests.ConnectionError("boom")
        with self.assertRaises(FlaskException) as cm:
            verify_bearer_token(_header())
        self.assertEqual(cm.exception.status_code, HTTPStatus.INTERNAL_SERVER_ERROR)
        self.assertEqual(cm.exception.code, "token_verification_unavailable")

    def test_missing_unique_id_config_is_500(self) -> None:
        with patch.dict("os.environ", {"EDOVO_WIF_SA_UNIQUE_ID": ""}):
            with self.assertRaises(FlaskException) as cm:
                verify_bearer_token(_header())
        self.assertEqual(cm.exception.status_code, HTTPStatus.INTERNAL_SERVER_ERROR)
        self.assertEqual(cm.exception.code, "token_verification_misconfigured")
        self.mock_get.assert_not_called()

    def test_non_bearer_scheme_is_401(self) -> None:
        with self.assertRaises(AuthorizationError) as cm:
            verify_bearer_token("HMAC-SHA256 KeyId=k1")
        self.assertEqual(cm.exception.status_code, HTTPStatus.UNAUTHORIZED)
        self.mock_get.assert_not_called()

    def test_missing_header_is_401(self) -> None:
        with self.assertRaises(AuthorizationError) as cm:
            verify_bearer_token("")
        self.assertEqual(cm.exception.status_code, HTTPStatus.UNAUTHORIZED)

    def test_empty_bearer_token_is_401(self) -> None:
        with self.assertRaises(AuthorizationError) as cm:
            verify_bearer_token("Bearer   ")
        self.assertEqual(cm.exception.status_code, HTTPStatus.UNAUTHORIZED)
        self.mock_get.assert_not_called()


class TestPositiveCache(TestCase):
    """Tests for the positive verification cache."""

    def setUp(self) -> None:
        self.project_patcher = patch(f"{MODULE}.project_id", return_value=_PROJECT_ID)
        self.project_patcher.start()
        self.env_patcher = patch.dict(
            "os.environ", {"EDOVO_WIF_SA_UNIQUE_ID": _EXPECTED_UNIQUE_ID}
        )
        self.env_patcher.start()
        self.get_patcher = patch(f"{MODULE}.requests.get")
        self.mock_get = self.get_patcher.start()
        wif_verifier._positive_cache.clear()  # pylint: disable=protected-access

    def tearDown(self) -> None:
        self.project_patcher.stop()
        self.env_patcher.stop()
        self.get_patcher.stop()

    def test_verified_token_skips_tokeninfo_on_reuse(self) -> None:
        self.mock_get.return_value = _tokeninfo_response(200, _valid_body())
        verify_bearer_token("Bearer reused-token")
        verify_bearer_token("Bearer reused-token")
        self.assertEqual(self.mock_get.call_count, 1)

    def test_cache_lapses_after_ttl(self) -> None:
        self.mock_get.return_value = _tokeninfo_response(200, _valid_body())
        # monotonic calls, in order: store (first verify), lookup (second verify,
        # now post-TTL -> miss), store (second verify re-caches). The first
        # verify's lookup short-circuits on the empty cache without calling it.
        with patch(f"{MODULE}.time.monotonic", side_effect=[0.0, 400.0, 400.0]):
            verify_bearer_token("Bearer short-lived")
            verify_bearer_token("Bearer short-lived")
        self.assertEqual(self.mock_get.call_count, 2)

    def test_ttl_capped_by_token_expiry(self) -> None:
        self.mock_get.return_value = _tokeninfo_response(
            200, _valid_body(expires_in="60")
        )
        # TTL is min(300, expires_in) = 60s, so a lookup at t=61 re-verifies.
        with patch(f"{MODULE}.time.monotonic", side_effect=[0.0, 61.0, 61.0]):
            verify_bearer_token("Bearer nearly-expired")
            verify_bearer_token("Bearer nearly-expired")
        self.assertEqual(self.mock_get.call_count, 2)

    def test_rejected_tokens_are_not_cached(self) -> None:
        self.mock_get.return_value = _tokeninfo_response(400, {"error": "bad"})
        for _ in range(2):
            with self.assertRaises(AuthorizationError):
                verify_bearer_token("Bearer bad-token")
        self.assertEqual(self.mock_get.call_count, 2)

    def test_wrong_identity_tokens_are_not_cached(self) -> None:
        self.mock_get.return_value = _tokeninfo_response(
            200, _valid_body(aud="999999999999999999999")
        )
        for _ in range(2):
            with self.assertRaises(FlaskException):
                verify_bearer_token("Bearer wrong-sa-token")
        self.assertEqual(self.mock_get.call_count, 2)

    def test_unparseable_expires_in_is_not_cached(self) -> None:
        self.mock_get.return_value = _tokeninfo_response(
            200, _valid_body(expires_in="not-a-number")
        )
        verify_bearer_token("Bearer odd-expiry")
        verify_bearer_token("Bearer odd-expiry")
        self.assertEqual(self.mock_get.call_count, 2)
