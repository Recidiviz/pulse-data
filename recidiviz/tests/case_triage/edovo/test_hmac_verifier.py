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
"""Unit tests for the Edovo HMAC-SHA256 request verifier."""
import base64
import hashlib
import hmac
import time
from unittest import TestCase
from unittest.mock import MagicMock, patch

from recidiviz.case_triage.edovo.hmac_verifier import (
    load_secret_and_verify,
    verify_hmac_signature,
)
from recidiviz.utils.auth.auth0 import AuthorizationError

MODULE = "recidiviz.case_triage.edovo.hmac_verifier"

_NOW = 1_700_000_000
_SECRET_KEY = b"test-secret-key-32-bytes-long!!!"
_BODY = b'{"person_id": "012345", "state_code": "US_CO", "course_id": "foo-bar"}'
_KEY_ID = "edovo-key-001"


def _make_auth_header(
    body: bytes = _BODY,
    key_id: str = _KEY_ID,
    secret_key: bytes = _SECRET_KEY,
    timestamp: int | None = None,
) -> str:
    if timestamp is None:
        timestamp = int(time.time())
    body_hash = hashlib.sha256(body).hexdigest()
    string_to_sign = f"POST\n/edovo/course-completions\n{timestamp}\n{body_hash}"
    signature = base64.b64encode(
        hmac.new(secret_key, string_to_sign.encode(), hashlib.sha256).digest()
    ).decode()
    return f"HMAC-SHA256 KeyId={key_id}, Signature={signature}, Timestamp={timestamp}"


class TestVerifyHmacSignature(TestCase):
    def test_valid_signature_passes(self) -> None:
        header = _make_auth_header()
        verify_hmac_signature(_BODY, header, _SECRET_KEY)

    def test_tampered_body_fails(self) -> None:
        header = _make_auth_header()
        tampered = _BODY + b" TAMPERED"
        with self.assertRaises(AuthorizationError) as cm:
            verify_hmac_signature(tampered, header, _SECRET_KEY)
        self.assertEqual(cm.exception.code, "invalid_signature")

    def test_wrong_key_fails(self) -> None:
        header = _make_auth_header()
        wrong_key = b"completely-different-secret-key!"
        with self.assertRaises(AuthorizationError) as cm:
            verify_hmac_signature(_BODY, header, wrong_key)
        self.assertEqual(cm.exception.code, "invalid_signature")

    @patch(f"{MODULE}.time")
    def test_stale_timestamp_past_fails(self, mock_time: MagicMock) -> None:
        mock_time.time.return_value = _NOW
        header = _make_auth_header(timestamp=_NOW - 301)
        with self.assertRaises(AuthorizationError) as cm:
            verify_hmac_signature(_BODY, header, _SECRET_KEY)
        self.assertEqual(cm.exception.code, "stale_request")

    @patch(f"{MODULE}.time")
    def test_stale_timestamp_future_fails(self, mock_time: MagicMock) -> None:
        mock_time.time.return_value = _NOW
        header = _make_auth_header(timestamp=_NOW + 301)
        with self.assertRaises(AuthorizationError) as cm:
            verify_hmac_signature(_BODY, header, _SECRET_KEY)
        self.assertEqual(cm.exception.code, "stale_request")

    @patch(f"{MODULE}.time")
    def test_timestamp_at_boundary_passes(self, mock_time: MagicMock) -> None:
        mock_time.time.return_value = _NOW
        header = _make_auth_header(timestamp=_NOW - 300)
        verify_hmac_signature(_BODY, header, _SECRET_KEY)

    def test_malformed_header_fails(self) -> None:
        with self.assertRaises(AuthorizationError) as cm:
            verify_hmac_signature(_BODY, "Bearer some-token", _SECRET_KEY)
        self.assertEqual(cm.exception.code, "invalid_authorization_header")

    def test_missing_timestamp_fails(self) -> None:
        header = f"HMAC-SHA256 KeyId={_KEY_ID}, Signature=abc123"
        with self.assertRaises(AuthorizationError) as cm:
            verify_hmac_signature(_BODY, header, _SECRET_KEY)
        self.assertEqual(cm.exception.code, "invalid_authorization_header")

    def test_reordered_params_still_verifies(self) -> None:
        canonical = _make_auth_header()
        # Reverse the parameter order — should still be accepted.
        scheme, params = canonical.split(" ", 1)
        reversed_header = scheme + " " + ", ".join(reversed(params.split(", ")))
        verify_hmac_signature(_BODY, reversed_header, _SECRET_KEY)

    def test_unknown_param_fails(self) -> None:
        header = f"HMAC-SHA256 KeyId={_KEY_ID}, Signature=abc, Timestamp=1, Bogus=x"
        with self.assertRaises(AuthorizationError) as cm:
            verify_hmac_signature(_BODY, header, _SECRET_KEY)
        self.assertEqual(cm.exception.code, "invalid_authorization_header")


class TestLoadSecretAndVerify(TestCase):
    @patch(f"{MODULE}.get_secret")
    def test_valid_request_passes(self, mock_get_secret: MagicMock) -> None:
        mock_get_secret.return_value = base64.b64encode(_SECRET_KEY).decode()
        header = _make_auth_header()
        load_secret_and_verify(_BODY, header)
        mock_get_secret.assert_called_once_with(f"edovo_hmac_{_KEY_ID}")

    @patch(f"{MODULE}.get_secret")
    def test_unknown_key_id_fails(self, mock_get_secret: MagicMock) -> None:
        mock_get_secret.return_value = None
        header = _make_auth_header()
        with self.assertRaises(AuthorizationError) as cm:
            load_secret_and_verify(_BODY, header)
        self.assertEqual(cm.exception.code, "unknown_key")
