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
"""HMAC-SHA256 request verifier for the Edovo course-completion API.

The Authorization header format is:
    HMAC-SHA256 KeyId=<key_id>, Signature=<signature>, Timestamp=<unix_ts>

The canonical string signed by Edovo is:
    POST\n/edovo/course-completions\n<unix_ts>\n<sha256_hex_of_body>

Secret keys are stored in Secret Manager under the name ``edovo_hmac_<key_id>``
as base64-encoded 256-bit values.
"""
import base64
import hashlib
import hmac
import re
import time
from typing import NamedTuple

from recidiviz.utils.auth.auth0 import AuthorizationError
from recidiviz.utils.secrets import get_secret

_ENDPOINT_PATH = "/edovo/course-completions"
_MAX_CLOCK_SKEW_SECONDS = 300

_AUTH_HEADER_RE = re.compile(
    r"^HMAC-SHA256\s+"
    r"KeyId=(?P<key_id>[^,\s]+),\s*"
    r"Signature=(?P<signature>[^,\s]+),\s*"
    r"Timestamp=(?P<timestamp>\d+)\s*$"
)


class _ParsedAuthHeader(NamedTuple):
    key_id: str
    signature: str
    timestamp: int


def _parse_authorization_header(authorization_header: str) -> _ParsedAuthHeader:
    match = _AUTH_HEADER_RE.match(authorization_header.strip())
    if not match:
        raise AuthorizationError(
            code="invalid_authorization_header",
            description="Authorization header does not match expected HMAC-SHA256 format",
        )
    return _ParsedAuthHeader(
        key_id=match.group("key_id"),
        signature=match.group("signature"),
        timestamp=int(match.group("timestamp")),
    )


def verify_hmac_signature(
    request_body: bytes,
    authorization_header: str,
    secret_key: bytes,
) -> None:
    """Verify the HMAC-SHA256 signature of an inbound Edovo request.

    Raises AuthorizationError if the header cannot be parsed, the timestamp is
    outside the 5-minute window, or the computed signature does not match.
    """
    _verify_with_parsed(
        request_body, _parse_authorization_header(authorization_header), secret_key
    )


def load_secret_and_verify(
    request_body: bytes,
    authorization_header: str,
) -> None:
    """Look up the HMAC secret from Secret Manager by KeyId and verify the signature.

    This is the entry point called by the Flask blueprint.
    """
    parsed = _parse_authorization_header(authorization_header)

    if (secret := get_secret(f"edovo_hmac_{parsed.key_id}")) is None:
        raise AuthorizationError(
            code="unknown_key",
            description=f"No HMAC secret found for KeyId {parsed.key_id!r}",
        )

    # Pass the already-parsed header fields directly to avoid re-parsing.
    _verify_with_parsed(request_body, parsed, base64.b64decode(secret))


def _verify_with_parsed(
    request_body: bytes,
    parsed: _ParsedAuthHeader,
    secret_key: bytes,
) -> None:
    now = int(time.time())
    if abs(now - parsed.timestamp) > _MAX_CLOCK_SKEW_SECONDS:
        raise AuthorizationError(
            code="stale_request",
            description="Request timestamp is outside the acceptable 5-minute window",
        )

    body_hash = hashlib.sha256(request_body).hexdigest()
    string_to_sign = f"POST\n{_ENDPOINT_PATH}\n{parsed.timestamp}\n{body_hash}"
    expected = base64.b64encode(
        hmac.new(secret_key, string_to_sign.encode(), hashlib.sha256).digest()
    ).decode()

    if not hmac.compare_digest(expected, parsed.signature):
        raise AuthorizationError(
            code="invalid_signature",
            description="HMAC signature does not match",
        )
