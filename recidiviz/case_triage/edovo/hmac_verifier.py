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
    POST\n<request_path>\n<unix_ts>\n<sha256_hex_of_body>

Secret keys are stored in Secret Manager under the name ``edovo_hmac_<key_id>``
as base64-encoded 256-bit values.

HMAC is the launch authentication mechanism. The spec's preferred mechanism is
Workload Identity Federation (WIF); once WIF verification lands this entire
module is expected to be replaced.
TODO(OBT-27565): Replace HMAC verification with WIF.
"""
import base64
import hashlib
import hmac
import re
import time
from typing import NamedTuple

from recidiviz.utils.auth.auth0 import AuthorizationError
from recidiviz.utils.secrets import get_secret

_MAX_CLOCK_SKEW_SECONDS = 300

_SCHEME_PREFIX = "HMAC-SHA256"
_PARAM_RE = re.compile(r"^(?P<key>[A-Za-z]+)=(?P<value>[^,\s]+)$")
_EXPECTED_PARAMS = {"KeyId", "Signature", "Timestamp"}


class _ParsedAuthHeader(NamedTuple):
    key_id: str
    signature: str
    timestamp: int


def _parse_authorization_header(authorization_header: str) -> _ParsedAuthHeader:
    """Parse the HMAC ``Authorization`` header into its KeyId/Signature/Timestamp parts."""
    stripped = authorization_header.strip()
    if not stripped.startswith(_SCHEME_PREFIX + " "):
        raise AuthorizationError(
            code="invalid_authorization_header",
            description=f"Authorization header must start with {_SCHEME_PREFIX!r} scheme",
        )

    params: dict[str, str] = {}
    for raw in stripped[len(_SCHEME_PREFIX) :].split(","):
        match = _PARAM_RE.match(raw.strip())
        if match is None or match.group("key") not in _EXPECTED_PARAMS:
            raise AuthorizationError(
                code="invalid_authorization_header",
                description="Authorization header parameter is malformed or unknown",
            )
        params[match.group("key")] = match.group("value")

    if params.keys() != _EXPECTED_PARAMS:
        raise AuthorizationError(
            code="invalid_authorization_header",
            description=f"Authorization header missing required parameters: {sorted(_EXPECTED_PARAMS - params.keys())}",
        )

    try:
        timestamp = int(params["Timestamp"])
    except ValueError as exc:
        raise AuthorizationError(
            code="invalid_authorization_header",
            description="Authorization Timestamp must be an integer",
        ) from exc

    return _ParsedAuthHeader(
        key_id=params["KeyId"],
        signature=params["Signature"],
        timestamp=timestamp,
    )


def verify_hmac_signature(
    request_body: bytes,
    authorization_header: str,
    secret_key: bytes,
    request_path: str,
) -> None:
    """Verify the HMAC-SHA256 signature of an inbound Edovo request.

    Raises AuthorizationError if the header cannot be parsed, the timestamp is
    outside the 5-minute window, or the computed signature does not match.
    """
    _verify_with_parsed(
        request_body,
        _parse_authorization_header(authorization_header),
        secret_key,
        request_path,
    )


def load_secret_and_verify(
    request_body: bytes,
    authorization_header: str,
    request_path: str,
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

    _verify_with_parsed(request_body, parsed, base64.b64decode(secret), request_path)


def _verify_with_parsed(
    request_body: bytes,
    parsed: _ParsedAuthHeader,
    secret_key: bytes,
    request_path: str,
) -> None:
    now = int(time.time())
    if abs(now - parsed.timestamp) > _MAX_CLOCK_SKEW_SECONDS:
        raise AuthorizationError(
            code="stale_request",
            description="Request timestamp is outside the acceptable 5-minute window",
        )

    body_hash = hashlib.sha256(request_body).hexdigest()
    string_to_sign = f"POST\n{request_path}\n{parsed.timestamp}\n{body_hash}"
    expected = base64.b64encode(
        hmac.new(secret_key, string_to_sign.encode(), hashlib.sha256).digest()
    ).decode()

    if not hmac.compare_digest(expected, parsed.signature):
        raise AuthorizationError(
            code="invalid_signature",
            description="HMAC signature does not match",
        )
