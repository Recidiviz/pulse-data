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
"""Workload Identity Federation token verifier for the Edovo course-completion API.

Edovo federates to the ``edovo-wif@`` service account (edovo-wif.tf, OBT-23211)
and calls the endpoint with the GCP access token federation mints, as
``Authorization: Bearer``. We verify it via Google's ``tokeninfo`` endpoint and
assert it belongs to ``edovo-wif@``.

Identity is matched on ``aud``/``azp`` — for a service-account access token
these hold the SA's numeric unique ID regardless of scope, whereas ``email`` is
present only with the ``userinfo.email`` scope (the default WIF flow uses
``cloud-platform`` only). ``email`` is therefore checked only as defense in depth
when present. The expected unique ID comes from the ``EDOVO_WIF_SA_UNIQUE_ID``
env var (cloud-run.tf); missing config fails closed.

``tokeninfo`` is opaque and rate-limited, so successful verifications are cached
by token hash for up to 5 minutes (failures never cached).

Sole authentication mechanism for this endpoint (OBT-27565).
"""
import hashlib
import os
import time
from http import HTTPStatus
from typing import Any

import requests

from recidiviz.utils.auth.auth0 import AuthorizationError
from recidiviz.utils.flask_exception import FlaskException
from recidiviz.utils.metadata import project_id

_BEARER_SCHEME = "Bearer"
_TOKENINFO_URL = "https://oauth2.googleapis.com/tokeninfo"
_TOKENINFO_TIMEOUT_SECONDS = 10

# Terraform-injected numeric unique ID of the edovo-wif@ service account for
# this environment (cloud-run.tf). Read at call time so tests and deploys that
# set the environment after import behave correctly.
_UNIQUE_ID_ENV_VAR = "EDOVO_WIF_SA_UNIQUE_ID"

# Cap on how long a successfully verified token is trusted without re-checking
# with Google (also capped by the token's own expiry).
_POSITIVE_CACHE_MAX_SECONDS = 300

# SHA-256 hex digest of a verified token -> monotonic time at which the cache
# entry lapses. Only ever holds hashes of tokens that fully passed
# verification, so it stays tiny (Edovo re-uses each ~1h token).
_positive_cache: dict[str, float] = {}


def _expected_service_account_email() -> str:
    """Returns the ``edovo-wif@`` service account email this environment trusts."""
    return f"edovo-wif@{project_id()}.iam.gserviceaccount.com"


def _expected_unique_id() -> str:
    """Returns the trusted service account's numeric unique ID from the
    environment, failing closed (500) if the deployment is misconfigured."""
    unique_id = os.environ.get(_UNIQUE_ID_ENV_VAR, "").strip()
    if not unique_id:
        raise FlaskException(
            code="token_verification_misconfigured",
            description=(
                f"{_UNIQUE_ID_ENV_VAR} is not configured for this environment"
            ),
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
        )
    return unique_id


def _token_from_header(authorization_header: str) -> str:
    """Returns the bearer token from the header, rejecting a non-Bearer or empty header (401)."""
    prefix = _BEARER_SCHEME + " "
    stripped = authorization_header.strip()
    if not stripped.startswith(prefix):
        raise AuthorizationError(
            code="invalid_authorization_header",
            description=f"Authorization header must use the {_BEARER_SCHEME!r} scheme",
        )
    token = stripped[len(prefix) :].strip()
    if not token:
        raise AuthorizationError(
            code="invalid_authorization_header",
            description="Bearer token is empty",
        )
    return token


def _introspect(token: str) -> dict[str, Any]:
    """Returns Google's tokeninfo for |token|; raises 401 if it is rejected, or
    500 (fail closed) if Google cannot be reached."""
    try:
        response = requests.get(
            _TOKENINFO_URL,
            params={"access_token": token},
            timeout=_TOKENINFO_TIMEOUT_SECONDS,
        )
    except requests.RequestException as error:
        # Could not reach Google to verify the token — fail closed, but as a
        # server-side error rather than an auth rejection.
        raise FlaskException(
            code="token_verification_unavailable",
            description="Could not reach Google to verify the access token",
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
        ) from error

    if response.status_code != HTTPStatus.OK:
        # tokeninfo returns 400 for an invalid or expired token.
        raise AuthorizationError(
            code="invalid_bearer_token",
            description="Access token was rejected by Google (invalid or expired)",
        )
    return response.json()


def _assert_expected_identity(tokeninfo: dict[str, Any]) -> None:
    """Asserts the introspected token belongs to the edovo-wif@ service account.

    ``aud`` must equal the SA's unique ID; ``azp`` and ``email`` must also match
    when present (they are scope-dependent and may legitimately be absent).
    """
    expected_unique_id = _expected_unique_id()
    aud = tokeninfo.get("aud")
    azp = tokeninfo.get("azp")
    email = tokeninfo.get("email")
    if (
        aud != expected_unique_id
        or (azp is not None and azp != expected_unique_id)
        or (email is not None and email != _expected_service_account_email())
    ):
        raise FlaskException(
            code="wrong_identity",
            description="Access token does not belong to the expected service account",
            status_code=HTTPStatus.FORBIDDEN,
        )


def _cache_lookup(token_hash: str) -> bool:
    """Returns True if this token recently passed full verification."""
    lapses_at = _positive_cache.get(token_hash)
    return lapses_at is not None and time.monotonic() < lapses_at


def _cache_store(token_hash: str, expires_in_seconds: int) -> None:
    """Caches a fully verified token hash until min(5 minutes, token expiry),
    pruning lapsed entries so the cache cannot grow unbounded."""
    ttl = min(_POSITIVE_CACHE_MAX_SECONDS, expires_in_seconds)
    if ttl <= 0:
        return
    now = time.monotonic()
    for cached_hash, lapses_at in list(_positive_cache.items()):
        if lapses_at <= now:
            # pop (not del) so two threads pruning the same lapsed entry
            # concurrently can't raise KeyError under a threaded server.
            _positive_cache.pop(cached_hash, None)
    _positive_cache[token_hash] = now + ttl


def verify_bearer_token(authorization_header: str) -> None:
    """Verify a GCP access token from an ``Authorization: Bearer <token>`` header.

    Raises ``AuthorizationError`` (401) if the header is malformed or the token
    is rejected by Google's tokeninfo endpoint (invalid or expired), and
    ``FlaskException`` for a valid token that does not belong to the expected
    service account (403), a missing unique-ID configuration (500), or an
    unreachable tokeninfo endpoint (500).
    """
    token = _token_from_header(authorization_header)

    # Resolve configuration before consulting the cache so a misconfigured
    # deployment always surfaces as 500, never as a stale cached acceptance.
    _expected_unique_id()

    token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
    if _cache_lookup(token_hash):
        return

    tokeninfo = _introspect(token)
    _assert_expected_identity(tokeninfo)

    try:
        expires_in = int(tokeninfo.get("expires_in", 0))
    except (TypeError, ValueError):
        expires_in = 0
    _cache_store(token_hash, expires_in)
