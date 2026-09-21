#!/usr/bin/env bash

# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""
File with helper functions used by recidiviz/case_triage/jii/jii_texts_routes.py and
recidiviz/case_triage/workflows/workflows_routes.py
"""
import logging
import re
from http import HTTPStatus
from typing import Callable, Optional

import werkzeug.wrappers
from flask import Response, g, make_response, request
from google.auth.exceptions import GoogleAuthError
from google.auth.transport import requests as google_auth_requests
from google.oauth2 import id_token
from werkzeug.http import parse_set_header

from recidiviz.case_triage.workflows.twilio_validation import TwilioValidator
from recidiviz.utils.auth.auth0 import AuthorizationError
from recidiviz.utils.environment import in_gcp
from recidiviz.utils.metadata import CloudRunMetadata
from recidiviz.utils.params import get_bool_param_value

if in_gcp():
    cloud_run_metadata = CloudRunMetadata.build_from_metadata_server(
        CloudRunMetadata.Service.CASE_TRIAGE
    )
else:
    cloud_run_metadata = CloudRunMetadata(
        project_id="123",
        region="us-central1",
        url="http://localhost:5000",
        service_account_email="fake-acct@fake-project.iam.gserviceaccount.com",
    )

ALLOWED_ORIGINS = [
    r"http\://localhost:3000",
    r"http\://localhost:5000",
    r"https\://dashboard-staging\.recidiviz\.org$",
    r"https\://dashboard-demo\.recidiviz\.org$",
    r"https\://dashboard\.recidiviz\.org$",
    r"https\://recidiviz-dashboard-stag-e1108--[^.]+?\.web\.app$",
    r"https\://recidiviz-dashboard--[^.]+?\.web\.app$",
    r"https\://app-staging\.recidiviz\.org$",
    cloud_run_metadata.url,
]

proxy_endpoint = "workflows.proxy"

twilio_validator = TwilioValidator()


def validate_twilio(
    handle_authorization: Callable[[], None],
    handle_recidiviz_only_authorization: Callable[[], None],
) -> None:
    del handle_authorization
    if get_bool_param_value("IsTest", request.values, default=False):
        handle_recidiviz_only_authorization()
        return
    logging.info("Twilio webhook endpoint request origin: [%s]", request.origin)
    signature = request.headers["X-Twilio-Signature"]
    twilio_validator.validate(
        url=request.url, params=request.values.to_dict(), signature=signature
    )


def _verify_cloud_task_oidc_token() -> bool:
    """Returns whether the request carries a valid OIDC identity token that
    Cloud Tasks minted for our own service account, audience-bound to this
    exact URL.

    Never raises: an absent or malformed token means this isn't a Cloud Tasks
    request, not that verification failed with an error, so callers can fall
    back to another auth method.
    """
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return False
    token = auth_header.removeprefix("Bearer ")
    try:
        claims = id_token.verify_oauth2_token(
            token, google_auth_requests.Request(), audience=request.base_url
        )
    except (ValueError, GoogleAuthError):
        return False
    return (
        bool(claims.get("email_verified"))
        and claims.get("email") == cloud_run_metadata.service_account_email
    )


def validate_cloud_task_request(
    handle_authorization: Callable[[], None],
    handle_recidiviz_only_authorization: Callable[[], None],
) -> None:
    """Validator for a route that is a Cloud Tasks target only and is never
    called directly by the frontend."""
    del handle_authorization, handle_recidiviz_only_authorization
    if not in_gcp():
        # No real Cloud Tasks queue, and no Google-signed tokens to present,
        # outside a hosted GCP environment.
        return
    if not _verify_cloud_task_oidc_token():
        raise AuthorizationError(
            code="not_authorized",
            description="Missing or invalid Cloud Tasks OIDC token",
        )


def validate_cloud_task_or_dashboard_auth(
    handle_authorization: Callable[[], None],
    handle_recidiviz_only_authorization: Callable[[], None],
) -> None:
    """Validator for a route that is called directly by the frontend
    (dashboard Auth0 session) and is also its own Cloud Tasks target.

    Sets g.is_cloud_task_request so the route can skip checks that only make
    sense for the original, human-initiated call.
    """
    del handle_recidiviz_only_authorization
    if in_gcp() and _verify_cloud_task_oidc_token():
        g.is_cloud_task_request = True
        return
    g.is_cloud_task_request = False
    handle_authorization()


# Maps endpoints to their validator, or None if auth is inside the route.
# None entries are external system callbacks authenticated via shared secret.
endpoint_validators: dict[
    str, Callable[[Callable[[], None], Callable[[], None]], None] | None
] = {
    "jii.handle_twilio_status": validate_twilio,
    "jii.handle_twilio_incoming_message": validate_twilio,
    "workflows.handle_twilio_status": validate_twilio,
    "workflows.handle_twilio_incoming_message": validate_twilio,
    "workflows.handle_mcp_us_ia_early_discharge_callback": None,
    "workflows.handle_send_sms_request": validate_cloud_task_request,
    "workflows.handle_update_docstars_early_termination_date": validate_cloud_task_request,
    "workflows.handle_early_discharge_form": validate_cloud_task_request,
    "workflows.handle_insert_tepe_contact_note": validate_cloud_task_request,
    "workflows.insert_contact_note": validate_cloud_task_or_dashboard_auth,
}


def validate_request_helper(
    handle_authorization: Callable, handle_recidiviz_only_authorization: Callable
) -> None:
    if request.method == "OPTIONS":
        return
    if request.endpoint in endpoint_validators:
        validator = endpoint_validators[request.endpoint]
        if validator is not None:
            validator(handle_authorization, handle_recidiviz_only_authorization)
        return
    if request.endpoint == proxy_endpoint:
        handle_recidiviz_only_authorization()
        return
    handle_authorization()


# Endpoints with no Origin header to validate because a browser never calls
# them directly — signature-verified webhooks, the MCP callback, and the
# recidiviz-only proxy endpoint. This is deliberately its own list rather than
# derived from endpoint_validators above: several endpoint_validators entries
# are Cloud Tasks targets that *can* still receive a request carrying a
# browser's Origin header, forwarded through the queue from the original
# dashboard call, and still need it checked.
_NO_ORIGIN_HEADER_ENDPOINTS = frozenset(
    {
        "jii.handle_twilio_status",
        "jii.handle_twilio_incoming_message",
        "workflows.handle_twilio_status",
        "workflows.handle_twilio_incoming_message",
        "workflows.handle_mcp_us_ia_early_discharge_callback",
        proxy_endpoint,
    }
)


def validate_cors_helper() -> Optional[Response]:
    if request.endpoint in _NO_ORIGIN_HEADER_ENDPOINTS:
        # Server-to-server requests have no browser Origin header to validate.
        return None

    is_allowed = request.origin is not None and any(
        re.match(allowed_origin, request.origin) for allowed_origin in ALLOWED_ORIGINS
    )

    if not is_allowed:
        response = make_response()
        response.status_code = HTTPStatus.FORBIDDEN
        return response

    return None


def add_cors_headers_helper(
    response: werkzeug.wrappers.Response,
) -> werkzeug.wrappers.Response:
    # Don't cache access control headers across origins
    response.vary = "Origin"
    response.access_control_allow_origin = request.origin
    response.access_control_allow_headers = parse_set_header(
        # `baggage` is added by sentry. It only seems to reach the server during local development
        "authorization, sentry-trace, x-csrf-token, content-type, baggage"
    )
    response.access_control_allow_credentials = True
    # Cache preflight responses for 2 hours
    response.access_control_max_age = 2 * 60 * 60
    return response
