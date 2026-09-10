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
"""Backend entry point for the Identity Service API server."""
import datetime
import logging
from http import HTTPStatus

from flask import Flask, Response, g, request
from flask_smorest import Api
from google.auth.exceptions import GoogleAuthError
from google.auth.transport import requests as google_auth_requests
from google.oauth2 import id_token

from recidiviz.common.constants.tenants import Tenant
from recidiviz.common.google_cloud.single_cloud_task_queue_manager import (
    get_cloud_task_json_body,
)
from recidiviz.services.identity.authorization import CallerRole, get_caller
from recidiviz.services.identity.constants import (
    CLOUD_RUN_METADATA_CONFIG_KEY,
    DEV_CALLER_EMAIL_HEADER,
    IAP_BACKEND_SERVICE_ID_SECRET_NAME,
    IDENTITY_SERVICE_API_VERSION,
    IMPORT_PROCESS_INTERNAL_ROUTE,
)
from recidiviz.services.identity.error_handlers import register_error_handlers
from recidiviz.services.identity.exceptions import UnknownCallerError
from recidiviz.services.identity.identity_blueprint import identity_blueprint
from recidiviz.services.identity.import_task import (
    SNAPSHOT_TIMESTAMP_BODY_KEY,
    TENANT_BODY_KEY,
    process_import,
)
from recidiviz.utils import structured_logging
from recidiviz.utils.auth.gce import build_compute_engine_auth_decorator
from recidiviz.utils.environment import in_development, in_gcp
from recidiviz.utils.metadata import CloudRunMetadata

app = Flask(__name__)

api = Api(
    app,
    spec_kwargs={
        "title": "Recidiviz Identity Service",
        "version": IDENTITY_SERVICE_API_VERSION,
        "openapi_version": "3.1.0",
    },
)
app.register_blueprint(
    identity_blueprint, url_prefix=f"/{IDENTITY_SERVICE_API_VERSION}"
)
register_error_handlers(app)

if in_gcp():
    structured_logging.setup_gunicorn()
    cloud_run_metadata = CloudRunMetadata.build_from_metadata_server(
        CloudRunMetadata.Service.IDENTITY_SERVICE
    )
else:
    logging.basicConfig(level=logging.INFO)
    cloud_run_metadata = CloudRunMetadata(
        project_id="123",
        region="us-central1",
        url="http://localhost:5000",
        service_account_email="fake-acct@fake-project.iam.gserviceaccount.com",
    )

# Stashed on the app so the trigger_import endpoint can read the service URL and
# service account it needs to enqueue the import Cloud Task.
app.config[CLOUD_RUN_METADATA_CONFIG_KEY] = cloud_run_metadata

authenticate_iap_jwt = build_compute_engine_auth_decorator(
    IAP_BACKEND_SERVICE_ID_SECRET_NAME
)

HEALTH_ENDPOINT_PATH = "/health"

# Endpoints served by something other than a role-scoped blueprint MethodView
# that are exempt from role authorization: /health (Cloud Run probes and uptime
# checks carry no IAP identity; see auth_middleware), Flask's built-in
# static-file endpoint, and the internal import processing endpoint
# (authenticated as the service's own service account via a Cloud Tasks OIDC
# token, so the caller role model does not apply). Any other non-MethodView
# endpoint fails closed, denying every caller.
ROLE_EXEMPT_ENDPOINTS = frozenset({"health", "static", "handle_import_task"})


@authenticate_iap_jwt
def _enforce_iap_authentication() -> None:
    """Validates the request's IAP JWT and sets g.caller_email."""


def _authenticate_cloud_tasks_request() -> tuple[str, HTTPStatus] | None:
    """Authenticates a request to the internal import processing endpoint.

    Returns None when the caller is the service's own service account, or an
    error response otherwise.

    Cloud Tasks calls that endpoint directly on the Cloud Run URL with an OIDC
    token minted from the service's own service account. The load balancer also
    routes this path, so this check verifies the bearer token rather than
    trusting the path, denying any caller that is not the service itself.
    Development requests carry no token and are allowed through, matching how
    IAP is bypassed in development.
    """
    if in_development():
        return None
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return "Error: missing OIDC bearer token", HTTPStatus.UNAUTHORIZED
    token = auth_header.removeprefix("Bearer ")
    expected_audience = f"{cloud_run_metadata.url}{IMPORT_PROCESS_INTERNAL_ROUTE}"
    try:
        claims = id_token.verify_oauth2_token(
            token, google_auth_requests.Request(), audience=expected_audience
        )
    except (ValueError, GoogleAuthError) as e:
        logging.info(
            "Rejecting internal import request with invalid OIDC token: [%s]", e
        )
        return "Error: invalid OIDC token", HTTPStatus.UNAUTHORIZED
    if (
        not claims.get("email_verified")
        or claims.get("email") != cloud_run_metadata.service_account_email
    ):
        logging.info(
            "Rejecting internal import request from unauthorized caller [%s].",
            claims.get("email"),
        )
        return "Error: unauthorized caller", HTTPStatus.FORBIDDEN
    return None


@app.before_request
def auth_middleware() -> tuple[str, HTTPStatus] | None:
    """Authenticate every request except /health.

    The Cloud Run startup probe and GCP uptime checks hit /health directly on
    the container, bypassing the load balancer and IAP, so those requests
    cannot carry an IAP JWT. Cloud Tasks calls the internal import processing
    endpoint with an OIDC token, not through IAP, so that endpoint
    authenticates against the token instead.
    """
    if request.path == HEALTH_ENDPOINT_PATH:
        return None
    if request.path == IMPORT_PROCESS_INTERNAL_ROUTE:
        return _authenticate_cloud_tasks_request()
    return _enforce_iap_authentication()


def _allowed_roles_for_request() -> frozenset[CallerRole] | None:
    """Returns the roles permitted to call the matched endpoint with the
    request's HTTP method.

    Returns None when the request needs no role check: no route matched (Flask
    will return 404), or the matched endpoint is in ROLE_EXEMPT_ENDPOINTS.
    Otherwise authorization fails closed: an endpoint that is not a blueprint
    MethodView, or an HTTP method with no entry in the MethodView's
    ALLOWED_ROLES_BY_METHOD, gets an empty set, denying every caller.
    """
    if request.endpoint is None:
        return None
    if request.endpoint in ROLE_EXEMPT_ENDPOINTS:
        return None
    view_func = app.view_functions[request.endpoint]
    view_class = getattr(view_func, "view_class", None)
    if view_class is None:
        return frozenset()
    roles_by_method: dict[str, frozenset[CallerRole]] = getattr(
        view_class, "ALLOWED_ROLES_BY_METHOD", {}
    )
    return roles_by_method.get(request.method, frozenset())


@app.before_request
def authorize_caller_middleware() -> tuple[str, HTTPStatus] | None:
    """Resolve the caller from its authenticated email and authorize it against
    the matched endpoint's allowed roles.
    """
    if request.path == HEALTH_ENDPOINT_PATH:
        return None
    if request.path == IMPORT_PROCESS_INTERNAL_ROUTE:
        # Already authenticated as the service's own service account in
        # auth_middleware; the caller role model does not apply to Cloud Tasks.
        return None

    if in_development() and g.caller_email is None:
        # Local requests carry no IAP identity, so they must pick a dev caller
        # via header; a request without one fails below as an unknown caller.
        g.caller_email = request.headers.get(DEV_CALLER_EMAIL_HEADER)

    try:
        g.caller = get_caller(g.caller_email)
    except UnknownCallerError:
        logging.info("Unknown caller email: [%s]", g.caller_email)
        return f"Error: Unknown caller {g.caller_email}", HTTPStatus.FORBIDDEN

    g.source_product_app = g.caller.source_product_app

    allowed_roles = _allowed_roles_for_request()
    if allowed_roles is not None and g.caller.role not in allowed_roles:
        logging.info(
            "Caller [%s] with role [%s] is not authorized for endpoint [%s].",
            g.caller_email,
            g.caller.role,
            request.endpoint,
        )
        return (
            f"Error: Caller {g.caller_email} is not authorized for this endpoint",
            HTTPStatus.FORBIDDEN,
        )
    return None


@app.after_request
def set_headers(response: Response) -> Response:
    if not in_development():
        # max age of 2 years
        response.headers["Strict-Transport-Security"] = "max-age=63072000"
    response.headers["Content-Security-Policy"] = "frame-ancestors 'none'"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-Content-Type-Options"] = "nosniff"
    if "Cache-Control" not in response.headers:
        response.headers["Cache-Control"] = "no-store, max-age=0"
    return response


@app.route(IMPORT_PROCESS_INTERNAL_ROUTE, methods=["POST"])
def handle_import_task() -> tuple[str, HTTPStatus]:
    """Runs a tenant's import, invoked by Cloud Tasks.

    auth_middleware authenticates the caller as the service's own service
    account before the request reaches this handler.
    """
    body = get_cloud_task_json_body()
    process_import(
        tenant=Tenant(body[TENANT_BODY_KEY]),
        snapshot_timestamp=datetime.datetime.fromisoformat(
            body[SNAPSHOT_TIMESTAMP_BODY_KEY]
        ),
    )
    return "", HTTPStatus.OK


@app.route(HEALTH_ENDPOINT_PATH)
def health() -> tuple[str, HTTPStatus]:
    """Returns 200, used by GCP uptime checks to verify that workers are up."""
    return "", HTTPStatus.OK
