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
import logging
from http import HTTPStatus

from flask import Flask, Response, g

from recidiviz.services.identity.constants import (
    DEV_CALLER_SERVICE_ACCOUNT,
    IAP_BACKEND_SERVICE_ID_SECRET_NAME,
)
from recidiviz.services.identity.exceptions import UnknownCallerError
from recidiviz.services.identity.helpers import get_source_product_app
from recidiviz.utils import structured_logging
from recidiviz.utils.auth.gce import build_compute_engine_auth_decorator
from recidiviz.utils.environment import in_development, in_gcp

app = Flask(__name__)

if in_gcp():
    structured_logging.setup_gunicorn()
else:
    logging.basicConfig(level=logging.INFO)

authenticate_iap_jwt = build_compute_engine_auth_decorator(
    IAP_BACKEND_SERVICE_ID_SECRET_NAME
)


@app.before_request
@authenticate_iap_jwt
def auth_middleware() -> None:
    """Authenticate against IAP on every request."""


@app.before_request
def set_source_product_middleware() -> tuple[str, HTTPStatus] | None:
    """
    Set the source_product_app based on caller_email from auth_middleware.
    """
    if in_development() and g.caller_email is None:
        g.caller_email = DEV_CALLER_SERVICE_ACCOUNT

    try:
        g.source_product_app = get_source_product_app(g.caller_email)
    except UnknownCallerError:
        logging.info("No product app found for caller email: [%s]", g.caller_email)
        return f"Error: Unknown caller {g.caller_email}", HTTPStatus.FORBIDDEN
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


@app.route("/health")
def health() -> tuple[str, HTTPStatus]:
    """Returns 200, used by GCP uptime checks to verify that workers are up."""
    return "", HTTPStatus.OK
