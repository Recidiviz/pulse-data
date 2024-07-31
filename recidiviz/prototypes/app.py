# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Prototype endpoints."""
import re
from http import HTTPStatus
from typing import Optional, Union

from quart import Quart, Response, jsonify, make_response, request
from werkzeug.http import parse_set_header
from werkzeug.wrappers import Response as WerkzeugResponse

from recidiviz.case_triage.authorization_utils import build_authorization_handler
from recidiviz.prototypes.case_note_search.case_note_authorization import (
    on_successful_authorization,
)
from recidiviz.prototypes.case_note_search.case_note_search import case_note_search

ALLOWED_ORIGINS = [
    r"http\://localhost:3000",
    r"http\://localhost:5000",
    r"https\://dashboard-staging\.recidiviz\.org$",
    r"https\://dashboard-demo\.recidiviz\.org$",
    r"https\://dashboard\.recidiviz\.org$",
    r"https\://recidiviz-dashboard-stag-e1108--[^.]+?\.web\.app$",
    r"https\://recidiviz-dashboard--[^.]+?\.web\.app$",
]

app = Quart(__name__)


@app.before_request
async def validate_authentication() -> None:
    # OPTIONS requests do not require authentication
    if request.method == "OPTIONS":
        return

    # Bypass authentication for health check endpoint.
    if request.path == "/health":
        return

    handle_authorization = build_authorization_handler(
        on_successful_authorization=on_successful_authorization,
        secret_name="dashboard_auth0",  # nosec
        auth_header=request.headers.get("Authorization", None),
    )
    handle_authorization()


@app.before_request
async def validate_cors() -> Optional[Union[Response, WerkzeugResponse]]:
    origin_is_allowed = any(
        re.match(allowed_origin, request.origin) for allowed_origin in ALLOWED_ORIGINS
    )

    if not origin_is_allowed:
        response = await make_response()
        response.status_code = HTTPStatus.FORBIDDEN
        return response

    return None


@app.after_request
async def add_cors_headers(
    response: Response,
) -> Response:
    # Don't cache access control headers across origins
    response.vary = "Origin"
    response.access_control_allow_origin = request.origin
    response.access_control_allow_headers = parse_set_header(
        "authorization, sentry-trace, baggage, content-type"
    )
    response.access_control_allow_methods = parse_set_header("GET, PATCH")
    # Cache preflight responses for 2 hours
    response.access_control_max_age = 2 * 60 * 60
    return response


@app.route("/search", methods=["GET"])
async def search_case_notes() -> Response:
    query = request.args.get("query", type=str)
    if query is None:
        response = jsonify({"error": "Missing required parameter 'query'"})
        response.status_code = 400
        return response

    page_size = request.args.get("page_size", default=20, type=int)
    with_snippet = request.args.get(
        "with_snippet", default=True, type=lambda v: v.lower() == "true"
    )
    external_id = request.args.get("external_id", default=None, type=str)

    case_note_response = case_note_search(
        query=query,
        page_size=page_size,
        with_snippet=with_snippet,
        filter_conditions={"external_id": [external_id]}
        if external_id is not None
        else None,
    )
    return jsonify(case_note_response)
