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
from copy import deepcopy
from http import HTTPStatus
from typing import Optional, Union

import sentry_sdk
from quart import Quart, Response, jsonify, make_response, request
from sentry_sdk.integrations.asyncio import AsyncioIntegration
from sentry_sdk.integrations.quart import QuartIntegration
from werkzeug.http import parse_set_header
from werkzeug.wrappers import Response as WerkzeugResponse

from recidiviz.case_triage.authorization_utils import build_authorization_handler
from recidiviz.prototypes.case_note_search.case_note_authorization import (
    on_successful_authorization,
)
from recidiviz.prototypes.case_note_search.case_note_search import case_note_search
from recidiviz.prototypes.case_note_search.case_note_search_record import (
    record_case_note_search,
)
from recidiviz.prototypes.case_note_search.exact_match import STATE_CODE_CONVERTER
from recidiviz.utils.environment import get_gcp_environment

ALLOWED_ORIGINS = [
    r"http\://localhost:3000",
    r"http\://localhost:5000",
    r"https\://dashboard-staging\.recidiviz\.org$",
    r"https\://dashboard-demo\.recidiviz\.org$",
    r"https\://dashboard\.recidiviz\.org$",
    r"https\://recidiviz-dashboard-stag-e1108--[^.]+?\.web\.app$",
    r"https\://recidiviz-dashboard--[^.]+?\.web\.app$",
]

# Case Note Search Sentry Data Source Name. See monitoring page at https://recidiviz-inc.sentry.io/projects/justice-counts/?project=4507895390404608
CASE_NOTE_SEARCH_SENTRY_DSN = (
    # not a secret!
    "https://ccca0e2f57773e9f50c7ac4cff056cc5@o432474.ingest.us.sentry.io/4507895390404608"
)


# pylint: disable=abstract-class-instantiated
sentry_sdk.init(
    dsn=CASE_NOTE_SEARCH_SENTRY_DSN,
    traces_sample_rate=0.25,
    environment=get_gcp_environment(),
    # profile 10% of transactions
    profiles_sample_rate=0.1,
    integrations=[QuartIntegration(), AsyncioIntegration()],
)

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
    # Bypass authentication for health check endpoint.
    if request.path == "/health":
        return None

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
    """Endpoint which queries the Discovery Engine, records the input parameters and
    results, and returns the engine output.

    Required params:
        * query: The case note search query.
        * user_id: The external id of the user making the query.
        * state_code: The state case notes to query.

    Optional params:
        * page_size (default 20): The number of case notes to return.
        * with_snippet (default True): Whether to request engine-created snippets.
        * external_id (defauly None): If populated, we only fetch case notes pertaining
            to the provided external id.
    """
    # Required params.
    query = request.args.get("query", type=str)
    if query is None:
        response = jsonify({"error": "Missing required parameter 'query'"})
        response.status_code = 400
        return response
    user_id = request.args.get("user_id", type=str)
    if user_id is None:
        response = jsonify({"error": "Missing required parameter 'user_id'"})
        response.status_code = 400
        return response
    state_code = request.args.get("state_code", type=str)
    if state_code is None or state_code.lower() not in STATE_CODE_CONVERTER:
        response = jsonify(
            {"error": f"State code {state_code} not supported for this endpoint."}
        )
        response.status_code = 400
        return response

    # Optional params.
    page_size = request.args.get("page_size", default=20, type=int)
    with_snippet = request.args.get(
        "with_snippet", default=True, type=lambda v: v.lower() == "true"
    )
    external_id = request.args.get("external_id", default=None, type=str)

    def get_state_code_for_filter(state_code: str) -> str:
        """Some state codes used for filtering are different than standard state codes.
        e.g. Idaho notes are stored as US_IX for now."""
        return STATE_CODE_CONVERTER[state_code.lower()]

    # Query the Discovery Engine.
    filter_conditions = {}
    if external_id is not None:
        filter_conditions["external_id"] = [external_id]
    if state_code is not None:
        filter_conditions["state_code"] = [get_state_code_for_filter(state_code)]
    case_note_response = await case_note_search(
        query=query,
        page_size=page_size,
        with_snippet=with_snippet,
        filter_conditions=filter_conditions,
    )

    # Record the query and results in the case note records table.
    try:
        await record_case_note_search(
            user_external_id=user_id,
            client_external_id=external_id,
            state_code=state_code,
            query=query,
            page_size=page_size,
            filter_conditions=filter_conditions,
            with_snippet=with_snippet,
            results=deepcopy(case_note_response),
        )
    except Exception as e:
        sentry_sdk.capture_exception(e)
        response = jsonify({"error": str(e)})
        response.status_code = 500
        return response

    return jsonify(case_note_response)
