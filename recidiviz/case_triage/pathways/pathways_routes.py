# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Implements routes for the Pathways Flask blueprint. """
import re
from collections import defaultdict
from http import HTTPStatus
from typing import Dict, List, Optional

import werkzeug.wrappers
from flask import Blueprint, Response, jsonify, make_response, request
from werkzeug.http import parse_set_header

from recidiviz.case_triage.api_schemas import load_api_schema
from recidiviz.case_triage.pathways.metrics import FetchMetricParams, MetricFactory
from recidiviz.case_triage.pathways.pathways_api_schemas import (
    FETCH_METRIC_SCHEMAS_BY_NAME,
)
from recidiviz.case_triage.pathways.pathways_authorization import (
    build_authorization_handler,
)
from recidiviz.common.constants.states import StateCode

PATHWAYS_ALLOWED_ORIGINS = [
    r"http\://localhost:3000",
    r"https\://dashboard-staging\.recidiviz\.org$",
    r"https\://dashboard\.recidiviz\.org$",
    r"https\://recidiviz-dashboard-stag-e1108--[^.]+?\.web\.app$",
]
FILTER_STRING_PATTERN = r"filters\[(\w+)\]"


def match_filter_string(value: str) -> Optional[str]:
    match_result = re.match(FILTER_STRING_PATTERN, value)

    if match_result:
        return match_result.group(1)

    return None


def load_filters_from_query_string() -> Dict[str, List[str]]:
    filters: Dict[str, List[str]] = defaultdict(list)

    for key, values in request.args.to_dict(flat=False).items():
        filter_dimension = match_filter_string(key)

        if filter_dimension:
            filters[filter_dimension] = [*filters[filter_dimension], *values]

    return filters


def create_pathways_api_blueprint() -> Blueprint:
    """Creates the API blueprint for Pathways"""
    api = Blueprint("pathways", __name__)

    handle_authorization = build_authorization_handler()

    @api.before_request
    def validate_authentication() -> None:
        # OPTIONS requests do not require authentication
        if request.method != "OPTIONS":
            handle_authorization()

    @api.before_request
    def validate_cors() -> Optional[Response]:
        origin_is_allowed = any(
            re.match(allowed_origin, request.origin)
            for allowed_origin in PATHWAYS_ALLOWED_ORIGINS
        )

        if not origin_is_allowed:
            response = make_response()
            response.status_code = HTTPStatus.FORBIDDEN
            return response

        return None

    @api.after_request
    def add_cors_headers(
        response: werkzeug.wrappers.Response,
    ) -> werkzeug.wrappers.Response:
        # Don't cache access control headers across origins
        response.vary = "Origin"
        response.access_control_allow_origin = request.origin
        response.access_control_allow_headers = parse_set_header(
            "authorization, sentry-trace"
        )
        return response

    @api.get("/<state>/<metric_name>")
    def metrics(state: str, metric_name: str) -> Response:
        state_code = StateCode(state)
        metric_factory = MetricFactory(state_code)
        metric = metric_factory.build_metric(metric_name)

        fetch_metric_params_schema = load_api_schema(
            FETCH_METRIC_SCHEMAS_BY_NAME[metric_name],
            source_data={
                "group": request.args.get("group"),
                "since": request.args.get("since"),
                "filters": load_filters_from_query_string(),
            },
        )
        fetch_metric_params = FetchMetricParams(**fetch_metric_params_schema)

        return jsonify(metric.fetch(fetch_metric_params))

    return api
