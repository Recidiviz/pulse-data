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
"""Shared blueprint for Pathways API endpoints."""
import re
from collections import defaultdict
from http import HTTPStatus
from typing import Any, Callable, Dict, List

import werkzeug.wrappers
from flask import Blueprint, Response, jsonify, make_response, request
from werkzeug.exceptions import BadRequest
from werkzeug.http import parse_set_header

from recidiviz.case_triage.api_schemas_utils import load_api_schema
from recidiviz.case_triage.authorization_utils import build_authorization_handler
from recidiviz.case_triage.shared_pathways.dimensions.dimension import Dimension
from recidiviz.case_triage.shared_pathways.dimensions.dimension_transformer import (
    get_dimension_transformer,
)
from recidiviz.case_triage.shared_pathways.exceptions import MetricNotEnabledError
from recidiviz.case_triage.shared_pathways.metric_cache import PathwaysMetricCache
from recidiviz.case_triage.shared_pathways.metric_fetcher import PathwaysMetricFetcher
from recidiviz.case_triage.shared_pathways.pathways_api_schemas import (
    build_fetch_metric_schemas_by_name,
)
from recidiviz.case_triage.shared_pathways.query_builders.metric_query_builder import (
    MetricQueryBuilder,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema.pathways.schema import (
    MetricMetadata as PathwaysMetricMetadata,
)
from recidiviz.persistence.database.schema.public_pathways.schema import (
    MetricMetadata as PublicPathwaysMetricMetadata,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.utils.environment import in_offline_mode

FILTER_STRING_PATTERN = r"filters\[(\w+)\]"


def match_filter_string(value: str) -> str | None:
    match_result = re.match(FILTER_STRING_PATTERN, value)

    if match_result:
        return match_result.group(1)

    return None


def load_filters_from_query_string() -> Dict[str, List[str]]:
    filters: Dict[str, List[str]] = defaultdict(list)

    if "time_period" in request.args:
        filters["time_period"] = [request.args["time_period"]]

    for param_key, values in request.args.to_dict(flat=False).items():
        filter_dimension = match_filter_string(param_key)

        if filter_dimension:
            filters[filter_dimension] = [*filters[filter_dimension], *values]

    return {
        filter_dimension: get_dimension_transformer(Dimension(filter_dimension))(values)
        for filter_dimension, values in filters.items()
    }


class SharedPathwaysBlueprint:
    """Configurable blueprint for Pathways API endpoints."""

    def __init__(
        self,
        blueprint_name: str,
        auth_handler_name: str,
        on_successful_authorization: Callable,
        allowed_origins: list[str],
        schema_type: SchemaType,
        enabled_states: list[str],
        enabled_metrics: list[MetricQueryBuilder],
        metric_metadata: type[PathwaysMetricMetadata]
        | type[PublicPathwaysMetricMetadata],
    ) -> None:
        self.schema_type = schema_type
        self.api = Blueprint(blueprint_name, __name__)

        metrics_by_name = {metric.name: metric for metric in enabled_metrics}
        enabled_metrics_by_state: dict[StateCode, dict[str, MetricQueryBuilder]] = {
            StateCode(state_code): metrics_by_name for state_code in enabled_states
        }
        fetch_metric_schemas = build_fetch_metric_schemas_by_name(enabled_metrics)

        handle_authorization = build_authorization_handler(
            on_successful_authorization, auth_handler_name
        )

        @self.api.before_request
        def validate_authentication() -> None:
            # OPTIONS requests do not require authentication
            if request.method != "OPTIONS":
                handle_authorization()

        @self.api.before_request
        def validate_cors() -> Response | None:
            if request.origin is None:
                response = make_response()
                response.status_code = HTTPStatus.FORBIDDEN
                return response

            origin_is_allowed = any(
                re.match(allowed_origin, request.origin)
                for allowed_origin in allowed_origins
            )

            if not origin_is_allowed:
                response = make_response()
                response.status_code = HTTPStatus.FORBIDDEN
                return response

            return None

        @self.api.after_request
        def add_cors_headers(
            response: werkzeug.wrappers.Response,
        ) -> werkzeug.wrappers.Response:
            # Don't cache access control headers across origins
            response.vary = "Origin"
            response.access_control_allow_origin = request.origin
            response.access_control_allow_headers = parse_set_header(
                "authorization, sentry-trace"
            )
            # Cache preflight responses for 2 hours
            response.access_control_max_age = 2 * 60 * 60
            return response

        @self.api.get("/<state>/<metric_name>")
        def metrics(state: str, metric_name: str) -> Response:
            try:
                state_code = StateCode(state)
            except ValueError as e:
                raise BadRequest(f"Invalid state code: {state}") from e

            try:
                metric_mapper = enabled_metrics_by_state[state_code][metric_name]
            except KeyError as e:
                raise MetricNotEnabledError(
                    metric_name=metric_name, state_code=state_code
                ) from e

            source_data: Dict[str, Any] = {
                "filters": load_filters_from_query_string(),
            }

            if "group" in request.args:
                source_data["group"] = request.args["group"]

            if state_code == StateCode.US_OZ:
                source_data["demo"] = True

            fetch_metric_params_schema = load_api_schema(
                fetch_metric_schemas[metric_name],
                source_data=source_data,
            )
            fetch_metric_params = metric_mapper.build_params(fetch_metric_params_schema)

            if in_offline_mode():
                # The cache only adds extra complexity for offline mode because it would need to be
                # updated when fixture files change, adding startup time to the container. The DBs are
                # small enough in offline mode that we don't need to worry too much about slow queries,
                # so just use the fetcher directly instead of the cache.
                return jsonify(
                    PathwaysMetricFetcher(
                        state_code=state_code,
                        enabled_states=enabled_states,
                        metric_metadata=metric_metadata,
                        schema_type=schema_type,
                    ).fetch(metric_mapper, fetch_metric_params)
                )
            return jsonify(
                PathwaysMetricCache(
                    state_code=state_code,
                    schema_type=schema_type,
                    metric_fetcher=PathwaysMetricFetcher(
                        state_code=state_code,
                        enabled_states=enabled_states,
                        metric_metadata=metric_metadata,
                        schema_type=schema_type,
                    ),
                ).fetch(metric_mapper, fetch_metric_params)
            )
