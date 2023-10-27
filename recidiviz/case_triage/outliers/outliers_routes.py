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
"""Implements routes for the Outliers Flask blueprint. """
import logging
import re
from datetime import datetime
from http import HTTPStatus
from typing import Optional

import werkzeug
from flask import Blueprint, Response, g, jsonify, make_response, request
from werkzeug.http import parse_set_header

from recidiviz.case_triage.authorization_utils import build_authorization_handler
from recidiviz.case_triage.outliers.outliers_authorization import (
    on_successful_authorization,
)
from recidiviz.case_triage.outliers.user_context import UserContext
from recidiviz.common.common_utils import convert_nested_dictionary_keys
from recidiviz.common.constants.states import StateCode
from recidiviz.common.str_field_utils import snake_to_camel
from recidiviz.outliers.querier.querier import OutliersQuerier
from recidiviz.outliers.types import PersonName

ALLOWED_ORIGINS = [
    r"http\://localhost:3000",
    r"http\://localhost:5000",
    r"https\://dashboard-staging\.recidiviz\.org$",
    r"https\://dashboard-demo\.recidiviz\.org$",
    r"https\://dashboard\.recidiviz\.org$",
    r"https\://recidiviz-dashboard-stag-e1108--[^.]+?\.web\.app$",
]


def create_outliers_api_blueprint() -> Blueprint:
    """Creates the API blueprint for Outliers"""

    api = Blueprint("outliers", __name__)

    handle_authorization = build_authorization_handler(
        on_successful_authorization, "dashboard_auth0"
    )

    @api.before_request
    def validate_authentication() -> None:
        # OPTIONS requests do not require authentication
        if request.method != "OPTIONS":
            handle_authorization()

    @api.before_request
    def validate_cors() -> Optional[Response]:
        origin_is_allowed = any(
            re.match(allowed_origin, request.origin)
            for allowed_origin in ALLOWED_ORIGINS
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
        # Cache preflight responses for 2 hours
        response.access_control_max_age = 2 * 60 * 60
        return response

    @api.get("/<state>/configuration")
    def state_configuration(state: str) -> Response:
        state_code = StateCode(state.upper())
        config = OutliersQuerier().get_outliers_config(state_code)

        config_json = convert_nested_dictionary_keys(
            config.to_json(),
            snake_to_camel,
        )
        return jsonify({"config": config_json})

    @api.get("/<state>/supervisors")
    def supervisors(state: str) -> Response:
        state_code = StateCode(state.upper())
        supervisor_entities = OutliersQuerier().get_supervisors(state_code)

        return jsonify(
            {
                "supervisors": [
                    convert_nested_dictionary_keys(
                        entity.to_json(),
                        snake_to_camel,
                    )
                    for entity in supervisor_entities
                ]
            }
        )

    @api.get("/<state>/supervisor/<supervisor_pseudonymized_id>/officers")
    def officers_for_supervisor(
        state: str, supervisor_pseudonymized_id: str
    ) -> Response:
        state_code = StateCode(state.upper())

        try:
            num_lookback_periods = (
                int(request.args["num_lookback_periods"])
                if "num_lookback_periods" in request.args
                else None
            )

            period_end_date = (
                datetime.strptime(request.args["period_end_date"], "%Y-%m-%d")
                if "period_end_date" in request.args
                else None
            )
        except ValueError as e:
            return make_response(
                jsonify(f"Invalid parameters provided. Error: {str(e)}"),
                HTTPStatus.BAD_REQUEST,
            )

        querier = OutliersQuerier()
        supervisor = querier.get_supervisor_from_pseudonymized_id(
            state_code, supervisor_pseudonymized_id
        )

        if supervisor is None:
            return make_response(
                jsonify(
                    f"Supervisor with pseudonymized_id doesn't exist in DB. Pseudonymized id: {supervisor_pseudonymized_id}"
                ),
                HTTPStatus.NOT_FOUND,
            )

        officer_entities = querier.get_officers_for_supervisor(
            state_code, supervisor.external_id, num_lookback_periods, period_end_date
        )

        officers = [
            convert_nested_dictionary_keys(
                entity.to_json(),
                snake_to_camel,
            )
            for entity in officer_entities
        ]
        return jsonify({"officers": officers})

    @api.get("/<state>/benchmarks")
    def benchmarks(
        state: str,
    ) -> Response:
        try:
            state_code = StateCode(state.upper())

            num_lookback_periods = (
                int(request.args["num_lookback_periods"])
                if "num_lookback_periods" in request.args
                else None
            )

            period_end_date = (
                datetime.strptime(request.args["period_end_date"], "%Y-%m-%d")
                if "period_end_date" in request.args
                else None
            )
        except ValueError as e:
            return make_response(
                jsonify(f"Invalid parameters provided. Error: {str(e)}"),
                HTTPStatus.BAD_REQUEST,
            )

        benchmarks = OutliersQuerier().get_benchmarks(
            state_code, num_lookback_periods, period_end_date
        )

        result = [
            convert_nested_dictionary_keys(
                entity,
                snake_to_camel,
            )
            for entity in benchmarks
        ]
        return jsonify({"metrics": result})

    @api.get("/<state>/officer/<pseudonymized_officer_id>/events")
    def events_by_officer(state: str, pseudonymized_officer_id: str) -> Response:
        try:
            state_code = StateCode(state.upper())

            # Get all requested metrics from URL
            metric_ids = request.args.getlist("metric_id")

            period_end_date = (
                datetime.strptime(request.args["period_end_date"], "%Y-%m-%d")
                if "period_end_date" in request.args
                else None
            )
        except ValueError as e:
            return make_response(
                jsonify(f"Invalid parameters provided. Error: {str(e)}"),
                HTTPStatus.BAD_REQUEST,
            )

        querier = OutliersQuerier()

        state_config = querier.get_outliers_config(state_code)
        state_metrics = [metric.name for metric in state_config.metrics]

        # Check that the requested metrics are configured for the state
        if len(metric_ids) > 0:
            allowed_metric_ids = [
                metric_id for metric_id in metric_ids if metric_id in state_metrics
            ]

            disallowed_metric_ids = [
                metric_id
                for metric_id in metric_ids
                if metric_id not in allowed_metric_ids
            ]

            # Return an error if there are invalid requested metrics
            if len(disallowed_metric_ids) > 0:
                return make_response(
                    jsonify(
                        f"Must provide valid metric_ids for {state_code.value} in the request"
                    ),
                    HTTPStatus.BAD_REQUEST,
                )
        # If no metric ids are provided, use the configured metrics for the state.
        else:
            allowed_metric_ids = state_metrics

        # Check that the requested officer exists and has metrics for the period.
        officer_entity = querier.get_supervision_officer_entity(
            state_code=state_code,
            pseudonymized_officer_id=pseudonymized_officer_id,
            num_lookback_periods=0,
            period_end_date=period_end_date,
        )
        if officer_entity is None:
            return make_response(
                jsonify(
                    f"Officer with psuedonymized id not found: {pseudonymized_officer_id}"
                ),
                HTTPStatus.NOT_FOUND,
            )

        user_context: UserContext = g.user_context
        supervisor = querier.get_supervisor_from_external_id(
            state_code, user_context.user_external_id
        )

        # If the current user is identified as a supervisor, ensure that they supervise the requested officer.
        # If the user is not a supervisor, we can assume that the user is leadership or Recidiviz and can thus view any officer.
        if (
            supervisor
            and officer_entity.supervisor_external_id != supervisor.external_id
        ):
            return make_response(
                jsonify(
                    "User is a supervisor, but does not supervise the requested officer."
                ),
                HTTPStatus.UNAUTHORIZED,
            )

        # Get the metric ids the officer is an Outlier for
        outlier_metric_ids = [
            metric_id
            for metric_id in allowed_metric_ids
            if any(
                metric["metric_id"] == metric_id
                for metric in officer_entity.outlier_metrics
            )
        ]

        # If the officer is not an Outlier on any of the requested metrics, do not return the events.
        if len(outlier_metric_ids) == 0:
            return make_response(
                jsonify("Officer is not an outlier on any of the requested metrics."),
                HTTPStatus.UNAUTHORIZED,
            )

        # If the officer is not an Outlier on requested metrics, log and continue.
        if len(outlier_metric_ids) != allowed_metric_ids:
            nonoutlier_metric_ids = [
                metric_id
                for metric_id in allowed_metric_ids
                if metric_id not in outlier_metric_ids
            ]
            logging.error(
                "Officer %s is not an outlier on the following requested metrics: %s",
                officer_entity.external_id,
                nonoutlier_metric_ids,
            )

        events = querier.get_events_by_officer(
            state_code,
            officer_entity.pseudonymized_id,
            outlier_metric_ids,
            period_end_date,
        )

        results = [
            {
                "stateCode": event.state_code,
                "metricId": event.metric_id,
                "eventDate": datetime.strftime(event.event_date, "%Y-%m-%d"),
                "clientId": event.client_id,
                "clientName": {
                    "givenNames": PersonName(**event.client_name).given_names,
                    "middleNames": PersonName(**event.client_name).middle_names,
                    "surname": PersonName(**event.client_name).surname,
                },
                "officerId": event.officer_id,
                "attributes": event.attributes,
            }
            for event in events
        ]
        return jsonify({"events": results})

    @api.get("/<state>/officer/<pseudonymized_officer_id>")
    def officer(state: str, pseudonymized_officer_id: str) -> Response:

        state_code = StateCode(state.upper())

        try:
            num_lookback_periods = (
                int(request.args["num_lookback_periods"])
                if "num_lookback_periods" in request.args
                else None
            )

            period_end_date = (
                datetime.strptime(request.args["period_end_date"], "%Y-%m-%d")
                if "period_end_date" in request.args
                else None
            )
        except ValueError as e:
            return make_response(
                jsonify(f"Invalid parameters provided. Error: {str(e)}"),
                HTTPStatus.BAD_REQUEST,
            )

        querier = OutliersQuerier()

        # Check that the requested officer exists
        officer_entity = querier.get_supervision_officer_entity(
            state_code, pseudonymized_officer_id, num_lookback_periods, period_end_date
        )
        if officer_entity is None:
            return make_response(
                jsonify(
                    f"Officer with psuedonymized id not found: {pseudonymized_officer_id}"
                ),
                HTTPStatus.NOT_FOUND,
            )

        user_context: UserContext = g.user_context
        supervisor = querier.get_supervisor_from_external_id(
            state_code, user_context.user_external_id
        )

        # If the current user is identified as a supervisor, ensure that they supervise the requested officer.
        # If the user is not a supervisor, we can assume that the user is leadership or Recidiviz and can thus view any officer.
        if (
            supervisor
            and officer_entity.supervisor_external_id != supervisor.external_id
        ):
            return make_response(
                jsonify(
                    "User is supervisor, but does not supervise the requested officer."
                ),
                HTTPStatus.UNAUTHORIZED,
            )

        return jsonify(
            {
                "officer": convert_nested_dictionary_keys(
                    officer_entity.to_json(),
                    snake_to_camel,
                )
            }
        )

    return api
