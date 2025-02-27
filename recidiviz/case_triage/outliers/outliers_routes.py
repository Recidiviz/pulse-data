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
    grant_endpoint_access,
    on_successful_authorization,
)
from recidiviz.case_triage.outliers.user_context import UserContext
from recidiviz.case_triage.workflows.utils import jsonify_response
from recidiviz.common.common_utils import convert_nested_dictionary_keys
from recidiviz.common.constants.states import StateCode
from recidiviz.common.str_field_utils import snake_to_camel
from recidiviz.outliers.querier.querier import OutliersQuerier
from recidiviz.outliers.types import PersonName
from recidiviz.utils.flask_exception import FlaskException

ALLOWED_ORIGINS = [
    r"http\://localhost:3000",
    r"http\://localhost:5000",
    r"https\://dashboard-staging\.recidiviz\.org$",
    r"https\://dashboard-demo\.recidiviz\.org$",
    r"https\://dashboard\.recidiviz\.org$",
    r"https\://recidiviz-dashboard-stag-e1108--[^.]+?\.web\.app$",
    r"https\://recidiviz-dashboard--[^.]+?\.web\.app$",
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
        config = OutliersQuerier(state_code).get_outliers_config()

        config_json = convert_nested_dictionary_keys(
            config.to_json(),
            snake_to_camel,
        )
        return jsonify({"config": config_json})

    @api.get("/<state>/supervisors")
    def supervisors(state: str) -> Response:
        state_code = StateCode(state.upper())
        user_context: UserContext = g.user_context
        user_role = user_context.role

        if user_role in ["leadership_role", "supervision_leadership"]:
            supervisor_entities = OutliersQuerier(
                state_code
            ).get_supervision_officer_supervisor_entities()

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

        return jsonify_response(
            f"Non-leadership user {user_context.user_external_id} is not authorized to access the /supervisors endpoint",
            HTTPStatus.UNAUTHORIZED,
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
            return jsonify_response(
                f"Invalid parameters provided. Error: {str(e)}", HTTPStatus.BAD_REQUEST
            )

        querier = OutliersQuerier(state_code)
        supervisor = querier.get_supervisor_entity_from_pseudonymized_id(
            supervisor_pseudonymized_id
        )

        if supervisor is None:
            return jsonify_response(
                f"Supervisor with pseudonymized_id doesn't exist in DB. Pseudonymized id: {supervisor_pseudonymized_id}",
                HTTPStatus.NOT_FOUND,
            )

        officer_entities = querier.get_officers_for_supervisor(
            supervisor.external_id, num_lookback_periods, period_end_date
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
            return jsonify_response(
                f"Invalid parameters provided. Error: {str(e)}", HTTPStatus.BAD_REQUEST
            )

        benchmarks = OutliersQuerier(state_code).get_benchmarks(
            num_lookback_periods, period_end_date
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
            return jsonify_response(
                f"Invalid parameters provided. Error: {str(e)}", HTTPStatus.BAD_REQUEST
            )

        querier = OutliersQuerier(state_code)

        state_config = querier.get_outliers_config()
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
                return jsonify_response(
                    f"Must provide valid metric_ids for {state_code.value} in the request",
                    HTTPStatus.BAD_REQUEST,
                )

        # If no metric ids are provided, use the configured metrics for the state.
        else:
            allowed_metric_ids = state_metrics

        # Check that the requested officer exists and has metrics for the period.
        officer_entity = querier.get_supervision_officer_entity(
            pseudonymized_officer_id=pseudonymized_officer_id,
            num_lookback_periods=0,
            period_end_date=period_end_date,
        )
        if officer_entity is None:
            return jsonify_response(
                f"Officer with psuedonymized id not found: {pseudonymized_officer_id}",
                HTTPStatus.NOT_FOUND,
            )

        user_context: UserContext = g.user_context
        supervisor = querier.get_supervisor_from_external_id(
            user_context.user_external_id
        )

        # If the current user is identified as a supervisor, ensure that they supervise the requested officer.
        # If the user is not a supervisor, we can assume that the user is leadership or Recidiviz and can thus view any officer.
        if (
            supervisor
            and officer_entity.supervisor_external_id != supervisor.external_id
        ):
            return jsonify_response(
                "User is a supervisor, but does not supervise the requested officer.",
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
            return jsonify_response(
                "Officer is not an outlier on any of the requested metrics.",
                HTTPStatus.UNAUTHORIZED,
            )

        # If the user requested specific metrics and the officer is not an Outlier on requested metrics, log and continue.
        if len(metric_ids) > 0 and len(outlier_metric_ids) != len(allowed_metric_ids):
            nonoutlier_metric_ids = [
                metric_id
                for metric_id in allowed_metric_ids
                if metric_id not in outlier_metric_ids
            ]
            logging.error(
                "Officer %s is not an outlier on the following requested metrics: %s",
                officer_entity.pseudonymized_id,
                nonoutlier_metric_ids,
            )

        events = querier.get_events_by_officer(
            officer_entity.pseudonymized_id, outlier_metric_ids, period_end_date
        )

        results = [
            {
                "stateCode": event.state_code,
                "metricId": event.metric_id,
                "eventDate": event.event_date.isoformat(),
                "clientId": event.client_id,
                "clientName": {
                    "givenNames": PersonName(**event.client_name).given_names,
                    "middleNames": PersonName(**event.client_name).middle_names,
                    "surname": PersonName(**event.client_name).surname,
                },
                "officerId": event.officer_id,
                "pseudonymizedClientId": event.pseudonymized_client_id,
                "attributes": convert_nested_dictionary_keys(
                    event.attributes, snake_to_camel
                )
                if event.attributes
                else None,
                "officerAssignmentDate": event.officer_assignment_date.isoformat(),
                "officerAssignmentEndDate": event.officer_assignment_end_date.isoformat(),
                "supervisionStartDate": event.supervision_start_date.isoformat(),
                "supervisionEndDate": event.supervision_end_date.isoformat(),
                "supervisionType": event.supervision_type,
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
            return jsonify_response(
                f"Invalid parameters provided. Error: {str(e)}", HTTPStatus.BAD_REQUEST
            )

        querier = OutliersQuerier(state_code)

        # Check that the requested officer exists
        officer_entity = querier.get_supervision_officer_entity(
            pseudonymized_officer_id, num_lookback_periods, period_end_date
        )
        if officer_entity is None:
            return jsonify_response(
                f"Officer with psuedonymized id not found: {pseudonymized_officer_id}",
                HTTPStatus.NOT_FOUND,
            )

        user_context: UserContext = g.user_context
        supervisor = querier.get_supervisor_from_external_id(
            user_context.user_external_id
        )

        # If the current user is identified as a supervisor, ensure that they supervise the requested officer.
        # If the user is not a supervisor, we can assume that the user is leadership or Recidiviz and can thus view any officer.
        if (
            supervisor
            and officer_entity.supervisor_external_id != supervisor.external_id
        ):
            return jsonify_response(
                "User is supervisor, but does not supervise the requested officer.",
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

    @api.get("/<state>/user-info/<pseudonymized_id>")
    def user_info(state: str, pseudonymized_id: str) -> Response:
        state_code = StateCode(state.upper())
        querier = OutliersQuerier(state_code)

        user_context: UserContext = g.user_context
        user_external_id = user_context.user_external_id
        user_pseudonymized_id = user_context.pseudonymized_id

        entity_json, role = None, None

        if (
            pseudonymized_id != user_pseudonymized_id
            and user_external_id != "RECIDIVIZ"
        ):
            # Return an unauthorized error if the requesting user is requesting information about someone else
            # and the requesting user is not a Recidiviz user
            return jsonify_response(
                f"Non-recidiviz user {user_external_id} with pseudonymized_id {user_pseudonymized_id} is requesting user-info about a user that isn't themselves: {pseudonymized_id}",
                HTTPStatus.UNAUTHORIZED,
            )

        supervisor_entity = querier.get_supervisor_entity_from_pseudonymized_id(
            pseudonymized_id
        )
        if supervisor_entity:
            # If the requested pseudonymized id is a supervisor that exists and the requesting user is that supervisor,
            # or the requesting user is a recidiviz user, return the entity
            entity_json = supervisor_entity.to_json()
            role = "supervision_officer_supervisor"

        return jsonify(
            {
                "entity": convert_nested_dictionary_keys(
                    entity_json,
                    snake_to_camel,
                )
                if entity_json
                else None,
                "role": role,
            }
        )

    @api.get("/<state>/client/<pseudonymized_id>/events")
    def events_by_client(state: str, pseudonymized_id: str) -> Response:
        try:
            state_code = StateCode(state.upper())

            # Get all requested metrics from URL
            event_metric_ids = request.args.getlist("metric_id")

            period_end_date = (
                datetime.strptime(request.args["period_end_date"], "%Y-%m-%d")
                if "period_end_date" in request.args
                else None
            )
        except ValueError as e:
            return jsonify_response(
                f"Invalid parameters provided. Error: {str(e)}", HTTPStatus.BAD_REQUEST
            )

        if period_end_date is None:
            return jsonify_response("Must provide an end date", HTTPStatus.BAD_REQUEST)

        querier = OutliersQuerier(state_code)

        state_config = querier.get_outliers_config()
        state_client_events = [event.name for event in state_config.client_events]

        # Check that the requested events are configured for the state
        if len(event_metric_ids) > 0:
            disallowed_metric_ids = [
                metric_id
                for metric_id in event_metric_ids
                if metric_id not in state_client_events
            ]

            # Return an error if there are invalid requested events
            if len(disallowed_metric_ids) > 0:
                return jsonify_response(
                    f"Must provide valid event metric_ids for {state_code.value} in the request",
                    HTTPStatus.BAD_REQUEST,
                )

        # If no metric ids are provided, use the configured event names for the state.
        else:
            event_metric_ids = state_client_events

        user_context: UserContext = g.user_context
        try:
            grant_endpoint_access(querier, user_context)
        except FlaskException as e:
            return jsonify_response(str(e.description), e.status_code)

        events = querier.get_events_by_client(
            pseudonymized_id, event_metric_ids, period_end_date
        )

        results = [
            {
                "stateCode": event.state_code,
                "metricId": event.metric_id,
                "eventDate": event.event_date.isoformat(),
                "clientId": event.client_id,
                "clientName": {
                    "givenNames": PersonName(**event.client_name).given_names,
                    "middleNames": PersonName(**event.client_name).middle_names,
                    "surname": PersonName(**event.client_name).surname,
                },
                "officerId": event.officer_id,
                "pseudonymizedClientId": event.pseudonymized_client_id,
                "attributes": convert_nested_dictionary_keys(
                    event.attributes,
                    snake_to_camel,
                )
                if event.attributes
                else None,
            }
            for event in events
        ]
        return jsonify({"events": results})

    @api.get("/<state>/client/<pseudonymized_client_id>")
    def client(state: str, pseudonymized_client_id: str) -> Response:
        state_code = StateCode(state.upper())
        querier = OutliersQuerier(state_code)

        user_context: UserContext = g.user_context
        try:
            grant_endpoint_access(querier, user_context)
        except FlaskException as e:
            return jsonify_response(str(e.description), e.status_code)

        client = querier.get_client_from_pseudonymized_id(
            pseudonymized_id=pseudonymized_client_id
        )

        if client is None:
            return jsonify_response(
                f"Client {pseudonymized_client_id} not found in the DB",
                HTTPStatus.NOT_FOUND,
            )

        client_json = {
            "stateCode": client.state_code,
            "clientId": client.client_id,
            "pseudonymizedClientId": client.pseudonymized_client_id,
            "clientName": {
                "givenNames": PersonName(**client.client_name).given_names,
                "middleNames": PersonName(**client.client_name).middle_names,
                "surname": PersonName(**client.client_name).surname,
            },
            "birthdate": client.birthdate.isoformat(),
            "gender": client.gender,
            "raceOrEthnicity": client.race_or_ethnicity,
        }

        return jsonify({"client": client_json})

    return api
