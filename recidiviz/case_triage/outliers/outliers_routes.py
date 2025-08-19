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
"""Implements routes for the Outliers Flask blueprint."""
import logging
import re
from datetime import datetime
from http import HTTPStatus
from typing import List, Optional

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
from recidiviz.common.str_field_utils import snake_to_camel, to_snake_case
from recidiviz.outliers.outliers_action_strategy_qualifier import (
    OutliersActionStrategyQualifier,
)
from recidiviz.outliers.querier.querier import OutliersQuerier
from recidiviz.outliers.roster_ticket_service import RosterTicketService
from recidiviz.outliers.types import (
    ActionStrategySurfacedEvent,
    PersonName,
    RosterChangeRequestSchema,
    SupervisionOfficerEntity,
    VitalsMetric,
)
from recidiviz.utils.environment import in_gcp_production
from recidiviz.utils.flask_exception import FlaskException
from recidiviz.utils.types import assert_type

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
            "authorization, sentry-trace, baggage, content-type"
        )
        response.access_control_allow_methods = parse_set_header("GET, PATCH")
        # Cache preflight responses for 2 hours
        response.access_control_max_age = 2 * 60 * 60
        return response

    @api.get("/<state>/configuration")
    def state_configuration(state: str) -> Response:
        state_code = StateCode(state.upper())
        user_context: UserContext = g.user_context
        config = OutliersQuerier(
            state_code, user_context.feature_variants
        ).get_product_configuration(user_context)

        config_json = convert_nested_dictionary_keys(
            config.to_json(),
            snake_to_camel,
        )
        # The FE is expecting snake case keys in the actionStrategyCopy
        # object so we will revert the snake_to_camel for that attr
        config_json["actionStrategyCopy"] = config.action_strategy_copy
        return jsonify({"config": config_json})

    @api.get("/<state>/supervisors")
    def supervisors(state: str) -> Response:
        state_code = StateCode(state.upper())
        user_context: UserContext = g.user_context

        if user_context.can_access_all_supervisors:
            supervisor_entities = OutliersQuerier(
                state_code, user_context.feature_variants
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
            f"User {user_context.pseudonymized_id} is not authorized to access the /supervisors endpoint",
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

        except ValueError as e:
            return jsonify_response(
                f"Invalid parameters provided. Error: {str(e)}", HTTPStatus.BAD_REQUEST
            )

        user_context: UserContext = g.user_context

        # If the current user cannot access data about all supervisors, ensure that they are the requested supervisor.
        if not user_context.can_access_all_supervisors and (
            supervisor_pseudonymized_id != user_context.pseudonymized_id
        ):
            return jsonify_response(
                f"User with pseudo id [{user_context.pseudonymized_id}] cannot access requested supervisor with pseudo id [{supervisor_pseudonymized_id}].",
                HTTPStatus.UNAUTHORIZED,
            )

        querier = OutliersQuerier(state_code, user_context.feature_variants)
        supervisor = querier.get_supervisor_entity_from_pseudonymized_id(
            supervisor_pseudonymized_id
        )

        if supervisor is None:
            return jsonify_response(
                f"Supervisor with pseudonymized_id doesn't exist in DB. Pseudonymized id: {supervisor_pseudonymized_id}",
                HTTPStatus.NOT_FOUND,
            )

        primary_category_type = querier.get_product_configuration(
            user_context
        ).primary_category_type

        officer_entities = querier.get_officers_for_supervisor(
            supervisor.external_id,
            primary_category_type,
            user_context.can_access_supervision_workflows,
            num_lookback_periods,
        )

        officers = [
            convert_nested_dictionary_keys(
                entity.to_json(),
                snake_to_camel,
            )
            for entity in officer_entities
        ]
        return jsonify({"officers": officers})

    @api.get("/<state>/supervisor/<supervisor_pseudonymized_id>/outcomes")
    def outcomes_for_supervisor(
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

        user_context: UserContext = g.user_context

        # If the current user cannot access data about all supervisors, ensure that they are the requested supervisor.
        if not user_context.can_access_all_supervisors and (
            supervisor_pseudonymized_id != user_context.pseudonymized_id
        ):
            return jsonify_response(
                f"User with pseudo id [{user_context.pseudonymized_id}] cannot access requested supervisor with pseudo id [{supervisor_pseudonymized_id}].",
                HTTPStatus.UNAUTHORIZED,
            )

        querier = OutliersQuerier(state_code, user_context.feature_variants)
        supervisor = querier.get_supervisor_entity_from_pseudonymized_id(
            supervisor_pseudonymized_id
        )

        if supervisor is None:
            return jsonify_response(
                f"Supervisor with pseudonymized_id doesn't exist in DB. Pseudonymized id: {supervisor_pseudonymized_id}",
                HTTPStatus.NOT_FOUND,
            )

        primary_category_type = querier.get_product_configuration(
            user_context
        ).primary_category_type

        officer_outcomes = querier.get_officer_outcomes_for_supervisor(
            supervisor.external_id,
            primary_category_type,
            num_lookback_periods,
            period_end_date,
        )

        outcomes_list = [
            convert_nested_dictionary_keys(
                entity.to_json(),
                snake_to_camel,
            )
            for entity in officer_outcomes
        ]
        return jsonify({"outcomes": outcomes_list})

    @api.get("/<state>/officer/<pseudonymized_officer_id>/outcomes")
    def outcomes(state: str, pseudonymized_officer_id: str) -> Response:
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

        user_context: UserContext = g.user_context
        querier = OutliersQuerier(state_code, user_context.feature_variants)

        category_type_to_compare = querier.get_product_configuration(
            user_context
        ).primary_category_type

        # Check officer exists
        officer = querier.get_supervision_officer_from_pseudonymized_id(
            pseudonymized_officer_id
        )

        if officer is None:
            return jsonify_response(
                f"Officer with pseudonymized id not found: {pseudonymized_officer_id}",
                HTTPStatus.NOT_FOUND,
            )

        # If the current user cannot access data about all supervisors, ensure that they supervise the requested officer.
        if not user_context.can_access_all_supervisors and (
            not querier.supervisor_exists_with_external_id(
                user_context.user_external_id
            )
            or user_context.user_external_id not in officer.supervisor_external_ids
        ):
            return jsonify_response(
                "User cannot access all supervisors and does not supervise the requested officer.",
                HTTPStatus.UNAUTHORIZED,
            )

        # Retrieve requested outcomes info
        officer_outcomes = querier.get_supervision_officer_outcomes(
            pseudonymized_officer_id,
            category_type_to_compare,
            num_lookback_periods,
            period_end_date,
        )
        if officer_outcomes is None:
            return jsonify_response(
                f"Outcomes info not found for officer with pseudonymized id: {pseudonymized_officer_id}",
                HTTPStatus.NOT_FOUND,
            )

        return jsonify(
            {
                "outcomes": convert_nested_dictionary_keys(
                    officer_outcomes.to_json(),
                    snake_to_camel,
                )
            }
        )

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

        user_context: UserContext = g.user_context
        querier = OutliersQuerier(state_code, user_context.feature_variants)

        primary_category_type = querier.get_product_configuration(
            user_context
        ).primary_category_type

        benchmarks = querier.get_benchmarks(
            primary_category_type,
            num_lookback_periods,
            period_end_date,
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

        user_context: UserContext = g.user_context
        querier = OutliersQuerier(state_code, user_context.feature_variants)

        state_config = querier.get_outliers_backend_config()
        state_metrics = [metric.name for metric in state_config.get_all_metrics()]

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

        category_type_to_compare = querier.get_product_configuration(
            user_context
        ).primary_category_type

        # Check that the requested officer exists.
        officer_entity = querier.get_supervision_officer_entity(
            category_type_to_compare=category_type_to_compare,
            pseudonymized_officer_id=pseudonymized_officer_id,
            include_workflows_info=user_context.can_access_supervision_workflows,
            num_lookback_periods=0,
        )
        if officer_entity is None:
            return jsonify_response(
                f"Officer with pseudonymized id not found: {pseudonymized_officer_id}",
                HTTPStatus.NOT_FOUND,
            )

        # If the current user cannot access data about all supervisors, ensure that they supervise the requested officer.
        if not user_context.can_access_all_supervisors and (
            not querier.supervisor_exists_with_external_id(
                user_context.user_external_id
            )
            or (
                user_context.user_external_id
                not in officer_entity.supervisor_external_ids
            )
        ):
            return jsonify_response(
                "User cannot access all supervisors and does not supervise the requested officer.",
                HTTPStatus.UNAUTHORIZED,
            )

        # Check that the officer outcomes exist for the period.
        officer_outcomes = querier.get_supervision_officer_outcomes(
            category_type_to_compare=category_type_to_compare,
            pseudonymized_officer_id=pseudonymized_officer_id,
            num_lookback_periods=0,
            period_end_date=period_end_date,
        )
        if officer_outcomes is None:
            return jsonify_response(
                f"Officer outcomes data for pseudonymized id not found: {pseudonymized_officer_id}",
                HTTPStatus.NOT_FOUND,
            )

        # Get the metric ids the officer is an Outlier for
        outlier_metric_ids = [
            metric_id
            for metric_id in allowed_metric_ids
            if any(
                metric["metric_id"] == metric_id
                for metric in officer_outcomes.outlier_metrics
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
                "eventDate": event.event_date.isoformat() if event.event_date else None,
                "clientId": event.client_id,
                "clientName": (
                    {
                        "givenNames": PersonName(**event.client_name).given_names,
                        "middleNames": PersonName(**event.client_name).middle_names,
                        "surname": PersonName(**event.client_name).surname,
                    }
                    if event.client_name
                    else None
                ),
                "officerId": event.officer_id,
                "pseudonymizedClientId": event.pseudonymized_client_id,
                "attributes": (
                    convert_nested_dictionary_keys(event.attributes, snake_to_camel)
                    if event.attributes
                    else None
                ),
                "officerAssignmentDate": (
                    event.officer_assignment_date.isoformat()
                    if event.officer_assignment_date
                    else None
                ),
                "officerAssignmentEndDate": (
                    event.officer_assignment_end_date.isoformat()
                    if event.officer_assignment_end_date
                    else None
                ),
                "supervisionStartDate": (
                    event.supervision_start_date.isoformat()
                    if event.supervision_start_date
                    else None
                ),
                "supervisionEndDate": (
                    event.supervision_end_date.isoformat()
                    if event.supervision_end_date
                    else None
                ),
                "supervisionType": event.supervision_type,
            }
            for event in events
        ]
        return jsonify({"events": results})

    @api.get("/<state>/officers")
    def officers(state: str) -> Response:

        state_code = StateCode(state.upper())
        user_context: UserContext = g.user_context
        querier = OutliersQuerier(state_code, user_context.feature_variants)

        user_external_id: str = user_context.user_external_id

        if (
            user_external_id != "RECIDIVIZ"
            and not user_context.can_access_all_supervisors
            and not querier.supervisor_exists_with_external_id(user_external_id)
        ):
            return jsonify_response(
                "User cannot access all officers",
                HTTPStatus.UNAUTHORIZED,
            )

        officers: List[
            SupervisionOfficerEntity
        ] = querier.get_all_supervision_officers_required_info_only()

        return jsonify(
            {
                "officers": [
                    convert_nested_dictionary_keys(
                        officer.to_json(),
                        snake_to_camel,
                    )
                    for officer in officers
                ],
            }
        )

    @api.get("/<state>/officer/<pseudonymized_officer_id>")
    def officer(state: str, pseudonymized_officer_id: str) -> Response:
        state_code = StateCode(state.upper())

        try:
            num_lookback_periods = (
                int(request.args["num_lookback_periods"])
                if "num_lookback_periods" in request.args
                else None
            )

        except ValueError as e:
            return jsonify_response(
                f"Invalid parameters provided. Error: {str(e)}", HTTPStatus.BAD_REQUEST
            )

        user_context: UserContext = g.user_context
        querier = OutliersQuerier(state_code, user_context.feature_variants)

        category_type_to_compare = querier.get_product_configuration(
            user_context
        ).primary_category_type

        # Check that the requested officer exists
        officer_entity = querier.get_supervision_officer_entity(
            pseudonymized_officer_id,
            category_type_to_compare,
            user_context.can_access_supervision_workflows,
            num_lookback_periods,
        )
        if officer_entity is None:
            return jsonify_response(
                f"Officer with pseudonymized id not found: {pseudonymized_officer_id}",
                HTTPStatus.NOT_FOUND,
            )

        # If the current user cannot access data about all supervisors, ensure that they supervise the requested officer.
        if not user_context.can_access_all_supervisors and (
            not querier.supervisor_exists_with_external_id(
                user_context.user_external_id
            )
            or user_context.user_external_id
            not in officer_entity.supervisor_external_ids
        ):
            return jsonify_response(
                "User cannot access all supervisors and does not supervise the requested officer.",
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
        user_context: UserContext = g.user_context
        querier = OutliersQuerier(state_code, user_context.feature_variants)

        user_external_id = user_context.user_external_id
        user_pseudonymized_id = user_context.pseudonymized_id

        if (
            pseudonymized_id != user_pseudonymized_id
            and user_external_id != "RECIDIVIZ"
        ):
            # Return an unauthorized error if the requesting user is requesting information about someone else
            # and the requesting user is not a Recidiviz user
            return jsonify_response(
                f"Non-recidiviz user with pseudonymized_id {user_pseudonymized_id} is requesting user-info about a user that isn't themselves: {pseudonymized_id}",
                HTTPStatus.UNAUTHORIZED,
            )

        user_info = None
        if querier.supervisor_exists_with_external_id(
            user_external_id
        ) or user_external_id in ["RECIDIVIZ", "CSG"]:
            user_info = querier.get_supervisor_user_info(pseudonymized_id)
        else:
            user_info = querier.get_officer_user_info(
                pseudonymized_id,
                user_context.can_access_supervision_workflows,
                querier.get_product_configuration(user_context).primary_category_type,
            )

        user_info_json = convert_nested_dictionary_keys(
            user_info.to_json(),
            snake_to_camel,
        )
        return jsonify(user_info_json)

    # TODO(#46300): Add a check to patch for officers and supervisors.
    @api.patch("/<state>/user-info/<pseudonymized_id>")
    def patch_user_info(state: str, pseudonymized_id: str) -> Response:
        state_code = StateCode(state.upper())
        user_context: UserContext = g.user_context
        querier = OutliersQuerier(state_code, user_context.feature_variants)

        user_external_id = user_context.user_external_id
        user_pseudonymized_id = user_context.pseudonymized_id

        if pseudonymized_id != user_pseudonymized_id:
            # Return an unauthorized error if the requesting user is attempting to update information about someone else
            return jsonify_response(
                f"User with pseudonymized_id {user_pseudonymized_id if user_pseudonymized_id else user_external_id} is attempting to update user-info about a user that isn't themselves: {pseudonymized_id}",
                HTTPStatus.UNAUTHORIZED,
            )

        if not request.json:
            return jsonify_response("Missing request body", HTTPStatus.BAD_REQUEST)

        request_json = convert_nested_dictionary_keys(
            assert_type(request.json, dict), to_snake_case
        )

        expected_keys = {
            "has_seen_onboarding",
            "has_dismissed_data_unavailable_note",
            "has_dismissed_rate_over_100_percent_note",
        }
        if len(request_json) == 0 or not set(request_json.keys()).issubset(
            expected_keys
        ):
            return jsonify_response(
                f"Invalid request body. Expected keys: {[snake_to_camel(k) for k in sorted(expected_keys)]}. Found keys: {[snake_to_camel(k) for k in request_json]}",
                HTTPStatus.BAD_REQUEST,
            )

        querier.update_user_metadata(pseudonymized_id, request_json)

        # Return updated user info
        user_info = querier.get_supervisor_user_info(pseudonymized_id)
        user_info_json = convert_nested_dictionary_keys(
            user_info.to_json(),
            snake_to_camel,
        )
        return jsonify(user_info_json)

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

        user_context: UserContext = g.user_context
        querier = OutliersQuerier(state_code, user_context.feature_variants)

        state_config = querier.get_outliers_backend_config()
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
                "clientName": (
                    {
                        "givenNames": PersonName(**event.client_name).given_names,
                        "middleNames": PersonName(**event.client_name).middle_names,
                        "surname": PersonName(**event.client_name).surname,
                    }
                    if event.client_name
                    else None
                ),
                "officerId": event.officer_id,
                "pseudonymizedClientId": event.pseudonymized_client_id,
                "attributes": (
                    convert_nested_dictionary_keys(
                        event.attributes,
                        snake_to_camel,
                    )
                    if event.attributes
                    else None
                ),
            }
            for event in events
        ]
        return jsonify({"events": results})

    @api.get("/<state>/client/<pseudonymized_client_id>")
    def client(state: str, pseudonymized_client_id: str) -> Response:
        state_code = StateCode(state.upper())
        user_context: UserContext = g.user_context
        querier = OutliersQuerier(state_code, user_context.feature_variants)

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
            "clientName": (
                {
                    "givenNames": PersonName(**client.client_name).given_names,
                    "middleNames": PersonName(**client.client_name).middle_names,
                    "surname": PersonName(**client.client_name).surname,
                }
                if client.client_name
                else None
            ),
            "birthdate": client.birthdate.isoformat(),
            "gender": client.gender,
            "raceOrEthnicity": client.race_or_ethnicity,
        }

        return jsonify({"client": client_json})

    @api.get("/<state>/action_strategies/<supervisor_pseudonymized_id>")
    def action_strategies(state: str, supervisor_pseudonymized_id: str) -> Response:
        state_code = StateCode(state.upper())

        user_context: UserContext = g.user_context
        querier = OutliersQuerier(state_code, user_context.feature_variants)

        user_external_id = user_context.user_external_id
        user_pseudonymized_id = user_context.pseudonymized_id

        if (
            not user_context.can_access_all_supervisors
            and supervisor_pseudonymized_id != user_pseudonymized_id
            and user_external_id != "RECIDIVIZ"
        ):
            # Return an unauthorized error if the requesting user is requesting information about someone else
            # and the requesting user is not a Recidiviz user
            return jsonify_response(
                f"Non-recidiviz user with pseudonymized_id {user_pseudonymized_id} is requesting action strategies for a user that isn't themselves: {supervisor_pseudonymized_id}",
                HTTPStatus.UNAUTHORIZED,
            )

        supervisor = querier.get_supervisor_entity_from_pseudonymized_id(
            supervisor_pseudonymized_id
        )

        if supervisor is None:
            return jsonify({supervisor_pseudonymized_id: None})

        category_type_to_compare = querier.get_product_configuration(
            user_context
        ).primary_category_type

        # For action strategies, we should only look at officers included in outcomes
        officer_entities = [
            o
            for o in querier.get_officers_for_supervisor(
                supervisor.external_id,
                category_type_to_compare,
                user_context.can_access_supervision_workflows,
            )
            if o.include_in_outcomes
        ]
        id_to_outcomes = querier.get_id_to_supervision_officer_outcomes_entities(
            category_type_to_compare,
            supervisor_external_id=supervisor.external_id,
        )

        action_strategies_json = {}

        supervisor_events = querier.get_action_strategy_surfaced_events_for_supervisor(
            supervisor.pseudonymized_id
        )

        product_config = querier.get_outliers_backend_config()

        qualifier = OutliersActionStrategyQualifier(
            events=supervisor_events, config=product_config
        )

        missing_officer_outcomes = []
        for officer in officer_entities:
            officer_outcomes = id_to_outcomes.get(officer.external_id)
            if officer_outcomes is None:
                missing_officer_outcomes.append(officer.pseudonymized_id)
            else:
                action_strategies_json[
                    officer.pseudonymized_id
                ] = qualifier.get_eligible_action_strategy_for_officer(
                    officer=officer, officer_outcomes=officer_outcomes
                )

        if len(missing_officer_outcomes) > 0:
            return jsonify_response(
                f"Missing outcomes data for officers with the pseudo ids: {missing_officer_outcomes}",
                HTTPStatus.NOT_FOUND,
            )

        action_strategies_json[
            supervisor.pseudonymized_id
        ] = qualifier.get_eligible_action_strategy_for_supervisor(
            officers_outcomes=list(id_to_outcomes.values())
        )
        return jsonify(action_strategies_json)

    @api.patch("/<state>/action_strategies/<supervisor_pseudonymized_id>")
    def patch_action_strategies(
        state: str, supervisor_pseudonymized_id: str
    ) -> Response:
        state_code = StateCode(state.upper())
        user_context: UserContext = g.user_context
        querier = OutliersQuerier(state_code, user_context.feature_variants)

        user_external_id = user_context.user_external_id
        user_pseudonymized_id = user_context.pseudonymized_id

        if (
            supervisor_pseudonymized_id != user_pseudonymized_id
            and user_external_id != "RECIDIVIZ"
        ):
            # Return an unauthorized error if the requesting user is updating information about someone else
            # and the requesting user is not a Recidiviz user
            return jsonify_response(
                f"Non-recidiviz user with pseudonymized_id {user_pseudonymized_id} is attempting to update action strategies for a user that isn't themselves: {supervisor_pseudonymized_id}",
                HTTPStatus.UNAUTHORIZED,
            )
        if not request.json:
            return jsonify_response("Missing request body", HTTPStatus.BAD_REQUEST)

        request_json = convert_nested_dictionary_keys(
            assert_type(request.json, dict), to_snake_case
        )

        expected_keys = {
            "user_pseudonymized_id",
            "officer_pseudonymized_id",
            "action_strategy",
        }
        if len(request_json) == 0 or not set(request_json.keys()).issubset(
            expected_keys
        ):
            return jsonify_response(
                f"Invalid request body. Expected keys: {[snake_to_camel(k) for k in sorted(expected_keys)]}. Found keys: {[snake_to_camel(k) for k in request_json]}",
                HTTPStatus.BAD_REQUEST,
            )

        event = ActionStrategySurfacedEvent(
            state_code=state_code.value,
            user_pseudonymized_id=request_json["user_pseudonymized_id"],
            officer_pseudonymized_id=(
                request_json["officer_pseudonymized_id"]
                if "officer_pseudonymized_id" in request_json
                else None
            ),
            action_strategy=request_json["action_strategy"],
            timestamp=datetime.now(),
        )
        querier.insert_action_strategy_surfaced_event(event)

        # Return new action strategy surfaced event
        new_event = (
            querier.get_most_recent_action_strategy_surfaced_event_for_supervisor(
                supervisor_pseudonymized_id
            )
        )
        event_json = convert_nested_dictionary_keys(new_event.to_json(), snake_to_camel)
        return jsonify(event_json)

    @api.get("/<state>/supervisor/<supervisor_pseudonymized_id>/vitals")
    def vitals_metrics_for_supervisor(
        state: str, supervisor_pseudonymized_id: str
    ) -> Response:
        state_code = StateCode(state.upper())
        user_context: UserContext = g.user_context
        querier = OutliersQuerier(state_code, user_context.feature_variants)

        user_external_id = user_context.user_external_id
        user_pseudonymized_id = user_context.pseudonymized_id

        if (
            supervisor_pseudonymized_id != user_pseudonymized_id
            and user_external_id != "RECIDIVIZ"
            and not user_context.can_access_all_supervisors
        ):
            # Return an unauthorized error if the requesting user is requesting information about someone else
            # and the requesting user is not a Recidiviz user
            return jsonify_response(
                f"Non-recidiviz user with pseudonymized_id {user_pseudonymized_id} is requesting vitals for a user they do not have access to: {supervisor_pseudonymized_id}",
                HTTPStatus.UNAUTHORIZED,
            )

        vitals_metrics: List[
            VitalsMetric
        ] = querier.get_vitals_metrics_for_supervision_officer_supervisor(
            supervisor_pseudonymized_id,
            user_context.can_access_all_supervisors,
        )
        return jsonify(
            [
                convert_nested_dictionary_keys(metric.to_json(), snake_to_camel)
                for metric in vitals_metrics
            ]
        )

    @api.get("/<state>/officer/<pseudonymized_officer_id>/vitals")
    def vitals_metrics_for_officer(
        state: str, pseudonymized_officer_id: str
    ) -> Response:
        state_code = StateCode(state.upper())
        user_context: UserContext = g.user_context
        querier = OutliersQuerier(state_code, user_context.feature_variants)

        user_external_id = user_context.user_external_id
        user_pseudonymized_id = user_context.pseudonymized_id

        officer = querier.get_supervision_officer_from_pseudonymized_id(
            pseudonymized_officer_id
        )

        if officer is None:
            return jsonify_response(
                f"Officer with pseudonymized id not found: {pseudonymized_officer_id}",
                HTTPStatus.NOT_FOUND,
            )

        if (
            user_external_id not in officer.supervisor_external_ids
            and user_external_id != "RECIDIVIZ"
            and not user_context.can_access_all_supervisors
        ):
            # Return an unauthorized error if the requesting user is requesting information about someone else
            # and the requesting user is not a Recidiviz user
            return jsonify_response(
                f" User with pseudonymized_id {user_pseudonymized_id} is requesting vitals for officers they do not have access to: {pseudonymized_officer_id}",
                HTTPStatus.UNAUTHORIZED,
            )

        vitals_metrics: List[
            VitalsMetric
        ] = querier.get_vitals_metrics_for_supervision_officer(officer.pseudonymized_id)

        return jsonify(
            [
                convert_nested_dictionary_keys(metric.to_json(), snake_to_camel)
                for metric in vitals_metrics
            ]
        )

    @api.post("/<state>/supervisor/<supervisor_pseudonymized_id>/roster_change_request")
    def submit_roster_change_request(
        state: str, supervisor_pseudonymized_id: str
    ) -> Response:
        state_code = StateCode(state.upper())
        user_context: UserContext = g.user_context
        querier = OutliersQuerier(state_code, user_context.feature_variants)
        roster_ticket_service = RosterTicketService(querier=querier)

        user_external_id = user_context.user_external_id
        email_address = user_context.email_address
        has_feature_variant = "reportIncorrectRosters" in user_context.feature_variants
        user_pseudonymized_id = user_context.pseudonymized_id

        target_supervisor = querier.get_supervisor_entity_from_pseudonymized_id(
            supervisor_pseudonymized_id
        )

        if target_supervisor is None:
            return jsonify_response(
                f"Target supervisor with pseudonymized_id, {supervisor_pseudonymized_id}, not found.",
                HTTPStatus.NOT_FOUND,
            )

        if (not has_feature_variant and user_external_id != "RECIDIVIZ") or (
            not user_context.can_access_all_supervisors
            and not querier.supervisor_exists_with_external_id(user_external_id)
        ):
            return jsonify_response(
                f"User with pseudonymized_id {user_pseudonymized_id} cannot make roster change request.",
                HTTPStatus.UNAUTHORIZED,
            )

        if not request.json:
            return jsonify_response("Missing request body.", HTTPStatus.BAD_REQUEST)

        try:
            # Validate request body against schema
            request_data = request.get_json()
            roster_change_request_schema = RosterChangeRequestSchema(**request_data)

        except TypeError as e:
            human_err = re.sub(r"^.*RosterChangeRequestSchema\S*\s*", "", str(e))
            return jsonify_response(
                f"Invalid request data: {human_err}", HTTPStatus.BAD_REQUEST
            )

        # Call the request_roster_change method
        roster_ticket_service_response = roster_ticket_service.request_roster_change(
            requester=roster_change_request_schema.requester_name,
            email=email_address,
            change_type=roster_change_request_schema.request_change_type,
            target_supervisor_id=target_supervisor.external_id,
            officer_ids=roster_change_request_schema.affected_officers_external_ids,
            note=roster_change_request_schema.request_note,
            is_test=user_external_id == "RECIDIVIZ" or not in_gcp_production(),
        )

        return jsonify(
            convert_nested_dictionary_keys(
                roster_ticket_service_response.to_json(), snake_to_camel
            )
        )

    return api
