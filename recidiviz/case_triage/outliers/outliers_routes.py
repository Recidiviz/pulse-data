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
import re
from datetime import datetime
from http import HTTPStatus
from typing import Optional

import werkzeug
from flask import Blueprint, Response, jsonify, make_response, request
from werkzeug.http import parse_set_header

from recidiviz.case_triage.authorization_utils import build_authorization_handler
from recidiviz.case_triage.outliers.outliers_authorization import (
    on_successful_authorization,
)
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
    def supervisors_with_outliers(state: str) -> Response:
        state_code = StateCode(state.upper())
        supervisor_entities = OutliersQuerier().get_supervisors_with_outliers(
            state_code
        )

        supervisors = [
            {
                "externalId": supervisor.external_id,
                "fullName": {
                    "givenNames": PersonName(**supervisor.full_name).given_names,
                    "middleNames": PersonName(**supervisor.full_name).middle_names,
                    "surname": PersonName(**supervisor.full_name).surname,
                },
                "supervisionDistrict": supervisor.supervision_district,
                "email": supervisor.email,
            }
            for supervisor in supervisor_entities
        ]
        return jsonify({"supervisors": supervisors})

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

    return api
