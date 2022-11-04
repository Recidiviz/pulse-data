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
"""Implements routes to support external requests from Workflows."""
from flask import Blueprint, Response, current_app, jsonify, request
from flask_wtf.csrf import generate_csrf

from recidiviz.case_triage.authorization_utils import build_authorization_handler
from recidiviz.case_triage.workflows.interface import WorkflowsExternalRequestInterface
from recidiviz.case_triage.workflows.workflows_authorization import (
    on_successful_authorization,
)
from recidiviz.utils.types import assert_type


def create_workflows_api_blueprint() -> Blueprint:
    """Creates the API blueprint for Workflows"""
    workflows_api = Blueprint("workflows", __name__)

    handle_authorization = build_authorization_handler(
        on_successful_authorization, "dashboard_auth0"
    )

    @workflows_api.before_request
    def validate_request() -> None:
        if request.method != "OPTIONS":
            handle_authorization()

    @workflows_api.route("/<state>/init")
    def init(state: str) -> Response:  # pylint: disable=unused-argument
        return jsonify({"csrf": generate_csrf(current_app.secret_key)})

    @workflows_api.post("/external_request/<state>/insert_contact_note")
    def insert_contact_note(
        state: str,  # pylint: disable=unused-argument
    ) -> Response:
        data = assert_type(request.json, dict)
        response = WorkflowsExternalRequestInterface.insert_contact_note(data)
        return response

    return workflows_api
