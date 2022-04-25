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
"""Implements API routes for the Justice Counts Control Panel backend API."""
from typing import Callable, Optional

from flask import Blueprint, Response, g, jsonify, make_response, request
from flask_sqlalchemy_session import current_session
from flask_wtf.csrf import generate_csrf

from recidiviz.justice_counts.control_panel.constants import ControlPanelPermission
from recidiviz.justice_counts.control_panel.utils import get_user_account_id
from recidiviz.justice_counts.exceptions import JusticeCountsPermissionError
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.persistence.database.schema.justice_counts.schema import UserAccount
from recidiviz.utils.types import assert_type


def get_api_blueprint(
    auth_decorator: Callable, secret_key: Optional[str] = None
) -> Blueprint:
    """Api endpoints for Justice Counts control panel and admin panel"""
    api_blueprint = Blueprint("api", __name__)

    @api_blueprint.route("/init")
    @auth_decorator
    def init() -> Response:
        if not secret_key:
            return make_response("Unable to find secret key", 500)

        return jsonify({"csrf": generate_csrf(secret_key)})

    @api_blueprint.route("/users", methods=["POST"])
    @auth_decorator
    def create_or_update_user() -> Response:
        """Creates a new user in the JC DB"""
        request_json = assert_type(request.json, dict)
        email_address = request_json["email_address"]
        name = request_json.get("name")
        auth0_user_id = request_json.get("auth0_user_id")
        agency_ids = request_json.get("agency_ids")
        UserAccountInterface.create_or_update_user(
            session=current_session,
            email_address=email_address,
            name=name,
            auth0_user_id=auth0_user_id,
            agency_ids=agency_ids,
        )

        updated_user: UserAccount = UserAccountInterface.get_user_by_email_address(
            session=current_session, email_address=email_address
        )
        permissions = g.user_context.permissions if "user_context" in g else None
        return make_response(updated_user.to_json(permissions=permissions), 200)

    @api_blueprint.route("/reports", methods=["GET"])
    @auth_decorator
    def get_reports() -> Response:
        request_dict = assert_type(request.args.to_dict(), dict)
        agency_id = int(assert_type(request_dict.get("agency_id"), str))
        user_id = get_user_account_id(request_dict=request_dict)

        reports = ReportInterface.get_reports_by_agency_id(
            session=current_session,
            agency_id=agency_id,
            user_account_id=user_id,
        )
        report_json = jsonify(
            [
                ReportInterface.to_json_response(session=current_session, report=r)
                for r in reports
            ]
        )
        return make_response(report_json, 200)

    @api_blueprint.route("/reports", methods=["POST"])
    @auth_decorator
    def create_report() -> Response:
        request_json = assert_type(request.json, dict)
        agency_id = assert_type(request_json.get("agency_id"), int)
        month = assert_type(request_json.get("month"), int)
        year = assert_type(request_json.get("year"), int)
        frequency = assert_type(request_json.get("frequency"), str)
        user_id = get_user_account_id(request_dict=request_json)
        permissions = g.user_context.permissions
        if (
            not permissions
            or ControlPanelPermission.CREATE_REPORT.value not in permissions
        ):
            raise JusticeCountsPermissionError(
                code="justice_counts_create_report_permission",
                description=f"User (user_id: {user_id}) does not have permission to create reports for current agency (agency_id: {agency_id}).",
            )

        report = ReportInterface.create_report(
            session=current_session,
            agency_id=agency_id,
            user_account_id=user_id,
            month=month,
            year=year,
            frequency=frequency,
        )
        report_response = ReportInterface.to_json_response(
            session=current_session, report=report
        )
        return make_response(report_response, 200)

    return api_blueprint
