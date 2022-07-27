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
import logging
from http import HTTPStatus
from typing import Callable, Optional

from flask import Blueprint, Response, g, jsonify, make_response, request
from flask_sqlalchemy_session import current_session
from flask_wtf.csrf import generate_csrf
from psycopg2.errors import UniqueViolation  # pylint: disable=no-name-in-module
from sqlalchemy.exc import IntegrityError

from recidiviz.auth.auth0_client import Auth0Client
from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.control_panel.constants import ControlPanelPermission
from recidiviz.justice_counts.control_panel.utils import (
    get_auth0_user_id,
    get_user_account_id,
    raise_if_user_is_unauthorized,
)
from recidiviz.justice_counts.datapoint import DatapointInterface
from recidiviz.justice_counts.exceptions import (
    JusticeCountsBadRequestError,
    JusticeCountsPermissionError,
    JusticeCountsServerError,
)
from recidiviz.justice_counts.metrics.metric_interface import (
    DatapointGetRequestEntryPoint,
    MetricInterface,
)
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.utils.flask_exception import FlaskException
from recidiviz.utils.types import assert_type


def get_api_blueprint(
    auth_decorator: Callable,
    secret_key: Optional[str] = None,
    auth0_client: Optional[Auth0Client] = None,
) -> Blueprint:
    """Api endpoints for Justice Counts control panel and admin panel"""
    api_blueprint = Blueprint("api", __name__)

    @api_blueprint.route("/init")
    @auth_decorator
    def init() -> Response:
        if not secret_key:
            return make_response("Unable to find secret key", 500)

        return jsonify({"csrf": generate_csrf(secret_key)})

    @api_blueprint.route("/users/update", methods=["POST"])
    @auth_decorator
    def update_user_email_and_name() -> Response:
        """
        This endpoint updates name and email in Auth0 and name in UserAccount table
        """
        try:
            request_json = assert_type(request.json, dict)
            auth0_user_id = get_auth0_user_id(request_dict=request_json)
            name = request_json.get("name")
            email = request_json.get("email")
            onboarding_topics_completed = request_json.get(
                "onboarding_topics_completed"
            )
            if auth0_client is None:
                return make_response(
                    "auth0_client could not be initialized. Environment is not development or gcp.",
                    500,
                )

            if name is not None or email is not None:
                auth0_client.update_user_name_and_email(
                    user_id=auth0_user_id,
                    name=name,
                    email=email,
                    email_verified=email is None,
                )

            if name is not None:
                UserAccountInterface.create_or_update_user(
                    session=current_session, name=name, auth0_user_id=auth0_user_id
                )

            if email is not None:
                auth0_client.send_verification_email(user_id=auth0_user_id)

            if onboarding_topics_completed is not None:
                auth0_client.update_user_app_metadata(
                    user_id=auth0_user_id,
                    app_metadata={
                        "onboarding_topics_completed": onboarding_topics_completed
                    },
                )

            return jsonify({"status": "ok", "status_code": HTTPStatus.OK})
        except Exception as e:
            raise _get_error(error=e) from e

    @api_blueprint.route("/users", methods=["POST"])
    @auth_decorator
    def create_user_if_necessary() -> Response:
        """Returns user agencies and permissions"""
        try:
            request_dict = assert_type(request.json, dict)
            auth0_user_id = get_auth0_user_id(request_dict)
            user_account = UserAccountInterface.create_or_update_user(
                session=current_session,
                name=request_dict.get("name"),
                auth0_user_id=auth0_user_id,
            )
            agency_ids = g.user_context.agency_ids if "user_context" in g else []
            agencies = AgencyInterface.get_agencies_by_id(
                session=current_session, agency_ids=agency_ids
            )
            permissions = g.user_context.permissions if "user_context" in g else []
            return jsonify(
                {
                    "id": user_account.id,
                    "agencies": [agency.to_json() for agency in agencies or []],
                    "permissions": permissions or [],
                }
            )
        except Exception as e:
            raise _get_error(error=e) from e

    @api_blueprint.route("/reports/<report_id>", methods=["GET"])
    @auth_decorator
    @raise_if_user_is_unauthorized
    def get_report_by_id(report_id: Optional[str] = None) -> Response:
        try:
            report_id_int = int(assert_type(report_id, str))
            report = ReportInterface.get_report_by_id(
                session=current_session, report_id=report_id_int
            )
            report_metrics = ReportInterface.get_metrics_by_report(
                report=report, session=current_session
            )

            report_definition_json = ReportInterface.to_json_response(
                session=current_session,
                report=report,
            )
            metrics_json = [report_metric.to_json() for report_metric in report_metrics]
            report_definition_json["metrics"] = metrics_json

            return jsonify(report_definition_json)
        except Exception as e:
            raise _get_error(error=e) from e

    @api_blueprint.route("/reports/<report_id>", methods=["POST"])
    @auth_decorator
    @raise_if_user_is_unauthorized
    def update_report(report_id: Optional[str] = None) -> Response:
        try:
            report_id_int = int(assert_type(report_id, str))
            request_dict = assert_type(request.json, dict)
            user_account_id = get_user_account_id(request_dict=request_dict)
            user_account = UserAccountInterface.get_user_by_id(
                session=current_session, user_account_id=user_account_id
            )
            report = ReportInterface.get_report_by_id(
                session=current_session,
                report_id=report_id_int,
            )
            ReportInterface.update_report_metadata(
                session=current_session,
                report=report,
                editor_id=user_account_id,
                status=request_dict["status"],
            )

            for metric_json in request_dict.get("metrics", []):
                report_metric = MetricInterface.from_json(
                    json=metric_json,
                    entry_point=DatapointGetRequestEntryPoint.REPORT_PAGE,
                )
                ReportInterface.add_or_update_metric(
                    session=current_session,
                    report=report,
                    report_metric=report_metric,
                    user_account=user_account,
                )
            return jsonify({"status": "ok", "status_code": HTTPStatus.OK})
        except Exception as e:
            raise _get_error(error=e) from e

    @api_blueprint.route("/reports", methods=["GET"])
    @auth_decorator
    @raise_if_user_is_unauthorized
    def get_reports_by_agency_id() -> Response:
        try:
            request_dict = assert_type(request.args.to_dict(), dict)
            agency_id = request_dict.get("agency_id")

            if agency_id is None:
                # If no agency_id is specified, pick one of the agencies
                # that the user belongs to as a default for the home page
                if len(g.user_context.agency_ids) == 0:
                    raise JusticeCountsPermissionError(
                        code="justice_counts_agency_permission",
                        description="User does not belong to any agencies.",
                    )
                agency_id = g.user_context.agency_ids[0]

            agency_id = int(agency_id)

            reports = ReportInterface.get_reports_by_agency_id(
                session=current_session, agency_id=agency_id
            )
            editor_ids_to_names = ReportInterface.get_editor_ids_to_names(
                session=current_session, reports=reports
            )
            report_json = [
                ReportInterface.to_json_response(
                    session=current_session,
                    report=r,
                    editor_ids_to_names=editor_ids_to_names,
                )
                for r in reports
            ]

            return jsonify(report_json)
        except Exception as e:
            raise _get_error(error=e) from e

    @api_blueprint.route("/reports", methods=["POST"])
    @auth_decorator
    def create_report() -> Response:
        try:
            request_json = assert_type(request.json, dict)
            agency_id = assert_type(request_json.get("agency_id"), int)
            month = assert_type(request_json.get("month"), int)
            year = assert_type(request_json.get("year"), int)
            frequency = assert_type(request_json.get("frequency"), str)
            user_id = get_user_account_id(request_dict=request_json)
            permissions = g.user_context.permissions
            if (
                not permissions
                or ControlPanelPermission.RECIDIVIZ_ADMIN.value not in permissions
            ):
                raise JusticeCountsPermissionError(
                    code="justice_counts_create_report_permission",
                    description=(
                        f"User {user_id} does not have permission to "
                        "create reports for agency {agency_id}."
                    ),
                )

            try:
                report = ReportInterface.create_report(
                    session=current_session,
                    agency_id=agency_id,
                    user_account_id=user_id,
                    month=month,
                    year=year,
                    frequency=frequency,
                )
            except IntegrityError as e:
                if isinstance(e.orig, UniqueViolation):
                    raise JusticeCountsBadRequestError(
                        code="justice_counts_create_report_uniqueness",
                        description="A report of that date range has already been created.",
                    ) from e
                raise e
            report_response = ReportInterface.to_json_response(
                session=current_session, report=report
            )
            return jsonify(report_response)
        except Exception as e:
            raise _get_error(error=e) from e

    @api_blueprint.route("/reports", methods=["DELETE"])
    @auth_decorator
    def delete_reports() -> Response:
        try:
            request_json = assert_type(request.json, dict)
            report_ids = request_json.get("report_ids")
            user_id = get_user_account_id(request_dict=request_json)
            permissions = g.user_context.permissions
            if (
                not permissions
                or ControlPanelPermission.RECIDIVIZ_ADMIN.value not in permissions
            ):
                raise JusticeCountsPermissionError(
                    code="justice_counts_delete_report_permission",
                    description=(
                        f"User {user_id} does not have permission to delete reports."
                    ),
                )

            if report_ids is None or len(report_ids) == 0:
                raise JusticeCountsBadRequestError(
                    code="justice_counts_bad_request",
                    description="Empty list of report_ids passed to delete reports endpoint.",
                )

            report_ids = map(int, report_ids)
            ReportInterface.delete_reports_by_id(current_session, report_ids=report_ids)
            return jsonify({"status": "ok", "status_code": HTTPStatus.OK})
        except Exception as e:
            raise _get_error(error=e) from e

    @api_blueprint.route("/metrics/update", methods=["POST"])
    @auth_decorator
    @raise_if_user_is_unauthorized
    def update_metric_settings() -> Response:
        try:
            request_json = assert_type(request.json, dict)
            agency_id = assert_type(request_json.get("agency_id"), int)
            agency = AgencyInterface.get_agency_by_id(
                session=current_session, agency_id=agency_id
            )
            for metric_json in request_json.get("metrics", []):
                agency_metric = MetricInterface.from_json(
                    json=metric_json,
                    entry_point=DatapointGetRequestEntryPoint.METRICS_TAB,
                )
                DatapointInterface.add_or_update_agency_datapoints(
                    session=current_session, agency_metric=agency_metric, agency=agency
                )

            return jsonify({"status": "ok", "status_code": HTTPStatus.OK})
        except Exception as e:
            raise _get_error(error=e) from e

    return api_blueprint


def _get_error(error: Exception) -> FlaskException:
    # If we already raised a specific FlaskException, re-raise
    if isinstance(error, FlaskException):
        return error

    # Else, if error is unexpected, first log the error, then wrap
    # in FlaskException and return to frontend
    logging.exception(error)
    return JusticeCountsServerError(
        code="server_error",
        description="A server error occurred. See the logs for more information.",
    )
