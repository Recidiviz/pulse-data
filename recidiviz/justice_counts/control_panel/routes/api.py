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
from collections import defaultdict
from http import HTTPStatus
from itertools import groupby
from typing import Callable, DefaultDict, List, Optional

import pandas as pd
from flask import Blueprint, Response, jsonify, make_response, request, send_file
from flask_sqlalchemy_session import current_session
from flask_wtf.csrf import generate_csrf
from psycopg2.errors import UniqueViolation  # pylint: disable=no-name-in-module
from sqlalchemy.exc import IntegrityError
from werkzeug.wrappers import response

from recidiviz.auth.auth0_client import Auth0Client
from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.agency_setting import AgencySettingInterface
from recidiviz.justice_counts.agency_user_account_association import (
    AgencyUserAccountAssociationInterface,
)
from recidiviz.justice_counts.control_panel.utils import (
    get_auth0_user_id,
    raise_if_user_is_not_in_agency,
    raise_if_user_is_wrong_role,
)
from recidiviz.justice_counts.datapoint import DatapointInterface
from recidiviz.justice_counts.exceptions import JusticeCountsServerError
from recidiviz.justice_counts.metrics.metric_interface import (
    DatapointGetRequestEntryPoint,
    MetricInterface,
)
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.justice_counts.spreadsheet import SpreadsheetInterface
from recidiviz.justice_counts.types import DatapointJson
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.utils.environment import in_development
from recidiviz.utils.flask_exception import FlaskException
from recidiviz.utils.types import assert_type

ALLOWED_EXTENSIONS = ["xlsx", "xls"]


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

    ### Agencies ###

    @api_blueprint.route("/agencies/<agency_id>", methods=["PATCH"])
    @auth_decorator
    def update_agency(agency_id: int) -> Response:
        """
        Currently, the supported updates are:
            - Changing the set of systems associated with the agency
            - Updating AgencySettings
        """
        try:
            request_json = assert_type(request.json, dict)
            user = UserAccountInterface.get_user_by_auth0_user_id(
                session=current_session,
                auth0_user_id=get_auth0_user_id(request_dict=request_json),
            )

            raise_if_user_is_wrong_role(
                user=user,
                agency_id=agency_id,
                allowed_roles={
                    schema.UserAccountRole.AGENCY_ADMIN,
                    schema.UserAccountRole.JUSTICE_COUNTS_ADMIN,
                },
            )

            systems = request_json.get("systems")
            if systems is not None:
                AgencyInterface.update_agency_systems(
                    session=current_session,
                    agency_id=agency_id,
                    systems={schema.System[s] for s in systems},
                )

            settings = request_json.get("settings")
            if settings is not None:
                for setting in settings:
                    setting_type = setting["setting_type"]
                    setting_value = setting["value"]
                    AgencySettingInterface.create_or_update_agency_setting(
                        session=current_session,
                        agency_id=agency_id,
                        setting_type=schema.AgencySettingType(setting_type),
                        value=setting_value,
                    )

            current_session.commit()
            return jsonify({"status": "ok", "status_code": HTTPStatus.OK})
        except Exception as e:
            raise _get_error(error=e) from e

    @api_blueprint.route("/agencies/<agency_id>", methods=["GET"])
    @auth_decorator
    def get_agency_settings(agency_id: int) -> Response:
        """
        This endpoint gets the settings for a Agency record.
        """
        try:
            request_json = assert_type(request.values, dict)
            user = UserAccountInterface.get_user_by_auth0_user_id(
                session=current_session,
                auth0_user_id=get_auth0_user_id(request_dict=request_json),
            )

            raise_if_user_is_not_in_agency(user=user, agency_id=agency_id)

            agency_settings = AgencySettingInterface.get_agency_settings(
                session=current_session,
                agency_id=agency_id,
            )
            return jsonify(
                {"settings": [setting.to_json() for setting in agency_settings]}
            )
        except Exception as e:
            raise _get_error(error=e) from e

    ### Users ###

    @api_blueprint.route("/agencies/<agency_id>/users", methods=["POST"])
    @auth_decorator
    def invite_user_to_agency(agency_id: int) -> Response:
        """
        This endpoint creates a user in Auth0 and in the Justice Counts Database.
        We save the information in both Justice Counts DB and Auth0 because the Justice Counts
        Database, especially the UserAccount table acts as a cache for user metadata. The
        new user will receive a email asking them to authenticate their account,
        """
        try:
            if auth0_client is None:
                return make_response(
                    "auth0_client could not be initialized. Environment is not development or gcp.",
                    500,
                )

            request_json = assert_type(request.json, dict)
            user = UserAccountInterface.get_user_by_auth0_user_id(
                session=current_session,
                auth0_user_id=get_auth0_user_id(request_dict=request_json),
            )

            raise_if_user_is_wrong_role(
                user=user,
                agency_id=agency_id,
                allowed_roles={
                    schema.UserAccountRole.JUSTICE_COUNTS_ADMIN,
                    schema.UserAccountRole.AGENCY_ADMIN,
                },
            )

            request_json = assert_type(request.json, dict)
            invite_name = request_json.get("invite_name")
            invite_email = request_json.get("invite_email")

            AgencyUserAccountAssociationInterface.invite_user_to_agency(
                name=assert_type(invite_name, str),
                email=assert_type(invite_email, str),
                agency_id=int(agency_id),
                auth0_client=auth0_client,
                session=current_session,
            )

            current_session.commit()
            return jsonify({"status": "ok", "status_code": HTTPStatus.OK})
        except Exception as e:
            raise _get_error(error=e) from e

    @api_blueprint.route("/agencies/<agency_id>/users", methods=["DELETE"])
    @auth_decorator
    def remove_user_from_agency(agency_id: int) -> Response:
        """
        This endpoint removes a user from an agency in Auth0 and in the Justice Counts Database.
        """
        try:
            if auth0_client is None:
                return make_response(
                    "auth0_client could not be initialized. Environment is not development or gcp.",
                    500,
                )

            request_json = assert_type(request.json, dict)
            user = UserAccountInterface.get_user_by_auth0_user_id(
                session=current_session,
                auth0_user_id=get_auth0_user_id(request_dict=request_json),
            )

            raise_if_user_is_wrong_role(
                user=user,
                agency_id=agency_id,
                allowed_roles={
                    schema.UserAccountRole.JUSTICE_COUNTS_ADMIN,
                    schema.UserAccountRole.AGENCY_ADMIN,
                },
            )

            email = request_json.get("email")
            if email is None:
                return make_response(
                    "no email was provided in the request body.",
                    500,
                )
            AgencyUserAccountAssociationInterface.remove_user_from_agency(
                email=email,
                agency_id=int(agency_id),
                auth0_client=auth0_client,
                session=current_session,
            )

            current_session.commit()
            return jsonify({"status": "ok", "status_code": HTTPStatus.OK})
        except Exception as e:
            raise _get_error(error=e) from e

    @api_blueprint.route("/agencies/<agency_id>/users", methods=["PATCH"])
    @auth_decorator
    def update_user_role(agency_id: int) -> Response:
        """
        This endpoint updates a user's role in an agency in Auth0.
        """
        try:
            request_json = assert_type(request.json, dict)
            user = UserAccountInterface.get_user_by_auth0_user_id(
                session=current_session,
                auth0_user_id=get_auth0_user_id(request_dict=request_json),
            )

            raise_if_user_is_wrong_role(
                user=user,
                agency_id=agency_id,
                allowed_roles={
                    schema.UserAccountRole.JUSTICE_COUNTS_ADMIN,
                    schema.UserAccountRole.AGENCY_ADMIN,
                },
            )

            role = request_json.get("role")
            email = request_json.get("email")
            if role is None:
                return make_response(
                    "no role was provided in the request body.",
                    500,
                )

            users = UserAccountInterface.get_users_by_email(
                session=current_session, emails={assert_type(email, str)}
            )
            if len(users) != 1:
                return make_response(
                    "Multiple users were found with the same email.",
                    500,
                )
            agency = AgencyInterface.get_agency_by_id(
                session=current_session, agency_id=int(agency_id)
            )

            AgencyUserAccountAssociationInterface.update_user_role(
                role=role,
                user=users[0],
                agency=agency,
                session=current_session,
            )

            current_session.commit()
            return jsonify({"status": "ok", "status_code": HTTPStatus.OK})
        except Exception as e:
            raise _get_error(error=e) from e

    @api_blueprint.route("/users", methods=["PATCH"])
    @auth_decorator
    def update_user_in_auth0() -> Response:
        """
        This method updates a user's name and email in Auth0 and in the Justice Counts Database.
        We save the information in both Justice Counts DB and Auth0 because the Justice Counts
        Database, especially the UserAccount table acts as a cache for user metadata.
        """
        try:
            if auth0_client is None:
                return make_response(
                    "auth0_client could not be initialized. Environment is not development or gcp.",
                    500,
                )

            request_json = assert_type(request.json, dict)
            auth0_user_id = get_auth0_user_id(request_dict=request_json)
            name = request_json.get("name")
            email = request_json.get("email")
            onboarding_topics_completed = request_json.get(
                "onboarding_topics_completed"
            )

            if name is not None or email is not None:
                auth0_client.update_user(
                    user_id=auth0_user_id,
                    name=name,
                    email=email,
                    email_verified=email is None,
                )

            if name is not None:
                UserAccountInterface.create_or_update_user(
                    session=current_session,
                    name=name,
                    auth0_user_id=auth0_user_id,
                    email=email,
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

            current_session.commit()
            return jsonify({"status": "ok", "status_code": HTTPStatus.OK})
        except Exception as e:
            raise _get_error(error=e) from e

    @api_blueprint.route("/users", methods=["PUT"])
    @auth_decorator
    def create_user_if_necessary() -> Response:
        """
        Returns user agencies and permissions. If email_verified is passed in,
        we will update the user's invitation_status.
        """
        try:
            request_dict = assert_type(request.json, dict)
            auth0_user_id = get_auth0_user_id(request_dict)
            email = request_dict.get("email")
            is_email_verified = request_dict.get("email_verified")

            user = UserAccountInterface.create_or_update_user(
                session=current_session,
                name=request_dict.get("name"),
                auth0_user_id=auth0_user_id,
                email=email,
            )

            if in_development():
                # When running locally, our AgencyUserAccountAssociation table
                # won't be up-to-date (it's hard to do this via fixtures),
                # so just give access to all agencies.
                agency_ids = AgencyInterface.get_agency_ids(session=current_session)
            else:
                agency_ids = [assoc.agency_id for assoc in user.agency_assocs]

            agencies = AgencyInterface.get_agencies_by_id(
                session=current_session, agency_ids=agency_ids
            )

            UserAccountInterface.add_or_update_user_agency_association(
                session=current_session,
                user=user,
                agencies=agencies,
                invitation_status=schema.UserAccountInvitationStatus.ACCEPTED
                if is_email_verified is True
                else None,
            )

            agency_json = [agency.to_json() for agency in agencies or []]
            current_session.commit()
            return jsonify(
                {
                    "id": user.id,
                    "agencies": agency_json,
                    "permissions": [],
                }
            )
        except Exception as e:
            raise _get_error(error=e) from e

    ### Reports Overview ###

    @api_blueprint.route("/agencies/<agency_id>/reports", methods=["GET"])
    @auth_decorator
    def get_reports_by_agency_id(agency_id: int) -> Response:
        """Gets a list of reports for the specified agency id.
        Used for rendering the Reports Overview page.
        """
        try:
            request_json = assert_type(request.values, dict)
            user = UserAccountInterface.get_user_by_auth0_user_id(
                session=current_session,
                auth0_user_id=get_auth0_user_id(request_dict=request_json),
            )

            raise_if_user_is_not_in_agency(user=user, agency_id=agency_id)

            reports = ReportInterface.get_reports_by_agency_id(
                session=current_session, agency_id=agency_id
            )
            editor_id_to_json = (
                AgencyUserAccountAssociationInterface.get_editor_id_to_json(
                    session=current_session, reports=reports
                )
            )
            report_json = [
                ReportInterface.to_json_response(
                    report=r,
                    editor_id_to_json=editor_id_to_json,
                )
                for r in reports
            ]

            return jsonify(report_json)
        except Exception as e:
            raise _get_error(error=e) from e

    ### Individual Reports ###

    @api_blueprint.route("/reports/<report_id>", methods=["GET"])
    @auth_decorator
    def get_report_by_id(report_id: int) -> Response:
        """Get the details for a specified report. Used for rendering
        the Data Entry page.
        """
        try:
            request_json = assert_type(request.values, dict)
            user = UserAccountInterface.get_user_by_auth0_user_id(
                session=current_session,
                auth0_user_id=get_auth0_user_id(request_dict=request_json),
            )
            report = ReportInterface.get_report_by_id(
                session=current_session, report_id=int(report_id)
            )

            raise_if_user_is_not_in_agency(user=user, agency_id=report.source_id)

            report_metrics = ReportInterface.get_metrics_by_report(
                report=report, session=current_session
            )
            editor_id_to_json = (
                AgencyUserAccountAssociationInterface.get_editor_id_to_json(
                    session=current_session, reports=[report]
                )
            )

            report_definition_json = ReportInterface.to_json_response(
                report=report,
                editor_id_to_json=editor_id_to_json,
            )
            metrics_json = [
                report_metric.to_json(
                    entry_point=DatapointGetRequestEntryPoint.REPORT_PAGE,
                )
                for report_metric in report_metrics
            ]
            report_definition_json["metrics"] = metrics_json

            return jsonify(report_definition_json)
        except Exception as e:
            raise _get_error(error=e) from e

    @api_blueprint.route("/reports/<report_id>", methods=["PATCH"])
    @auth_decorator
    def update_report(report_id: str) -> Response:
        """Update a report's metadata and/or the metrics on the report.
        Used by the Data Entry page.
        """
        try:
            request_json = assert_type(request.json, dict)
            user = UserAccountInterface.get_user_by_auth0_user_id(
                session=current_session,
                auth0_user_id=get_auth0_user_id(request_dict=request_json),
            )
            report = ReportInterface.get_report_by_id(
                session=current_session, report_id=int(report_id)
            )

            raise_if_user_is_not_in_agency(user=user, agency_id=report.source_id)

            ReportInterface.update_report_metadata(
                report=report,
                editor_id=user.id,
                status=request_json["status"],
            )

            existing_datapoints_dict = ReportInterface.get_existing_datapoints_dict(
                reports=[report]
            )

            for metric_json in request_json.get("metrics", []):
                report_metric = MetricInterface.from_json(
                    json=metric_json,
                    entry_point=DatapointGetRequestEntryPoint.REPORT_PAGE,
                )
                ReportInterface.add_or_update_metric(
                    session=current_session,
                    report=report,
                    report_metric=report_metric,
                    user_account=user,
                    existing_datapoints_dict=existing_datapoints_dict,
                )

            editor_id_to_json = (
                AgencyUserAccountAssociationInterface.get_editor_id_to_json(
                    session=current_session, reports=[report]
                )
            )
            report_json = ReportInterface.to_json_response(
                report=report,
                editor_id_to_json=editor_id_to_json,
            )

            current_session.commit()
            return jsonify(report_json)
        except Exception as e:
            raise _get_error(error=e) from e

    @api_blueprint.route("/reports", methods=["POST"])
    @auth_decorator
    def create_report() -> Response:
        """Create a new report.
        Used by the Reports Overview page.
        """
        try:
            request_json = assert_type(request.json, dict)
            agency_id = assert_type(request_json.get("agency_id"), int)
            user = UserAccountInterface.get_user_by_auth0_user_id(
                session=current_session,
                auth0_user_id=get_auth0_user_id(request_dict=request_json),
            )

            raise_if_user_is_wrong_role(
                user=user,
                agency_id=agency_id,
                allowed_roles={
                    schema.UserAccountRole.AGENCY_ADMIN,
                    schema.UserAccountRole.JUSTICE_COUNTS_ADMIN,
                },
            )

            month = assert_type(request_json.get("month"), int)
            year = assert_type(request_json.get("year"), int)
            frequency = assert_type(request_json.get("frequency"), str)

            try:
                report = ReportInterface.create_report(
                    session=current_session,
                    agency_id=agency_id,
                    user_account_id=user.id,
                    month=month,
                    year=year,
                    frequency=frequency,
                )
                current_session.commit()
            except IntegrityError as e:
                if isinstance(e.orig, UniqueViolation):
                    raise JusticeCountsServerError(
                        code="justice_counts_create_report_uniqueness",
                        description="A report of that date range has already been created.",
                    ) from e
                raise e

            assocs = [a for a in user.agency_assocs if a.agency_id == agency_id]
            editor_id_to_json = {
                user.id: {
                    "name": user.name,
                    "role": assocs[0].role.value
                    if assocs[0].role is not None
                    else None,
                }
            }
            report_response = ReportInterface.to_json_response(
                report=report,
                editor_id_to_json=editor_id_to_json,
            )

            return jsonify(report_response)
        except Exception as e:
            raise _get_error(error=e) from e

    @api_blueprint.route("/reports", methods=["DELETE"])
    @auth_decorator
    def delete_reports() -> Response:
        """Delete a report.
        Used by the Reports Overview page.
        """
        try:
            request_json = assert_type(request.json, dict)
            agency_id = assert_type(request_json.get("agency_id"), int)
            user = UserAccountInterface.get_user_by_auth0_user_id(
                session=current_session,
                auth0_user_id=get_auth0_user_id(request_dict=request_json),
            )

            raise_if_user_is_wrong_role(
                user=user,
                agency_id=agency_id,
                allowed_roles={schema.UserAccountRole.JUSTICE_COUNTS_ADMIN},
            )

            report_ids = request_json.get("report_ids")
            if report_ids is None or len(report_ids) == 0:
                raise JusticeCountsServerError(
                    code="justice_counts_bad_request",
                    description="Empty list of report_ids passed to delete reports endpoint.",
                )

            report_ids = map(int, report_ids)
            ReportInterface.delete_reports_by_id(current_session, report_ids=report_ids)

            current_session.commit()
            return jsonify({"status": "ok", "status_code": HTTPStatus.OK})
        except Exception as e:
            raise _get_error(error=e) from e

    ### Metric Settings ###

    @api_blueprint.route("/agencies/<agency_id>/metrics", methods=["GET"])
    @auth_decorator
    def get_agency_metric_settings(agency_id: int) -> Response:
        """Get the contexts and configuration for each of an agency's metrics.
        Used by the Metric Configuration page in Settings.
        """
        try:
            request_json = assert_type(request.values, dict)
            user = UserAccountInterface.get_user_by_auth0_user_id(
                session=current_session,
                auth0_user_id=get_auth0_user_id(request_dict=request_json),
            )

            raise_if_user_is_not_in_agency(user=user, agency_id=int(agency_id))

            agency = AgencyInterface.get_agency_by_id(
                session=current_session, agency_id=agency_id
            )
            metrics = DatapointInterface.get_metric_settings_by_agency(
                session=current_session, agency=agency
            )
            metrics_json = [
                metric.to_json(entry_point=DatapointGetRequestEntryPoint.METRICS_TAB)
                for metric in metrics
            ]
            return jsonify(metrics_json)

        except Exception as e:
            raise _get_error(error=e) from e

    @api_blueprint.route("/agencies/<agency_id>/metrics", methods=["PUT"])
    @auth_decorator
    def update_metric_settings(agency_id: int) -> Response:
        """Update the contexts and configuration for an agency's metrics.
        Used by the Metric Configuration page in Settings.
        """
        try:
            request_json = assert_type(request.json, dict)
            user = UserAccountInterface.get_user_by_auth0_user_id(
                session=current_session,
                auth0_user_id=get_auth0_user_id(request_dict=request_json),
            )

            raise_if_user_is_not_in_agency(user=user, agency_id=int(agency_id))

            agency = AgencyInterface.get_agency_by_id(
                session=current_session, agency_id=agency_id
            )
            for metric_json in request_json.get("metrics", []):
                agency_metric = MetricInterface.from_json(
                    json=metric_json,
                    entry_point=DatapointGetRequestEntryPoint.METRICS_TAB,
                )
                DatapointInterface.add_or_update_agency_datapoints(
                    session=current_session,
                    agency_metric=agency_metric,
                    agency=agency,
                    user_account=user,
                )

            current_session.commit()
            return jsonify({"status": "ok", "status_code": HTTPStatus.OK})
        except Exception as e:
            raise _get_error(error=e) from e

    ### Bulk Upload ###

    @api_blueprint.route("agencies/<agency_id>/spreadsheets", methods=["GET"])
    @auth_decorator
    def get_spreadsheets(agency_id: int) -> Response:
        """Get a list of spreadsheets uploaded by an agency.
        Used for rendering the Uploaded Files page.
        """
        request_json = assert_type(request.values, dict)
        user = UserAccountInterface.get_user_by_auth0_user_id(
            session=current_session,
            auth0_user_id=get_auth0_user_id(request_dict=request_json),
        )

        raise_if_user_is_not_in_agency(user=user, agency_id=int(agency_id))

        try:
            spreadsheets = SpreadsheetInterface.get_agency_spreadsheets(
                agency_id=agency_id, session=current_session
            )
            return jsonify(
                SpreadsheetInterface.get_spreadsheets_json(
                    spreadsheets=spreadsheets,
                    session=current_session,
                )
            )
        except Exception as e:
            raise _get_error(error=e) from e

    @api_blueprint.route("/spreadsheets/<spreadsheet_id>", methods=["GET"])
    @auth_decorator
    def download_spreadsheet(
        spreadsheet_id: str,
    ) -> response.Response:
        """Download a spreadsheet from GCP and return the file.
        Used by the Uploaded Files page.
        """
        request_json = assert_type(request.values, dict)
        user = UserAccountInterface.get_user_by_auth0_user_id(
            session=current_session,
            auth0_user_id=get_auth0_user_id(request_dict=request_json),
        )
        agency_ids = [assoc.agency_id for assoc in user.agency_assocs]

        file = SpreadsheetInterface.download_spreadsheet(
            spreadsheet_id=int(spreadsheet_id),
            agency_ids=agency_ids,
            session=current_session,
        )

        return send_file(path_or_file=file.local_file_path, as_attachment=True)

    @api_blueprint.route("/spreadsheets", methods=["POST"])
    @auth_decorator
    def upload_spreadsheet() -> Response:
        """Upload a spreadsheet for an agency.
        Used by the Bulk Upload flow.
        """
        data = assert_type(request.form, dict)
        agency_id = int(data["agency_id"])
        system = data["system"]
        auth0_user_id = get_auth0_user_id(request_dict=data)
        file = request.files["file"]
        ingest_on_upload = data.get("ingest_on_upload")
        if file is None:
            raise JusticeCountsServerError(
                "no_file_on_upload", "No file was sent for upload."
            )
        if not allowed_file(file.filename):
            raise JusticeCountsServerError(
                "file_type_error", "Invalid file type: All files must be of type .xlsx."
            )
        # Upload spreadsheet to GCS
        spreadsheet = SpreadsheetInterface.upload_spreadsheet(
            session=current_session,
            file_storage=file,
            agency_id=agency_id,
            auth0_user_id=auth0_user_id,
            system=system,
        )
        if ingest_on_upload == "True":
            agency = AgencyInterface.get_agency_by_id(
                session=current_session, agency_id=agency_id
            )
            agency_datapoints = DatapointInterface.get_agency_datapoints(
                session=current_session, agency_id=agency_id
            )
            agency_datapoints_sorted_by_metric_key = sorted(
                agency_datapoints, key=lambda d: d.metric_definition_key
            )
            metric_key_to_agency_datapoints = {
                k: list(v)
                for k, v in groupby(
                    agency_datapoints_sorted_by_metric_key,
                    key=lambda d: d.metric_definition_key,
                )
            }
            metric_definitions = MetricInterface.get_metric_definitions_for_systems(
                systems={
                    schema.System[system]
                    for system in agency.systems or []
                    if schema.System[system] in schema.System.supervision_subsystems()
                }
                if system == "SUPERVISION"
                # Only send over metric definitions for the current system unless
                # the agency is uploading for supervision, which sheets contain
                # data for many supervision systems such as POST_RELEASE, PAROLE,
                # and PROBATION
                else {schema.System[system]},
            )

            (
                metric_key_to_datapoint_jsons,
                metric_key_to_errors,
            ) = SpreadsheetInterface.ingest_spreadsheet(
                session=current_session,
                spreadsheet=spreadsheet,
                auth0_user_id=auth0_user_id,
                xls=pd.ExcelFile(file),
                agency_id=agency_id,
                metric_key_to_agency_datapoints=metric_key_to_agency_datapoints,
                metric_definitions=metric_definitions,
            )

            current_session.commit()

            return jsonify(
                SpreadsheetInterface.get_ingest_spreadsheet_json(
                    metric_key_to_errors=metric_key_to_errors,
                    metric_key_to_datapoint_jsons=metric_key_to_datapoint_jsons,
                    metric_definitions=metric_definitions,
                )
            )

        current_session.commit()
        return jsonify(
            SpreadsheetInterface.get_spreadsheets_json(
                spreadsheets=[spreadsheet],
                session=current_session,
            ).pop()
        )

    @api_blueprint.route("/spreadsheets/<spreadsheet_id>", methods=["PATCH"])
    @auth_decorator
    def update_spreadsheet(spreadsheet_id: str) -> Response:
        """Update a spreadsheet's metadata.
        Used by the Uploaded Files page.
        """
        try:
            request_json = assert_type(request.json, dict)
            auth0_user_id = get_auth0_user_id(request_dict=request_json)
            user = UserAccountInterface.get_user_by_auth0_user_id(
                session=current_session,
                auth0_user_id=auth0_user_id,
            )
            spreadsheet = SpreadsheetInterface.get_spreadsheet_by_id(
                session=current_session, spreadsheet_id=int(spreadsheet_id)
            )

            raise_if_user_is_wrong_role(
                user=user,
                agency_id=spreadsheet.agency_id,
                allowed_roles={schema.UserAccountRole.JUSTICE_COUNTS_ADMIN},
            )

            status = assert_type(request_json.get("status"), str)
            spreadsheet = SpreadsheetInterface.update_spreadsheet(
                spreadsheet=spreadsheet,
                status=status,
                auth0_user_id=auth0_user_id,
            )

            current_session.commit()
            return jsonify(
                SpreadsheetInterface.get_spreadsheets_json(
                    spreadsheets=[spreadsheet],
                    session=current_session,
                ).pop()
            )
        except Exception as e:
            raise _get_error(error=e) from e

    @api_blueprint.route("/spreadsheets/<spreadsheet_id>", methods=["DELETE"])
    @auth_decorator
    def delete_spreadsheet(spreadsheet_id: str) -> Response:
        """Delete a spreadsheet.
        Used by the Uploaded Files page.
        """
        try:
            SpreadsheetInterface.delete_spreadsheet(
                session=current_session,
                spreadsheet_id=int(spreadsheet_id),
            )

            current_session.commit()
            return jsonify({"status": "ok", "status_code": HTTPStatus.OK})
        except Exception as e:
            raise _get_error(error=e) from e

    def allowed_file(filename: Optional[str] = None) -> bool:
        return (
            filename is not None
            and "." in filename
            and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS
        )

    ### Dashboards ###

    def get_agency_datapoints(agency_id: int) -> Response:
        reports = ReportInterface.get_reports_by_agency_id(
            session=current_session,
            agency_id=agency_id,
            # we are only fetching reports here to get the list of report
            # ids in an agency, so no need to fetch datapoints here
            include_datapoints=False,
        )
        report_id_to_status = {report.id: report.status for report in reports}
        report_id_to_frequency = {
            report.id: ReportInterface.get_reporting_frequency(report)
            for report in reports
        }

        # fetch non-context datapoints
        datapoints = DatapointInterface.get_datapoints_by_report_ids(
            session=current_session,
            report_ids=list(report_id_to_status.keys()),
            include_contexts=False,
        )

        datapoints_json = [
            DatapointInterface.to_json_response(
                datapoint=d,
                is_published=report_id_to_status[d.report_id]
                == schema.ReportStatus.PUBLISHED,
                frequency=report_id_to_frequency[d.report_id],
            )
            for d in datapoints
        ]

        agency = AgencyInterface.get_agency_by_id(
            session=current_session, agency_id=agency_id
        )

        metric_definitions = MetricInterface.get_metric_definitions_for_systems(
            systems={schema.System[system] for system in agency.systems or []},
        )
        dimension_names_by_metric_and_disaggregation = {}
        for metric_definition in metric_definitions:
            disaggregations = metric_definition.aggregated_dimensions or []
            disaggregation_name_to_dimension_names = {
                disaggregation.dimension.human_readable_name(): [
                    # TODO(#14940) Have DimensionBase extend from Enum rather than having
                    # classes that inherit it must also inherit from Enum
                    i.value
                    for i in disaggregation.dimension  # type: ignore[attr-defined]
                ]
                for disaggregation in disaggregations
            }
            dimension_names_by_metric_and_disaggregation[
                metric_definition.key
            ] = disaggregation_name_to_dimension_names

        return jsonify(
            {
                "datapoints": datapoints_json,
                "dimension_names_by_metric_and_disaggregation": dimension_names_by_metric_and_disaggregation,
            }
        )

    @api_blueprint.route("agencies/<agency_id>/datapoints", methods=["GET"])
    @auth_decorator
    def get_datapoints_by_agency_id(agency_id: int) -> Response:
        try:
            request_json = assert_type(request.values, dict)
            user = UserAccountInterface.get_user_by_auth0_user_id(
                session=current_session,
                auth0_user_id=get_auth0_user_id(request_dict=request_json),
            )

            raise_if_user_is_not_in_agency(user=user, agency_id=int(agency_id))

            return get_agency_datapoints(agency_id=agency_id)
        except Exception as e:
            raise _get_error(error=e) from e

    @api_blueprint.route("/agencies/<agency_id>/published_data", methods=["GET"])
    def get_agency_published_data(agency_id: int) -> Response:
        try:
            agency = AgencyInterface.get_agency_by_id(
                session=current_session, agency_id=agency_id
            )
            reports = ReportInterface.get_reports_by_agency_id(
                session=current_session,
                agency_id=agency_id,
                # we are only fetching reports here to get the list of report
                # ids in an agency, so no need to fetch datapoints here
                include_datapoints=False,
                published_only=True,
            )
            report_id_to_status = {report.id: report.status for report in reports}
            report_id_to_frequency = {
                report.id: ReportInterface.get_reporting_frequency(report)
                for report in reports
            }
            # fetch non-context datapoints
            datapoints = DatapointInterface.get_datapoints_by_report_ids(
                session=current_session,
                report_ids=list(report_id_to_status.keys()),
                include_contexts=False,
            )

            # Serialize datapoints into json format.
            # group aggregate datapoints by metric.
            metric_key_to_aggregate_datapoints_json: DefaultDict[
                str, List[DatapointJson]
            ] = defaultdict(list)
            # group breakdown datapoints by metric, disaggregation, and dimension.
            metric_key_to_dimension_id_to_dimension_member_to_datapoints_json: DefaultDict[
                str, DefaultDict[str, DefaultDict[str, List[DatapointJson]]]
            ] = defaultdict(
                lambda: defaultdict(lambda: defaultdict(list))
            )
            for datapoint in datapoints:
                datapoint_json = DatapointInterface.to_json_response(
                    datapoint=datapoint,
                    is_published=True,
                    frequency=report_id_to_frequency[datapoint.report_id],
                )
                (
                    dimension_id,
                    dimension_member,
                ) = datapoint.get_dimension_id_and_member()
                if dimension_id is not None and dimension_member is not None:
                    metric_key_to_dimension_id_to_dimension_member_to_datapoints_json[
                        datapoint.metric_definition_key
                    ][dimension_id][dimension_member].append(datapoint_json)
                else:
                    metric_key_to_aggregate_datapoints_json[
                        datapoint.metric_definition_key
                    ].append(datapoint_json)

            metrics = DatapointInterface.get_metric_settings_by_agency(
                session=current_session, agency=agency
            )
            metrics_json = [
                metric.to_json(
                    entry_point=DatapointGetRequestEntryPoint.METRICS_TAB,
                    aggregate_datapoints_json=metric_key_to_aggregate_datapoints_json.get(
                        metric.key
                    ),
                    dimension_id_to_dimension_member_to_datapoints_json=metric_key_to_dimension_id_to_dimension_member_to_datapoints_json.get(
                        metric.key
                    ),
                )
                for metric in metrics
            ]

            return jsonify(
                {
                    "agency": agency.to_public_json(),
                    "metrics": metrics_json,
                }
            )

        except Exception as e:
            raise _get_error(error=e) from e

    return api_blueprint


def _get_error(error: Exception) -> FlaskException:
    # Always log the error so it goes to Sentry and GCP Logs
    logging.exception(error)

    # If we already raised a specific FlaskException, re-raise
    if isinstance(error, FlaskException):
        return error

    # Else, wrap in FlaskException and return to frontend
    return JusticeCountsServerError(
        code="server_error",
        description="A server error occurred. See the logs for more information.",
    )
