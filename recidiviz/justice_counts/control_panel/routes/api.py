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
from flask import Blueprint, Response, g, jsonify, make_response, request, send_file
from flask_sqlalchemy_session import current_session
from flask_wtf.csrf import generate_csrf
from psycopg2.errors import UniqueViolation  # pylint: disable=no-name-in-module
from sqlalchemy.exc import IntegrityError
from werkzeug.wrappers import response

from recidiviz.auth.auth0_client import Auth0Client
from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.agency_setting import (
    AgencySettingInterface,
    AgencySettingType,
)
from recidiviz.justice_counts.control_panel.constants import ControlPanelPermission
from recidiviz.justice_counts.control_panel.utils import (
    get_agency_ids_from_session,
    get_auth0_user_id,
    get_user_account_id,
    raise_if_user_is_unauthorized,
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
    @raise_if_user_is_unauthorized
    def update_agency(agency_id: int) -> Response:
        """
        Currently, the supported updates are:
            - Changing the set of systems associated with the agency
            - Adding a description of the agency's purpose and functions
        """
        try:
            request_json = assert_type(request.json, dict)
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
                        setting_type=AgencySettingType(setting_type),
                        value=setting_value,
                    )

            current_session.commit()
            return jsonify({"status": "ok", "status_code": HTTPStatus.OK})
        except Exception as e:
            raise _get_error(error=e) from e

    @api_blueprint.route("/agencies/<agency_id>", methods=["GET"])
    @auth_decorator
    @raise_if_user_is_unauthorized
    def get_agency_settings(agency_id: int) -> Response:
        """
        This endpoint gets the settings for a Agency record.
        """

        try:
            agency_settings = AgencySettingInterface.get_agency_settings(
                session=current_session,
                agency_id=agency_id,
            )
            # Need to add this line in order to avoid jsonify error
            agency_settings_unpacked = [
                {
                    "setting_type": setting.setting_type,
                    "value": setting.value,
                    "source_id": setting.source_id,
                }
                for setting in agency_settings
            ]
            return jsonify({"settings": agency_settings_unpacked})
        except Exception as e:
            raise _get_error(error=e) from e

    ### Users ###

    @api_blueprint.route("/users", methods=["PATCH"])
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
                auth0_client.update_user(
                    user_id=auth0_user_id,
                    name=name,
                    email=email,
                    email_verified=email is None,
                )
            agencies = AgencyInterface.get_agencies_by_id(
                session=current_session, agency_ids=get_agency_ids_from_session()
            )
            if name is not None:
                UserAccountInterface.create_or_update_user(
                    session=current_session,
                    name=name,
                    auth0_user_id=auth0_user_id,
                    agencies=agencies,
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
        """Returns user agencies and permissions"""
        try:
            request_dict = assert_type(request.json, dict)
            auth0_user_id = get_auth0_user_id(request_dict)
            agencies = AgencyInterface.get_agencies_by_id(
                session=current_session, agency_ids=get_agency_ids_from_session()
            )
            email = request_dict.get("email")
            user_account = UserAccountInterface.create_or_update_user(
                session=current_session,
                name=request_dict.get("name"),
                auth0_user_id=auth0_user_id,
                agencies=agencies,
                email=email,
            )
            permissions = g.user_context.permissions if "user_context" in g else []
            if ControlPanelPermission.RECIDIVIZ_ADMIN.value in permissions:
                agencies = AgencyInterface.get_agencies(session=current_session)
            else:
                agency_ids = g.user_context.agency_ids if "user_context" in g else []
                agencies = AgencyInterface.get_agencies_by_id(
                    session=current_session, agency_ids=agency_ids
                )
            current_session.commit()
            return jsonify(
                {
                    "id": user_account.id,
                    "agencies": [agency.to_json() for agency in agencies or []],
                    "permissions": permissions or [],
                }
            )
        except Exception as e:
            raise _get_error(error=e) from e

    ### Reports Overview ###

    @api_blueprint.route("/agencies/<agency_id>/reports", methods=["GET"])
    @auth_decorator
    @raise_if_user_is_unauthorized
    def get_reports_by_agency_id(agency_id: str) -> Response:
        """Gets a list of reports for the specified agency id.
        Used for rendering the Reports Overview page.
        """
        try:
            agency_ids = get_agency_ids_from_session()
            if int(agency_id) not in agency_ids:
                raise JusticeCountsServerError(
                    code="justice_counts_agency_permission",
                    description="User does not belong to the agency they are attempting to get agencies for.",
                )
            reports = ReportInterface.get_reports_by_agency_id(
                session=current_session, agency_id=int(agency_id)
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

    ### Individual Reports ###

    @api_blueprint.route("/reports/<report_id>", methods=["GET"])
    @auth_decorator
    @raise_if_user_is_unauthorized
    def get_report_by_id(report_id: Optional[str] = None) -> Response:
        """Get the details for a specified report. Used for rendering
        the Data Entry page.
        """
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
    @raise_if_user_is_unauthorized
    def update_report(report_id: Optional[str] = None) -> Response:
        """Update a report's metadata and/or the metrics on the report.
        Used by the Data Entry page.
        """
        try:
            report_id_int = int(assert_type(report_id, str))
            request_dict = assert_type(request.json, dict)
            time_loaded_by_client = request_dict["time_loaded"]
            user_account_id = get_user_account_id(request_dict=request_dict)
            user_account = UserAccountInterface.get_user_by_id(
                session=current_session, user_account_id=user_account_id
            )
            report = ReportInterface.get_report_by_id(
                session=current_session,
                report_id=report_id_int,
            )

            _check_for_conflicts(
                report=report,
                user_account_id=user_account_id,
                time_loaded_by_client=time_loaded_by_client,
            )

            ReportInterface.update_report_metadata(
                report=report,
                editor_id=user_account_id,
                status=request_dict["status"],
            )

            existing_datapoints_dict = ReportInterface.get_existing_datapoints_dict(
                reports=[report]
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
                    existing_datapoints_dict=existing_datapoints_dict,
                )

            editor_ids_to_names = ReportInterface.get_editor_ids_to_names(
                session=current_session, reports=[report]
            )
            report_json = ReportInterface.to_json_response(
                session=current_session,
                report=report,
                editor_ids_to_names=editor_ids_to_names,
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
            month = assert_type(request_json.get("month"), int)
            year = assert_type(request_json.get("year"), int)
            frequency = assert_type(request_json.get("frequency"), str)
            user_id = get_user_account_id(request_dict=request_json)
            permissions = g.user_context.permissions
            if (
                not permissions
                or ControlPanelPermission.RECIDIVIZ_ADMIN.value not in permissions
            ):
                raise JusticeCountsServerError(
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
                current_session.commit()
            except IntegrityError as e:
                if isinstance(e.orig, UniqueViolation):
                    raise JusticeCountsServerError(
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
        """Delete a report.
        Used by the Reports Overview page.
        """
        try:
            request_json = assert_type(request.json, dict)
            report_ids = request_json.get("report_ids")
            user_id = get_user_account_id(request_dict=request_json)
            permissions = g.user_context.permissions
            if (
                not permissions
                or ControlPanelPermission.RECIDIVIZ_ADMIN.value not in permissions
            ):
                raise JusticeCountsServerError(
                    code="justice_counts_delete_report_permission",
                    description=(
                        f"User {user_id} does not have permission to delete reports."
                    ),
                )

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
    def get_agency_metric_settings(agency_id: str) -> Response:
        """Get the contexts and configuration for each of an agency's metrics.
        Used by the Metric Configuration page in Settings.
        """
        try:
            agency = AgencyInterface.get_agency_by_id(
                session=current_session, agency_id=int(agency_id)
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
    @raise_if_user_is_unauthorized
    def update_metric_settings(agency_id: str) -> Response:
        """Update the contexts and configuration for an agency's metrics.
        Used by the Metric Configuration page in Settings.
        """
        try:
            request_json = assert_type(request.json, dict)
            agency = AgencyInterface.get_agency_by_id(
                session=current_session, agency_id=int(agency_id)
            )
            for metric_json in request_json.get("metrics", []):
                agency_metric = MetricInterface.from_json(
                    json=metric_json,
                    entry_point=DatapointGetRequestEntryPoint.METRICS_TAB,
                )
                user_account_id = get_user_account_id(request_dict=request_json)
                user_account = UserAccountInterface.get_user_by_id(
                    session=current_session, user_account_id=user_account_id
                )
                DatapointInterface.add_or_update_agency_datapoints(
                    session=current_session,
                    agency_metric=agency_metric,
                    agency=agency,
                    user_account=user_account,
                )

            current_session.commit()
            return jsonify({"status": "ok", "status_code": HTTPStatus.OK})
        except Exception as e:
            raise _get_error(error=e) from e

    ### Bulk Upload ###

    @api_blueprint.route("agencies/<agency_id>/spreadsheets", methods=["GET"])
    @auth_decorator
    def get_spreadsheets(agency_id: str) -> Response:
        """Get a list of spreadsheets uploaded by an agency.
        Used for rendering the Uploaded Files page.
        """
        agency_ids = get_agency_ids_from_session()
        if int(agency_id) not in agency_ids:
            raise JusticeCountsServerError(
                code="bad_user_permissions",
                description="User does not have the permissions to view this list of spreadsheets because they do not belong to the correct agency.",
            )
        try:
            spreadsheets = SpreadsheetInterface.get_agency_spreadsheets(
                agency_id=int(agency_id), session=current_session
            )
            return jsonify(
                [
                    SpreadsheetInterface.get_spreadsheet_json(
                        spreadsheet=spreadsheet, session=current_session
                    )
                    for spreadsheet in spreadsheets
                ]
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
        agency_ids = get_agency_ids_from_session()
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
                metric_key_to_disaggregation_status={
                    metric_key: DatapointInterface.is_metric_disaggregated_by_supervision_subsystem(
                        agency_datapoints=metric_key_to_agency_datapoints.get(
                            metric_key, []
                        ),
                    )
                    for metric_key, _ in metric_key_to_agency_datapoints.items()
                }
                if schema.System.SUPERVISION.value in agency.systems
                else {},
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
            SpreadsheetInterface.get_spreadsheet_json(
                spreadsheet=spreadsheet, session=current_session
            )
        )

    @api_blueprint.route("/spreadsheets/<spreadsheet_id>", methods=["PATCH"])
    @auth_decorator
    def update_spreadsheet(spreadsheet_id: str) -> Response:
        """Update a spreadsheet's metadata.
        Used by the Uploaded Files page.
        """
        try:
            permissions = g.user_context.permissions if "user_context" in g else []
            request_json = assert_type(request.json, dict)
            auth0_user_id = get_auth0_user_id(request_dict=request_json)
            if (
                not permissions
                or ControlPanelPermission.RECIDIVIZ_ADMIN.value not in permissions
            ):
                raise JusticeCountsServerError(
                    code="justice_counts_create_report_permission",
                    description=(
                        f"User {auth0_user_id} does not have permission to "
                        "update a spreadsheet status."
                    ),
                )
            status = assert_type(request_json.get("status"), str)
            spreadsheet = SpreadsheetInterface.update_spreadsheet(
                session=current_session,
                spreadsheet_id=int(spreadsheet_id),
                status=status,
                auth0_user_id=auth0_user_id,
            )

            current_session.commit()
            return jsonify(
                SpreadsheetInterface.get_spreadsheet_json(
                    spreadsheet=spreadsheet, session=current_session
                )
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

    def get_agency_datapoints(agency_id: str) -> Response:
        permissions = g.user_context.permissions if "user_context" in g else []
        if agency_id is None:
            # If no agency_id is specified, pick one of the agencies
            # that the user belongs to as a default for the home page
            if (
                len(g.user_context.agency_ids) == 0
                and ControlPanelPermission.RECIDIVIZ_ADMIN.value not in permissions
            ):
                raise JusticeCountsServerError(
                    code="justice_counts_agency_permission",
                    description="User does not belong to any agencies.",
                )
            agency_id = g.user_context.agency_ids[0]

        reports = ReportInterface.get_reports_by_agency_id(
            session=current_session,
            agency_id=int(agency_id),
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
            session=current_session, agency_id=int(agency_id)
        )
        metric_key_to_disaggregation_status = {}

        if schema.System.SUPERVISION.value in agency.systems:
            agency_datapoints = DatapointInterface.get_agency_datapoints(
                session=current_session, agency_id=agency.id
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
            metric_key_to_disaggregation_status = {
                metric_key: DatapointInterface.is_metric_disaggregated_by_supervision_subsystem(
                    agency_datapoints=metric_key_to_agency_datapoints.get(
                        metric_key, []
                    ),
                )
                for metric_key, _ in metric_key_to_agency_datapoints.items()
            }

        metric_definitions = MetricInterface.get_metric_definitions_for_systems(
            systems={schema.System[system] for system in agency.systems or []},
            metric_key_to_disaggregation_status=metric_key_to_disaggregation_status,
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
    @raise_if_user_is_unauthorized
    def get_datapoints_by_agency_id(agency_id: str) -> Response:
        try:
            return get_agency_datapoints(agency_id=agency_id)
        except Exception as e:
            raise _get_error(error=e) from e

    @api_blueprint.route("/agencies/<agency_id>/published_data", methods=["GET"])
    def get_agency_published_data(agency_id: str) -> Response:
        try:
            agency = AgencyInterface.get_agency_by_id(
                session=current_session, agency_id=int(agency_id)
            )
            reports = ReportInterface.get_reports_by_agency_id(
                session=current_session,
                agency_id=int(agency_id),
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
                    "agency": agency.to_json(),
                    "metrics": metrics_json,
                }
            )

        except Exception as e:
            raise _get_error(error=e) from e

    return api_blueprint


def _check_for_conflicts(
    report: schema.Report, user_account_id: int, time_loaded_by_client: float
) -> None:
    last_modified_at = (
        report.last_modified_at.timestamp()
        if report.last_modified_at is not None
        else None
    )
    last_modified_by = (
        report.modified_by[-1]
        if report.modified_by is not None and len(report.modified_by) > 0
        else None
    )

    if (
        last_modified_at is not None
        and last_modified_by is not None
        and last_modified_at > time_loaded_by_client
        and last_modified_by != user_account_id
    ):
        logging.warning(
            "Version conflict: last_modified_at: %s and time_loaded_by_client: %s",
            last_modified_at,
            time_loaded_by_client,
        )


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
