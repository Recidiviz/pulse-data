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
import json
import logging
import os
import re
import tempfile
import urllib.parse
from collections import defaultdict
from http import HTTPStatus
from typing import Any, Callable, DefaultDict, Dict, List, Optional, Set

from flask import Blueprint, Response, jsonify, make_response, request, send_file
from flask_sqlalchemy_session import current_session
from flask_wtf.csrf import CSRFProtect, generate_csrf
from google.auth.transport import requests
from google.cloud import storage
from google.oauth2 import id_token
from psycopg2.errors import UniqueViolation  # pylint: disable=no-name-in-module
from sqlalchemy.exc import IntegrityError
from werkzeug.wrappers import response

from recidiviz.auth.auth0_client import Auth0Client
from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.agency_jurisdictions import AgencyJurisdictionInterface
from recidiviz.justice_counts.agency_setting import AgencySettingInterface
from recidiviz.justice_counts.agency_user_account_association import (
    AgencyUserAccountAssociationInterface,
)
from recidiviz.justice_counts.bulk_upload.template_generator import (
    generate_bulk_upload_template,
)
from recidiviz.justice_counts.control_panel.utils import (
    get_auth0_user_id,
    raise_if_user_is_not_in_agency,
    raise_if_user_is_wrong_role,
)
from recidiviz.justice_counts.datapoint import DatapointInterface
from recidiviz.justice_counts.exceptions import (
    JusticeCountsBulkUploadException,
    JusticeCountsServerError,
)
from recidiviz.justice_counts.feed import FeedInterface
from recidiviz.justice_counts.metric_setting import MetricSettingInterface
from recidiviz.justice_counts.metrics.metric_definition import MetricDefinition
from recidiviz.justice_counts.metrics.metric_interface import (
    DatapointGetRequestEntryPoint,
    MetricInterface,
)
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.justice_counts.spreadsheet import SpreadsheetInterface
from recidiviz.justice_counts.types import DatapointJson
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.justice_counts.utils.constants import (
    AUTOMATIC_UPLOAD_BUCKET_REGEX,
    ERRORS_WARNINGS_JSON_BUCKET_PROD,
    ERRORS_WARNINGS_JSON_BUCKET_STAGING,
    UploadMethod,
)
from recidiviz.justice_counts.utils.datapoint_utils import (
    filter_deprecated_datapoints,
    get_dimension_id_and_member,
)
from recidiviz.justice_counts.utils.email import send_confirmation_email
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.utils.environment import (
    in_ci,
    in_development,
    in_gcp,
    in_gcp_production,
    in_gcp_staging,
    in_test,
)
from recidiviz.utils.flask_exception import FlaskException
from recidiviz.utils.pubsub_helper import (
    BUCKET_ID,
    OBJECT_ID,
    extract_pubsub_message_from_json,
)
from recidiviz.utils.types import assert_type

ALLOWED_EXTENSIONS = ["xlsx", "xls", "csv"]


def get_api_blueprint(
    auth_decorator: Callable,
    csrf: CSRFProtect,
    service_account_emails: Set[str],
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

        environment = "unknown"
        if in_gcp() is False:
            environment = "local"
        elif in_gcp_staging() is True:
            environment = "staging"
        elif in_gcp_production() is True:
            environment = "production"

        return jsonify({"csrf": generate_csrf(secret_key), "env": environment})

    @api_blueprint.route("/env")
    def env() -> Response:
        environment = "unknown"
        if in_gcp() is False:
            environment = "local"
        elif in_gcp_staging() is True:
            environment = "staging"
        elif in_gcp_production() is True:
            environment = "production"

        return jsonify({"env": environment})

    ### Agencies ###

    @api_blueprint.route("/agencies/<agency_id>", methods=["PATCH"])
    @auth_decorator
    def update_agency(agency_id: int) -> Response:
        """
        Currently, the supported updates are:
            - Changing the set of systems associated with the agency
            - Updating AgencySettings
            - Updating AgencyJurisdictions
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
                    systems=systems,
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

            jurisdictions = request_json.get("jurisdictions")
            if jurisdictions is not None:
                AgencyJurisdictionInterface.create_or_update_agency_jurisdictions(
                    session=current_session,
                    agency_id=agency_id,
                    included_jurisdiction_ids=jurisdictions["included"],
                    excluded_jurisdiction_ids=jurisdictions["excluded"],
                )
            current_session.commit()
            return jsonify({"status": "ok", "status_code": HTTPStatus.OK})
        except Exception as e:
            raise _get_error(error=e) from e

    @api_blueprint.route("/agencies/<agency_id>", methods=["GET"])
    @auth_decorator
    def get_agency_settings(agency_id: int) -> Response:
        """
        This endpoint gets the settings for an Agency as well as
        information about if the current logged-in user is subscribed
        to emails.
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
            jurisdictions = AgencyJurisdictionInterface.to_json(
                session=current_session, agency_id=agency_id
            )

            current_user_agency_association = (
                AgencyUserAccountAssociationInterface.get_association_by_ids(
                    agency_id=agency_id,
                    user_account_id=user.id,
                    session=current_session,
                )
            )

            is_subscribed_to_emails = current_user_agency_association.subscribed is True

            return jsonify(
                {
                    "settings": [setting.to_json() for setting in agency_settings],
                    "jurisdictions": jurisdictions,
                    "is_subscribed_to_emails": is_subscribed_to_emails,
                    "days_after_time_period_to_send_email": current_user_agency_association.days_after_time_period_to_send_email,
                }
            )
        except Exception as e:
            raise _get_error(error=e) from e

    @api_blueprint.route("/agencies/<super_agency_id>/children", methods=["GET"])
    @auth_decorator
    def get_child_agencies_for_superagency(super_agency_id: int) -> Response:
        """
        This endpoint returns information about the child agencies of a superagency.
        If <super_agency_id> is not a superagency, list will be empty.

        Returns:
        {
            child_agencies: List[Dict[<ID, Name, Systems>]]
        }
        """
        try:
            request_json = assert_type(request.values, dict)
            user = UserAccountInterface.get_user_by_auth0_user_id(
                session=current_session,
                auth0_user_id=get_auth0_user_id(request_dict=request_json),
            )

            raise_if_user_is_not_in_agency(user=user, agency_id=super_agency_id)

            super_agency = AgencyInterface.get_agency_by_id(
                session=current_session, agency_id=super_agency_id
            )
            return jsonify(
                {"child_agencies": _get_child_agency_json(agency=super_agency)}
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
                    schema.UserAccountRole.CONTRIBUTOR,
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
            AgencyUserAccountAssociationInterface.remove_user_from_agencies(
                email=email,
                agency_ids=[int(agency_id)],
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

            UserAccountInterface.create_or_update_user(
                session=current_session,
                name=name,
                auth0_user_id=auth0_user_id,
                email=email,
                auth0_client=auth0_client,
            )

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
        Returns user id and agencies. If email_verified is passed in,
        we will update the user's invitation_status.
        """
        try:
            if auth0_client is None:
                return make_response(
                    "auth0_client could not be initialized. Environment is not development or gcp.",
                    500,
                )

            request_dict = assert_type(request.json, dict)
            auth0_user_id = get_auth0_user_id(request_dict)
            email = request_dict.get("email")
            is_email_verified = request_dict.get("email_verified", False)

            user = UserAccountInterface.create_or_update_user(
                session=current_session,
                name=request_dict.get("name"),
                auth0_user_id=auth0_user_id,
                email=email,
                auth0_client=auth0_client,
                is_email_verified=is_email_verified,
            )
            user_id = user.id

            if in_development():
                # When running locally, our AgencyUserAccountAssociation table
                # won't be up-to-date (it's hard to do this via fixtures),
                # so just give access to all agencies.
                agency_ids = AgencyInterface.get_agency_ids(session=current_session)
                role = schema.UserAccountRole.JUSTICE_COUNTS_ADMIN

            else:
                agency_ids = [assoc.agency_id for assoc in user.agency_assocs]
                role = None

            agencies = AgencyInterface.get_agencies_by_id(
                session=current_session, agency_ids=agency_ids
            )

            if in_development():
                # As above, when running locally, fill the AgencyUserAccountAssociation
                # to give user access to all agencies
                UserAccountInterface.add_or_update_user_agency_association(
                    session=current_session,
                    user=user,
                    agencies=agencies,
                    invitation_status=schema.UserAccountInvitationStatus.ACCEPTED,
                    role=role,
                )

            if is_email_verified is True:
                for assoc in user.agency_assocs:
                    if (
                        assoc.invitation_status
                        == schema.UserAccountInvitationStatus.PENDING
                    ):
                        assoc.invitation_status = (
                            schema.UserAccountInvitationStatus.ACCEPTED
                        )
                        assoc.role = role

            agency_jsons = [a.to_json() for a in agencies]

            current_session.commit()
            return jsonify(
                {
                    "id": user_id,
                    "agencies": agency_jsons,
                }
            )
        except Exception as e:
            raise _get_error(error=e) from e

    ### Guidance ###

    @api_blueprint.route("/users/agencies/<agency_id>/guidance", methods=["GET"])
    @auth_decorator
    def get_guidance_progress(agency_id: int) -> Response:
        try:
            request_json = assert_type(request.values, dict)
            user = UserAccountInterface.get_user_by_auth0_user_id(
                session=current_session,
                auth0_user_id=get_auth0_user_id(request_dict=request_json),
            )

            raise_if_user_is_not_in_agency(user=user, agency_id=agency_id)

            current_association, *_ = [
                assoc
                for assoc in user.agency_assocs
                if assoc.agency_id == int(agency_id)
            ]

            if (
                current_association.guidance_progress is None
                or len(current_association.guidance_progress) == 0
            ):
                current_association.guidance_progress = [
                    {"topicID": "WELCOME", "topicCompleted": False},
                    {"topicID": "AGENCY_SETUP", "topicCompleted": False},
                    {"topicID": "METRIC_CONFIG", "topicCompleted": False},
                    {"topicID": "ADD_DATA", "topicCompleted": False},
                    {"topicID": "PUBLISH_DATA", "topicCompleted": False},
                ]
                current_session.add(current_association)
                current_session.commit()

            return jsonify({"guidance_progress": current_association.guidance_progress})
        except Exception as e:
            raise _get_error(error=e) from e

    @api_blueprint.route("/users/agencies/<agency_id>/guidance", methods=["PUT"])
    @auth_decorator
    def update_guidance_progress(agency_id: int) -> Response:
        """
        `updated_topic` {dictionary} - provides a single dictionary status update for a guidance topic
        Structure: { "topicID": String, "topicCompleted": Boolean }
        Example: { "topicID": "ADD_DATA", "topicCompleted": True } indicates user has completed the ADD_DATA
                                                                    step of the guidance flow

        ---

        `current_association.guidance_progress` {list of dictionaries} - provides a list of all guidance topics and a user's
                                                            status for each topic
        Structure of dictionary item: { "topicID": String, "topicCompleted": Boolean }
        Example (user is currently on the ADD_DATA step):
            [
                { "topicID": "WELCOME", "topicCompleted": True },
                { "topicID": "AGENCY_SETUP", "topicCompleted": True },
                { "topicID": "METRIC_CONFIG", "topicCompleted": True },
                { "topicID": "ADD_DATA", "topicCompleted": False },
                { "topicID": "PUBLISH_DATA", "topicCompleted": False },
            ]
        """
        try:
            request_dict = assert_type(request.json, dict)
            user = UserAccountInterface.get_user_by_auth0_user_id(
                session=current_session,
                auth0_user_id=get_auth0_user_id(request_dict=request_dict),
            )

            raise_if_user_is_not_in_agency(user=user, agency_id=agency_id)

            updated_topic = assert_type(request_dict.get("updated_topic"), dict)

            current_association, *_ = [
                assoc
                for assoc in user.agency_assocs
                if assoc.agency_id == int(agency_id)
            ]

            # Updates a topic status within the `guidance_progress` list of dictionaries with the `updated_topic`
            updated_guidance_progress = [
                (
                    updated_topic
                    if topic.get("topicID") == updated_topic.get("topicID")
                    else topic
                )
                for topic in current_association.guidance_progress
            ]

            current_association.guidance_progress = updated_guidance_progress
            current_session.add(current_association)
            current_session.commit()

            return jsonify({"guidance_progress": updated_guidance_progress})
        except Exception as e:
            raise _get_error(error=e) from e

    ### Home ###

    @api_blueprint.route("/<user_id>/<agency_id>/page_visit", methods=["PUT"])
    @auth_decorator
    def record_user_agency_page_visit(user_id: int, agency_id: int) -> Response:
        """Given a user_id, agency_id pair, records that the user visited a page
        associated with the given agency on a particular date. This is used to track
        login activity.
        """
        try:
            request_json = assert_type(request.values, dict)
            user = UserAccountInterface.get_user_by_auth0_user_id(
                session=current_session,
                auth0_user_id=get_auth0_user_id(request_dict=request_json),
            )
            raise_if_user_is_not_in_agency(user=user, agency_id=agency_id)

            AgencyUserAccountAssociationInterface.record_user_agency_page_visit(
                session=current_session, user_id=user_id, agency_id=agency_id
            )
            current_session.commit()
            return jsonify({"status": "ok", "status_code": HTTPStatus.OK})
        except Exception as e:
            raise _get_error(error=e) from e

    @api_blueprint.route("/home/<agency_id>", methods=["GET"])
    @auth_decorator
    def get_home_metadata(agency_id: int) -> Response:
        """Get the agency metrics, latest annual and monthly reports
        to power the task cards in the homepage.

        Returns:
        {
            agency_metrics: List[Metrics],
            monthly_report: Report,
            annual_reports: Dict[<Starting Month>, Report]
            child_agencies: List[Dict[<ID, Name, Systems>]]
        }
        """
        try:
            request_json = assert_type(request.values, dict)
            user = UserAccountInterface.get_user_by_auth0_user_id(
                session=current_session,
                auth0_user_id=get_auth0_user_id(request_dict=request_json),
            )
            raise_if_user_is_not_in_agency(user=user, agency_id=agency_id)

            agency = AgencyInterface.get_agency_by_id(
                session=current_session, agency_id=agency_id
            )
            reports = ReportInterface.get_reports_by_agency_id(
                session=current_session, agency_id=int(agency_id)
            )
            sorted_reports = sorted(
                reports, key=lambda report: report.date_range_start, reverse=True
            )
            # Find the first (latest) monthly report in the sorted reports list
            latest_monthly_report = next(
                (
                    report
                    for report in sorted_reports
                    if ReportInterface.get_reporting_frequency(report).value
                    == "MONTHLY"
                ),
                None,
            )
            # Get a list of all annual reports
            annual_reports: List[schema.Report] = [
                report
                for report in sorted_reports
                if ReportInterface.get_reporting_frequency(report).value == "ANNUAL"
            ]
            # Build a dictionary to group annual reports by starting month (Jan == 1, Dec == 12)
            # e.g. { 1: [<Annual Reports>], 2: ... }
            latest_annual_reports: dict[int, list] = defaultdict(list)

            for report in annual_reports:
                starting_month = report.date_range_start.month
                latest_annual_reports[starting_month].append(report)

            for starting_month, reports in latest_annual_reports.items():
                latest_annual_reports[starting_month] = [
                    sorted(
                        reports,
                        key=lambda report: report.date_range_start,
                        reverse=True,
                    )
                ][0]

            # Prep our latest monthly & annual reports to be jsonified
            latest_monthly_report_definition_json = {}
            latest_annual_report_definition_json = {}
            latest_annual_reports_json = {}

            agency_datapoints = DatapointInterface.get_agency_datapoints(
                session=current_session, agency_id=agency_id
            )

            if latest_monthly_report is not None:
                latest_monthly_report_metrics = ReportInterface.get_metrics_by_report(
                    session=current_session,
                    report=latest_monthly_report,
                    agency_datapoints=agency_datapoints,
                )
                latest_monthly_report_definition_json = (
                    ReportInterface.to_json_response(
                        report=latest_monthly_report,
                        editor_id_to_json={},
                    )
                )
                latest_monthly_report_metrics_json = [
                    report_metric.to_json(
                        entry_point=DatapointGetRequestEntryPoint.REPORT_PAGE,
                    )
                    for report_metric in latest_monthly_report_metrics
                ]
                latest_monthly_report_definition_json[
                    "metrics"
                ] = latest_monthly_report_metrics_json

            for month, annual_reports_list in latest_annual_reports.items():
                latest_annual_report = annual_reports_list[0]
                latest_annual_report_metrics = ReportInterface.get_metrics_by_report(
                    session=current_session,
                    report=latest_annual_report,
                    agency_datapoints=agency_datapoints,
                )
                latest_annual_report_definition_json = ReportInterface.to_json_response(
                    report=latest_annual_report,
                    editor_id_to_json={},
                )
                latest_annual_report_metrics_json = [
                    report_metric.to_json(
                        entry_point=DatapointGetRequestEntryPoint.REPORT_PAGE,
                    )
                    for report_metric in latest_annual_report_metrics
                ]
                latest_annual_report_definition_json[
                    "metrics"
                ] = latest_annual_report_metrics_json
                latest_annual_reports_json[month] = latest_annual_report_definition_json

            agency_metrics = MetricSettingInterface.get_agency_metric_interfaces(
                session=current_session,
                agency=agency,
                agency_datapoints=agency_datapoints,
            )
            agency_metrics_json = [
                metric.to_json(entry_point=DatapointGetRequestEntryPoint.METRICS_TAB)
                for metric in agency_metrics
            ]

            return jsonify(
                {
                    "agency_metrics": agency_metrics_json,
                    "monthly_report": latest_monthly_report_definition_json,
                    "annual_reports": latest_annual_reports_json,
                    "child_agencies": _get_child_agency_json(agency=agency),
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
            reports = []

            reports = ReportInterface.get_reports_by_agency_id(
                session=current_session, agency_id=agency_id
            )

            editor_id_to_json = (
                AgencyUserAccountAssociationInterface.get_editor_id_to_json(
                    session=current_session, reports=reports, user=user
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

    ### Multiple Reports ###

    @api_blueprint.route("/reports", methods=["GET"])
    @auth_decorator
    def get_reports_by_id() -> Response:
        """Provided a list of report IDs, this method returns a list of reports
        (with datapoints instead of metrics) for each report ID.
        """
        try:
            # The request will include the following query params in the request body:
            # 1. agency_id (String): the agency id
            # 2. report_ids (String): a comma separated string of report ids the FE is requesting
            # Example path URL for requesting reports 1877 and 1878 for agency 147:
            # /api/reports?agency_id=147&report_ids=1877,1878

            request_json = assert_type(request.values, dict)
            raw_report_ids = assert_type(request_json.get("report_ids"), str)
            report_ids = (
                [int(id) for id in raw_report_ids.split(",")]
                if raw_report_ids != ""
                else None
            )
            raw_agency_id = assert_type(request_json.get("agency_id"), str)
            agency_id = int(raw_agency_id)
            user = UserAccountInterface.get_user_by_auth0_user_id(
                session=current_session,
                auth0_user_id=get_auth0_user_id(request_dict=request_json),
            )

            raise_if_user_is_not_in_agency(user=user, agency_id=agency_id)

            if report_ids is None or len(report_ids) == 0:
                raise JusticeCountsServerError(
                    code="justice_counts_bad_request",
                    description="Empty list of report_ids passed to get reports endpoint.",
                )

            all_reports = ReportInterface.get_reports_by_id(
                session=current_session, report_ids=report_ids, include_datapoints=True
            )
            report_jsons = []
            datapoints = []
            report_id_to_datapoints: Dict[str, list] = defaultdict(list)
            report_id_to_frequency: Dict[str, schema.ReportingFrequency] = {}
            report_id_to_published_status: Dict[str, bool] = {}

            for report in all_reports:
                datapoints.extend(report.datapoints)
                report_id_to_frequency[
                    report.id
                ] = ReportInterface.get_reporting_frequency(report)
                report_id_to_published_status[report.id] = (
                    report.status == schema.ReportStatus.PUBLISHED
                )

            non_deprecated_datapoints = filter_deprecated_datapoints(datapoints)
            for datapoint in non_deprecated_datapoints:
                report_id = datapoint.report_id
                agency_name = datapoint.report.source.name
                report_frequency = report_id_to_frequency[report_id]
                is_report_published = report_id_to_published_status[report_id]

                # Filter out report-level context datapoints as they are deprecated
                if datapoint.context_key is not None:
                    continue

                report_id_to_datapoints[report_id].append(
                    DatapointInterface.to_json_response(
                        datapoint=datapoint,
                        is_published=is_report_published,
                        frequency=report_frequency,
                        agency_name=agency_name,
                    )
                )

            for report in all_reports:
                report_definition_json = ReportInterface.to_json_response(
                    report=report,
                    # Right now, this method is only used for bulk publishing
                    # which does not require the editor details.
                    editor_id_to_json={},
                )
                report_definition_json["datapoints"] = report_id_to_datapoints[
                    report.id
                ]
                report_jsons.append(report_definition_json)

            return jsonify(report_jsons)
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
            is_superagency = None
            if report is not None and report.agency is not None:
                is_superagency = report.agency.is_superagency

            raise_if_user_is_not_in_agency(user=user, agency_id=report.source_id)

            agency_datapoints = DatapointInterface.get_agency_datapoints(
                session=current_session, agency_id=report.source_id
            )
            report_metrics = ReportInterface.get_metrics_by_report(
                session=current_session,
                report=report,
                is_superagency=is_superagency,
                agency_datapoints=agency_datapoints,
            )
            editor_id_to_json = (
                AgencyUserAccountAssociationInterface.get_editor_id_to_json(
                    session=current_session, reports=[report], user=user
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
            inserts: List[schema.Datapoint] = []
            updates: List[schema.Datapoint] = []
            histories: List[schema.DatapointHistory] = []
            for metric_json in request_json.get("metrics", []):
                report_metric = MetricInterface.from_json(
                    json=metric_json,
                    entry_point=DatapointGetRequestEntryPoint.REPORT_PAGE,
                )
                ReportInterface.add_or_update_metric(
                    session=current_session,
                    inserts=inserts,
                    updates=updates,
                    histories=histories,
                    report=report,
                    report_metric=report_metric,
                    user_account=user,
                    existing_datapoints_dict=existing_datapoints_dict,
                    upload_method=UploadMethod.MANUAL_ENTRY,
                )

            editor_id_to_json = (
                AgencyUserAccountAssociationInterface.get_editor_id_to_json(
                    session=current_session, reports=[report], user=user
                )
            )
            report_json = ReportInterface.to_json_response(
                report=report,
                editor_id_to_json=editor_id_to_json,
            )

            DatapointInterface.flush_report_datapoints(
                session=current_session,
                inserts=inserts,
                updates=updates,
                histories=histories,
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
                    "role": (
                        assocs[0].role.value if assocs[0].role is not None else None
                    ),
                }
            }
            report_response = ReportInterface.to_json_response(
                report=report,
                editor_id_to_json=editor_id_to_json,
            )

            return jsonify(report_response)
        except Exception as e:
            raise _get_error(error=e) from e

    @api_blueprint.route("/reports", methods=["PATCH"])
    @auth_decorator
    def update_multiple_report_statuses() -> Response:
        """Update multiple reports' statuses."""
        try:
            request_json = assert_type(request.json, dict)
            agency_id = assert_type(request_json.get("agency_id"), int)
            user = UserAccountInterface.get_user_by_auth0_user_id(
                session=current_session,
                auth0_user_id=get_auth0_user_id(request_dict=request_json),
            )

            raise_if_user_is_not_in_agency(user=user, agency_id=agency_id)

            report_ids = request_json.get("report_ids")
            updated_reports_json = []

            if report_ids is None or len(report_ids) == 0:
                raise JusticeCountsServerError(
                    code="justice_counts_bad_request",
                    description="Empty list of report ids passed to update multiple reports endpoint.",
                )

            all_reports = ReportInterface.get_reports_by_id(
                current_session, report_ids, include_datapoints=False
            )
            editor_id_to_json = (
                AgencyUserAccountAssociationInterface.get_editor_id_to_json(
                    session=current_session, reports=all_reports, user=user
                )
            )

            for current_report in all_reports:
                ReportInterface.update_report_metadata(
                    report=current_report,
                    editor_id=user.id,
                    status=request_json["status"],
                )
                report_json = ReportInterface.to_json_response(
                    report=current_report,
                    editor_id_to_json=editor_id_to_json,
                )
                updated_reports_json.append(report_json)

            current_session.commit()
            return jsonify(updated_reports_json)
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
            metrics = MetricSettingInterface.get_agency_metric_interfaces(
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
                MetricSettingInterface.add_or_update_agency_metric_setting(
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

    # This endpoint is triggered by a pub/sub subscription on an agency's GCS bucket.
    @csrf.exempt  # type: ignore[arg-type]
    @api_blueprint.route("/ingest-spreadsheet", methods=["POST"])
    def ingest_workbook_from_gcs() -> response.Response:
        try:
            bearer_token = request.headers.get("Authorization")
            if bearer_token is None:
                logging.exception(
                    JusticeCountsServerError(
                        code="justice_counts_no_bearer_token",
                        description="No Bearer Token was sent in Authorization header of the request.",
                    )
                )
                # Return 200 to acknowledge the message from pubsub
                return jsonify({"status": "ok", "status_code": HTTPStatus.OK})

            token = bearer_token.split(" ")[1]
            # Verify and decode the JWT. `verify_oauth2_token` verifies
            # the JWT signature, the `aud` claim, and the `exp` claim.
            claim = id_token.verify_oauth2_token(token, requests.Request())
            if (
                # TODO(#22175) Only accept pubsub requests from justice-counts-staging and
                # justice-counts-production.
                claim["email"] not in service_account_emails
                or claim["email_verified"] is not True
            ):
                logging.exception(
                    JusticeCountsServerError(
                        code="request_not_authenticated",
                        description="This request could not be authenticated",
                    )
                )
                # Return 200 to acknowledge the message from pubsub
                return jsonify({"status": "ok", "status_code": HTTPStatus.OK})

            try:
                message = extract_pubsub_message_from_json(request.get_json())
            except Exception as e:
                raise _get_error(error=e) from e

            if (
                not message.attributes
                or OBJECT_ID not in message.attributes
                or BUCKET_ID not in message.attributes
            ):
                logging.exception(
                    JusticeCountsServerError(
                        code="justice_counts_bad_pub_sub",
                        description="Invalid Pub/Sub message.",
                    )
                )
                # Return 200 to acknowledge the message from pubsub
                return jsonify({"status": "ok", "status_code": HTTPStatus.OK})

            attributes = message.attributes

            file_name = attributes[OBJECT_ID]
            bucket_name = attributes[BUCKET_ID]

            # Bucket name format is recidiviz-<project>-justice-counts-ingest-agency-<agency_id>
            # The agency_id will be inferred by the bucket name.
            match_obj: Optional[re.Match] = re.match(
                AUTOMATIC_UPLOAD_BUCKET_REGEX, bucket_name
            )

            if match_obj is None:
                logging.exception(
                    JusticeCountsServerError(
                        code="invalid_ingest_bucket",
                        description=f"Invalid ingest bucket [{bucket_name}]",
                    )
                )
                # Return 200 to acknowledge the message from pubsub
                return jsonify({"status": "ok", "status_code": HTTPStatus.OK})

            agency_id_str = match_obj.group("agency_id")
            if agency_id_str is None:
                logging.exception(
                    JusticeCountsServerError(
                        code="no_bucket_for_agency",
                        description=f"No agency or system associated with GCS bucket with id {bucket_name}",
                    )
                )
                # Return 200 to acknowledge the message from pubsub
                return jsonify({"status": "ok", "status_code": HTTPStatus.OK})

            agency = AgencyInterface.get_agency_by_id(
                session=current_session, agency_id=int(agency_id_str)
            )

            if file_name is None:
                logging.exception(
                    JusticeCountsServerError(
                        code="justice_counts_missing_file_name_sub",
                        description="Missing Filename",
                    )
                )
                # Return 200 to acknowledge the message from pubsub
                return jsonify({"status": "ok", "status_code": HTTPStatus.OK})

            if not allowed_file(file_name):
                logging.exception(
                    JusticeCountsServerError(
                        code="file_type_error",
                        description="Invalid file type: All files must be of type .xlsx or .csv.",
                    )
                )
                # Return 200 to acknowledge the message from pubsub
                return jsonify({"status": "ok", "status_code": HTTPStatus.OK})

            # Superagencies can share their own capacity and cost metrics
            # (superagency sector) or the metrics for their child agencies (other sector).
            # We're assuming superagencies won't be bulk uploading their capacity and
            # cost metrics, but just the metrics for their child agencies, so for the
            # sake of bulk upload, it's safe to ignore the superagency sector.
            if schema.System.SUPERAGENCY.value in agency.systems:
                system_strs = [
                    sys
                    for sys in agency.systems
                    if sys != schema.System.SUPERAGENCY.value
                ]
            else:
                system_strs = agency.systems

            system = schema.System[system_strs[0]]
            if len(system_strs) > 1:
                # Agencies with multiple systems will have a folder in their GCS bucket
                # for each system that they can report for. When a file is uploaded to
                # a folder within a bucket, the filepath is prefixed with the folder
                # name (e.g <SYSTEM>/<FILE_NAME>, LAW_ENFORCEMENT/my_upload.xlsx).
                system_str, _ = os.path.split(attributes[OBJECT_ID])
                system = schema.System[system_str]

            # get metric definition for spreadsheet
            metric_definitions = (
                SpreadsheetInterface.get_metric_definitions_for_workbook(
                    system=system, agency=agency
                )
            )
            (
                metric_key_to_datapoint_jsons,
                metric_key_to_errors,
                updated_reports,
                existing_report_ids,
                unchanged_reports,
                spreadsheet,
            ) = SpreadsheetInterface.ingest_workbook_from_gcs(
                session=current_session,
                bucket_name=bucket_name,
                system=system,
                agency=agency,
                filename=file_name,
                metric_definitions=metric_definitions,
                upload_method=UploadMethod.AUTOMATED_BULK_UPLOAD,
            )

            if in_ci() or in_test():
                current_session.commit()
                return jsonify({"status": "ok", "status_code": HTTPStatus.OK})

            metric_key_to_metric_interface = (
                MetricSettingInterface.get_metric_key_to_metric_interface(
                    session=current_session, agency=agency
                )
            )

            # Get and save ingested spreadsheet json to GCP bucket if not in test/ci
            ingested_spreadsheet_json = _get_ingest_spreadsheet_json(
                agency=agency,
                agency_id=agency.id,
                existing_report_ids=existing_report_ids,
                updated_reports=updated_reports,
                unchanged_reports=unchanged_reports,
                metric_key_to_errors=metric_key_to_errors,
                metric_key_to_datapoint_jsons=metric_key_to_datapoint_jsons,
                metric_definitions=metric_definitions,
                metric_key_to_metric_interface=metric_key_to_metric_interface,
                spreadsheet=spreadsheet,
            )
            SpreadsheetInterface.save_ingested_spreadsheet_json(
                ingested_spreadsheet_json=ingested_spreadsheet_json,
                spreadsheet=spreadsheet,
            )

            current_session.commit()

            send_confirmation_email(
                session=current_session,
                success=True,
                file_name=file_name,
                agency_id=agency_id_str,
                spreadsheet_id=spreadsheet.id,
                metric_key_to_errors=metric_key_to_errors,
            )

            return jsonify({"status": "ok", "status_code": HTTPStatus.OK})
        except Exception as e:
            logging.exception(e)
            send_confirmation_email(
                session=current_session,
                success=False,
                file_name=file_name,
                agency_id=agency_id_str,
            )

            # Return 200 to acknowledge the message from pubsub
            return jsonify({"status": "ok", "status_code": HTTPStatus.OK})

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
                    spreadsheets=spreadsheets, session=current_session, user=user
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
        try:
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
        except Exception as e:
            raise _get_error(error=e) from e

    @api_blueprint.route("/spreadsheets", methods=["POST"])
    @auth_decorator
    def upload_spreadsheet() -> Response:
        """Upload a spreadsheet for an agency.
        Used by the Bulk Upload flow.
        """
        try:
            data = assert_type(request.form, dict)
            agency_id = int(data["agency_id"])
            system = data["system"]
            auth0_user_id = get_auth0_user_id(request_dict=data)
            file = request.files["file"]
            if file is None:
                raise JusticeCountsServerError(
                    "no_file_on_upload", "No file was sent for upload."
                )
            if not allowed_file(file.filename):
                raise JusticeCountsServerError(
                    "file_type_error",
                    "Invalid file type: All files must be of type .xlsx or .csv.",
                )

            (
                xls,
                new_file_name,
                upload_filetype,
            ) = SpreadsheetInterface.convert_file_to_excel(
                file=file, filename=file.filename  # type: ignore[arg-type]
            )

            # Upload spreadsheet to GCS
            spreadsheet = SpreadsheetInterface.upload_spreadsheet(
                session=current_session,
                file_storage=file,
                agency_id=agency_id,
                auth0_user_id=auth0_user_id,
                system=system,
            )

            agency = AgencyInterface.get_agency_by_id(
                session=current_session, agency_id=agency_id
            )

            metric_key_to_metric_interface = (
                MetricSettingInterface.get_metric_key_to_metric_interface(
                    session=current_session, agency=agency
                )
            )

            # get metric definitions for spreadsheet
            metric_definitions = (
                SpreadsheetInterface.get_metric_definitions_for_workbook(
                    agency=agency, system=schema.System[system]
                )
            )
            (
                metric_key_to_datapoint_jsons,
                metric_key_to_errors,
                updated_reports,
                existing_report_ids,
                unchanged_reports,
                spreadsheet,
            ) = SpreadsheetInterface.ingest_spreadsheet(
                session=current_session,
                spreadsheet=spreadsheet,
                auth0_user_id=auth0_user_id,
                xls=xls,
                agency=agency,
                metric_key_to_metric_interface=metric_key_to_metric_interface,
                metric_definitions=metric_definitions,
                filename=new_file_name,
                upload_method=UploadMethod.BULK_UPLOAD,
                upload_filetype=upload_filetype,
            )

            ingested_spreadsheet_json = _get_ingest_spreadsheet_json(
                agency=agency,
                agency_id=agency_id,
                existing_report_ids=existing_report_ids,
                updated_reports=updated_reports,
                unchanged_reports=unchanged_reports,
                metric_key_to_errors=metric_key_to_errors,
                metric_key_to_datapoint_jsons=metric_key_to_datapoint_jsons,
                metric_definitions=metric_definitions,
                metric_key_to_metric_interface=metric_key_to_metric_interface,
                spreadsheet=spreadsheet,
            )

            if in_ci() or in_test():
                current_session.commit()
                return jsonify(ingested_spreadsheet_json)

            # Save ingested spreadsheet json to GCP bucket if not in test/ci
            SpreadsheetInterface.save_ingested_spreadsheet_json(
                ingested_spreadsheet_json=ingested_spreadsheet_json,
                spreadsheet=spreadsheet,
            )

            current_session.commit()
            return jsonify(ingested_spreadsheet_json)
        except Exception as e:
            raise _get_error(error=e) from e

    def _get_ingest_spreadsheet_json(
        agency: schema.Agency,
        agency_id: int,
        existing_report_ids: List[int],
        updated_reports: Set[schema.Report],
        unchanged_reports: Set[schema.Report],
        metric_key_to_errors: Dict[
            Optional[str], List[JusticeCountsBulkUploadException]
        ],
        metric_key_to_datapoint_jsons: Dict[str, List[DatapointJson]],
        metric_definitions: List[MetricDefinition],
        metric_key_to_metric_interface: Dict[str, MetricInterface],
        spreadsheet: schema.Spreadsheet,
    ) -> Dict[str, Any]:
        child_agencies = AgencyInterface.get_child_agencies_for_agency(
            session=current_session, agency=agency
        )

        all_reports = ReportInterface.get_reports_by_agency_ids(
            session=current_session,
            agency_ids=[a.id for a in child_agencies] + [agency_id],
            include_datapoints=False,
        )

        new_report_jsons = [
            ReportInterface.to_json_response(
                report=report,
                editor_id_to_json={},
                agency_name=report.source.name,
            )
            for report in all_reports
            if report.id not in existing_report_ids
        ]
        updated_report_jsons = [
            ReportInterface.to_json_response(
                report=r, editor_id_to_json={}, agency_name=r.source.name
            )
            for r in updated_reports
        ]
        unchanged_report_jsons = [
            ReportInterface.to_json_response(
                report=r, editor_id_to_json={}, agency_name=r.source.name
            )
            for r in unchanged_reports
        ]

        ingested_spreadsheet_json = SpreadsheetInterface.get_ingest_spreadsheet_json(
            metric_key_to_errors=metric_key_to_errors,
            metric_key_to_datapoint_jsons=metric_key_to_datapoint_jsons,
            metric_definitions=metric_definitions,
            metric_key_to_metric_interface=metric_key_to_metric_interface,
            updated_report_jsons=updated_report_jsons,
            new_report_jsons=new_report_jsons,
            unchanged_report_jsons=unchanged_report_jsons,
            spreadsheet=spreadsheet,
        )
        return ingested_spreadsheet_json

    @api_blueprint.route("/<spreadsheet_id>/bulk-upload-json", methods=["GET"])
    @auth_decorator
    def get_bulk_upload_json(spreadsheet_id: str) -> Response:
        """For a given uploaded spreadsheet, get the saved json
        that is stored in GCP bucket. The json contains both errors/warnings
        data as well as review data.
        """
        try:
            request_json = assert_type(request.values, dict)
            user = UserAccountInterface.get_user_by_auth0_user_id(
                session=current_session,
                auth0_user_id=get_auth0_user_id(request_dict=request_json),
            )
            spreadsheet = SpreadsheetInterface.get_spreadsheet_by_id(
                session=current_session, spreadsheet_id=int(spreadsheet_id)
            )
            agency_id = spreadsheet.agency_id
            raise_if_user_is_not_in_agency(user=user, agency_id=int(agency_id))

            json_location = spreadsheet.standardized_name.replace("xlsx", "json")

            # Download Errors/Warnings json from GCP
            # Source: https://cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.blob.Blob#google_cloud_storage_blob_Blob_download_as_bytes
            storage_client = storage.Client()
            if in_gcp_staging():
                bucket = storage_client.bucket(ERRORS_WARNINGS_JSON_BUCKET_STAGING)
            else:
                bucket = storage_client.bucket(ERRORS_WARNINGS_JSON_BUCKET_PROD)
            blob = bucket.blob(json_location)
            content_bytes = blob.download_as_bytes()
            contents = json.loads(content_bytes)

            return jsonify(contents)
        except Exception as e:
            raise _get_error(error=e) from e

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
                    spreadsheets=[spreadsheet], session=current_session, user=user
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

    @api_blueprint.route("/template/<agency_id>/<system>", methods=["GET"])
    @auth_decorator
    def get_bulk_upload_template(agency_id: str, system: str) -> response.Response:
        if system is None:
            return make_response(
                "No system was provided in the request body.",
                500,
            )
        if agency_id is None:
            return make_response(
                "No agency ID was provided in the request.",
                500,
            )
        is_single_page_template = request.args.get("singlePage") == "true"
        is_generic_template = request.args.get("isGeneric") == "true"

        system_enum = schema.System[system]

        with tempfile.TemporaryDirectory() as tempbulkdir:
            file_path = os.path.join(tempbulkdir, str(system) + ".xlsx")
            agency = AgencyInterface.get_agency_by_id(
                agency_id=int(agency_id), session=current_session
            )
            generate_bulk_upload_template(
                system=system_enum,
                file_path=file_path,
                session=current_session,
                agency=agency,
                is_single_page_template=is_single_page_template,
                is_generic_template=is_generic_template,
            )
            try:
                return send_file(file_path, as_attachment=True)
            except Exception as e:
                raise _get_error(error=e) from e

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

        metric_definitions = MetricInterface.get_metric_definitions_by_systems(
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
            metric_key_to_dimension_id_to_dimension_member_to_datapoints_json: (
                DefaultDict[
                    str, DefaultDict[str, DefaultDict[str, List[DatapointJson]]]
                ]
            ) = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
            for datapoint in datapoints:
                datapoint_json = DatapointInterface.to_json_response(
                    datapoint=datapoint,
                    is_published=True,
                    frequency=report_id_to_frequency[datapoint.report_id],
                )
                (
                    dimension_id,
                    dimension_member,
                ) = get_dimension_id_and_member(datapoint=datapoint)
                if dimension_id is not None and dimension_member is not None:
                    metric_key_to_dimension_id_to_dimension_member_to_datapoints_json[
                        datapoint.metric_definition_key
                    ][dimension_id][dimension_member].append(datapoint_json)
                else:
                    metric_key_to_aggregate_datapoints_json[
                        datapoint.metric_definition_key
                    ].append(datapoint_json)

            metrics = MetricSettingInterface.get_agency_metric_interfaces(
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

    @api_blueprint.route("/v2/agencies/<agency_slug>/published_data", methods=["GET"])
    def get_agency_published_data_v2(agency_slug: str) -> Response:
        # TODO(#22143): Deprecate get_agency_published_data (above) and replace with this
        # This endpoint should behave the same as the one above, except:
        # - It takes as input an agency slug, rather than id
        # - It has some performance improvements
        try:
            # Agency slug will be encoded (e.g "Washington%20Department%20of%20Corrections")
            agency_name = urllib.parse.unquote(agency_slug)
            agency = AgencyInterface.get_agency_by_name(
                session=current_session, name=agency_name, with_settings=True
            )
            if not agency:
                raise JusticeCountsServerError(
                    code="agency_not_found",
                    description=f"No agency exists with the slug {agency_slug}",
                )

            agency_id = agency.id
            reports = ReportInterface.get_reports_for_agency_dashboard(
                session=current_session,
                agency_id=agency_id,
            )
            report_ids = [report.id for report in reports]
            report_id_to_frequency = {
                report.id: ReportInterface.get_reporting_frequency(report)
                for report in reports
            }
            report_datapoints = (
                DatapointInterface.get_report_datapoints_for_agency_dashboard(
                    session=current_session,
                    report_ids=report_ids,
                )
            )

            # Group aggregate datapoints by metric key.
            metric_key_to_aggregate_datapoints_json: DefaultDict[
                str, List[DatapointJson]
            ] = defaultdict(list)

            # Group breakdown datapoints by metric key, disaggregation, and dimension.
            # e.g. map[LAW_ENFORCEMENT_ARRESTS][GENDER][MALE] = <datapoint containing number of male arrests>
            metric_key_to_dimension_id_to_dimension_member_to_datapoints_json: (
                DefaultDict[
                    str, DefaultDict[str, DefaultDict[str, List[DatapointJson]]]
                ]
            ) = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

            for datapoint in report_datapoints:
                if (
                    datapoint.is_report_datapoint is False
                    or datapoint.report_id is None
                ):
                    raise ValueError(
                        f"Expected get_report_datapoints_for_agency_dashboard to only return report datapoints. Instead, got a datapoint object with is_report_datapoint as {datapoint.is_report_datapoint} and report_id as {datapoint.report_id}."
                    )
                if datapoint.dimension_identifier_to_member is None:
                    dimension_id, dimension_member = None, None
                else:
                    (
                        dimension_id,
                        dimension_member,
                    ) = get_dimension_id_and_member(datapoint=datapoint)

                datapoint_json = DatapointInterface.to_json_response(
                    datapoint=datapoint,
                    is_published=True,
                    frequency=report_id_to_frequency[datapoint.report_id],
                )

                if dimension_id is not None and dimension_member is not None:
                    metric_key_to_dimension_id_to_dimension_member_to_datapoints_json[
                        datapoint.metric_definition_key
                    ][dimension_id][dimension_member].append(datapoint_json)
                else:
                    metric_key_to_aggregate_datapoints_json[
                        datapoint.metric_definition_key
                    ].append(datapoint_json)

            metrics = MetricSettingInterface.get_agency_metric_interfaces(
                session=current_session,
                agency=agency,
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

    @api_blueprint.route("/agencies", methods=["GET"])
    def get_agencies_metadata() -> Response:
        """Gets a list of all agencies and their metadata (agency ID, agency name and number of metrics published)
        to display in the Agency Dashboard's home page.
        """
        try:
            all_agencies = AgencyInterface.get_agencies(session=current_session)
            all_agencies_metadata = [
                {"name": agency.name, "id": agency.id} for agency in all_agencies
            ]
            all_agencies_ids = [agency["id"] for agency in all_agencies_metadata]

            reports = ReportInterface.get_reports_by_agency_ids(
                session=current_session,
                agency_ids=all_agencies_ids,
                include_datapoints=False,
                published_only=True,
            )

            agency_id_to_num_published_reports: Dict[int, int] = defaultdict(int)
            for report in reports:
                agency_id_to_num_published_reports[report.source_id] += 1

            for agency in all_agencies_metadata:
                agency[
                    "number_of_published_records"
                ] = agency_id_to_num_published_reports[agency["id"]]

            return jsonify(
                {
                    "agencies": all_agencies_metadata,
                }
            )

        except Exception as e:
            raise _get_error(error=e) from e

    @api_blueprint.route("/feed/<agency_id>", methods=["GET"])
    def get_csv_for_upload_data(agency_id: int) -> Response:
        """Returns a csv for an agency containing all of their uploaded data,
        formatted according to the Technical Specification."""

        return FeedInterface.get_csv_of_feed(
            session=current_session,
            agency_id=agency_id,
            metric=assert_type(request.args.get("metric"), str),
            include_unpublished_data=True,
            system=request.args.get("system"),
        )

    # Email Notifications

    @api_blueprint.route("/agency/<agency_id>/subscription/<user_id>", methods=["PUT"])
    def update_user_email_subscription(agency_id: int, user_id: int) -> Response:
        """Updates a user's subscription to emails in a given agency"""
        try:
            request_dict = assert_type(request.json, dict)
            is_subscribed = assert_type(request_dict.get("is_subscribed"), bool)
            days_after_time_period_to_send_email = request_dict.get(
                "days_after_time_period_to_send_email"
            )

            association = AgencyUserAccountAssociationInterface.get_association_by_ids(
                agency_id=agency_id,
                user_account_id=user_id,
                session=current_session,
            )

            # Update subscription status
            association.subscribed = is_subscribed
            if days_after_time_period_to_send_email is not None:

                if days_after_time_period_to_send_email < 0:
                    raise JusticeCountsServerError(
                        code="negative_days_after_time_period_to_send_email",
                        description="You cannot have a negative value for days_after_time_period_to_send_email.",
                    )

                association.days_after_time_period_to_send_email = assert_type(
                    days_after_time_period_to_send_email, int
                )

            current_session.commit()

            return jsonify({"status": "ok", "status_code": HTTPStatus.OK})

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


def _get_child_agency_json(agency: schema.Agency) -> List[Dict[str, Any]]:
    """If agency is a superagency, fetches child agencies and returns a
    an array of (simple) JSON representations. If agency is not a superagency,
    returns an empty array.
    """

    if not agency.is_superagency:
        return []

    child_agencies = AgencyInterface.get_child_agencies_for_agency(
        session=current_session, agency=agency
    )
    return [child_agency.to_json_simple() for child_agency in child_agencies]
