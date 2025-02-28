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
"""Implements api routes for the Justice Counts Publisher Admin Panel."""
from collections import defaultdict
from http import HTTPStatus
from typing import Any, Callable, Dict, List, Optional

from flask import Blueprint, Response, jsonify, make_response, request
from google.cloud import run_v2
from sqlalchemy import or_
from sqlalchemy.dialects.postgresql import insert

from recidiviz.auth.auth0_client import Auth0Client
from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.agency_setting import AgencySettingInterface
from recidiviz.justice_counts.agency_user_account_association import (
    AgencyUserAccountAssociationInterface,
)
from recidiviz.justice_counts.exceptions import JusticeCountsServerError
from recidiviz.justice_counts.metric_setting import MetricSettingInterface
from recidiviz.justice_counts.metrics.metric_interface import MetricInterface
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.justice_counts.utils.agency_utils import delete_agency
from recidiviz.justice_counts.utils.constants import (
    COPY_SUPERAGENCY_METRIC_SETTINGS_TO_CHILD_AGENCIES_JOB_NAME,
    VALID_SYSTEMS,
)
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.sqlalchemy_flask_utils import current_session
from recidiviz.utils.environment import (
    GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
    GCP_PROJECT_JUSTICE_COUNTS_STAGING,
    in_gcp_production,
)
from recidiviz.utils.string import StrictStringFormatter
from recidiviz.utils.types import assert_type

_auth0_client = None


def _get_auth0_client() -> Auth0Client:
    """Returns a Justice Counts Auth0 client, lazily generating one if we have not
    already.
    """
    global _auth0_client
    if not _auth0_client:
        _auth0_client = Auth0Client(  # nosec
            domain_secret_name="justice_counts_auth0_api_domain",
            client_id_secret_name="justice_counts_auth0_api_client_id",
            client_secret_secret_name="justice_counts_auth0_api_client_secret",
        )
    return _auth0_client


def get_admin_blueprint(
    auth_decorator: Callable,
    auth0_client: Optional[Auth0Client] = None,
) -> Blueprint:
    """API methods for Publisher Admin Panel"""
    admin_blueprint = Blueprint("admin", __name__)

    # UserAccount
    @admin_blueprint.route("/user/overview", methods=["GET"])
    @auth_decorator
    def fetch_users_overview() -> Response:
        """
        Return the user fields necessary to render the admin panel's User Provisioning
        Table Overview.

        Do not return user-agency associations for all agencies since this is very slow.
        User-agency associations will be fetched on-demand when a User Panel is opened.
        """
        users_overview_json = UserAccountInterface.get_users_overview(
            session=current_session
        )
        return jsonify({"users": users_overview_json})

    @admin_blueprint.route("/user/<user_id>", methods=["GET"])
    @auth_decorator
    def get_user(user_id: int) -> Response:
        """Returns metadata for an individual user."""
        user = UserAccountInterface.get_user_by_id(
            session=current_session, user_account_id=user_id
        )
        agencies = [assoc.agency for assoc in user.agency_assocs]
        return jsonify(user.to_json(agencies=agencies))

    @admin_blueprint.route("/user/<user_id>/agencies", methods=["GET"])
    @auth_decorator
    def get_user_agencies(user_id: int) -> Response:
        """Returns the agencies that a user has access to. This will be called every
        time a User Panel is opened."""
        user = UserAccountInterface.get_user_by_id(
            session=current_session, user_account_id=user_id
        )
        agencies = [assoc.agency for assoc in user.agency_assocs]
        user_json = user.to_json(agencies=agencies)
        return jsonify({"agencies": user_json["agencies"]})

    @admin_blueprint.route("/user/<user_id>", methods=["DELETE"])
    @auth_decorator
    def delete_user(user_id: int) -> Response:
        """Erases an individual user."""
        if auth0_client is None:
            return make_response(
                "auth0_client could not be initialized. Environment is not development or gcp.",
                500,
            )

        user = UserAccountInterface.get_user_by_id(
            session=current_session, user_account_id=user_id
        )
        agencies = [assoc.agency for assoc in user.agency_assocs]

        # Delete all AgencyUserAccountAssociation entries corresponding to the user.
        AgencyUserAccountAssociationInterface.delete_agency_user_acccount_associations_for_user(
            session=current_session, user_account_id=user.id
        )

        # Nullify all user_account_id fields in DatapointHistory that are associated
        # with the user.
        current_session.query(schema.DatapointHistory).filter(
            schema.DatapointHistory.user_account_id == user.id
        ).update({"user_account_id": None})

        # Nullify all uploaded_by fields in Spreadsheet that are associated
        # with the user.
        current_session.query(schema.Spreadsheet).filter(
            schema.Spreadsheet.uploaded_by == user.auth0_user_id
        ).update({"uploaded_by": None})

        # Delete the user's auth. Must by done before deleting the user from UserAccount.
        auth0_client.delete_JC_user(user_id=user.auth0_user_id)

        # Delete the user's UserAccount entry.
        current_session.delete(user)

        current_session.commit()
        return jsonify(user.to_json(agencies=agencies))

    @admin_blueprint.route("/user", methods=["PUT"])
    @auth_decorator
    def create_or_update_users() -> Response:
        """
        Creates/Updates users.
        """
        if auth0_client is None:
            return make_response(
                "auth0_client could not be initialized. Environment is not development or gcp.",
                500,
            )

        user_jsons: List[Dict[str, Any]] = []
        request_json = assert_type(request.json, dict)
        user_request_list = assert_type(request_json.get("users"), list)

        for user_request in user_request_list:
            name = assert_type(user_request.get("name"), str)
            email = assert_type(user_request.get("email"), str)

            # If a user is being updated, user_account_id WILL NOT be None.
            # If the user is being created, user_account_id WILL be None.
            user_account_id = user_request.get("user_account_id")
            agency_ids = assert_type(user_request.get("agency_ids"), list)

            user = UserAccountInterface.get_user_by_email(
                session=current_session, email=email
            )

            if user_account_id is None and user is not None:
                raise JusticeCountsServerError(
                    code="user_already_exists",
                    description=f"User with email '{email}' already exists",
                )

            auth0_user_id = user.auth0_user_id if user is not None else None

            # Create/Update user in our DB
            user = UserAccountInterface.create_or_update_user(
                session=current_session,
                name=name,
                auth0_user_id=auth0_user_id,
                email=email,
                auth0_client=auth0_client,
            )

            agencies = AgencyInterface.get_agencies_by_id(
                session=current_session, agency_ids=agency_ids
            )

            curr_agencies = {assoc.agency_id for assoc in user.agency_assocs}
            agency_ids_to_add = {id for id in agency_ids if id not in curr_agencies}
            child_agencies_to_add = AgencyInterface.get_child_agencies_by_agency_ids(
                session=current_session, agency_ids=list(agency_ids_to_add)
            )

            agencies_ids_to_remove = [
                id for id in curr_agencies if id not in agency_ids
            ]
            child_agencies_to_remove = AgencyInterface.get_child_agencies_by_agency_ids(
                session=current_session, agency_ids=list(agencies_ids_to_remove)
            )

            # Add user to agencies
            if len(agency_ids_to_add) > 0:
                agencies_to_add = set(
                    [a for a in agencies if a.id in agency_ids_to_add]
                    + child_agencies_to_add
                )
                UserAccountInterface.add_or_update_user_agency_association(
                    session=current_session,
                    user=user,
                    agencies=list(agencies_to_add),
                )

            # Remove user from agencies
            if len(agencies_ids_to_remove) > 0:
                UserAccountInterface.remove_user_from_agencies(
                    session=current_session,
                    user=user,
                    agency_ids=agencies_ids_to_remove
                    + [a.id for a in child_agencies_to_remove],
                )

            user_jsons.append(user.to_json(agencies=agencies))

        current_session.commit()
        return jsonify(
            {
                "users": user_jsons,
            }
        )

    # Agency
    @admin_blueprint.route("/agency/overview", methods=["GET"])
    @auth_decorator
    def fetch_agencies_overview() -> Response:
        """
        Return the agency fields necessary to render the admin panel's Agency
        Provisioning Table Overview.

        Do not return user-agency associations for all agencies since this is very slow.
        Instead, associations will be fetched on-demand when an Agency Panel is opened.
        """
        agencies = AgencyInterface.get_agencies(
            session=current_session, with_users=False, with_settings=True
        )
        super_agency_id_to_child_agency_ids = defaultdict(list)
        for agency in agencies:
            if agency.super_agency_id is not None:
                super_agency_id_to_child_agency_ids[agency.super_agency_id].append(
                    agency.id
                )

        agency_jsons: List[Dict[str, Any]] = []
        for agency in agencies:
            agency_json = agency.to_json(with_team=False, with_settings=False)
            agency_json["child_agency_ids"] = super_agency_id_to_child_agency_ids.get(
                agency.id, []
            )
            agency_setting_type_to_value = {
                a.setting_type: a.value for a in agency.agency_settings
            }
            agency_json["agency_url"] = agency_setting_type_to_value.get(
                schema.AgencySettingType.HOMEPAGE_URL.value
            )
            agency_json["agency_description"] = agency_setting_type_to_value.get(
                schema.AgencySettingType.PURPOSE_AND_FUNCTIONS.value
            )
            agency_jsons.append(agency_json)

        return jsonify(
            {
                "agencies": agency_jsons,
                # also send list of possible systems to use in the dropdown
                # when users can assign a system role to an agency.
                "systems": [enum.value for enum in VALID_SYSTEMS],
            }
        )

    @admin_blueprint.route("/agency/<agency_id>/team", methods=["GET"])
    @auth_decorator
    def get_agency_team(agency_id: int) -> Response:
        """Returns just team data for an individual agency. This will be called whenever
        an Agency Panel is opened."""
        agency = AgencyInterface.get_agency_by_id(
            session=current_session,
            agency_id=agency_id,
            with_users=True,
            with_settings=False,
        )
        agency_json = agency.to_json(with_team=True, with_settings=False)
        return jsonify(
            {
                "team": agency_json["team"],
            }
        )

    @admin_blueprint.route("/agency/<agency_id>", methods=["GET"])
    @auth_decorator
    def get_agency(agency_id: int) -> Response:
        """Returns metadata for an individual agency."""
        agency = AgencyInterface.get_agency_by_id(
            session=current_session, agency_id=agency_id
        )

        metrics = []
        # Pull the list of metrics that the agency has done configuration for
        # This list will be shown on the frontend so the user can choose which
        # metric settings to copy over to child agencies
        metric_settings = MetricSettingInterface.get_agency_metric_interfaces(
            session=current_session,
            agency=agency,
        )
        for setting in metric_settings:
            metrics.append(
                {
                    "key": setting.key,
                    "name": setting.metric_definition.display_name,
                    "sector": setting.metric_definition.system.name.replace("_", " "),
                }
            )
        agency_json = agency.to_json(with_team=False, with_settings=False)

        return jsonify(
            {
                "agency": agency_json,
                "metrics": metrics,
            }
        )

    @admin_blueprint.route("/agency/<agency_id>", methods=["DELETE"])
    @auth_decorator
    def delete_agency_endpoint(agency_id: int) -> Response:
        """Deletes an individual agency."""
        if auth0_client is None:
            return make_response(
                "auth0_client could not be initialized. Environment is not development or gcp.",
                500,
            )
        agency_json = delete_agency(
            session=current_session, agency_id=agency_id, dry_run=False
        )
        return jsonify(agency_json)

    @admin_blueprint.route("/agency", methods=["PUT"])
    @auth_decorator
    def create_or_update_agency() -> Response:
        """
        Creates/Updates an Agency.
        """
        request_json = assert_type(request.json, dict)
        name = assert_type(request_json.get("name"), str)
        state_code = assert_type(request_json.get("state_code"), str)
        systems = assert_type(request_json.get("systems"), list)
        agency = AgencyInterface.create_or_update_agency(
            session=current_session,
            name=name,
            agency_id=request_json.get("agency_id"),
            systems=[schema.System[system] for system in systems],
            state_code=state_code,
            fips_county_code=request_json.get("fips_county_code"),
            super_agency_id=request_json["super_agency_id"],
            is_dashboard_enabled=request_json["is_dashboard_enabled"],
            with_users=True,
        )

        child_agencies = []
        if request_json.get("child_agency_ids") is not None:
            # Get the current child agencies for the given agency
            curr_child_agencies = AgencyInterface.get_child_agencies_for_agency(
                session=current_session, agency=agency
            )
            curr_child_agency_ids = {a.id for a in curr_child_agencies}

            # Get the list of child agencies specified in the request
            child_agencies = AgencyInterface.get_agencies_by_id(
                session=current_session,
                agency_ids=request_json.get("child_agency_ids", []),
            )
            new_child_agency_ids = {a.id for a in child_agencies}

            # Determine which agencies need to be added and removed
            ids_to_add = new_child_agency_ids - curr_child_agency_ids
            ids_to_remove = curr_child_agency_ids - new_child_agency_ids

            # Process ids_to_add: update their super_agency_id to the current agency's ID
            for child_agency in child_agencies:
                if child_agency.id in ids_to_add:
                    child_agency.super_agency_id = agency.id

            # Process ids_to_remove: update their super_agency_id to None or another value as needed
            for curr_child_agency in curr_child_agencies:
                if curr_child_agency.id in ids_to_remove:
                    curr_child_agency.super_agency_id = None

        # Update `is_superagency` after editing the child agencies because if we
        # call AgencyInterface.get_child_agencies_for_agency after `is_superagency`
        # has been flipped to False, the helper function will return an empty list
        # and the child agencies will not be updated.
        agency.is_superagency = request_json["is_superagency"]

        if request_json.get("super_agency_id") is not None:
            AgencyUserAccountAssociationInterface.add_child_agencies_to_super_agency_and_copy_users(
                session=current_session,
                child_agency_ids=[agency.id],
                super_agency_id=request_json["super_agency_id"],
            )
            current_session.refresh(agency)

        if request_json.get("agency_description") is not None:
            AgencySettingInterface.create_or_update_agency_setting(
                session=current_session,
                agency_id=agency.id,
                setting_type=schema.AgencySettingType.PURPOSE_AND_FUNCTIONS,
                value=request_json.get("agency_description"),
            )

        if request_json.get("agency_url") is not None:
            AgencySettingInterface.create_or_update_agency_setting(
                session=current_session,
                agency_id=agency.id,
                setting_type=schema.AgencySettingType.HOMEPAGE_URL,
                value=request_json.get("agency_url"),
            )

        if request_json.get("team") is not None:
            # Prepare all the values that should be "upserted" to the DB
            values = []
            user_account_ids = set()
            agency_ids = [agency.id] + request_json.get("child_agency_ids", [])
            for user_json in request_json.get("team", []):
                user_account_ids.add(user_json.get("user_account_id"))
                for agency_id in agency_ids:
                    value = {
                        "agency_id": agency_id,
                        "user_account_id": user_json.get("user_account_id"),
                        "role": (
                            schema.UserAccountRole[user_json.get("role")]
                            if user_json.get("role")
                            else None
                        ),
                    }
                    values.append(value)
            if len(values) > 0:
                insert_statement = insert(schema.AgencyUserAccountAssociation).values(
                    values
                )

                insert_statement = insert_statement.on_conflict_do_update(
                    constraint="agency_user_account_association_pkey",
                    set_={"role": insert_statement.excluded.role},
                )

                current_session.execute(insert_statement)

            # Delete team members that are in the agency's assocs, but not in the
            # list of team members that are sent over.
            # NOTE: Just look at the agency when doing this, not the child agencies.
            # If a team member is missing from the superagency but present
            # in the child agency, that's fine! Don't delete.
            for assoc in agency.user_account_assocs:
                if assoc.user_account_id not in user_account_ids:
                    current_session.delete(assoc)

        current_session.commit()
        # Re-fetching so that agency and users are associated in this session.
        # The commit will clear assocs from session and we need the agency users
        # in the JSON response.
        agency = AgencyInterface.get_agency_by_id(
            session=current_session,
            agency_id=agency.id,
            with_users=True,
        )

        agency_json = agency.to_json()

        url_agency_setting = AgencySettingInterface.get_agency_setting_by_setting_type(
            session=current_session,
            agency_id=agency.id,
            setting_type=schema.AgencySettingType.HOMEPAGE_URL,
        )

        description_setting = AgencySettingInterface.get_agency_setting_by_setting_type(
            session=current_session,
            agency_id=agency.id,
            setting_type=schema.AgencySettingType.PURPOSE_AND_FUNCTIONS,
        )

        agency_json["agency_url"] = (
            url_agency_setting.value if url_agency_setting is not None else None
        )
        agency_json["agency_description"] = (
            description_setting.value if description_setting is not None else None
        )

        agency_json["child_agency_ids"] = [a.id for a in child_agencies]
        return jsonify(agency_json)

    @admin_blueprint.route(
        "/agency/<super_agency_id>/child-agency/copy", methods=["POST"]
    )
    @auth_decorator
    def copy_metric_config_to_child_agencies(super_agency_id: int) -> Response:
        """Copies metric settings from a super agency to its child agencies via a Cloud Run Job."""
        request_json = assert_type(request.json, dict)
        agency_name = assert_type(request_json.get("agency_name"), str)
        user_email = assert_type(request_json.get("user_email"), str)
        metric_definition_key_subset = (
            assert_type(request_json.get("metric_definition_key_subset"), list) or []
        )
        child_agency_id_subset_raw = request_json.get("child_agency_id_subset")
        if child_agency_id_subset_raw is not None:
            child_agency_id_subset = assert_type(
                request_json.get("child_agency_id_subset"), list
            )
            child_agency_id_subset_as_string = ",".join(child_agency_id_subset)
        else:
            child_agency_id_subset_as_string = None

        metric_definition_key_subset_as_string = ",".join(metric_definition_key_subset)
        project_id = (
            GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION
            if in_gcp_production() is True
            else GCP_PROJECT_JUSTICE_COUNTS_STAGING
        )

        # Trigger a cloud run job with container overrides to handle the copying of metric
        # settings and emailing the user a confirmation when the job is complete.
        client = run_v2.JobsClient()

        # Add container overrides to pass in custom arguments to the job script
        args = [
            "run",
            "python",
            "-m",
            "recidiviz.justice_counts.jobs.copy_superagency_metric_settings_to_child_agencies",
            "--super_agency_id",
            str(super_agency_id),
            "--agency_name",
            str(agency_name),
            "--metric_definition_key_subset",
            metric_definition_key_subset_as_string,
            "--user_email",
            str(user_email),
        ]
        if child_agency_id_subset_as_string is not None:
            args.extend(["--child_agency_id_subset", child_agency_id_subset_as_string])

        job_request = run_v2.RunJobRequest(
            name=StrictStringFormatter().format(
                COPY_SUPERAGENCY_METRIC_SETTINGS_TO_CHILD_AGENCIES_JOB_NAME,
                project_id=project_id,
            ),
            overrides={"container_overrides": [{"args": args}]},
        )

        # Trigger cloud run job
        client.run_job(request=job_request)

        current_session.commit()
        return jsonify({"status": "ok", "status_code": HTTPStatus.OK})

    @admin_blueprint.route("/agency/<agency_id>/reporting-agency", methods=["GET"])
    @auth_decorator
    def get_agency_reporting_agencies(agency_id: int) -> Response:
        """Fetch reporting agency options, agencies reporting metrics, and metric details for a specified agency."""

        agency = AgencyInterface.get_agency_by_id(
            session=current_session, agency_id=agency_id
        )

        metric_definitions = MetricInterface.get_metric_definitions_by_systems(
            systems={schema.System[system] for system in agency.systems or []},
        )

        metric_key_to_metric_interface = (
            MetricSettingInterface.get_metric_key_to_metric_interface(
                session=current_session, agency=agency
            )
        )

        # Query for agencies that can report metrics for the selected agency:
        # 1. Central State Government (CSG)
        # 2. Vendors
        # 3. Super agency (if applicable)
        reporting_agencies = (
            current_session.query(schema.Source)
            .filter(
                or_(
                    schema.Source.type == "csg",
                    schema.Source.type == "vendor",
                    schema.Source.id == agency.super_agency_id,
                )
            )
            .order_by(schema.Source.type)
            .all()
        )

        # Build a list of reporting agency options, categorized by type (CSG, vendor, super agency).
        # This will be used to populate a dropdown or selection field in the UI.
        reporting_agency_options_json = [
            {
                "reporting_agency_name": a.name,
                "reporting_agency_id": a.id,
                "category": a.type.upper(),
            }
            for a in reporting_agencies
        ]

        metric_json = defaultdict(list)
        system_to_reporting_agency_metadata = defaultdict(list)
        reporting_agency_id_to_agency_name = {a.id: a.name for a in reporting_agencies}

        for definition in metric_definitions:
            # Collect metric details by system for use in the response JSON.
            metric_json[definition.system.name].append(
                {"key": definition.key, "name": definition.display_name}
            )

            # For each metric, gather the reporting agency's information, including name and ID,
            # and whether the metric is self-reported by the agency.
            metric_interface = metric_key_to_metric_interface.get(definition.key)
            system_to_reporting_agency_metadata[definition.system.name].append(
                {
                    "metric_key": definition.key,
                    "reporting_agency_id": (
                        metric_interface.reporting_agency_id
                        if metric_interface is not None
                        else None
                    ),
                    "reporting_agency_name": (
                        reporting_agency_id_to_agency_name.get(
                            metric_interface.reporting_agency_id
                        )
                        if metric_interface is not None
                        else None
                    ),
                    "is_self_reported": (
                        metric_interface.is_self_reported
                        if metric_interface is not None
                        else None
                    ),
                }
            )

        # Return a JSON response containing:
        # 1. `reporting_agency_options`: Available agencies that can report metrics.
        # 2. `reporting_agencies`: Mapping of each system's metrics to the responsible agency.
        # 3. `metrics`: List of metrics grouped by system.
        return jsonify(
            {
                "reporting_agency_options": reporting_agency_options_json,
                "reporting_agencies": system_to_reporting_agency_metadata,
                "metrics": metric_json,
            }
        )

    @admin_blueprint.route("/vendors", methods=["GET"])
    @auth_decorator
    def get_vendors() -> Response:
        """
        Fetch all vendors and CSG with their respective homepage URLs.

        Returns:
            A list of dictionaries with id, name, and URL for vendors and CSG.
        """

        vendors = AgencyInterface.get_vendors(session=current_session)

        results = []
        # Add vendors
        for vendor in vendors:
            agency_setting_type_to_value = {
                setting.setting_type: setting.value
                for setting in vendor.agency_settings
            }
            url = agency_setting_type_to_value.get(
                schema.AgencySettingType.HOMEPAGE_URL.value
            )
            results.append({"id": vendor.id, "name": vendor.name, "url": url})

        return jsonify(results)

    return admin_blueprint
