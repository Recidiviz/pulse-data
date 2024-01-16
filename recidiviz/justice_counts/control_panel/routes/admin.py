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

from auth0.exceptions import Auth0Error
from flask import Blueprint, Response, jsonify, make_response, request
from flask_sqlalchemy_session import current_session
from sqlalchemy.dialects.postgresql import insert

from recidiviz.auth.auth0_client import Auth0Client
from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.agency_user_account_association import (
    AgencyUserAccountAssociationInterface,
)
from recidiviz.justice_counts.datapoint import DatapointInterface
from recidiviz.justice_counts.exceptions import JusticeCountsServerError
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.justice_counts.utils.constants import VALID_SYSTEMS
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.utils.types import assert_type


def get_admin_blueprint(
    auth_decorator: Callable,
    auth0_client: Optional[Auth0Client] = None,
) -> Blueprint:
    """API methods for Publisher Admin Panel"""
    admin_blueprint = Blueprint("admin", __name__)

    # UserAccount
    @admin_blueprint.route("/user", methods=["GET"])
    @auth_decorator
    def fetch_all_users() -> Response:
        """
        Fetches all users and their metadata to display the User Provisioning Table
        """
        db_users = UserAccountInterface.get_users(session=current_session)
        # AgencyUserAccountAssocs are loaded automatically when we
        # query for a user in the UserAccount table.

        agencies = AgencyInterface.get_agencies(session=current_session)
        agencies_by_id = {agency.id: agency for agency in agencies}

        user_json = []
        for user in db_users:
            user_agencies = list(
                filter(
                    None,
                    [
                        agencies_by_id.get(assoc.agency_id)
                        for assoc in user.agency_assocs
                    ],
                )
            )
            user_json.append(user.to_json(agencies=user_agencies))
        return jsonify({"users": user_json})

    @admin_blueprint.route("/user/<user_id>", methods=["GET"])
    @auth_decorator
    def get_user(user_id: int) -> Response:
        """Returns metadata for an individual user."""
        user = UserAccountInterface.get_user_by_id(
            session=current_session, user_account_id=user_id
        )
        agencies = [assoc.agency for assoc in user.agency_assocs]
        return jsonify(user.to_json(agencies=agencies))

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

            if user is None:
                # If there is no existing user, create user in auth0
                try:
                    auth0_user = auth0_client.create_JC_user(name=name, email=email)
                    auth0_user_id = auth0_user["user_id"]

                except Auth0Error as e:
                    if e.message == "The user already exists.":
                        auth0_users = auth0_client.get_all_users_by_email_addresses(
                            email_addresses=[email]
                        )
                        auth0_user = auth0_users[0]
                        auth0_user_id = auth0_user["user_id"]
                    else:
                        raise e

            # Create/Update user in our DB
            user = UserAccountInterface.create_or_update_user(
                session=current_session,
                name=name,
                auth0_user_id=auth0_user_id,
                email=email,
            )

            agencies = AgencyInterface.get_agencies_by_id(
                session=current_session, agency_ids=agency_ids
            )

            curr_agencies = {assoc.agency_id for assoc in user.agency_assocs}
            agency_ids_to_add = {id for id in agency_ids if id not in curr_agencies}
            agencies_ids_to_remove = [
                id for id in curr_agencies if id not in agency_ids
            ]

            # Add user to agencies
            if len(agency_ids_to_add) > 0:
                agencies_to_add = [a for a in agencies if a.id in agency_ids_to_add]
                UserAccountInterface.add_or_update_user_agency_association(
                    session=current_session,
                    user=user,
                    agencies=agencies_to_add,
                )

            # Remove user from agencies
            if len(agencies_ids_to_remove) > 0:
                UserAccountInterface.remove_user_from_agencies(
                    session=current_session,
                    user=user,
                    agency_ids=agencies_ids_to_remove,
                )

            user_jsons.append(user.to_json(agencies=agencies))

        current_session.commit()
        return jsonify(
            {
                "users": user_jsons,
            }
        )

    # Agency
    @admin_blueprint.route("/agency", methods=["GET"])
    @auth_decorator
    def fetch_all_agencies() -> Response:
        """
        Fetches all agencies and their metadata to display the Agency Provisioning Table
        """
        agencies = AgencyInterface.get_agencies(
            session=current_session, with_users=True, with_settings=False
        )

        agency_jsons: List[Dict[str, Any]] = []

        super_agency_id_to_child_agency_ids = defaultdict(list)
        for agency in agencies:
            if agency.super_agency_id is not None:
                super_agency_id_to_child_agency_ids[agency.super_agency_id].append(
                    agency.id
                )

        for agency in agencies:
            agency_json = agency.to_json(with_team=True, with_settings=False)
            agency_json["child_agency_ids"] = super_agency_id_to_child_agency_ids.get(
                agency.id, []
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

    @admin_blueprint.route("/agency/<agency_id>", methods=["GET"])
    @auth_decorator
    def get_agency(agency_id: int) -> Response:
        """Returns metadata for an individual agency."""
        agency = AgencyInterface.get_agency_by_id(
            session=current_session, agency_id=agency_id
        )

        child_agency_ids = AgencyInterface.get_child_agency_ids_for_agency(
            session=current_session, agency=agency
        )

        agency_json = agency.to_json(with_team=True)
        agency_json["child_agency_ids"] = child_agency_ids
        return jsonify(
            {
                "agency": agency_json,
                # also send list of possible roles to use in the dropdown
                # when users can assign a new role to user.
                "roles": [role.value for role in schema.UserAccountRole],
            }
        )

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

        if request_json.get("child_agency_ids") is not None:
            # Update child agencies
            curr_child_agencies = AgencyInterface.get_child_agencies_for_agency(
                session=current_session, agency=agency
            )
            curr_child_agency_ids = {a.id for a in curr_child_agencies}

            agency_ids_to_add = [
                id
                for id in request_json.get("child_agency_ids", [])
                if id not in curr_child_agency_ids
            ]

            agencies_to_remove = [
                a
                for a in curr_child_agencies
                if a.id not in request_json.get("child_agency_ids", [])
            ]

            AgencyUserAccountAssociationInterface.add_child_agencies_to_super_agency(
                session=current_session,
                super_agency_id=agency.id,
                child_agency_ids=agency_ids_to_add,
            )

            AgencyUserAccountAssociationInterface.remove_child_agencies_from_super_agency(
                session=current_session,
                super_agency_id=agency.id,
                child_agencies=agencies_to_remove,
            )

        # Update `is_superagency` after editing the child agencies because if we
        # call AgencyInterface.get_child_agencies_for_agency after `is_superagency`
        # has been flipped to False, the helper function will return an empty list
        # and the child agencies will not be updated.
        agency.is_superagency = request_json["is_superagency"]

        if request_json.get("team") is not None:
            # Add users to agency

            # Prepare all the values that should be "upserted" to the DB
            values = []
            user_account_ids = set()
            for user_json in request_json.get("team", []):
                user_account_ids.add(user_json.get("user_account_id"))
                value = {
                    "agency_id": agency.id,
                    "user_account_id": user_json.get("user_account_id"),
                }

                if user_json.get("role") is not None:
                    value["role"] = schema.UserAccountRole[user_json.get("role")]

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
            for assoc in agency.user_account_assocs:
                if assoc.user_account_id not in user_account_ids:
                    current_session.delete(assoc)

        current_session.commit()
        child_agency_ids = AgencyInterface.get_child_agency_ids_for_agency(
            session=current_session, agency=agency
        )
        agency_json = agency.to_json()
        agency_json["child_agency_ids"] = child_agency_ids
        return jsonify(agency_json)

    @admin_blueprint.route(
        "/agency/<super_agency_id>/child-agency/copy", methods=["POST"]
    )
    @auth_decorator
    def copy_metric_config_to_child_agencies(super_agency_id: int) -> Response:
        """Copies metric settings from a super agency to its child agency."""
        request_json = assert_type(request.json, dict)
        metric_definition_key_subset = request_json.get(
            "metric_definition_key_subset", []
        )
        super_agency = AgencyInterface.get_agency_by_id(
            session=current_session, agency_id=super_agency_id
        )
        child_agencies = AgencyInterface.get_child_agencies_by_agency_ids(
            session=current_session, agency_ids=[super_agency_id]
        )
        super_agency_metric_settings = DatapointInterface.get_metric_settings_by_agency(
            session=current_session,
            agency=super_agency,
        )

        for child_agency in child_agencies:
            for metric_setting in super_agency_metric_settings:
                if "ALL" in set(
                    metric_definition_key_subset
                ) or metric_setting.metric_definition.key in set(
                    metric_definition_key_subset
                ):
                    DatapointInterface.add_or_update_agency_datapoints(
                        session=current_session,
                        agency=child_agency,
                        agency_metric=metric_setting,
                    )

        current_session.commit()
        return jsonify({"status": "ok", "status_code": HTTPStatus.OK})

    return admin_blueprint
