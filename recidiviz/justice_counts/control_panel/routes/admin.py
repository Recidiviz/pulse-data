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
from http import HTTPStatus
from typing import Callable

from flask import Blueprint, Response, jsonify, request
from flask_sqlalchemy_session import current_session
from sqlalchemy.dialects.postgresql import insert

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.agency_user_account_association import (
    AgencyUserAccountAssociationInterface,
)
from recidiviz.justice_counts.datapoint import DatapointInterface
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.utils.types import assert_type


def get_admin_blueprint(
    auth_decorator: Callable,
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
        return jsonify(
            {
                "agencies": [
                    agency.to_json(with_team=True, with_settings=False)
                    for agency in agencies
                ],
                # also send list of possible systems to use in the dropdown
                # when users can assign a system role to an agency.
                "systems": [enum.value for enum in schema.System],
            }
        )

    @admin_blueprint.route("/agency/<agency_id>", methods=["GET"])
    @auth_decorator
    def get_agency(agency_id: int) -> Response:
        """Returns metadata for an individual agency."""
        return jsonify(
            {
                "agency": AgencyInterface.get_agency_by_id(
                    session=current_session, agency_id=agency_id
                ).to_json(),
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
            systems=[schema.System[system] for system in systems],
            state_code=state_code,
            fips_county_code=request_json.get("fips_county_code"),
            is_superagency=request_json.get("is_superagency", None),
            super_agency_id=request_json.get("super_agency_id"),
        )

        if request_json.get("child_agency_ids") is not None:
            # Add child agencies
            AgencyUserAccountAssociationInterface.add_child_agencies_to_super_agency(
                session=current_session,
                super_agency_id=agency.id,
                child_agency_ids=request_json.get("child_agency_ids", []),
            )
        if request_json.get("team") is not None:
            # Add users to agency

            # Prepare all the values that should be "upserted" to the DB
            values = []
            for user_json in request_json.get("team", []):
                value = {
                    "agency_id": agency.id,
                    "user_account_id": user_json.get("id"),
                }

                if user_json.get("role") is not None:
                    value["role"] = schema.UserAccountRole[user_json.get("role")]

                values.append(value)

            insert_statement = insert(schema.AgencyUserAccountAssociation).values(
                values
            )

            insert_statement = insert_statement.on_conflict_do_update(
                constraint="agency_user_account_association_pkey",
                set_={"role": insert_statement.excluded.role},
            )

            current_session.execute(insert_statement)
        current_session.commit()
        return jsonify(
            {
                "agency": agency.to_json(),
            }
        )

    @admin_blueprint.route(
        "/agency/<super_agency_id>/child-agency/copy", methods=["POST"]
    )
    @auth_decorator
    def copy_metric_config_to_child_agencies(super_agency_id: int) -> Response:
        """Copies metric settings from a super agency to its child agency."""
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
                DatapointInterface.add_or_update_agency_datapoints(
                    session=current_session,
                    agency=child_agency,
                    agency_metric=metric_setting,
                )

        current_session.commit()
        return jsonify({"status": "ok", "status_code": HTTPStatus.OK})

    return admin_blueprint
