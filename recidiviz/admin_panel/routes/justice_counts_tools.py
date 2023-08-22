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
"""Defines routes for the Justice Counts API endpoints in the admin panel."""

from http import HTTPStatus
from typing import List, Tuple

from flask import Blueprint, Response, jsonify, request
from psycopg2.errors import UniqueViolation  # pylint: disable=no-name-in-module
from sqlalchemy.exc import IntegrityError

from recidiviz.auth.auth0_client import Auth0Client
from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.agency_user_account_association import (
    AgencyUserAccountAssociationInterface,
)
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.persistence.database.constants import JUSTICE_COUNTS_DB_SECRET_PREFIX
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.environment import in_gcp_production
from recidiviz.utils.types import assert_type, non_optional

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


def add_justice_counts_tools_routes(bp: Blueprint) -> None:
    """Adds the relevant Justice Counts Admin Panel API routes to an input Blueprint."""

    # Agency
    @bp.route("/api/justice_counts_tools/agencies", methods=["POST"])
    @requires_gae_auth
    def create_agency() -> Tuple[Response, HTTPStatus]:
        """Creates an Agency and returns the created Agency.
        Returns an error message if the Agency already exists with that name.
        """
        try:
            with SessionFactory.using_database(
                database_key=SQLAlchemyDatabaseKey.for_schema(
                    SchemaType.JUSTICE_COUNTS
                ),
                secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
            ) as session:
                request_json = assert_type(request.json, dict)
                name = assert_type(request_json.get("name"), str)
                systems: List[str] = non_optional(request_json.get("systems"))
                state_code = assert_type(request_json.get("state_code"), str)
                fips_county_code = request_json.get("fips_county_code")
                agency = AgencyInterface.create_agency(
                    session=session,
                    name=name,
                    systems=[schema.System[system] for system in systems],
                    state_code=state_code,
                    fips_county_code=fips_county_code,
                )

                # Add all CSG accounts as READ_ONLY in production
                if "test" not in name.lower() and in_gcp_production():
                    csg_users = UserAccountInterface.get_csg_users(session=session)
                    for csg_user in csg_users:
                        AgencyUserAccountAssociationInterface.update_user_role(
                            role="READ_ONLY",
                            user=csg_user,
                            agency=agency,
                            session=session,
                        )

                return (
                    jsonify({"agency": agency.to_json()}),
                    HTTPStatus.OK,
                )
        except IntegrityError as e:
            if isinstance(e.orig, UniqueViolation):  # proves the original exception
                return (
                    jsonify({"error": "Agency already exists."}),
                    HTTPStatus.UNPROCESSABLE_ENTITY,
                )
            raise e

    @bp.route("/api/justice_counts_tools/agency/<agency_id>", methods=["GET"])
    @requires_gae_auth
    def get_agency(agency_id: int) -> Tuple[Response, HTTPStatus]:
        """Returns Agency information."""
        with SessionFactory.using_database(
            database_key=SQLAlchemyDatabaseKey.for_schema(SchemaType.JUSTICE_COUNTS),
            secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
        ) as session:
            return (
                jsonify(
                    {
                        "agency": AgencyInterface.get_agency_by_id(
                            session=session, agency_id=agency_id
                        ).to_json(),
                        # also send list of possible roles
                        "roles": [role.value for role in schema.UserAccountRole],
                    }
                ),
                HTTPStatus.OK,
            )

    @bp.route("/api/justice_counts_tools/agencies", methods=["GET"])
    @requires_gae_auth
    def get_all_agencies() -> Tuple[Response, HTTPStatus]:
        """Returns all Agency records."""
        with SessionFactory.using_database(
            database_key=SQLAlchemyDatabaseKey.for_schema(SchemaType.JUSTICE_COUNTS),
            secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
        ) as session:
            return (
                jsonify(
                    {
                        "agencies": [
                            agency.to_json()
                            for agency in AgencyInterface.get_agencies(session=session)
                        ],
                        "systems": [enum.value for enum in schema.System],
                    }
                ),
                HTTPStatus.OK,
            )

    @bp.route("/api/justice_counts_tools/agency/<agency_id>", methods=["PUT"])
    @requires_gae_auth
    def update_agency(agency_id: int) -> Tuple[Response, HTTPStatus]:
        """
        Updates Agency name and systems in our DB.
        """
        with SessionFactory.using_database(
            database_key=SQLAlchemyDatabaseKey.for_schema(SchemaType.JUSTICE_COUNTS),
            secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
        ) as session:
            request_json = assert_type(request.json, dict)
            name = request_json.get("name")
            systems = request_json.get("systems")
            child_agency_ids = request_json.get("child_agency_ids")
            is_superagency = request_json.get("is_superagency")

            if systems is not None:
                AgencyInterface.update_agency_systems(
                    session=session, systems=systems, agency_id=agency_id
                )

            if name is not None:
                AgencyInterface.update_agency_name(
                    session=session, agency_id=agency_id, name=name
                )

            if is_superagency is not None:
                agency = AgencyInterface.update_is_superagency(
                    session=session, agency_id=agency_id, is_superagency=is_superagency
                )
                return (
                    jsonify({"agency": agency.to_json()}),
                    HTTPStatus.OK,
                )

            if child_agency_ids is not None:
                # Remove all child agencies
                agency = AgencyInterface.get_agency_by_id(
                    session=session,
                    agency_id=agency_id,
                )
                existing_child_agencies = AgencyInterface.get_child_agencies_for_agency(
                    session=session, agency=agency
                )
                AgencyUserAccountAssociationInterface.remove_child_agencies_from_super_agency(
                    session=session,
                    child_agencies=existing_child_agencies,
                    super_agency_id=agency_id,
                )

                # Add new child agencies
                AgencyUserAccountAssociationInterface.add_child_agencies_to_super_agency(
                    session=session,
                    super_agency_id=agency_id,
                    child_agency_ids=child_agency_ids,
                )

                session.commit()

                return (
                    jsonify(
                        {
                            "agencies": [
                                agency.to_json()
                                for agency in AgencyInterface.get_agencies(
                                    session=session
                                )
                            ],
                            "systems": [enum.value for enum in schema.System],
                        }
                    ),
                    HTTPStatus.OK,
                )

            return (
                jsonify({"status": "ok"}),
                HTTPStatus.OK,
            )

    # UserAccount

    @bp.route("/api/justice_counts_tools/users", methods=["POST"])
    @requires_gae_auth
    def create_user() -> Tuple[Response, HTTPStatus]:
        """
        Looks for an existing user in Auth0 and creates the User in the database.
        """
        with SessionFactory.using_database(
            database_key=SQLAlchemyDatabaseKey.for_schema(SchemaType.JUSTICE_COUNTS),
            secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
        ) as session:
            request_json = assert_type(request.json, dict)
            email = assert_type(request_json.get("email"), str)
            name = assert_type(request_json.get("name"), str)

            db_user = UserAccountInterface.get_user_by_email(
                session=session, email=email
            )
            if db_user:
                return (
                    jsonify(
                        {
                            "error": "User with this email already exists in the database."
                        }
                    ),
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                )

            auth0_client = _get_auth0_client()
            matching_users = auth0_client.get_all_users_by_email_addresses(
                email_addresses=[email]
            )
            if len(matching_users) != 0:
                return (
                    jsonify({"error": "User with this email already exists in Auth0."}),
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                )

            auth0_user = auth0_client.create_JC_user(name=name, email=email)
            auth0_user_id = auth0_user["user_id"]

            user = UserAccountInterface.create_or_update_user(
                session=session,
                name=name,
                email=email,
                auth0_user_id=auth0_user_id,
            )

            return (
                jsonify({"user": user.to_json()}),
                HTTPStatus.OK,
            )

    @bp.route("/api/justice_counts_tools/users", methods=["GET"])
    @requires_gae_auth
    def get_all_users() -> Tuple[Response, HTTPStatus]:
        """Returns all UserAccount records."""
        with SessionFactory.using_database(
            database_key=SQLAlchemyDatabaseKey.for_schema(SchemaType.JUSTICE_COUNTS),
            secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
        ) as session:
            db_users = UserAccountInterface.get_users(session=session)
            agency_ids_to_fetch = set()
            for user in db_users:
                for assoc in user.agency_assocs:
                    agency_ids_to_fetch.add(assoc.agency_id)

            agencies_by_id = {
                agency.id: agency
                for agency in AgencyInterface.get_agencies_by_id(
                    session=session, agency_ids=list(agency_ids_to_fetch)
                )
            }

            user_json = []
            for user in db_users:
                user_agency_ids = [assoc.agency_id for assoc in user.agency_assocs]
                user_json.append(
                    user.to_json(
                        agencies=list(
                            filter(
                                None,
                                [
                                    agencies_by_id.get(agency_id)
                                    for agency_id in user_agency_ids
                                ],
                            )
                        )
                    )
                )
            return (
                jsonify({"users": user_json}),
                HTTPStatus.OK,
            )

    @bp.route("/api/justice_counts_tools/users", methods=["PUT"])
    @requires_gae_auth
    def update_user() -> Tuple[Response, HTTPStatus]:
        """
        Updates a User's name in the DB.
        """
        with SessionFactory.using_database(
            database_key=SQLAlchemyDatabaseKey.for_schema(SchemaType.JUSTICE_COUNTS),
            secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
        ) as session:
            request_json = assert_type(request.json, dict)
            name = request_json.get("name")
            auth0_user_id = request_json.get("auth0_user_id")

            if auth0_user_id is None:
                raise ValueError("auth0_user_id is required")

            UserAccountInterface.create_or_update_user(
                session=session,
                name=name,
                auth0_user_id=auth0_user_id,
            )

            return (
                jsonify({"status": "ok"}),
                HTTPStatus.OK,
            )

    # AgencyUserAccountAssociation

    @bp.route("/api/justice_counts_tools/users", methods=["PATCH"])
    @requires_gae_auth
    def add_user_account_associations() -> Tuple[Response, HTTPStatus]:
        """
        Adds or Updates AgencyUserAccountAssociations for a user.
        """
        with SessionFactory.using_database(
            database_key=SQLAlchemyDatabaseKey.for_schema(SchemaType.JUSTICE_COUNTS),
            secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
        ) as session:
            request_json = assert_type(request.json, dict)
            email = assert_type(request_json.get("email"), str)
            agency_ids = assert_type(request_json.get("agency_ids"), list)

            agencies = AgencyInterface.get_agencies_by_id(
                session=session, agency_ids=agency_ids or []
            )

            child_agencies = AgencyInterface.get_child_agencies_by_agency_ids(
                session=session, agency_ids=agency_ids
            )

            child_agencies_to_add = [
                a for a in child_agencies if a.id not in set(agency_ids)
            ]

            new_agencies = agencies + child_agencies_to_add

            user = UserAccountInterface.get_user_by_email(session=session, email=email)

            if user is None:
                raise ValueError("No user can be found with the provided email.")

            UserAccountInterface.add_or_update_user_agency_association(
                session=session,
                user=user,
                agencies=new_agencies,
            )

            session.commit()

            user_json = user.to_json(
                agencies=[assoc.agency for assoc in user.agency_assocs]
            )

            return (
                jsonify({"user": user_json}),
                HTTPStatus.OK,
            )

    @bp.route("/api/justice_counts_tools/users", methods=["DELETE"])
    @requires_gae_auth
    def delete_user_account_associations() -> Tuple[Response, HTTPStatus]:
        """
        Deletes AgencyUserAccountAssociations for a user.
        """
        with SessionFactory.using_database(
            database_key=SQLAlchemyDatabaseKey.for_schema(SchemaType.JUSTICE_COUNTS),
            secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
        ) as session:
            request_json = assert_type(request.json, dict)
            email = assert_type(request_json.get("email"), str)
            agency_ids = assert_type(request_json.get("agency_ids"), list)

            agencies = AgencyInterface.get_agencies_by_id(
                session=session, agency_ids=agency_ids or []
            )

            child_agencies = AgencyInterface.get_child_agencies_by_agency_ids(
                session=session, agency_ids=agency_ids
            )

            AgencyUserAccountAssociationInterface.remove_user_from_agencies(
                session=session,
                email=email,
                agency_ids=[a.id for a in agencies + child_agencies],
            )

            session.commit()

            user = UserAccountInterface.get_user_by_email(session=session, email=email)

            if user is None:
                raise ValueError("No user can be found with the provided email.")

            user_json = user.to_json(
                agencies=[assoc.agency for assoc in user.agency_assocs]
            )

            return (
                jsonify({"user": user_json}),
                HTTPStatus.OK,
            )

    @bp.route("/api/justice_counts_tools/agency/<agency_id>", methods=["PATCH"])
    @requires_gae_auth
    def add_or_update_agency_associations(
        agency_id: int,
    ) -> Tuple[Response, HTTPStatus]:
        """
        Adds or Updates AgencyUserAccountAssociations for an agency.
        """
        with SessionFactory.using_database(
            database_key=SQLAlchemyDatabaseKey.for_schema(SchemaType.JUSTICE_COUNTS),
            secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
        ) as session:
            request_json = assert_type(request.json, dict)
            emails = assert_type(request_json.get("emails"), list)
            role = request_json.get("role")

            if emails is None:
                raise ValueError("emails are required")

            users = UserAccountInterface.get_users_by_email(
                session=session, emails=set(emails)
            )

            agency = AgencyInterface.get_agency_by_id(
                session=session, agency_id=agency_id
            )

            child_agencies = (
                AgencyInterface.get_child_agencies_for_agency(
                    session=session, agency=agency
                )
                if role is None
                else []  # We only want to update the child agencies if we are adding
                # users to an agency via the `AgencyProvisioning` page. In that case,
                # `role` will be None. `role` is not None when the request is coming
                # from the `AgencyDetails` page. In that case, emails will be a list
                # containing only one email.
            )

            updated_agencies = [agency] + child_agencies

            for user in users:
                UserAccountInterface.add_or_update_user_agency_association(
                    session=session,
                    user=user,
                    role=schema.UserAccountRole[role] if role is not None else None,
                    agencies=updated_agencies,
                )

            session.commit()

            return (
                jsonify(
                    {
                        "agencies": [
                            agency.to_json()
                            for agency in AgencyInterface.get_agencies(session=session)
                        ],
                        "systems": [enum.value for enum in schema.System],
                    }
                ),
                HTTPStatus.OK,
            )

    @bp.route("/api/justice_counts_tools/agency/<agency_id>", methods=["DELETE"])
    @requires_gae_auth
    def delete_agency_associations(agency_id: int) -> Tuple[Response, HTTPStatus]:
        """
        Deletes AgencyUserAccountAssociations for an agency.
        """
        with SessionFactory.using_database(
            database_key=SQLAlchemyDatabaseKey.for_schema(SchemaType.JUSTICE_COUNTS),
            secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
        ) as session:
            request_json = assert_type(request.json, dict)
            emails = request_json.get("emails")
            if emails is None:
                raise ValueError("emails are required")

            child_agencies = AgencyInterface.get_child_agencies_by_agency_ids(
                session=session, agency_ids=[agency_id]
            )

            child_agency_ids = [int(a.id) for a in child_agencies]

            for email in list(emails):
                agency_ids = child_agency_ids + [int(agency_id)]
                AgencyUserAccountAssociationInterface.remove_user_from_agencies(
                    session=session,
                    email=email,
                    agency_ids=agency_ids,
                )

            session.commit()

            return (
                jsonify(
                    {
                        "agencies": [
                            agency.to_json()
                            for agency in AgencyInterface.get_agencies(session=session)
                        ],
                        "systems": [enum.value for enum in schema.System],
                    }
                ),
                HTTPStatus.OK,
            )
