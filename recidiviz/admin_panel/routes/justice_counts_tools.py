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
from typing import Tuple

from flask import Blueprint, Response, jsonify, request
from psycopg2.errors import UniqueViolation  # pylint: disable=no-name-in-module
from sqlalchemy.exc import IntegrityError

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.persistence.database.schema.justice_counts.schema import System
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.types import assert_type


def add_justice_counts_tools_routes(bp: Blueprint) -> None:
    """Adds the relevant Justice Counts Admin Panel API routes to an input Blueprint."""

    @bp.route("/api/justice_counts_tools/agencies", methods=["GET", "POST"])
    @requires_gae_auth
    def agencies() -> Tuple[Response, HTTPStatus]:
        """On GET request: Returns all Agency records.
        On POST request: Creates an Agency and returns the created Agency. Returns an error message if the Agency already exists with that name.
        """
        try:
            with SessionFactory.using_database(
                SQLAlchemyDatabaseKey.for_schema(SchemaType.JUSTICE_COUNTS)
            ) as session:
                if request.method == "POST":
                    request_json = assert_type(request.json, dict)
                    name = assert_type(request_json.get("name"), str)
                    system = assert_type(request_json.get("system"), str)
                    state_code = assert_type(request_json.get("state_code"), str)
                    fips_county_code = assert_type(
                        request_json.get("fips_county_code"), str
                    )
                    agency = AgencyInterface.create_agency(
                        session=session,
                        name=name,
                        system=system,
                        state_code=state_code,
                        fips_county_code=fips_county_code,
                    )
                    return (
                        jsonify({"agency": agency.to_json()}),
                        HTTPStatus.OK,
                    )
                # else request.method must be "GET"
                return (
                    jsonify(
                        {
                            "agencies": [
                                agency.to_json()
                                for agency in AgencyInterface.get_agencies(
                                    session=session
                                )
                            ],
                            "systems": [enum.value for enum in System],
                        }
                    ),
                    HTTPStatus.OK,
                )
        except IntegrityError as e:
            if isinstance(e.orig, UniqueViolation):  # proves the original exception
                return (
                    jsonify({"error": "Agency already exists."}),
                    HTTPStatus.UNPROCESSABLE_ENTITY,
                )
            raise e

    @bp.route("/api/justice_counts_tools/users", methods=["GET", "POST", "PUT"])
    @requires_gae_auth
    def users() -> Tuple[Response, HTTPStatus]:
        """On GET request: Returns all UserAccount records.
        On POST request: Creates a User and returns the created User. Returns an error message if the user already exists with that email address.
        On PUT request: Creates or updates a User and returns the User.
        """
        try:
            with SessionFactory.using_database(
                SQLAlchemyDatabaseKey.for_schema(SchemaType.JUSTICE_COUNTS)
            ) as session:
                if request.method == "GET":
                    return (
                        jsonify(
                            {
                                "users": [
                                    user.to_json()
                                    for user in UserAccountInterface.get_users(
                                        session=session
                                    )
                                ]
                            }
                        ),
                        HTTPStatus.OK,
                    )
                request_json = assert_type(request.json, dict)
                email = assert_type(request_json.get("email"), str)
                agency_ids = request_json.get("agency_ids")
                name = request_json.get("name")
                try:
                    if request.method == "POST":
                        user = UserAccountInterface.create_user(
                            session=session,
                            email_address=email,
                            agency_ids=agency_ids,
                            name=name,
                        )
                    else:
                        user = UserAccountInterface.create_or_update_user(
                            session=session,
                            email_address=email,
                            agency_ids=agency_ids,
                            name=name,
                        )
                except ValueError as e:
                    return (
                        jsonify({"error": str(e)}),
                        HTTPStatus.UNPROCESSABLE_ENTITY,
                    )

                return (
                    jsonify({"user": user.to_json()}),
                    HTTPStatus.OK,
                )
        except IntegrityError as e:
            if isinstance(e.orig, UniqueViolation):  # proves the original exception
                return (
                    jsonify({"error": "User already exists."}),
                    HTTPStatus.UNPROCESSABLE_ENTITY,
                )
            raise e
