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

from recidiviz.justice_counts.agency import AgencyInterface
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
        On POST request: Takes in a 'name', creates an Agency with that name and returns the created Agency.
        """
        with SessionFactory.using_database(
            SQLAlchemyDatabaseKey.for_schema(SchemaType.JUSTICE_COUNTS)
        ) as session:
            if request.method == "POST":
                request_json = assert_type(request.json, dict)
                name = request_json["name"]
                agency = AgencyInterface.create_agency(session=session, name=name)
                return (
                    jsonify({"agency": agency.to_json()}),
                    HTTPStatus.OK,
                )
            return (
                jsonify(
                    {
                        "agencies": [
                            agency.to_json()
                            for agency in AgencyInterface.get_agencies(session=session)
                        ]
                    }
                ),
                HTTPStatus.OK,
            )
