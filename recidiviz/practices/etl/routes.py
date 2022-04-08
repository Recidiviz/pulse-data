# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Endpoints for Practices ETL"""
from http import HTTPStatus
from typing import Tuple

from flask import Blueprint, request

from recidiviz.practices.etl.archive import archive_etl_file
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.types import assert_type


def get_practices_etl_blueprint() -> Blueprint:
    """Creates a Flask Blueprint for Practices ETL routes."""
    practices_etl_blueprint = Blueprint("practices-etl", __name__)

    @practices_etl_blueprint.route("/archive-file", methods=["POST"])
    @requires_gae_auth
    def _archive_file() -> Tuple[str, HTTPStatus]:
        try:
            request_json = assert_type(request.json, dict)
        except ValueError:
            return "Invalid request body", HTTPStatus.BAD_REQUEST

        filename = request_json.get("filename")
        if filename is None:
            return "Missing filename", HTTPStatus.BAD_REQUEST

        archive_etl_file(filename)

        return "", HTTPStatus.OK

    return practices_etl_blueprint
