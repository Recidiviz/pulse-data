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
"""Implements authentication routes for the Justice Counts Control Panel backend API."""
from http import HTTPStatus

from flask import Blueprint, Response


def create_auth_blueprint() -> Blueprint:
    auth = Blueprint("auth", __name__)

    @auth.route("/logout", methods=["POST"])
    def logout() -> Response:
        # Deletes the session cookie and corresponding data
        # TODO(#11550): Implement logout endpoint
        return Response(status=HTTPStatus.OK)

    return auth
