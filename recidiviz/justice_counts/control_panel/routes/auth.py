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
from typing import Callable

from flask import Blueprint, Response, session


def get_auth_blueprint(
    auth_decorator: Callable,
) -> Blueprint:
    auth_blueprint = Blueprint("auth", __name__)

    @auth_blueprint.route("/logout", methods=["POST"])
    @auth_decorator
    def logout() -> Response:
        """Logging out requires 1) clearing the session 2) logging out of Auth0.
        This endpoint clears the session, but the user will be logged out of
        Auth0 in the FE.
        """
        session.clear()
        return Response(status=HTTPStatus.OK)

    return auth_blueprint
