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
"""Implements the application-level authorization blueprint. """
import os
import urllib.parse

from flask import Blueprint, Response, make_response, redirect, session

from recidiviz.utils.auth.auth0 import Auth0Config

os.environ.setdefault("APP_URL", "http://localhost:3000")


def create_auth_blueprint(authorization_config: Auth0Config) -> Blueprint:
    auth = Blueprint("auth", __name__)

    @auth.route("/log_out", methods=["POST"])
    def _log_out() -> Response:
        # Deletes the session cookie and corresponding data
        session.clear()

        query_parameters = urllib.parse.urlencode(
            {
                "client_id": authorization_config.client_id,
                "returnTo": os.environ.get("APP_URL"),
            }
        )

        return make_response(
            redirect(
                f"https://{authorization_config.domain}/v2/logout?{query_parameters}"
            )
        )

    return auth
