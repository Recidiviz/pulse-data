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
"""Implements API routes for the Justice Counts Control Panel backend API."""
from typing import Callable, Optional

from flask import Blueprint, Response, jsonify, make_response
from flask_wtf.csrf import generate_csrf


# TODO(#11504): Replace dummy endpoint
def get_api_blueprint(
    auth_decorator: Callable, secret_key: Optional[str] = None
) -> Blueprint:
    api_blueprint = Blueprint("api", __name__)

    @api_blueprint.route("/hello")
    @auth_decorator
    def hello() -> Response:
        return jsonify({"response": "Hello, World!"})

    @api_blueprint.route("/init")
    @auth_decorator
    def init() -> Response:
        if not secret_key:
            return make_response("Unable to find secret key", 500)

        return jsonify({"csrf": generate_csrf(secret_key)})

    return api_blueprint
