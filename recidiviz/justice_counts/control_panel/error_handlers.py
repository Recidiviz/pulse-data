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
""" Contains error handlers to our Flask app"""

from flask import Flask, Response, jsonify

from recidiviz.utils.flask_exception import FlaskException


def handle_error(ex: FlaskException) -> Response:
    response = jsonify(
        {
            "code": ex.code,
            "description": ex.description,
        }
    )
    response.status_code = ex.status_code
    return response


def register_error_handlers(app: Flask) -> None:
    """Registers error handlers"""
    app.errorhandler(FlaskException)(handle_error)
