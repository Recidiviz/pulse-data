# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Error handlers for the Identity Service Flask app."""
import logging
from http import HTTPStatus

from flask import Flask, Response, jsonify

from recidiviz.services.identity.exceptions import IdentityHistoryIntegrityException


def handle_identity_history_exception(
    ex: IdentityHistoryIntegrityException,
) -> Response:
    """Returns a structured JSON 500 instead of Flask's default HTML error page
    for a corrupt identity merge/split history -- a bug in our own stored data,
    not a caller error."""
    logging.error("Identity Service request failed: %s", ex)
    response = jsonify({"code": "identity_integrity_error", "description": str(ex)})
    response.status_code = HTTPStatus.INTERNAL_SERVER_ERROR
    return response


def register_error_handlers(app: Flask) -> None:
    """Registers error handlers for the Identity Service Flask app."""
    app.errorhandler(IdentityHistoryIntegrityException)(
        handle_identity_history_exception
    )
