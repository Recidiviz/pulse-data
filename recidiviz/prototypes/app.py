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
"""Prototype endpoints."""
from quart import Quart, Response, jsonify, request

from recidiviz.case_triage.authorization_utils import build_authorization_handler
from recidiviz.prototypes.case_note_search.case_note_authorization import (
    on_successful_authorization,
)

app = Quart(__name__)


@app.before_request
async def validate_authentication() -> None:
    # Bypass authentication for health check endpoint.
    if request.path == "/health":
        return

    handle_authorization = build_authorization_handler(
        on_successful_authorization=on_successful_authorization,
        secret_name="dashboard_auth0",  # nosec
        auth_header=request.headers.get("Authorization", None),
    )
    handle_authorization()


@app.route("/search", methods=["GET"])
async def search_case_notes() -> Response:
    return jsonify({"search": "results"})
