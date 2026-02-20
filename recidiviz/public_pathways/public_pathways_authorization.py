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
""" Implements authorization for Pubic Pathways routes"""
import os
from http import HTTPStatus
from typing import Any, Dict

from flask import request

from recidiviz.calculator.query.state.views.public_pathways.public_pathways_enabled_states import (
    get_public_pathways_enabled_states_for_cloud_sql,
)
from recidiviz.case_triage.authorization_utils import (
    on_successful_authorization_requested_state,
)
from recidiviz.utils.auth.auth0 import AuthorizationError
from recidiviz.utils.flask_exception import FlaskException


def on_successful_authorization(claims: Dict[str, Any]) -> None:
    """
    First, authorizes checks on the requested state.
    """

    on_successful_authorization_requested_state(
        claims=claims,
        enabled_states=get_public_pathways_enabled_states_for_cloud_sql(),
    )

    app_metadata = claims[f"{os.environ['AUTH0_CLAIM_NAMESPACE']}/app_metadata"]
    user_state_code = app_metadata["stateCode"].upper()

    # If the user is a recidiviz user, skip endpoint checks
    if user_state_code == "RECIDIVIZ":
        return

    if not request.view_args or "metric_name" not in request.view_args:
        raise FlaskException(
            code="metric_name",
            description="A metric name was expected in the route",
            status_code=HTTPStatus.BAD_REQUEST,
        )

    if user_state_code == "US_NY":
        return

    raise AuthorizationError(code="not_authorized", description="Access denied")
