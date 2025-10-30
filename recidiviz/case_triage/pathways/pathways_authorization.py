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
""" Implements authorization for Pathways routes"""
import os
from http import HTTPStatus
from typing import Any, Dict, Optional

from flask import request

from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_enabled_states import (
    get_pathways_enabled_states_for_cloud_sql,
)
from recidiviz.case_triage.authorization_utils import (
    on_successful_authorization_requested_state,
)
from recidiviz.common.str_field_utils import capitalize_first
from recidiviz.utils.auth.auth0 import AuthorizationError
from recidiviz.utils.flask_exception import FlaskException

CSG_ALLOWED_PATHWAYS_STATES = ["US_MI", "US_MO", "US_PA", "US_TN"]


def on_successful_authorization(
    claims: Dict[str, Any], offline_mode: Optional[bool] = False
) -> None:
    """
    First, authorizes checks on the requested state.
    """
    on_successful_authorization_requested_state(
        claims=claims,
        enabled_states=get_pathways_enabled_states_for_cloud_sql(),
        offline_mode=offline_mode,
        csg_enabled_states=CSG_ALLOWED_PATHWAYS_STATES,
    )

    # If in offline mode, skip endpoint checks
    if offline_mode:
        return

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

    requested_endpoint = request.view_args["metric_name"]
    enabled_endpoints = [
        # transform routes of the format system_prisonToSupervision --> PrisonToSupervision
        capitalize_first(route.removeprefix("system_"))
        for (route, enabled) in app_metadata.get("routes", {}).items()
        if enabled and route.startswith("system_")
    ]
    endpoint_allowed = any(
        requested_endpoint.startswith(endpoint)
        # Make sure we don't consider system_prison as eligible for PrisonToSupervisionTransitions
        and not requested_endpoint.startswith(endpoint + "To")
        for endpoint in enabled_endpoints
    )

    if endpoint_allowed:
        return

    raise AuthorizationError(code="not_authorized", description="Access denied")
