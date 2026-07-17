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

# Injected into view_args via the individual_level_export route's `defaults` (see
# public_pathways_routes.py) since that route has no <metric_name> path segment for
# on_successful_authorization to key off of. The route's parameter name must match
# this value exactly, since Flask calls the view function with `**view_args`.
INDIVIDUAL_LEVEL_EXPORT_VIEW_ARG = "individual_level_export"


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

    view_args = request.view_args or {}

    if "metric_name" in view_args or INDIVIDUAL_LEVEL_EXPORT_VIEW_ARG in view_args:
        if user_state_code == "US_NY":
            return
        raise AuthorizationError(code="not_authorized", description="Access denied")

    raise FlaskException(
        code="unrecognized_route",
        description="Neither a metric name nor a known non-metric route was found",
        status_code=HTTPStatus.BAD_REQUEST,
    )
