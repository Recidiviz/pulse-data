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
import json
import os
from http import HTTPStatus
from typing import Any, Callable, Dict

from flask import request

from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_enabled_states import (
    get_pathways_enabled_states,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.auth.auth0 import (
    Auth0Config,
    AuthorizationError,
    build_auth0_authorization_handler,
)
from recidiviz.utils.flask_exception import FlaskException
from recidiviz.utils.secrets import get_secret


def on_successful_authorization(claims: Dict[str, Any]) -> None:
    """No-ops if either:
    1. A recidiviz user is requesting a pathways enabled state
    2. A state user is requesting their own pathways enabled state
    Otherwise, raises an AuthorizationError
    """
    # All pathways routes are expected to require a `state_code` in their view args
    if not request.view_args or "state" not in request.view_args:
        raise FlaskException(
            code="state_required",
            description="A state must be passed to the route",
            status_code=HTTPStatus.BAD_REQUEST,
        )

    requested_state = request.view_args["state"]

    if not StateCode.is_state_code(requested_state):
        raise FlaskException(
            code="valid_state_required",
            description="A valid state must be passed to the route",
            status_code=HTTPStatus.BAD_REQUEST,
        )

    enabled_states = get_pathways_enabled_states()

    if requested_state not in enabled_states:
        raise FlaskException(
            code="pathways_not_enabled",
            description="Pathways is not enabled for this state",
            status_code=HTTPStatus.BAD_REQUEST,
        )

    app_metadata = claims[f"{os.environ['AUTH0_CLAIM_NAMESPACE']}/app_metadata"]
    user_state_code = app_metadata["state_code"]

    if user_state_code == "recidiviz":
        return

    if user_state_code == requested_state and user_state_code in enabled_states:
        return

    raise AuthorizationError(code="not_authorized", description="Access denied")


def build_authorization_handler() -> Callable:
    """Loads Auth0 configuration secret and builds the middleware"""
    dashboard_auth0_configuration = get_secret("dashboard_auth0")

    if not dashboard_auth0_configuration:
        raise ValueError("Missing Dashboard Auth0 configuration secret")

    authorization_config = Auth0Config.from_config_json(
        json.loads(dashboard_auth0_configuration)
    )

    return build_auth0_authorization_handler(
        authorization_config, on_successful_authorization
    )
