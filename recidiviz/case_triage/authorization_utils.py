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
"""
This module contains a helper for authenticating users accessing product APIs hosted on the Case Triage
backend.
"""
import json
import os
from http import HTTPStatus
from typing import Any, Callable, Dict, List, Optional

from flask import request

from recidiviz.common.constants.states import StateCode
from recidiviz.utils.auth.auth0 import (
    Auth0Config,
    AuthorizationError,
    build_auth0_authorization_handler,
)
from recidiviz.utils.environment import in_offline_mode
from recidiviz.utils.flask_exception import FlaskException
from recidiviz.utils.secrets import get_secret


def build_auth_config(secret_name: str) -> Auth0Config:
    """Get the secret using the string provided and build the auth config"""
    auth0_configuration = get_secret(secret_name)

    if not auth0_configuration:
        raise ValueError("Missing Auth0 configuration secret")

    authorization_config = Auth0Config.from_config_json(json.loads(auth0_configuration))

    return authorization_config


def build_authorization_handler(
    on_successful_authorization: Callable,
    secret_name: str,
    auth_header: Optional[Any] = None,
) -> Callable:
    """Loads Auth0 configuration secret and builds the middleware"""

    if in_offline_mode():
        # Offline mode does not require authorization since it is only returning fixture data.
        return lambda: on_successful_authorization({}, offline_mode=True)

    authorization_config = build_auth_config(secret_name)

    return build_auth0_authorization_handler(
        authorization_config,
        on_successful_authorization,
        auth_header=auth_header,
    )


def on_successful_authorization_requested_state(
    claims: Dict[str, Any],
    enabled_states: List[str],
    offline_mode: Optional[bool] = False,
    csg_enabled_states: Optional[List[str]] = None,
) -> None:
    """
    No-ops if:
    1. A recidiviz user is requesting an enabled state and is authorized for that state
    2. A state user is making an request for their own enabled state
    3. check_csg is True and a CSG user is making a request for a CSG allowed state
    Otherwise, raises an AuthorizationError
    """
    if not request.view_args or "state" not in request.view_args:
        raise FlaskException(
            code="state_required",
            description="A state must be passed to the route",
            status_code=HTTPStatus.BAD_REQUEST,
        )

    requested_state = request.view_args["state"].upper()

    if not StateCode.is_state_code(requested_state):
        raise FlaskException(
            code="valid_state_required",
            description="A valid state must be passed to the route",
            status_code=HTTPStatus.BAD_REQUEST,
        )

    if offline_mode:
        if requested_state != "US_OZ":
            raise FlaskException(
                code="offline_state_required",
                description="Offline mode requests may only be for US_OZ",
                status_code=HTTPStatus.BAD_REQUEST,
            )
        return

    if requested_state not in enabled_states:
        raise FlaskException(
            code="state_not_enabled",
            description="This product is not enabled for this state",
            status_code=HTTPStatus.BAD_REQUEST,
        )

    app_metadata = claims[f"{os.environ['AUTH0_CLAIM_NAMESPACE']}/app_metadata"]
    user_state_code = app_metadata["stateCode"].upper()
    recidiviz_allowed_states = app_metadata.get("allowedStates", [])

    if user_state_code == "RECIDIVIZ":
        if requested_state not in recidiviz_allowed_states:
            raise FlaskException(
                code="recidiviz_user_not_authorized",
                description="Recidiviz user does not have authorization for this state",
                status_code=HTTPStatus.UNAUTHORIZED,
            )
        return

    if csg_enabled_states and user_state_code == "CSG":
        if requested_state not in csg_enabled_states:
            raise FlaskException(
                code="csg_user_not_authorized",
                description="CSG user does not have authorization for this state",
                status_code=HTTPStatus.UNAUTHORIZED,
            )
        return

    if user_state_code == requested_state and user_state_code in enabled_states:
        return

    raise AuthorizationError(code="not_authorized", description="Access denied")
