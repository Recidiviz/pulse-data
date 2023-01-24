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
"""Implements user validations for workflows APIs. """
import os
from http import HTTPStatus
from typing import Any, Dict, List

from flask import request

from recidiviz.common.constants.states import StateCode
from recidiviz.utils.auth.auth0 import AuthorizationError
from recidiviz.utils.flask_exception import FlaskException


def on_successful_authorization(claims: Dict[str, Any]) -> None:
    """
    No-ops if:
    1. A recidiviz user is requesting a workflows enabled state
    2. A state user is making an external system request for their own state
    Otherwise, raises an AuthorizationError
    """
    app_metadata = claims[f"{os.environ['AUTH0_CLAIM_NAMESPACE']}/app_metadata"]
    user_state_code = app_metadata["state_code"].upper()

    if user_state_code == "RECIDIVIZ":
        return

    if not request.view_args or "state" not in request.view_args:
        raise FlaskException(
            code="state_required",
            description="A state must be passed to the route",
            status_code=HTTPStatus.BAD_REQUEST,
        )

    requested_state = request.view_args["state"]
    enabled_states = get_workflows_external_request_enabled_states()

    if not StateCode.is_state_code(requested_state):
        raise FlaskException(
            code="valid_state_required",
            description="A valid state must be passed to the route",
            status_code=HTTPStatus.BAD_REQUEST,
        )

    if requested_state not in enabled_states:
        raise FlaskException(
            code="external_requests_not_enabled",
            description="Workflows external system requests are not enabled for this state",
            status_code=HTTPStatus.BAD_REQUEST,
        )

    if user_state_code == requested_state and user_state_code in enabled_states:
        return

    raise AuthorizationError(code="not_authorized", description="Access denied")


def get_workflows_external_request_enabled_states() -> List[str]:
    """
    List of states in which we will make external system requests for Workflows
    """
    return [StateCode.US_TN.value]
