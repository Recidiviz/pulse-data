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
from typing import Any, Dict, List

from flask import g

from recidiviz.case_triage.authorization_utils import (
    on_successful_authorization_requested_state,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.auth.auth0 import AuthorizationError


def on_successful_authorization_recidiviz_only(claims: Dict[str, Any]) -> None:
    """Only allows users whose state code is RECIDIVIZ"""
    app_metadata = claims[f"{os.environ['AUTH0_CLAIM_NAMESPACE']}/app_metadata"]
    user_state_code = app_metadata["stateCode"].upper()
    g.authenticated_user_email = claims.get(
        f"{os.environ['AUTH0_CLAIM_NAMESPACE']}/email_address"
    )
    if not g.authenticated_user_email:
        raise AuthorizationError(
            code="not_authorized",
            description="Access denied, email is missing or invalid",
        )

    if user_state_code == "RECIDIVIZ":
        return

    raise AuthorizationError(code="not_authorized", description="Access denied")


def on_successful_authorization(claims: Dict[str, Any]) -> None:
    """
    Saves the user email, given the requested state authorization is a no-op.
    """
    on_successful_authorization_requested_state(
        claims, get_workflows_external_request_enabled_states()
    )
    g.authenticated_user_email = claims.get(
        f"{os.environ['AUTH0_CLAIM_NAMESPACE']}/email_address"
    )


def get_workflows_external_request_enabled_states() -> List[str]:
    """
    List of states in which we will make external system requests for Workflows
    """
    return [StateCode.US_TN.value, StateCode.US_CA.value]
