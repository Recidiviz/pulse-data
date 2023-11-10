# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Implements authorization for Outliers routes"""
import os
from typing import Any, Dict, Optional

from flask import g

from recidiviz.calculator.query.state.views.outliers.outliers_enabled_states import (
    get_outliers_enabled_states,
)
from recidiviz.case_triage.authorization_utils import (
    on_successful_authorization_requested_state,
)
from recidiviz.case_triage.outliers.user_context import UserContext
from recidiviz.utils.auth.auth0 import AuthorizationError


def on_successful_authorization(
    claims: Dict[str, Any], offline_mode: Optional[bool] = False
) -> None:
    """
    First, authorizes checks on the requested state. Saves the state code and external id via the UserContext
    to use in endpoint-specific validation.
    """
    on_successful_authorization_requested_state(
        claims=claims,
        enabled_states=get_outliers_enabled_states(),
        offline_mode=offline_mode,
        check_csg=True,
    )

    # If in offline mode, skip endpoint checks
    if offline_mode:
        return

    app_metadata = claims[f"{os.environ['AUTH0_CLAIM_NAMESPACE']}/app_metadata"]
    user_state_code = app_metadata["stateCode"].upper()

    user_external_id = (
        user_state_code
        if user_state_code == "RECIDIVIZ"
        else app_metadata["externalId"]
    )
    user_role = (
        "leadership_role" if user_state_code == "RECIDIVIZ" else app_metadata["role"]
    )
    user_pseudonymized_id = app_metadata.get("pseudonymizedId", None)

    g.user_context = UserContext(
        state_code_str=user_state_code,
        user_external_id=user_external_id,
        role=user_role,
        pseudonymized_id=user_pseudonymized_id,
    )

    # If the user is a recidiviz user, skip endpoint checks
    if user_state_code == "RECIDIVIZ":
        return

    if not app_metadata.get("routes", {}).get("outliers", False):
        raise AuthorizationError(code="not_authorized", description="Access denied")
