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
from http import HTTPStatus
from typing import Any, Dict, Optional

from flask import g

from recidiviz.calculator.query.state.views.outliers.outliers_enabled_states import (
    get_outliers_enabled_states,
)
from recidiviz.case_triage.authorization_utils import (
    on_successful_authorization_requested_state,
)
from recidiviz.case_triage.outliers.user_context import UserContext
from recidiviz.outliers.querier.querier import OutliersQuerier
from recidiviz.utils.auth.auth0 import AuthorizationError
from recidiviz.utils.flask_exception import FlaskException


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

    if not app_metadata.get("routes", {}).get("insights", False):
        raise AuthorizationError(code="not_authorized", description="Access denied")


def grant_endpoint_access(querier: OutliersQuerier, user_context: UserContext) -> None:
    """
    No-ops if:
    1. The user is not a supervision_staff user, i.e. leadership or recidiviz user
    2. The user is a supervisor who has outliers.
    Otherwises, raises an exception.
    """
    if user_context.role == "supervision_staff":
        if user_context.pseudonymized_id is None:
            raise FlaskException(
                code="no_pseudonymized_id",
                description="Supervision staff user should have a pseudonymized id.",
                status_code=HTTPStatus.BAD_REQUEST,
            )

        supervisor_entity = querier.get_supervisor_entity_from_pseudonymized_id(
            user_context.pseudonymized_id
        )

        if supervisor_entity is None:
            raise FlaskException(
                code="supervisor_not_found",
                description="Cannot find information on the requested user for the latest period",
                status_code=HTTPStatus.NOT_FOUND,
            )

        if not supervisor_entity.has_outliers:
            raise FlaskException(
                code="supervisor_has_no_outliers",
                description=f"Supervisors must have outliers in the latest period to access this endpoint. User {user_context.pseudonymized_id} does not have outliers.",
                status_code=HTTPStatus.UNAUTHORIZED,
            )
