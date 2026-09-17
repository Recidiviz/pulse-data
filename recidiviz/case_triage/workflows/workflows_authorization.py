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
"""Implements user validations for workflows APIs."""

import os
from typing import Any, Dict, List

from flask import g

from recidiviz.calculator.query.state.views.outliers.workflows_enabled_states import (
    get_workflows_enabled_states,
)
from recidiviz.case_triage.authorization_utils import (
    get_active_feature_variants,
    on_successful_authorization_requested_state,
)
from recidiviz.utils.auth.auth0 import AuthorizationError
from recidiviz.workflows.types import WorkflowsSystemType

# Maps each WorkflowsSystemType to the Auth0 routes claim that grants a user
# access to it.
_ROUTE_CLAIM_BY_WORKFLOWS_SYSTEM_TYPE: Dict[WorkflowsSystemType, str] = {
    WorkflowsSystemType.SUPERVISION: "workflowsSupervision",
    WorkflowsSystemType.INCARCERATION: "workflowsFacilities",
}


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
        claims,
        get_workflows_enabled_states(),
    )
    g.authenticated_user_email = claims.get(
        f"{os.environ['AUTH0_CLAIM_NAMESPACE']}/email_address"
    )
    app_metadata = claims[f"{os.environ['AUTH0_CLAIM_NAMESPACE']}/app_metadata"]
    user_state_code = app_metadata["stateCode"].upper()
    g.is_recidiviz_user = user_state_code == "RECIDIVIZ"

    is_recidiviz_or_csg = user_state_code in ("RECIDIVIZ", "CSG")
    # The external id in the token is sourced from the admin panel, so it may
    # not reliably exist where roster sync is not used.
    g.authenticated_user_external_id = (
        None if is_recidiviz_or_csg else app_metadata.get("externalId")
    )

    g.feature_variants = get_active_feature_variants(
        app_metadata.get("featureVariants", {}),
        app_metadata.get("pseudonymizedId", None),
    )
    g.routes = app_metadata.get("routes", {})


def get_allowed_workflows_system_types() -> List[WorkflowsSystemType]:
    """Returns the WorkflowsSystemTypes the authenticated caller's routes claim
    grants access to. Recidiviz users are exempt and are always allowed every
    system type."""
    if g.is_recidiviz_user:
        return list(WorkflowsSystemType)

    return [
        system_type
        for system_type, route_claim in _ROUTE_CLAIM_BY_WORKFLOWS_SYSTEM_TYPE.items()
        if g.routes.get(route_claim, False)
    ]


def require_workflows_system_type_permission(system_type: WorkflowsSystemType) -> None:
    """Raises AuthorizationError unless the authenticated caller's routes claim
    grants access to the given WorkflowsSystemType. Recidiviz users are exempt."""
    if system_type not in get_allowed_workflows_system_types():
        raise AuthorizationError(
            code="not_authorized",
            description=(
                f"Access denied: [{_ROUTE_CLAIM_BY_WORKFLOWS_SYSTEM_TYPE[system_type]}] "
                "permission required"
            ),
        )
