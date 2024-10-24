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
"""Implements authorization for the Case Note Search app."""
import os
from typing import Any, Dict

from quart import g

from recidiviz.case_triage.outliers.user_context import UserContext
from recidiviz.utils.environment import in_development


def on_successful_authorization(
    claims: Dict[str, Any],
) -> None:
    """
    Save the state code and external id via the UserContext to use in endpoint-specific
    validation.
    """
    # When using M2M authentication during development testing, our access token won't have
    # a custom email token claim (because the token doesn't actually belong to a user),
    # but this is okay and shouldn't raise an error.
    # Details: https://auth0.com/docs/get-started/authentication-and-authorization-flow/client-credentials-flow
    if in_development():
        return

    app_metadata = claims[f"{os.environ['AUTH0_CLAIM_NAMESPACE']}/app_metadata"]
    user_state_code = app_metadata["stateCode"].upper()
    is_recidiviz = user_state_code in ("RECIDIVIZ")

    user_external_id = user_state_code if is_recidiviz else app_metadata["externalId"]
    user_pseudonymized_id = app_metadata.get("pseudonymizedId", None)
    feature_variants = app_metadata.get("featureVariants", {})
    g.user_context = UserContext(
        state_code_str=user_state_code,
        user_external_id=user_external_id,
        pseudonymized_id=user_pseudonymized_id,
        can_access_all_supervisors=False,
        can_access_supervision_workflows=False,
        feature_variants=feature_variants,
    )
