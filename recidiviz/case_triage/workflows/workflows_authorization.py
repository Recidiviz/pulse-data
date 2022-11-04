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
from typing import Any, Dict

from recidiviz.utils.auth.auth0 import AuthorizationError


def on_successful_authorization(claims: Dict[str, Any]) -> None:
    """No-ops if:
    1. A recidiviz user is requesting a workflows enabled state
    Otherwise, raises an AuthorizationError
    """
    app_metadata = claims[f"{os.environ['AUTH0_CLAIM_NAMESPACE']}/app_metadata"]
    user_state_code = app_metadata["state_code"].upper()

    # Currently, workflows routes should only be accessed by Recidiviz users
    if user_state_code == "RECIDIVIZ":
        return

    raise AuthorizationError(code="not_authorized", description="Access denied")
