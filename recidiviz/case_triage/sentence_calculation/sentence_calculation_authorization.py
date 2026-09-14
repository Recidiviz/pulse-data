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
"""Implements authorization for Sentence Calculation routes."""
from typing import Any, Dict, Optional

from recidiviz.case_triage.authorization_utils import (
    on_successful_authorization_requested_state,
)
from recidiviz.common.constants.states import StateCode

SENTENCE_CALCULATION_ENABLED_STATES = [StateCode.US_NV.value]


def on_successful_authorization(
    claims: Dict[str, Any], offline_mode: Optional[bool] = False
) -> None:
    """Authorizes access to Sentence Calculation routes, gating on the requested
    state being enabled and the user being authorized for that state."""
    on_successful_authorization_requested_state(
        claims=claims,
        enabled_states=SENTENCE_CALCULATION_ENABLED_STATES,
        offline_mode=offline_mode,
    )
