# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
# ============================================================================
"""Specific entity matching utils for StateSupervisionViolation entities and its children."""

from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseRevocationType,
)
from recidiviz.persistence.database.schema.state import schema


def revoked_to_prison(svr: schema.StateSupervisionViolationResponse) -> bool:
    """Determines if the provided |svr| resulted in a revocation."""
    if not svr.revocation_type:
        return False
    reincarceration_types = [
        StateSupervisionViolationResponseRevocationType.REINCARCERATION.value,
        StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION.value,
        StateSupervisionViolationResponseRevocationType.TREATMENT_IN_PRISON.value,
    ]
    non_reincarceration_types = [
        StateSupervisionViolationResponseRevocationType.RETURN_TO_SUPERVISION.value
    ]
    if svr.revocation_type in reincarceration_types:
        return True
    if svr.revocation_type in non_reincarceration_types:
        return False
    raise ValueError(
        f"Unexpected StateSupervisionViolationRevocationType {svr.revocation_type} for [{svr}]."
    )
