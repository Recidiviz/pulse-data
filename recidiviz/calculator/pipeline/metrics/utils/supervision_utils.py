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
    Utils for determining whether supervision occurred out of state.
"""
from typing import Optional

from recidiviz.calculator.pipeline.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority


def is_supervision_out_of_state(
    custodial_authority: Optional[StateCustodialAuthority],
    deprecated_supervising_district_external_id: Optional[str],
    supervision_delegate: StateSpecificSupervisionDelegate,
) -> bool:
    """Helper for determining whether someone counts towards the out of state supervision
    population.
    """
    if custodial_authority is not None and custodial_authority in (
        StateCustodialAuthority.FEDERAL,
        StateCustodialAuthority.OTHER_COUNTRY,
        StateCustodialAuthority.OTHER_STATE,
    ):
        # If the custodial authority is out of state, the person is always in the
        # out of state population
        return True

    # Otherwise, for some states we can use the supervision location to determine
    # if they are out of state.
    return supervision_delegate.is_supervision_location_out_of_state(
        deprecated_supervising_district_external_id
    )
