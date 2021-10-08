# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""US_PA implementation of the incarceration delegate"""
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_incarceration_delegate import (
    StateSpecificIncarcerationDelegate,
)
from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_incarceration_period import (
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod


class UsPaIncarcerationDelegate(StateSpecificIncarcerationDelegate):
    """US_PA implementation of the incarceration delegate"""

    def is_period_included_in_state_population(
        self,
        incarceration_period: StateIncarcerationPeriod,
    ) -> bool:
        """
        US_PA includes incarceration periods under the supervision custodial authority only if the person
        is in the facility for shock incarceration. These represent the people who are in PVCs in US_PA.
        """
        return (
            incarceration_period.custodial_authority
            != StateCustodialAuthority.SUPERVISION_AUTHORITY
            or incarceration_period.specialized_purpose_for_incarceration
            == StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION
        )
