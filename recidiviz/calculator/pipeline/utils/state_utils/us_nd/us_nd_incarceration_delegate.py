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
"""US_ND implementation of the incarceration delegate"""
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_incarceration_delegate import (
    StateSpecificIncarcerationDelegate,
)
from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod


class UsNdIncarcerationDelegate(StateSpecificIncarcerationDelegate):
    """US_ND implementation of the incarceration delegate"""

    def is_period_included_in_state_population(  # pylint: disable=unused-argument
        self,
        incarceration_period: StateIncarcerationPeriod,
    ) -> bool:
        """In US_ND, only periods of incarceration that are under the custodial
        authority of the state prison are included in the state population.
        """
        return (
            # TODO(#3641): Delete the check for None once we're setting the
            #  custodial_authority field at ingest for US_ND
            incarceration_period.custodial_authority is None
            or incarceration_period.custodial_authority
            == StateCustodialAuthority.STATE_PRISON
        )
