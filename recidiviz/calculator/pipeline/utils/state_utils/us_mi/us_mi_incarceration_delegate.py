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
"""Contains US_MI implementation of the StateSpecificIncarcerationDelegate."""
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_incarceration_delegate import (
    StateSpecificIncarcerationDelegate,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod


class UsMiIncarcerationDelegate(StateSpecificIncarcerationDelegate):
    """US_MI implementation of the StateSpecificIncarcerationDelegate."""

    def is_period_included_in_state_population(  # pylint: disable=unused-argument
        self,
        incarceration_period: StateIncarcerationPeriod,
    ) -> bool:
        """In Michigan, a parole board hold is someone who is awaiting court mandate as
        to whether or not they've violated their terms of parole. In this case, while they
        are waiting in a facility, they are not to be counted towards the incarceration
        population.
        """
        return (
            incarceration_period.specialized_purpose_for_incarceration
            != StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD
        )
