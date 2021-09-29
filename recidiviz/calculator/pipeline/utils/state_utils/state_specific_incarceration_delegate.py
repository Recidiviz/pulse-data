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
"""Contains the StateSpecificIncarcerationDelegate, the interface
for state-specific decisions involved in categorizing various attributes of
incarceration."""
import abc

from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod


class StateSpecificIncarcerationDelegate(abc.ABC):
    """Interface for state-specific decisions involved in categorizing various attributes of incarceration."""

    def is_period_included_in_state_population(  # pylint: disable=unused-argument
        self,
        incarceration_period: StateIncarcerationPeriod,
    ) -> bool:
        """Determines whether the given incarceration period counts towards the state's incarceration population.

        Default behavior is that it is included in the state population.
        """
        return True
