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
# =============================================================================
"""US_ND specific enum helper methods."""

from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodStatus,
)


def incarceration_period_status_mapper(label: str) -> StateIncarcerationPeriodStatus:
    """Parses the custody status from a string containing the external movement edge direction and active flag."""

    # TODO(#2865): Update enum normalization so that we separate by a dash instead of spaces
    direction_code, active_flag = label.split(" ")

    if direction_code == "OUT":
        return StateIncarcerationPeriodStatus.NOT_IN_CUSTODY

    if direction_code == "IN":
        if active_flag == "Y":
            return StateIncarcerationPeriodStatus.IN_CUSTODY
        if active_flag == "N":
            # If the active flag is 'N' we know that the person has left this period of custody, even if the table
            # happens to be missing an OUT edge.
            return StateIncarcerationPeriodStatus.NOT_IN_CUSTODY

    raise ValueError(f"Unexpected incarceration period raw text value [{label}]")
