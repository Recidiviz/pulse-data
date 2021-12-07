#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2021 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Contains policy requirements that are specific to Missouri."""

from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
)

# TODO(#10286): Get correct copy for US_MO reports
US_MO_SUPERVISION_LEVEL_NAMES = {
    StateSupervisionLevel.MINIMUM: "Low",
    StateSupervisionLevel.MEDIUM: "Medium",
    StateSupervisionLevel.HIGH: "High",
}
