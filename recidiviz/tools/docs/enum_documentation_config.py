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
# TODO(#12127): Delete this file once all enums are documented
"""Temporary config file used by enum doc generator while enum documentation is
in-progress."""
from typing import List, Type

from recidiviz.common.constants.entity_enum import EntityEnum
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentDischargeReason,
)

ENUMS_WITH_INCOMPLETE_DOCS: List[Type[EntityEnum]] = [
    # TODO(#12766): Delete this deprecated enum
    StateProgramAssignmentDischargeReason,
]
