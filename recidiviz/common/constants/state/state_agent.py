# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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

"""Constants related to a StateAgent entity."""
from enum import unique
from typing import Dict

import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings

# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum


@unique
class StateAgentType(StateEntityEnum):
    PRESENT_WITHOUT_INFO = state_enum_strings.present_without_info
    CORRECTIONAL_OFFICER = state_enum_strings.state_agent_correctional_officer
    JUDGE = state_enum_strings.state_agent_judge
    JUSTICE = state_enum_strings.state_agent_justice
    PAROLE_BOARD_MEMBER = state_enum_strings.state_agent_parole_board_member
    # A parole/probation officer (PO)
    SUPERVISION_OFFICER = state_enum_strings.state_agent_supervision_officer
    UNIT_SUPERVISOR = state_enum_strings.state_agent_unit_supervisor
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, "StateAgentType"]:
        return _STATE_AGENT_TYPE_MAP


_STATE_AGENT_TYPE_MAP = {
    "PRESENT WITHOUT INFO": StateAgentType.PRESENT_WITHOUT_INFO,
    "CORRECTIONAL OFFICER": StateAgentType.CORRECTIONAL_OFFICER,
    "JUDGE": StateAgentType.JUDGE,
    "JUSTICE": StateAgentType.JUSTICE,
    "PAROLE BOARD MEMBER": StateAgentType.PAROLE_BOARD_MEMBER,
    "SUPERVISION OFFICER": StateAgentType.SUPERVISION_OFFICER,
    "UNIT SUPERVISOR": StateAgentType.UNIT_SUPERVISOR,
    "INTERNAL UNKNOWN": StateAgentType.INTERNAL_UNKNOWN,
}
