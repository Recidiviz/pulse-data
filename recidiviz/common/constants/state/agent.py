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

"""Constants related to an Agent entity."""

import recidiviz.common.constants.state.enum_canonical_strings as \
    state_enum_strings
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


class AgentType(EntityEnum, metaclass=EntityEnumMeta):
    JUDGE = state_enum_strings.agent_judge
    PAROLE_BOARD_MEMBER = state_enum_strings.agent_parole_board_member
    # A parole/probation officer (PO)
    SUPERVISION_OFFICER = state_enum_strings.agent_supervision_officer
    UNIT_SUPERVISOR = state_enum_strings.agent_unit_supervisor

    @staticmethod
    def _get_default_map():
        return _AGENT_TYPE_MAP


_AGENT_TYPE_MAP = {
    'JUDGE': AgentType.JUDGE,
    'PAROLE BOARD MEMBER': AgentType.PAROLE_BOARD_MEMBER,
    'SUPERVISION OFFICER': AgentType.SUPERVISION_OFFICER,
    'UNIT SUPERVISOR': AgentType.UNIT_SUPERVISOR,
}
