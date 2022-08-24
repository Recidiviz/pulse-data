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
    # A parole/probation officer (PO)
    SUPERVISION_OFFICER = state_enum_strings.state_agent_supervision_officer
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, "StateAgentType"]:
        return _STATE_AGENT_TYPE_MAP

    @classmethod
    def get_enum_description(cls) -> str:
        return "A type of official within the criminal justice system."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_AGENT_TYPE_VALUE_DESCRIPTIONS


_STATE_AGENT_TYPE_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateAgentType.CORRECTIONAL_OFFICER: "An official of the state department of "
    "corrections who oversees individuals while they are in an incarceration facility.",
    StateAgentType.JUDGE: "An official of the Judicial branch with authority to "
    "decide lawsuits and determine sentences.",
    StateAgentType.JUSTICE: "A `JUDGE` who sits on a Supreme Court.",
    StateAgentType.SUPERVISION_OFFICER: "An official of the state supervision "
    "department who oversees someone while they are on supervision. Also referred to "
    "as a probation/parole officer.",
}


_STATE_AGENT_TYPE_MAP = {
    "PRESENT WITHOUT INFO": StateAgentType.PRESENT_WITHOUT_INFO,
    "CORRECTIONAL OFFICER": StateAgentType.CORRECTIONAL_OFFICER,
    "JUDGE": StateAgentType.JUDGE,
    "JUSTICE": StateAgentType.JUSTICE,
    "SUPERVISION OFFICER": StateAgentType.SUPERVISION_OFFICER,
    "INTERNAL UNKNOWN": StateAgentType.INTERNAL_UNKNOWN,
    "EXTERNAL UNKNOWN": StateAgentType.EXTERNAL_UNKNOWN,
}


@unique
class StateAgentSubtype(StateEntityEnum):
    SUPERVISION_OFFICER = state_enum_strings.state_agent_subtype_supervision_officer
    SUPERVISION_OFFICER_SUPERVISOR = (
        state_enum_strings.state_agent_subtype_supervision_officer_supervisor
    )
    SUPERVISION_REGIONAL_MANAGER = (
        state_enum_strings.state_agent_subtype_supervision_regional_manager
    )
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, "StateAgentSubtype"]:
        return _STATE_AGENT_SUBTYPE_MAP

    @classmethod
    def get_enum_description(cls) -> str:
        return "A subtype of the agent's position within the DOC."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_AGENT_SUBTYPE_VALUE_DESCRIPTIONS


_STATE_AGENT_SUBTYPE_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateAgentSubtype.SUPERVISION_OFFICER: "A parole and/or probation officer/agent with"
    " regular contact with clients.",
    StateAgentSubtype.SUPERVISION_OFFICER_SUPERVISOR: "A parole and/or probation officer"
    "/agent who serves in a manager or supervisor role and doesn't supervise a typical caseload.",
    StateAgentSubtype.SUPERVISION_REGIONAL_MANAGER: "A district or regional manager, "
    "usually a supervisor of supervisors, but may sometimes supervise parole "
    "officers/agents directly and also sometimes carry a caseload when the agency is short-staffed.",
}

_STATE_AGENT_SUBTYPE_MAP = {
    "SUPERVISION OFFICER": StateAgentSubtype.SUPERVISION_OFFICER,
    "SUPERVISION OFFICER SUPERVISOR": StateAgentSubtype.SUPERVISION_OFFICER_SUPERVISOR,
    "SUPERVISION REGIONAL MANAGER": StateAgentSubtype.SUPERVISION_REGIONAL_MANAGER,
    "INTERNAL UNKNOWN": StateAgentSubtype.INTERNAL_UNKNOWN,
    "EXTERNAL UNKNOWN": StateAgentSubtype.EXTERNAL_UNKNOWN,
}
