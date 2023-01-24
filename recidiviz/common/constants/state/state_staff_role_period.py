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

"""Constants related to a StateStaff entity."""
from enum import unique
from typing import Dict

import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings

# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum


@unique
class StateStaffRoleType(StateEntityEnum):
    SUPERVISION_OFFICER = state_enum_strings.state_staff_role_type_supervision_officer
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, "StateStaffRoleType"]:
        return _STATE_STAFF_ROLE_TYPE_MAP

    @classmethod
    def get_enum_description(cls) -> str:
        return (
            "A general type describing a staff member’s role within the justice system."
        )

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_STAFF_ROLE_TYPE_VALUE_DESCRIPTIONS


_STATE_STAFF_ROLE_TYPE_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateStaffRoleType.SUPERVISION_OFFICER: "An official of the state supervision "
    "department who oversees someone while they are on supervision. Also referred to "
    "as a probation/parole officer.",
}


_STATE_STAFF_ROLE_TYPE_MAP = {
    "SUPERVISION OFFICER": StateStaffRoleType.SUPERVISION_OFFICER,
    "INTERNAL UNKNOWN": StateStaffRoleType.INTERNAL_UNKNOWN,
    "EXTERNAL UNKNOWN": StateStaffRoleType.EXTERNAL_UNKNOWN,
}


@unique
class StateStaffRoleSubtype(StateEntityEnum):
    SUPERVISION_OFFICER = (
        state_enum_strings.state_staff_role_subtype_supervision_officer
    )
    SUPERVISION_OFFICER_SUPERVISOR = (
        state_enum_strings.state_staff_role_subtype_supervision_officer_supervisor
    )
    SUPERVISION_REGIONAL_MANAGER = (
        state_enum_strings.state_staff_role_subtype_supervision_regional_manager
    )
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, "StateStaffRoleSubtype"]:
        return _STATE_STAFF_ROLE_TYPE_SUBTYPE_MAP

    @classmethod
    def get_enum_description(cls) -> str:
        return (
            "A subtype providing more detailed information about a staff member’s role "
            "within the justice system."
        )

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_STAFF_ROLE_TYPE_SUBTYPE_VALUE_DESCRIPTIONS


_STATE_STAFF_ROLE_TYPE_SUBTYPE_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateStaffRoleSubtype.SUPERVISION_OFFICER: "A parole and/or probation officer/agent with"
    " regular contact with clients.",
    StateStaffRoleSubtype.SUPERVISION_OFFICER_SUPERVISOR: "A parole and/or probation officer"
    "/agent who serves in a manager or supervisor role and doesn't supervise a typical caseload.",
    StateStaffRoleSubtype.SUPERVISION_REGIONAL_MANAGER: "A district or regional manager, "
    "usually a supervisor of supervisors, but may sometimes supervise parole "
    "officers/agents directly and also sometimes carry a caseload when the agency is short-staffed.",
}

_STATE_STAFF_ROLE_TYPE_SUBTYPE_MAP = {
    "SUPERVISION OFFICER": StateStaffRoleSubtype.SUPERVISION_OFFICER,
    "SUPERVISION OFFICER SUPERVISOR": StateStaffRoleSubtype.SUPERVISION_OFFICER_SUPERVISOR,
    "SUPERVISION REGIONAL MANAGER": StateStaffRoleSubtype.SUPERVISION_REGIONAL_MANAGER,
    "INTERNAL UNKNOWN": StateStaffRoleSubtype.INTERNAL_UNKNOWN,
    "EXTERNAL UNKNOWN": StateStaffRoleSubtype.EXTERNAL_UNKNOWN,
}
