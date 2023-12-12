# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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

"""Constants related to a StatePersonHousingStatusPeriod entity."""
from enum import unique
from typing import Dict

import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum


@unique
class StatePersonHousingStatusType(StateEntityEnum):
    UNHOUSED = state_enum_strings.state_person_housing_status_type_unhoused
    TEMPORARY_OR_SUPPORTIVE_HOUSING = (
        state_enum_strings.state_person_housing_status_type_temporary_or_supportive_housing
    )
    PERMANENT_RESIDENCE = (
        state_enum_strings.state_person_housing_status_type_permanent_residence
    )
    FACILITY = state_enum_strings.state_person_housing_status_type_facility
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return "The type of a state person housing status."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_PERSON_HOUSING_STATUS_VALUE_DESCRIPTIONS


_STATE_PERSON_HOUSING_STATUS_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StatePersonHousingStatusType.UNHOUSED: "This person is known to have no permanent or temporary housing available.",
    StatePersonHousingStatusType.TEMPORARY_OR_SUPPORTIVE_HOUSING: "This person is residing in temporary "
    "or supportive housing.",
    StatePersonHousingStatusType.PERMANENT_RESIDENCE: "This person resides in a permanent dwelling.",
    StatePersonHousingStatusType.FACILITY: "This person is housed in a custodial facility.",
}
