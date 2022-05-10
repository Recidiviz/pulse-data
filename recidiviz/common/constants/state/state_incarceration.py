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

"""Shared constants related to custody."""
from enum import unique
from typing import Dict

import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum


# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
@unique
class StateIncarcerationType(StateEntityEnum):
    COUNTY_JAIL = state_enum_strings.state_incarceration_type_county_jail
    FEDERAL_PRISON = state_enum_strings.state_incarceration_type_federal_prison
    OUT_OF_STATE = state_enum_strings.state_incarceration_type_out_of_state
    STATE_PRISON = state_enum_strings.state_incarceration_type_state_prison
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, "StateIncarcerationType"]:
        return _STATE_INCARCERATION_TYPE_MAP

    @classmethod
    def get_enum_description(cls) -> str:
        return (
            "The classification of incarceration. This enum is currently used "
            "more as a classification of the kind of facility that a person is "
            "in. There is outstanding work to migrate this enum to explicitly "
            "capture facility types (see TODO(#11509) and TODO(#2912))."
        )

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_INCARCERATION_TYPE_VALUE_DESCRIPTIONS


_STATE_INCARCERATION_TYPE_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateIncarcerationType.COUNTY_JAIL: "Describes being incarcerated in a county "
    "jail.",
    StateIncarcerationType.FEDERAL_PRISON: "Describes being incarcerated in a federal "
    "prison.",
    StateIncarcerationType.OUT_OF_STATE: "Describes being incarcerated in some kind "
    "of facility in a state other than the one in which the data is coming from.",
    StateIncarcerationType.STATE_PRISON: "Describes being incarcerated in a state "
    "prison.",
}


_STATE_INCARCERATION_TYPE_MAP: Dict[str, StateIncarcerationType] = {
    "JAIL": StateIncarcerationType.COUNTY_JAIL,
    "COUNTY JAIL": StateIncarcerationType.COUNTY_JAIL,
    "EXTERNAL UNKNOWN": StateIncarcerationType.EXTERNAL_UNKNOWN,
    "FEDERAL PRISON": StateIncarcerationType.FEDERAL_PRISON,
    "OUT OF STATE": StateIncarcerationType.OUT_OF_STATE,
    "PRISON": StateIncarcerationType.STATE_PRISON,
    "STATE PRISON": StateIncarcerationType.STATE_PRISON,
    "INTERNAL UNKNOWN": StateIncarcerationType.INTERNAL_UNKNOWN,
}
