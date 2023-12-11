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

"""Constants related to a StatePersonAddressPeriod entity."""
from enum import unique
from typing import Dict

import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum


@unique
class StatePersonAddressType(StateEntityEnum):
    PHYSICAL_RESIDENCE = state_enum_strings.state_person_address_type_physical_residence
    PHYSICAL_OTHER = state_enum_strings.state_person_address_type_physical_other
    MAILING_ONLY = state_enum_strings.state_person_address_type_mailing_only
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return "The type of a state person address."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_PERSON_ADDRESS_VALUE_DESCRIPTIONS


_STATE_PERSON_ADDRESS_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StatePersonAddressType.PHYSICAL_RESIDENCE: "A location where a person resides.",
    StatePersonAddressType.PHYSICAL_OTHER: "A location where a person has a physical presence"
    " (school, employer, street corner, etc).",
    StatePersonAddressType.MAILING_ONLY: "An address to which mail can be sent, "
    "even if the person doesn't reside there.",
}
