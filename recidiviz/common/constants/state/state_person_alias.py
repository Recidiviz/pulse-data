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

"""Constants related to a StatePersonAlias entity."""
from enum import unique
from typing import Dict

import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum


@unique
class StatePersonAliasType(StateEntityEnum):
    AFFILIATION_NAME = state_enum_strings.state_person_alias_alias_type_affiliation_name
    ALIAS = state_enum_strings.state_person_alias_alias_type_alias
    GIVEN_NAME = state_enum_strings.state_person_alias_alias_type_given_name
    MAIDEN_NAME = state_enum_strings.state_person_alias_alias_type_maiden_name
    NICKNAME = state_enum_strings.state_person_alias_alias_type_nickname
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return "The type of the alias (a name that a person is called)."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_PERSON_ALIAS_VALUE_DESCRIPTIONS


_STATE_PERSON_ALIAS_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StatePersonAliasType.AFFILIATION_NAME: "Used when the alias is a name associated "
    "with some sort of group affiliation (usually a gang affiliation).",
    StatePersonAliasType.ALIAS: "Used when the alias is an additional name "
    "that the person is called.",
    StatePersonAliasType.GIVEN_NAME: "Used when the alias is the person’s given "
    "name.",
    StatePersonAliasType.MAIDEN_NAME: "Used when the alias includes the person’s "
    "maiden name.",
    StatePersonAliasType.NICKNAME: "Used when the alias is a nickname for the "
    "person.",
}
