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

from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta
import recidiviz.common.constants.state.enum_canonical_strings as \
    state_enum_strings


class StatePersonAliasType(EntityEnum, metaclass=EntityEnumMeta):
    AFFILIATION_NAME = \
        state_enum_strings.state_person_alias_alias_type_affiliation_name
    ALIAS = state_enum_strings.state_person_alias_alias_type_alias
    GIVEN_NAME = state_enum_strings.state_person_alias_alias_type_given_name
    MAIDEN_NAME = state_enum_strings.state_person_alias_alias_type_maiden_name
    NICKNAME = state_enum_strings.state_person_alias_alias_type_nickname

    @staticmethod
    def _get_default_map():
        return _ALIAS_TYPE_MAP


_ALIAS_TYPE_MAP = {
    'AFFILIATION NAME': StatePersonAliasType.AFFILIATION_NAME,
    'GANG NAME': StatePersonAliasType.AFFILIATION_NAME,
    'ALIAS': StatePersonAliasType.ALIAS,
    'GIVEN': StatePersonAliasType.GIVEN_NAME,
    'GIVEN NAME': StatePersonAliasType.GIVEN_NAME,
    'MAIDEN': StatePersonAliasType.MAIDEN_NAME,
    'MAIDEN NAME': StatePersonAliasType.MAIDEN_NAME,
    'NICKNAME': StatePersonAliasType.NICKNAME,
}
