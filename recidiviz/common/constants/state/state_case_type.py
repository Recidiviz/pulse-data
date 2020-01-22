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

"""Constants related to a StateSupervisionCaseTypeEntry entity."""
import recidiviz.common.constants.state.enum_canonical_strings as \
    state_enum_strings
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


class StateSupervisionCaseType(EntityEnum, metaclass=EntityEnumMeta):
    DOMESTIC_VIOLENCE = \
        state_enum_strings.state_supervision_case_type_domestic_violence
    GENERAL = state_enum_strings.state_supervision_case_type_general
    SERIOUS_MENTAL_ILLNESS = \
        state_enum_strings.state_supervision_case_type_serious_mental_illness
    SEX_OFFENDER = state_enum_strings.state_supervision_case_type_sex_offender

    @staticmethod
    def _get_default_map():
        return _STATE_CASE_TYPE_MAP


_STATE_CASE_TYPE_MAP = {
    'DOMESTIC VIOLENCE': StateSupervisionCaseType.DOMESTIC_VIOLENCE,
    'GENERAL': StateSupervisionCaseType.GENERAL,
    'SERIOUS MENTAL ILLNESS': StateSupervisionCaseType.SERIOUS_MENTAL_ILLNESS,
    'SEX OFFENDER': StateSupervisionCaseType.SEX_OFFENDER,
}
