# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Constants related to a StatePerson entity."""

from typing import Dict

import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum
from recidiviz.common.demographics import Ethnicity, Gender, Race, Sex

# Backwards-compatible aliases (new code should import from
# recidiviz.common.demographics directly).
StateEthnicity = Ethnicity
StateGender = Gender
StateRace = Race
StateSex = Sex


class StateResidencyStatus(StateEntityEnum):
    HOMELESS = state_enum_strings.state_residency_status_homeless
    PERMANENT = state_enum_strings.state_residency_status_permanent
    TRANSIENT = state_enum_strings.state_residency_status_transient
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return "The status of the person’s permanent residency."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_RESIDENCY_STATUS_VALUE_DESCRIPTIONS


_STATE_RESIDENCY_STATUS_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateResidencyStatus.HOMELESS: "Used when the person is unhoused.",
    StateResidencyStatus.TRANSIENT: "Used when the person is living in temporary "
    "housing.",
    StateResidencyStatus.PERMANENT: "Used when the person is living in a permanent "
    "residence.",
}
