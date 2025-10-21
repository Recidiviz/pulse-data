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


class StateSex(StateEntityEnum):
    FEMALE = state_enum_strings.state_sex_female
    MALE = state_enum_strings.state_sex_male
    OTHER = state_enum_strings.state_sex_other
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return "The sex of record of the person."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_SEX_VALUE_DESCRIPTIONS


_STATE_SEX_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateSex.FEMALE: "Female",
    StateSex.MALE: "Male",
    StateSex.OTHER: "Other",
}


class StateGender(StateEntityEnum):
    FEMALE = state_enum_strings.state_gender_female
    MALE = state_enum_strings.state_gender_male
    NON_BINARY = state_enum_strings.state_gender_non_binary
    TRANS = state_enum_strings.state_gender_trans
    TRANS_FEMALE = state_enum_strings.state_gender_trans_female
    TRANS_MALE = state_enum_strings.state_gender_trans_male
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return "The gender of the person."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_GENDER_VALUE_DESCRIPTIONS


_STATE_GENDER_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateGender.FEMALE: "Female",
    StateGender.MALE: "Male",
    StateGender.NON_BINARY: "Non-binary",
    StateGender.TRANS: "Trans",
    StateGender.TRANS_FEMALE: "Trans female",
    StateGender.TRANS_MALE: "Trans male",
}


class StateRace(StateEntityEnum):
    AMERICAN_INDIAN_ALASKAN_NATIVE = state_enum_strings.state_race_american_indian
    ASIAN = state_enum_strings.state_race_asian
    BLACK = state_enum_strings.state_race_black
    NATIVE_HAWAIIAN_PACIFIC_ISLANDER = state_enum_strings.state_race_hawaiian
    OTHER = state_enum_strings.state_race_other
    WHITE = state_enum_strings.state_race_white
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return "A racial identity of the person."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_RACE_VALUE_DESCRIPTIONS


_STATE_RACE_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateRace.AMERICAN_INDIAN_ALASKAN_NATIVE: "American Indian Alaskan Native",
    StateRace.ASIAN: "Asian",
    StateRace.BLACK: "Black",
    StateRace.NATIVE_HAWAIIAN_PACIFIC_ISLANDER: "Native Hawaiian Pacific Islander",
    StateRace.OTHER: "Used when the state data explicitly indicates that the person’s "
    "race is “Other”. If there is a race value that does not map to one of our "
    "`StateRace` enum values, then `INTERNAL_UNKNOWN` should be used.",
    StateRace.WHITE: "White",
}


class StateEthnicity(StateEntityEnum):
    HISPANIC = state_enum_strings.state_ethnicity_hispanic
    NOT_HISPANIC = state_enum_strings.state_ethnicity_not_hispanic
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return "An ethnicity of the person."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_ETHNICITY_VALUE_DESCRIPTIONS


_STATE_ETHNICITY_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateEthnicity.HISPANIC: "Used when a person is Hispanic.",
    StateEthnicity.NOT_HISPANIC: "Used when a person is not Hispanic.",
}


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
