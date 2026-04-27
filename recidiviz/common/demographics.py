# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Person demographic enums shared across pipelines and tenants."""

from typing import Dict

from recidiviz.common import demographics_strings, entity_enum_strings
from recidiviz.common.entity_enum import EntityEnum


class Ethnicity(EntityEnum):
    HISPANIC = demographics_strings.ethnicity_hispanic
    NOT_HISPANIC = demographics_strings.ethnicity_not_hispanic
    INTERNAL_UNKNOWN = entity_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = entity_enum_strings.external_unknown
    PRESENT_WITHOUT_INFO = entity_enum_strings.present_without_info

    @classmethod
    def get_enum_description(cls) -> str:
        return "The ethnicity of the person."

    @classmethod
    def get_value_descriptions(cls) -> Dict["EntityEnum", str]:
        return _ETHNICITY_VALUE_DESCRIPTIONS


_ETHNICITY_VALUE_DESCRIPTIONS: Dict[EntityEnum, str] = {
    Ethnicity.HISPANIC: "Used when a person is Hispanic.",
    Ethnicity.NOT_HISPANIC: "Used when a person is not Hispanic.",
}


class Gender(EntityEnum):
    FEMALE = demographics_strings.gender_female
    MALE = demographics_strings.gender_male
    NON_BINARY = demographics_strings.gender_non_binary
    INTERNAL_UNKNOWN = entity_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = entity_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return "The gender of the person."

    @classmethod
    def get_value_descriptions(cls) -> Dict["EntityEnum", str]:
        return _GENDER_VALUE_DESCRIPTIONS


_GENDER_VALUE_DESCRIPTIONS: Dict[EntityEnum, str] = {
    Gender.FEMALE: "Female",
    Gender.MALE: "Male",
    Gender.NON_BINARY: "Non-binary",
}


class Race(EntityEnum):
    AMERICAN_INDIAN_ALASKAN_NATIVE = demographics_strings.race_american_indian
    ASIAN = demographics_strings.race_asian
    BLACK = demographics_strings.race_black
    NATIVE_HAWAIIAN_PACIFIC_ISLANDER = demographics_strings.race_hawaiian
    OTHER = demographics_strings.race_other
    WHITE = demographics_strings.race_white
    INTERNAL_UNKNOWN = entity_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = entity_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return "A racial identity of the person."

    @classmethod
    def get_value_descriptions(cls) -> Dict["EntityEnum", str]:
        return _RACE_VALUE_DESCRIPTIONS


_RACE_VALUE_DESCRIPTIONS: Dict[EntityEnum, str] = {
    Race.AMERICAN_INDIAN_ALASKAN_NATIVE: "American Indian Alaskan Native",
    Race.ASIAN: "Asian",
    Race.BLACK: "Black",
    Race.NATIVE_HAWAIIAN_PACIFIC_ISLANDER: "Native Hawaiian Pacific Islander",
    Race.OTHER: "Used when the tenant's data explicitly indicates that the person's "
    'race is "Other". If there is a race value that does not map to one of our '
    "`Race` enum values, then `INTERNAL_UNKNOWN` should be used.",
    Race.WHITE: "White",
}


class Sex(EntityEnum):
    FEMALE = demographics_strings.sex_female
    MALE = demographics_strings.sex_male
    OTHER = demographics_strings.sex_other
    INTERNAL_UNKNOWN = entity_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = entity_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return "The sex of record of the person."

    @classmethod
    def get_value_descriptions(cls) -> Dict["EntityEnum", str]:
        return _SEX_VALUE_DESCRIPTIONS


_SEX_VALUE_DESCRIPTIONS: Dict[EntityEnum, str] = {
    Sex.FEMALE: "Female",
    Sex.MALE: "Male",
    Sex.OTHER: "Other",
}
