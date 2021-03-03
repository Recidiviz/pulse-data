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

"""
Constants related to a person's characteristics shared between county and state
schemas.
"""
from typing import Optional, Dict

import recidiviz.common.constants.enum_canonical_strings as enum_strings
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


class Gender(EntityEnum, metaclass=EntityEnumMeta):
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    FEMALE = enum_strings.gender_female
    MALE = enum_strings.gender_male
    OTHER = enum_strings.gender_other
    TRANS = enum_strings.gender_trans
    TRANS_FEMALE = enum_strings.gender_trans_female
    TRANS_MALE = enum_strings.gender_trans_male

    @staticmethod
    def _get_default_map() -> Dict[str, Optional["Gender"]]:
        return _GENDER_MAP


class Race(EntityEnum, metaclass=EntityEnumMeta):
    AMERICAN_INDIAN_ALASKAN_NATIVE = enum_strings.race_american_indian
    ASIAN = enum_strings.race_asian
    BLACK = enum_strings.race_black
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    NATIVE_HAWAIIAN_PACIFIC_ISLANDER = enum_strings.race_hawaiian
    OTHER = enum_strings.race_other
    WHITE = enum_strings.race_white

    @staticmethod
    def _get_default_map() -> Dict[str, "Race"]:
        return _RACE_MAP


class Ethnicity(EntityEnum, metaclass=EntityEnumMeta):
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    HISPANIC = enum_strings.ethnicity_hispanic
    NOT_HISPANIC = enum_strings.ethnicity_not_hispanic

    @staticmethod
    def _get_default_map() -> Dict[str, "Ethnicity"]:
        return ETHNICITY_MAP


class ResidencyStatus(EntityEnum, metaclass=EntityEnumMeta):
    HOMELESS = enum_strings.residency_status_homeless
    PERMANENT = enum_strings.residency_status_permanent
    TRANSIENT = enum_strings.residency_status_transient

    @staticmethod
    def _get_default_map() -> Dict[str, "ResidencyStatus"]:
        raise RuntimeError("ResidencyStatus is not mapped directly")


PROTECTED_CLASSES = (Race, Ethnicity, Gender)

# MappableEnum.parse will strip punctuation and separate tokens with a single
# space. Add mappings here using a single space between words and numbers.
# For example, `N/A` can be written as `N A` and `(10%)` can be written as `10`.
_GENDER_MAP = {
    "F": Gender.FEMALE,
    "FEMALE": Gender.FEMALE,
    "M": Gender.MALE,
    "MALE": Gender.MALE,
    "NOT SPECIFIED": None,
    "O": Gender.OTHER,
    "OTHER": Gender.OTHER,
    "TRANS FEMALE TRANS WOMAN": Gender.TRANS_FEMALE,
    "TRANS MALE TRANS MAN": Gender.TRANS_MALE,
    "U": Gender.EXTERNAL_UNKNOWN,
    "UNKNOWN": Gender.EXTERNAL_UNKNOWN,
}

# MappableEnum.parse will strip punctuation and separate tokens with a single
# space. Add mappings here using a single space between words and numbers.
# For example, `N/A` can be written as `N A` and `(10%)` can be written as `10`.
_RACE_MAP = {
    "A": Race.ASIAN,
    "AFRICAN AMER": Race.BLACK,
    "AFRICAN AMERICAN": Race.BLACK,
    "ALL OTHERS": Race.OTHER,
    "AMER IND ALASKAN NAT": Race.AMERICAN_INDIAN_ALASKAN_NATIVE,
    "AMERICAN INDIAN": Race.AMERICAN_INDIAN_ALASKAN_NATIVE,
    "AMERICAN INDIAN ALASKAN": Race.AMERICAN_INDIAN_ALASKAN_NATIVE,
    "AMERICAN INDIAN ALASKAN NATIVE": Race.AMERICAN_INDIAN_ALASKAN_NATIVE,
    "AMERICAN INDIAN OF ALASKA NATIVE": Race.AMERICAN_INDIAN_ALASKAN_NATIVE,
    "AMERICAN INDIAN OR ALASKA NATIVE": Race.AMERICAN_INDIAN_ALASKAN_NATIVE,
    "AMERICAN INDIAN OR ALASKAN NATIVE": Race.AMERICAN_INDIAN_ALASKAN_NATIVE,
    "AMERICAN INDIAN ALASKA NATIVE": Race.AMERICAN_INDIAN_ALASKAN_NATIVE,
    "ASIAN": Race.ASIAN,
    "ASIAN PAC IS": Race.ASIAN,
    "ASIAN PAC ISLANDER": Race.ASIAN,
    "ASIAN PACIFIC ISLAND": Race.ASIAN,
    "ASIAN OR PACIFIC ISLANDER": Race.ASIAN,
    "ASIAN OR PACIFIC ISLAND ASIAN INDIANS POLYNESIONS": Race.ASIAN,
    "ASIAN PACIFIC ISLANDER": Race.ASIAN,
    "ASIAN PACIFICISLANDER": Race.ASIAN,
    "BI RACIAL": Race.OTHER,
    "BIRACIAL": Race.OTHER,
    "B": Race.BLACK,
    "BLACK": Race.BLACK,
    "BLACK OR AFRICAN AMERICAN": Race.BLACK,
    "BLACK ORIGINS OF AFRICA": Race.BLACK,
    "BROWN": Race.OTHER,
    "CAUCASIAN": Race.WHITE,
    "FILIPINO": Race.ASIAN,
    "FILLIPINO": Race.ASIAN,
    "HAWAIIAN PACIFIC ISLANDER": Race.NATIVE_HAWAIIAN_PACIFIC_ISLANDER,
    "I": Race.AMERICAN_INDIAN_ALASKAN_NATIVE,
    "INDIAN": Race.AMERICAN_INDIAN_ALASKAN_NATIVE,
    "INDIAN ALASKAN NATIVE": Race.AMERICAN_INDIAN_ALASKAN_NATIVE,
    "INDIAN OR ALASKAN NATIVE": Race.AMERICAN_INDIAN_ALASKAN_NATIVE,
    "KOREAN": Race.ASIAN,
    "LAOTIAN": Race.ASIAN,
    "MIDDLE EASTERN": Race.WHITE,
    "MULTIRACIAL": Race.OTHER,
    "N A": Race.EXTERNAL_UNKNOWN,
    "N ASIAN PACIFIC ISLANDER NON": Race.NATIVE_HAWAIIAN_PACIFIC_ISLANDER,
    "NATIVE AM": Race.AMERICAN_INDIAN_ALASKAN_NATIVE,
    "NATIV AMER": Race.AMERICAN_INDIAN_ALASKAN_NATIVE,
    "NATIVE AMERICAN": Race.AMERICAN_INDIAN_ALASKAN_NATIVE,
    "NATIVE HAW": Race.NATIVE_HAWAIIAN_PACIFIC_ISLANDER,
    "NATIVE HAWAIIAN PACIFIC ISLANDER": Race.NATIVE_HAWAIIAN_PACIFIC_ISLANDER,
    "NATIVE HAWAIIAN OR OTHER PACIFIC ISLANDER": Race.NATIVE_HAWAIIAN_PACIFIC_ISLANDER,
    "O": Race.OTHER,
    "ORIENTAL": Race.ASIAN,
    "OTHER": Race.OTHER,
    "OTHER ASIAN": Race.ASIAN,
    "OTHER PACIFIC ISLANDER": Race.ASIAN,
    "PACIFIC ISLANDER": Race.ASIAN,
    "SAMOAN": Race.NATIVE_HAWAIIAN_PACIFIC_ISLANDER,
    "U": Race.EXTERNAL_UNKNOWN,
    "UNAVAILABLE": Race.EXTERNAL_UNKNOWN,
    "UNKNOWN": Race.EXTERNAL_UNKNOWN,
    "UNKOWN": Race.EXTERNAL_UNKNOWN,
    "W": Race.WHITE,
    "WHTE": Race.WHITE,
    "WHITE": Race.WHITE,
    "WHITE EURP N AFR": Race.WHITE,
    "WHITE EURP N AFR MID EAS": Race.WHITE,
    "WHITE OR HISPANIC": Race.WHITE,
}

# MappableEnum.parse will strip punctuation and separate tokens with a single
# space. Add mappings here using a single space between words and numbers.
# For example, `N/A` can be written as `N A` and `(10%)` can be written as `10`.
# Public so that EnumOverrides default can map race to ethnicity.
ETHNICITY_MAP = {
    "H": Ethnicity.HISPANIC,
    "H WHITE LATIN HISPANIC": Ethnicity.HISPANIC,
    "HISP": Ethnicity.HISPANIC,
    "HISPANIC": Ethnicity.HISPANIC,
    "HISPANIC LATINO": Ethnicity.HISPANIC,
    "HISPANIC OR LATINO": Ethnicity.HISPANIC,
    "L": Ethnicity.HISPANIC,
    "NOT HISPANIC": Ethnicity.NOT_HISPANIC,
    "N": Ethnicity.NOT_HISPANIC,
    "REFUSED": Ethnicity.EXTERNAL_UNKNOWN,
    "U": Ethnicity.EXTERNAL_UNKNOWN,
    "UNKNOWN": Ethnicity.EXTERNAL_UNKNOWN,
}

RESIDENCY_STATUS_SUBSTRING_MAP = {
    "HOMELESS": ResidencyStatus.HOMELESS,
    "PERMANENT": ResidencyStatus.PERMANENT,
    "TRANSIENT": ResidencyStatus.TRANSIENT,
}
