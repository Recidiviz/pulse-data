# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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

"""Constants related to a person entity."""
import recidiviz.common.constants.enum_canonical_strings as enum_strings
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


class Gender(EntityEnum, metaclass=EntityEnumMeta):
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    FEMALE = enum_strings.gender_female
    MALE = enum_strings.gender_male
    OTHER = enum_strings.gender_other
    TRANS_FEMALE = enum_strings.gender_trans_female
    TRANS_MALE = enum_strings.gender_trans_male

    @staticmethod
    def _get_default_map():
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
    def _get_default_map():
        return _RACE_MAP


class Ethnicity(EntityEnum, metaclass=EntityEnumMeta):
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    HISPANIC = enum_strings.ethnicity_hispanic
    NOT_HISPANIC = enum_strings.ethnicity_not_hispanic

    @staticmethod
    def _get_default_map():
        return _ETHNICITY_MAP


# MappableEnum.parse will strip punctuation and separate tokens with a single
# space. Add mappings here using a single space between words and numbers.
# For example, `N/A` can be written as `N A` and `(10%)` can be written as `10`.
_GENDER_MAP = {
    'F': Gender.FEMALE,
    'FEMALE': Gender.FEMALE,
    'M': Gender.MALE,
    'MALE': Gender.MALE,
    'OTHER': Gender.OTHER,
    'TRANS FEMALE TRANS WOMAN': Gender.TRANS_FEMALE,
    'TRANS MALE TRANS MAN': Gender.TRANS_MALE,
    'UNKNOWN': Gender.EXTERNAL_UNKNOWN,
}

# MappableEnum.parse will strip punctuation and separate tokens with a single
# space. Add mappings here using a single space between words and numbers.
# For example, `N/A` can be written as `N A` and `(10%)` can be written as `10`.
_RACE_MAP = {
    'AMERICAN INDIAN': Race.AMERICAN_INDIAN_ALASKAN_NATIVE,
    'AMERICAN INDIAN ALASKAN NATIVE': Race.AMERICAN_INDIAN_ALASKAN_NATIVE,
    'AMERICAN INDIAN OR ALASKAN NATIVE': Race.AMERICAN_INDIAN_ALASKAN_NATIVE,
    'NATIVE AM': Race.AMERICAN_INDIAN_ALASKAN_NATIVE,
    'NATIV AMER': Race.AMERICAN_INDIAN_ALASKAN_NATIVE,
    'I': Race.AMERICAN_INDIAN_ALASKAN_NATIVE,
    'INDIAN': Race.AMERICAN_INDIAN_ALASKAN_NATIVE,
    'INDIAN ALASKAN NATIVE': Race.AMERICAN_INDIAN_ALASKAN_NATIVE,
    'ASIAN': Race.ASIAN,
    'ASIAN OR PACIFIC ISLANDER': Race.ASIAN,
    'ASIAN PACIFIC ISLANDER': Race.ASIAN,
    'FILIPINO': Race.ASIAN,
    'PACIFIC ISLANDER': Race.ASIAN,
    'AFRICAN AMERICAN': Race.BLACK,
    'B': Race.BLACK,
    'BLACK': Race.BLACK,
    'BLACK ORIGINS OF AFRICA': Race.BLACK,
    'NATIVE HAWAIIAN PACIFIC ISLANDER': Race.NATIVE_HAWAIIAN_PACIFIC_ISLANDER,
    'SAMOAN': Race.NATIVE_HAWAIIAN_PACIFIC_ISLANDER,
    'ALL OTHERS': Race.OTHER,
    'OTHER': Race.OTHER,
    'OTHER ASIAN': Race.ASIAN,
    'BIRACIAL': Race.OTHER,
    'N A': Race.EXTERNAL_UNKNOWN,
    'U': Race.EXTERNAL_UNKNOWN,
    'UNKNOWN': Race.EXTERNAL_UNKNOWN,
    'CAUCASIAN': Race.WHITE,
    'MIDDLE EASTERN': Race.WHITE,
    'W': Race.WHITE,
    'WHITE': Race.WHITE,
    'WHITE EURP N AFR MID EAS': Race.WHITE,
    'WHITE EURP N AFR': Race.WHITE,
}

# MappableEnum.parse will strip punctuation and separate tokens with a single
# space. Add mappings here using a single space between words and numbers.
# For example, `N/A` can be written as `N A` and `(10%)` can be written as `10`.
_ETHNICITY_MAP = {
    'H': Ethnicity.HISPANIC,
    'HISPANIC': Ethnicity.HISPANIC,
    'HISPANIC OR LATINO': Ethnicity.HISPANIC,
    'L': Ethnicity.HISPANIC,
    'NOT HISPANIC': Ethnicity.NOT_HISPANIC,
    'N': Ethnicity.NOT_HISPANIC,
    'UNKNOWN': Ethnicity.EXTERNAL_UNKNOWN,
}
