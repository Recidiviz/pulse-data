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


class Gender:
    FEMALE = enum_strings.gender_female
    MALE = enum_strings.gender_male
    TRANS_FEMALE = enum_strings.gender_trans_female
    TRANS_MALE = enum_strings.gender_trans_male

    @staticmethod
    def from_str(label):
        return _GENDER_MAP[label.upper()]


class Race:
    AMERICAN_INDIAN_ALASKAN_NATIVE = enum_strings.race_american_indian
    ASIAN = enum_strings.race_asian
    BLACK = enum_strings.race_black
    NATIVE_HAWAIIAN_PACIFIC_ISLANDER = enum_strings.race_hawaiian
    OTHER = enum_strings.race_other
    WHITE = enum_strings.race_white

    @staticmethod
    def from_str(label):
        return _RACE_MAP[label.upper()]


class Ethnicity:
    HISPANIC = enum_strings.ethnicity_hispanic
    NOT_HISPANIC = enum_strings.ethnicity_not_hispanic

    @staticmethod
    def from_str(label):
        return _ETHNICITY_MAP[label.upper()]


_GENDER_MAP = {
    'F': Gender.FEMALE,
    'FEMALE': Gender.FEMALE,
    'M': Gender.MALE,
    'MALE': Gender.MALE,
    'TRANS FEMALE/TRANS WOMAN': Gender.TRANS_FEMALE,
    'TRANS MALE/TRANS MAN': Gender.TRANS_MALE,
}

_RACE_MAP = {
    'AMERICAN INDIAN/ALASKAN NATIVE': Race.AMERICAN_INDIAN_ALASKAN_NATIVE,
    'ASIAN': Race.ASIAN,
    'B': Race.BLACK,
    'BLACK': Race.BLACK,
    'BLACK-ORIGINS OF AFRICA': Race.BLACK,
    'NATIVE HAWAIIAN/PACIFIC ISLANDER': Race.NATIVE_HAWAIIAN_PACIFIC_ISLANDER,
    'OTHER': Race.OTHER,
    'W': Race.WHITE,
    'WHITE': Race.WHITE,
    'WHITE/EURP/ N.AFR/MID EAS': Race.WHITE,
}

_ETHNICITY_MAP = {
    'HISPANIC': Ethnicity.HISPANIC,
    'H': Ethnicity.HISPANIC,
    'NOT HISPANIC': Ethnicity.NOT_HISPANIC,
}
