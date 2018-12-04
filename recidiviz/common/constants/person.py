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


class Gender(object):
    FEMALE = 'FEMALE'
    MALE = 'MALE'
    TRANS_FEMALE = 'TRANS FEMALE/TRANS WOMAN'
    TRANS_MALE = 'TRANS MALE/TRANS MAN'
    UNKNOWN = 'UNKNOWN'

class Race(object):
    AMERICAN_INDIAN_ALASKAN_NATIVE = 'AMERICAN INDIAN/ALASKAN NATIVE'
    ASIAN = 'ASIAN'
    BLACK = 'BLACK'
    NATIVE_HAWAIIAN_PACIFIC_ISLANDER = 'NATIVE HAWAIIAN/PACIFIC ISLANDER'
    OTHER = 'OTHER'
    WHITE = 'WHITE'
    UNKNOWN = 'UNKNOWN'


class Ethnicity(object):
    HISPANIC = 'HISPANIC'
    NOT_HISPANIC = 'NOT HISPANIC'
    UNKNOWN = 'UNKNOWN'

GENDER_MAP = {
    'FEMALE': Gender.FEMALE,
    'MALE': Gender.MALE,
    'TRANS FEMALE/TRANS WOMAN': Gender.TRANS_FEMALE,
    'TRANS MALE/TRANS MAN': Gender.TRANS_MALE,
    'UNKNOWN': Gender.UNKNOWN
}
RACE_MAP = {
    'AMERICAN INDIAN/ALASKAN NATIVE': Race.AMERICAN_INDIAN_ALASKAN_NATIVE,
    'ASIAN': Race.ASIAN,
    'BLACK': Race.BLACK,
    'NATIVE HAWAIIAN/PACIFIC ISLANDER': Race.NATIVE_HAWAIIAN_PACIFIC_ISLANDER,
    'OTHER': Race.OTHER,
    'WHITE': Race.WHITE,
    'UNKNOWN': Race.UNKNOWN
}
ETHNICITY_MAP = {
    'HISPANIC': Ethnicity.HISPANIC,
    'NOT HISPANIC': Ethnicity.NOT_HISPANIC,
    'UNKNOWN': Ethnicity.UNKNOWN
}
