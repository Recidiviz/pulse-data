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

from typing import Dict, Optional

import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum


# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
class StateGender(StateEntityEnum):
    FEMALE = state_enum_strings.state_gender_female
    MALE = state_enum_strings.state_gender_male
    OTHER = state_enum_strings.state_gender_other
    TRANS = state_enum_strings.state_gender_trans
    TRANS_FEMALE = state_enum_strings.state_gender_trans_female
    TRANS_MALE = state_enum_strings.state_gender_trans_male
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, Optional["StateGender"]]:
        return _STATE_GENDER_MAP


# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
class StateRace(StateEntityEnum):
    AMERICAN_INDIAN_ALASKAN_NATIVE = state_enum_strings.state_race_american_indian
    ASIAN = state_enum_strings.state_race_asian
    BLACK = state_enum_strings.state_race_black
    NATIVE_HAWAIIAN_PACIFIC_ISLANDER = state_enum_strings.state_race_hawaiian
    OTHER = state_enum_strings.state_race_other
    WHITE = state_enum_strings.state_race_white
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, "StateRace"]:
        return _STATE_RACE_MAP


# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
class StateEthnicity(StateEntityEnum):
    HISPANIC = state_enum_strings.state_ethnicity_hispanic
    NOT_HISPANIC = state_enum_strings.state_ethnicity_not_hispanic
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, "StateEthnicity"]:
        return STATE_ETHNICITY_MAP


# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
class StateResidencyStatus(StateEntityEnum):
    HOMELESS = state_enum_strings.state_residency_status_homeless
    PERMANENT = state_enum_strings.state_residency_status_permanent
    TRANSIENT = state_enum_strings.state_residency_status_transient
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, "StateResidencyStatus"]:
        return STATE_RESIDENCY_STATUS_SUBSTRING_MAP


_STATE_GENDER_MAP = {
    "EXTERNAL UNKNOWN": StateGender.EXTERNAL_UNKNOWN,
    "F": StateGender.FEMALE,
    "FEMALE": StateGender.FEMALE,
    "M": StateGender.MALE,
    "MALE": StateGender.MALE,
    "NOT SPECIFIED": None,
    "O": StateGender.OTHER,
    "OTHER": StateGender.OTHER,
    "TRANS": StateGender.TRANS,
    "TRANS FEMALE": StateGender.TRANS_FEMALE,
    "TRANS FEMALE TRANS WOMAN": StateGender.TRANS_FEMALE,
    "TRANS MALE": StateGender.TRANS_MALE,
    "TRANS MALE TRANS MAN": StateGender.TRANS_MALE,
    "U": StateGender.EXTERNAL_UNKNOWN,
    "UNKNOWN": StateGender.EXTERNAL_UNKNOWN,
    "INTERNAL UNKNOWN": StateGender.INTERNAL_UNKNOWN,
}


_STATE_RACE_MAP = {
    "A": StateRace.ASIAN,
    "AFRICAN AMER": StateRace.BLACK,
    "AFRICAN AMERICAN": StateRace.BLACK,
    "ALL OTHERS": StateRace.OTHER,
    "AMER IND ALASKAN NAT": StateRace.AMERICAN_INDIAN_ALASKAN_NATIVE,
    "AMERICAN INDIAN": StateRace.AMERICAN_INDIAN_ALASKAN_NATIVE,
    "AMERICAN INDIAN ALASKAN": StateRace.AMERICAN_INDIAN_ALASKAN_NATIVE,
    "AMERICAN INDIAN ALASKAN NATIVE": StateRace.AMERICAN_INDIAN_ALASKAN_NATIVE,
    "AMERICAN INDIAN OF ALASKA NATIVE": StateRace.AMERICAN_INDIAN_ALASKAN_NATIVE,
    "AMERICAN INDIAN OR ALASKA NATIVE": StateRace.AMERICAN_INDIAN_ALASKAN_NATIVE,
    "AMERICAN INDIAN OR ALASKAN NATIVE": StateRace.AMERICAN_INDIAN_ALASKAN_NATIVE,
    "AMERICAN INDIAN ALASKA NATIVE": StateRace.AMERICAN_INDIAN_ALASKAN_NATIVE,
    "ASIAN": StateRace.ASIAN,
    "ASIAN PAC IS": StateRace.ASIAN,
    "ASIAN PAC ISLANDER": StateRace.ASIAN,
    "ASIAN PACIFIC ISLAND": StateRace.ASIAN,
    "ASIAN OR PACIFIC ISLANDER": StateRace.ASIAN,
    "ASIAN OR PACIFIC ISLAND ASIAN INDIANS POLYNESIONS": StateRace.ASIAN,
    "ASIAN PACIFIC ISLANDER": StateRace.ASIAN,
    "ASIAN PACIFICISLANDER": StateRace.ASIAN,
    "BI RACIAL": StateRace.OTHER,
    "BIRACIAL": StateRace.OTHER,
    "B": StateRace.BLACK,
    "BLACK": StateRace.BLACK,
    "BLACK OR AFRICAN AMERICAN": StateRace.BLACK,
    "BLACK ORIGINS OF AFRICA": StateRace.BLACK,
    "BROWN": StateRace.OTHER,
    "CAUCASIAN": StateRace.WHITE,
    "FILIPINO": StateRace.ASIAN,
    "FILLIPINO": StateRace.ASIAN,
    "HAWAIIAN PACIFIC ISLANDER": StateRace.NATIVE_HAWAIIAN_PACIFIC_ISLANDER,
    "I": StateRace.AMERICAN_INDIAN_ALASKAN_NATIVE,
    "INDIAN": StateRace.AMERICAN_INDIAN_ALASKAN_NATIVE,
    "INDIAN ALASKAN NATIVE": StateRace.AMERICAN_INDIAN_ALASKAN_NATIVE,
    "INDIAN OR ALASKAN NATIVE": StateRace.AMERICAN_INDIAN_ALASKAN_NATIVE,
    "EXTERNAL UNKNOWN": StateRace.EXTERNAL_UNKNOWN,
    "KOREAN": StateRace.ASIAN,
    "LAOTIAN": StateRace.ASIAN,
    "MIDDLE EASTERN": StateRace.WHITE,
    "MULTIRACIAL": StateRace.OTHER,
    "N A": StateRace.EXTERNAL_UNKNOWN,
    "N ASIAN PACIFIC ISLANDER NON": StateRace.NATIVE_HAWAIIAN_PACIFIC_ISLANDER,
    "NATIVE AM": StateRace.AMERICAN_INDIAN_ALASKAN_NATIVE,
    "NATIV AMER": StateRace.AMERICAN_INDIAN_ALASKAN_NATIVE,
    "NATIVE AMERICAN": StateRace.AMERICAN_INDIAN_ALASKAN_NATIVE,
    "NATIVE HAW": StateRace.NATIVE_HAWAIIAN_PACIFIC_ISLANDER,
    "NATIVE HAWAIIAN PACIFIC ISLANDER": StateRace.NATIVE_HAWAIIAN_PACIFIC_ISLANDER,
    "NATIVE HAWAIIAN OR OTHER PACIFIC ISLANDER": StateRace.NATIVE_HAWAIIAN_PACIFIC_ISLANDER,
    "O": StateRace.OTHER,
    "ORIENTAL": StateRace.ASIAN,
    "OTHER": StateRace.OTHER,
    "OTHER ASIAN": StateRace.ASIAN,
    "OTHER PACIFIC ISLANDER": StateRace.ASIAN,
    "PACIFIC ISLANDER": StateRace.ASIAN,
    "SAMOAN": StateRace.NATIVE_HAWAIIAN_PACIFIC_ISLANDER,
    "U": StateRace.EXTERNAL_UNKNOWN,
    "UNAVAILABLE": StateRace.EXTERNAL_UNKNOWN,
    "UNKNOWN": StateRace.EXTERNAL_UNKNOWN,
    "UNKOWN": StateRace.EXTERNAL_UNKNOWN,
    "W": StateRace.WHITE,
    "WHTE": StateRace.WHITE,
    "WHITE": StateRace.WHITE,
    "WHITE EURP N AFR": StateRace.WHITE,
    "WHITE EURP N AFR MID EAS": StateRace.WHITE,
    "WHITE OR HISPANIC": StateRace.WHITE,
    "INTERNAL UNKNOWN": StateRace.INTERNAL_UNKNOWN,
}


# Public so that EnumOverrides default can map race to ethnicity.
STATE_ETHNICITY_MAP = {
    "EXTERNAL UNKNOWN": StateEthnicity.EXTERNAL_UNKNOWN,
    "H": StateEthnicity.HISPANIC,
    "H WHITE LATIN HISPANIC": StateEthnicity.HISPANIC,
    "HISP": StateEthnicity.HISPANIC,
    "HISPANIC": StateEthnicity.HISPANIC,
    "HISPANIC LATINO": StateEthnicity.HISPANIC,
    "HISPANIC OR LATINO": StateEthnicity.HISPANIC,
    "L": StateEthnicity.HISPANIC,
    "NOT HISPANIC": StateEthnicity.NOT_HISPANIC,
    "N": StateEthnicity.NOT_HISPANIC,
    "REFUSED": StateEthnicity.EXTERNAL_UNKNOWN,
    "U": StateEthnicity.EXTERNAL_UNKNOWN,
    "UNKNOWN": StateEthnicity.EXTERNAL_UNKNOWN,
    "INTERNAL UNKNOWN": StateEthnicity.INTERNAL_UNKNOWN,
}

STATE_RESIDENCY_STATUS_SUBSTRING_MAP = {
    "HOMELESS": StateResidencyStatus.HOMELESS,
    "PERMANENT": StateResidencyStatus.PERMANENT,
    "TRANSIENT": StateResidencyStatus.TRANSIENT,
    "INTERNAL UNKNOWN": StateResidencyStatus.INTERNAL_UNKNOWN,
    "EXTERNAL UNKNOWN": StateResidencyStatus.EXTERNAL_UNKNOWN,
}
