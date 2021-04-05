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

"""Constants related to a Charge entity in the county schema."""
from typing import Dict, Optional

from recidiviz.common.constants import enum_canonical_strings as enum_strings
from recidiviz.common.constants.county import (
    enum_canonical_strings as county_enum_strings,
)
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


class ChargeClass(EntityEnum, metaclass=EntityEnumMeta):
    CIVIL = county_enum_strings.charge_class_civil
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    FELONY = county_enum_strings.charge_class_felony
    INFRACTION = county_enum_strings.charge_class_infraction
    MISDEMEANOR = county_enum_strings.charge_class_misdemeanor
    OTHER = county_enum_strings.charge_class_other
    PAROLE_VIOLATION = county_enum_strings.charge_class_parole_violation
    PROBATION_VIOLATION = county_enum_strings.charge_class_probation_violation
    SUPERVISION_VIOLATION_FOR_SEX_OFFENSE = (
        county_enum_strings.charge_class_supervision_violation_for_sex_offense
    )

    @staticmethod
    def _get_default_map() -> Dict[str, Optional["ChargeClass"]]:
        return _CHARGE_CLASS_MAP


class ChargeDegree(EntityEnum, metaclass=EntityEnumMeta):
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    FIRST = county_enum_strings.degree_first
    SECOND = county_enum_strings.degree_second
    THIRD = county_enum_strings.degree_third
    FOURTH = county_enum_strings.degree_fourth

    @staticmethod
    def _get_default_map() -> Dict[str, "ChargeDegree"]:
        return _CHARGE_DEGREE_MAP


# MappableEnum.parse will strip punctuation and separate tokens with a single
# space. Add mappings here using a single space between words and numbers.
# For example, `N/A` can be written as `N A` and `(10%)` can be written as `10`.
_CHARGE_CLASS_MAP = {
    "*": None,
    "-": None,
    ".": None,
    "`": None,
    "0": None,
    "ADMINISTRATIVE": ChargeClass.CIVIL,
    "C": ChargeClass.CIVIL,
    "CITED CITATION": ChargeClass.INFRACTION,
    "CIVIL": ChargeClass.CIVIL,
    "COMM": ChargeClass.CIVIL,
    "CONDIT RELEASE VIOLATION": ChargeClass.PROBATION_VIOLATION,
    "CRIMINAL SUMMONS": ChargeClass.INFRACTION,
    "CRIMINAL TRAFFIC VIOLATION": ChargeClass.INFRACTION,
    "ENHANCEMENT": None,
    "F": ChargeClass.FELONY,
    "FEL": ChargeClass.FELONY,
    "FELON": ChargeClass.FELONY,
    "FELONY": ChargeClass.FELONY,
    "FLONY": ChargeClass.FELONY,
    "I": ChargeClass.INFRACTION,
    "IN": ChargeClass.INFRACTION,
    "INFRACTION": ChargeClass.INFRACTION,
    "INFRACTIONS": ChargeClass.INFRACTION,
    "M": ChargeClass.MISDEMEANOR,
    "M F": None,
    "MIS": ChargeClass.MISDEMEANOR,
    "MISD": ChargeClass.MISDEMEANOR,
    "MISDEMEANOR": ChargeClass.MISDEMEANOR,
    "MUNICIPAL ORDINANCE VIOLATION": ChargeClass.INFRACTION,
    "N A": None,
    "NA": None,
    "NON ARREST TRAFFIC VIOLATION": ChargeClass.INFRACTION,
    "NON MOVING TRAFFIC VIOLATION": ChargeClass.INFRACTION,
    "O": ChargeClass.OTHER,
    "OTH": ChargeClass.OTHER,
    "OTHER": ChargeClass.OTHER,
    "PAROLE": ChargeClass.PAROLE_VIOLATION,
    "PAROLE VIOLATION": ChargeClass.PAROLE_VIOLATION,
    "PROB VIOLATION": ChargeClass.PROBATION_VIOLATION,
    "PROBATION VIOLATION": ChargeClass.PROBATION_VIOLATION,
    "SEX OFFENDER VIOLATION": ChargeClass.SUPERVISION_VIOLATION_FOR_SEX_OFFENSE,
    "SEX PRED VIOLATION": ChargeClass.SUPERVISION_VIOLATION_FOR_SEX_OFFENSE,
    "SUMMONS": ChargeClass.INFRACTION,
    "U": ChargeClass.EXTERNAL_UNKNOWN,
    "UNKNOWN": ChargeClass.EXTERNAL_UNKNOWN,
    "VIOLATION OF COMMUNITY CONTROL": ChargeClass.PROBATION_VIOLATION,
    "VIOLATION OF PAROLE CRC": ChargeClass.PAROLE_VIOLATION,
    "VIOLATION OF PROBATION CRC": ChargeClass.PROBATION_VIOLATION,
    "WR VIOLATION": ChargeClass.PROBATION_VIOLATION,
}

_CHARGE_DEGREE_MAP = {
    "1": ChargeDegree.FIRST,
    "1ST": ChargeDegree.FIRST,
    "2": ChargeDegree.SECOND,
    "2ND": ChargeDegree.SECOND,
    "3": ChargeDegree.THIRD,
    "3RD": ChargeDegree.THIRD,
    "4": ChargeDegree.FOURTH,
    "4TH": ChargeDegree.FOURTH,
    "F": ChargeDegree.FIRST,
    "FIRST": ChargeDegree.FIRST,
    "FOURTH": ChargeDegree.FOURTH,
    "S": ChargeDegree.SECOND,
    "SECOND": ChargeDegree.SECOND,
    "T": ChargeDegree.THIRD,
    "THIRD": ChargeDegree.THIRD,
    "UNKNOWN": ChargeDegree.EXTERNAL_UNKNOWN,
}
