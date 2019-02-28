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

"""Constants related to a charge entity."""

import recidiviz.common.constants.enum_canonical_strings as enum_strings
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


class ChargeDegree(EntityEnum, metaclass=EntityEnumMeta):
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    FIRST = enum_strings.degree_first
    SECOND = enum_strings.degree_second
    THIRD = enum_strings.degree_third
    FOURTH = enum_strings.degree_fourth

    @staticmethod
    def _get_default_map():
        return _CHARGE_DEGREE_MAP


class ChargeClass(EntityEnum, metaclass=EntityEnumMeta):
    CIVIL = enum_strings.charge_class_civil
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    FELONY = enum_strings.charge_class_felony
    INFRACTION = enum_strings.charge_class_infraction
    MISDEMEANOR = enum_strings.charge_class_misdemeanor
    OTHER = enum_strings.charge_class_other
    PAROLE_VIOLATION = enum_strings.charge_class_parole_violation
    PROBATION_VIOLATION = enum_strings.charge_class_probation_violation

    @staticmethod
    def _get_default_map():
        return _CHARGE_CLASS_MAP


class ChargeStatus(EntityEnum, metaclass=EntityEnumMeta):
    ACQUITTED = enum_strings.charge_status_acquitted
    COMPLETED_SENTENCE = enum_strings.charge_status_completed
    CONVICTED = enum_strings.charge_status_convicted
    DROPPED = enum_strings.charge_status_dropped
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    INFERRED_DROPPED = enum_strings.charge_status_inferred_dropped
    PENDING = enum_strings.charge_status_pending
    PRETRIAL = enum_strings.charge_status_pretrial
    SENTENCED = enum_strings.charge_status_sentenced
    UNKNOWN_FOUND_IN_SOURCE = enum_strings.unknown_found_in_source
    UNKNOWN_REMOVED_FROM_SOURCE = enum_strings.unknown_removed_from_source

    @staticmethod
    def _get_default_map():
        return _CHARGE_STATUS_MAP


class CourtType(EntityEnum, metaclass=EntityEnumMeta):
    CIRCUIT = enum_strings.court_type_circuit
    CIVIL = enum_strings.court_type_civil
    DISTRICT = enum_strings.court_type_district
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    OTHER = enum_strings.court_type_other
    SUPERIOR = enum_strings.court_type_superior

    @staticmethod
    def _get_default_map():
        return _COURT_TYPE_MAP


# MappableEnum.parse will strip punctuation and separate tokens with a single
# space. Add mappings here using a single space between words and numbers.
# For example, `N/A` can be written as `N A` and `(10%)` can be written as `10`.
_CHARGE_DEGREE_MAP = {
    '1': ChargeDegree.FIRST,
    '1ST': ChargeDegree.FIRST,
    '2': ChargeDegree.SECOND,
    '2ND': ChargeDegree.SECOND,
    '3': ChargeDegree.THIRD,
    '3RD': ChargeDegree.THIRD,
    '4': ChargeDegree.FOURTH,
    '4TH': ChargeDegree.FOURTH,
    'FIRST': ChargeDegree.FIRST,
    'FOURTH': ChargeDegree.FOURTH,
    'SECOND': ChargeDegree.SECOND,
    'THIRD': ChargeDegree.THIRD,
    'UNKNOWN': ChargeDegree.EXTERNAL_UNKNOWN,
}

# MappableEnum.parse will strip punctuation and separate tokens with a single
# space. Add mappings here using a single space between words and numbers.
# For example, `N/A` can be written as `N A` and `(10%)` can be written as `10`.
_CHARGE_CLASS_MAP = {
    'CIVIL': ChargeClass.CIVIL,
    'FELONY': ChargeClass.FELONY,
    'F': ChargeClass.FELONY,
    'I': ChargeClass.INFRACTION,
    'INFRACTION': ChargeClass.INFRACTION,
    'MISDEMEANOR': ChargeClass.MISDEMEANOR,
    'M': ChargeClass.MISDEMEANOR,
    'O': ChargeClass.OTHER,
    'OTHER': ChargeClass.OTHER,
    'PAROLE VIOLATION': ChargeClass.PAROLE_VIOLATION,
    'PAROLE_VIOLATION': ChargeClass.PAROLE_VIOLATION,
    'PROBATION VIOLATION': ChargeClass.PROBATION_VIOLATION,
    'PROBATION_VIOLATION': ChargeClass.PROBATION_VIOLATION,
    'UNKNOWN': ChargeClass.EXTERNAL_UNKNOWN
}

# MappableEnum.parse will strip punctuation and separate tokens with a single
# space. Add mappings here using a single space between words and numbers.
# For example, `N/A` can be written as `N A` and `(10%)` can be written as `10`.
_CHARGE_STATUS_MAP = {
    'ACQUITTED': ChargeStatus.ACQUITTED,
    'AWAITING COURT': ChargeStatus.PRETRIAL,
    'AWAITING TRIAL': ChargeStatus.PRETRIAL,
    'CASE DISMISSED': ChargeStatus.DROPPED,
    'CASE DISPOSED': ChargeStatus.DROPPED,
    'CHARGES DROPPED': ChargeStatus.DROPPED,
    'CHARGES SATISFIED': ChargeStatus.COMPLETED_SENTENCE,
    'COMPLETED SENTENCE': ChargeStatus.COMPLETED_SENTENCE,
    'COURT RELEASED': ChargeStatus.DROPPED,
    'CONVICTED': ChargeStatus.CONVICTED,
    'COUNTY JAIL TIME': ChargeStatus.SENTENCED,
    'DECLINED TO PROSECUTE': ChargeStatus.DROPPED,
    'DISMISSAL': ChargeStatus.DROPPED,
    'DISMISSED': ChargeStatus.DROPPED,
    'DISMISSED AT COURT': ChargeStatus.DROPPED,
    'DISMISSED BY DISTRICT ATTORNEY': ChargeStatus.DROPPED,
    'DROPPED': ChargeStatus.DROPPED,
    'FILED PENDING TRIAL': ChargeStatus.PRETRIAL,
    'FTA': ChargeStatus.PRETRIAL,
    'GUILTY': ChargeStatus.SENTENCED,
    'INDICTED': ChargeStatus.PRETRIAL,
    'MUNICIPAL COURT': ChargeStatus.PRETRIAL,
    'NOLLED PROSSED': ChargeStatus.DROPPED,
    'NOLPROSSED': ChargeStatus.DROPPED,
    'NOT GUILTY': ChargeStatus.ACQUITTED,
    'OPEN': ChargeStatus.PENDING,
    'OTHER SEE NOTES': None,
    'OTHER W EXPLANATION': None,
    'PAROLE PROBATION REINSTATED': ChargeStatus.SENTENCED,
    'PAROLE PROBATION REVOKED': ChargeStatus.SENTENCED,
    'PAROLE/PROBATION REINSTATED': ChargeStatus.SENTENCED,
    'PAROLE/PROBATION REVOKED': ChargeStatus.SENTENCED,
    'PENDING': ChargeStatus.PENDING,
    'PRESENTENCED': ChargeStatus.PRETRIAL,
    'PRE TRIAL': ChargeStatus.PRETRIAL,
    'PRETRIAL': ChargeStatus.PRETRIAL,
    'PROB REVOKED': ChargeStatus.SENTENCED,
    'PROBATION': ChargeStatus.SENTENCED,
    'PROBATION REVOKED': ChargeStatus.SENTENCED,
    'RELEASE PER JUDGE': ChargeStatus.DROPPED,
    'RELEASED BY COURT': ChargeStatus.DROPPED,
    'RELEASED TIME SERVED': ChargeStatus.COMPLETED_SENTENCE,
    'REPORT IN': ChargeStatus.SENTENCED,
    'REVOKED': ChargeStatus.SENTENCED,
    'SENTENCED': ChargeStatus.SENTENCED,
    'SENTENCED TO PROBATION': ChargeStatus.SENTENCED,
    'SERVING TIME': ChargeStatus.SENTENCED,
    'SUPERVISED PROBATION': ChargeStatus.SENTENCED,
    'TEMPORARY CUSTODY ORDER': ChargeStatus.PENDING,
    'TIME SERVED': ChargeStatus.COMPLETED_SENTENCE,
    'TRUE BILL OF INDICTMENT': ChargeStatus.PRETRIAL,
    'UNDER SENTENCE': ChargeStatus.SENTENCED,
    'UNKNOWN': ChargeStatus.EXTERNAL_UNKNOWN,
    'UNSENTENCED': ChargeStatus.PRETRIAL,
    'WAITING FOR TRIAL': ChargeStatus.PRETRIAL,
    'WARRANT': ChargeStatus.PRETRIAL,
    'WARRANT SERVED': ChargeStatus.PRETRIAL,
    'WEEKENDER': ChargeStatus.SENTENCED,
    'WRIT OF HABEAS CORPUS': None,
}

# MappableEnum.parse will strip punctuation and separate tokens with a single
# space. Add mappings here using a single space between words and numbers.
# For example, `N/A` can be written as `N A` and `(10%)` can be written as `10`.
_COURT_TYPE_MAP = {
    'CIRCUIT': CourtType.CIRCUIT,
    'CIRCUIT COURT': CourtType.CIRCUIT,
    'CITY': CourtType.DISTRICT,
    'CIVIL': CourtType.CIVIL,
    'CHANCERY': CourtType.CIVIL,
    'CRIMINAL': CourtType.DISTRICT,
    'DISTRICT': CourtType.DISTRICT,
    'DISTRICT COURT': CourtType.DISTRICT,
    'OUT OF COUNTY COURTS': None,
    'MAGISTRATE': CourtType.DISTRICT,
    'MUNICIPAL': CourtType.DISTRICT,
    'OTHER': CourtType.OTHER,
    'STATE': CourtType.SUPERIOR,
    'SUPERIOR': CourtType.SUPERIOR,
    'SUPERIOR COURT': CourtType.SUPERIOR,
    'UNKNOWN': CourtType.EXTERNAL_UNKNOWN,
}
