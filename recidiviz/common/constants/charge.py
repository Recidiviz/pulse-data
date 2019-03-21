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
    PRESENT_WITHOUT_INFO = enum_strings.present_without_info
    REMOVED_WITHOUT_INFO = enum_strings.removed_without_info

    @staticmethod
    def _get_default_map():
        return _CHARGE_STATUS_MAP


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
    'F': ChargeDegree.FIRST,
    'FIRST': ChargeDegree.FIRST,
    'FOURTH': ChargeDegree.FOURTH,
    'S': ChargeDegree.SECOND,
    'SECOND': ChargeDegree.SECOND,
    'T': ChargeDegree.THIRD,
    'THIRD': ChargeDegree.THIRD,
    'UNKNOWN': ChargeDegree.EXTERNAL_UNKNOWN,
}

# MappableEnum.parse will strip punctuation and separate tokens with a single
# space. Add mappings here using a single space between words and numbers.
# For example, `N/A` can be written as `N A` and `(10%)` can be written as `10`.
_CHARGE_CLASS_MAP = {
    '*': None,
    'C': ChargeClass.CIVIL,
    'CIVIL': ChargeClass.CIVIL,
    'F': ChargeClass.FELONY,
    'FEL': ChargeClass.FELONY,
    'FELONY': ChargeClass.FELONY,
    'I': ChargeClass.INFRACTION,
    'INFRACTION': ChargeClass.INFRACTION,
    'M': ChargeClass.MISDEMEANOR,
    'MIS': ChargeClass.MISDEMEANOR,
    'MISD': ChargeClass.MISDEMEANOR,
    'MISDEMEANOR': ChargeClass.MISDEMEANOR,
    'N A': None,
    'O': ChargeClass.OTHER,
    'OTHER': ChargeClass.OTHER,
    'PAROLE VIOLATION': ChargeClass.PAROLE_VIOLATION,
    'PROB VIOLATION': ChargeClass.PROBATION_VIOLATION,
    'PROBATION VIOLATION': ChargeClass.PROBATION_VIOLATION,
    'UNKNOWN': ChargeClass.EXTERNAL_UNKNOWN
}

# MappableEnum.parse will strip punctuation and separate tokens with a single
# space. Add mappings here using a single space between words and numbers.
# For example, `N/A` can be written as `N A` and `(10%)` can be written as `10`.
_CHARGE_STATUS_MAP = {
    'ACQUITTED': ChargeStatus.ACQUITTED,
    'ADMINISTRATIVE RELEASE': ChargeStatus.PRETRIAL,
    'ALT SENT': ChargeStatus.SENTENCED,
    'AWAITING': ChargeStatus.PRETRIAL,
    'AWAITING COURT': ChargeStatus.PRETRIAL,
    'AWAITING TRIAL': ChargeStatus.PRETRIAL,
    'BAIL SET': ChargeStatus.PRETRIAL,
    'BOND OUT': ChargeStatus.PRETRIAL,
    'BOUND OVER': ChargeStatus.PRETRIAL,
    'BOUND OVER TO GRAND JURY': ChargeStatus.PRETRIAL,
    'POST CASH BAIL': ChargeStatus.PRETRIAL,
    'CASE DISMISSED': ChargeStatus.DROPPED,
    'CASE DISPOSED': ChargeStatus.DROPPED,
    'CASE RESOLVED': ChargeStatus.DROPPED,
    'CONFINED NOT CONVICTED': ChargeStatus.PRETRIAL,
    'CONFINEMENT IN RESPO VIOL': ChargeStatus.SENTENCED,
    'CHARGES DROPPED': ChargeStatus.DROPPED,
    'CHARGES SATISFIED': ChargeStatus.COMPLETED_SENTENCE,
    'CHG DISMISSED': ChargeStatus.DROPPED,
    'COMPLETED': ChargeStatus.COMPLETED_SENTENCE,
    'COMPLETED SENTENCE': ChargeStatus.COMPLETED_SENTENCE,
    'COURT RELEASE': ChargeStatus.DROPPED,
    'COURT RELEASED': ChargeStatus.DROPPED,
    'CONVICTED': ChargeStatus.CONVICTED,
    'COUNTY JAIL TIME': ChargeStatus.SENTENCED,
    'DECLINED TO PROSECUTE': ChargeStatus.DROPPED,
    'DISMISS': ChargeStatus.DROPPED,
    'DISMISSAL': ChargeStatus.DROPPED,
    'DISMISSED': ChargeStatus.DROPPED,
    'DISMISSED AT COURT': ChargeStatus.DROPPED,
    'DISMISSED BY DISTRICT ATTORNEY': ChargeStatus.DROPPED,
    'DROPPED': ChargeStatus.DROPPED,
    'ENTERED IN ERROR': None,
    'FILED PENDING TRIAL': ChargeStatus.PRETRIAL,
    'FINAL SENTENCED': ChargeStatus.SENTENCED,
    'FINES COST CLOSED': ChargeStatus.COMPLETED_SENTENCE,
    'FTA': ChargeStatus.PRETRIAL,
    'GENERAL': None,
    'GUILTY': ChargeStatus.SENTENCED,
    'HELD TO GRAND JURY': ChargeStatus.PENDING,
    'INDICTED': ChargeStatus.PRETRIAL,
    'LIFTED': ChargeStatus.DROPPED,
    'MINIMUM EXPIRATION': ChargeStatus.COMPLETED_SENTENCE,
    'MUNICIPAL COURT': ChargeStatus.PRETRIAL,
    'NOLLE PROS': ChargeStatus.DROPPED,
    'NOLLED PROSSED': ChargeStatus.DROPPED,
    'NOLPROSSED': ChargeStatus.DROPPED,
    'NG NOT GUILTY': ChargeStatus.ACQUITTED,
    'NOT GUILTY': ChargeStatus.ACQUITTED,
    'NOTICE OF APPEAL': ChargeStatus.SENTENCED,
    'NOTICE OF DISCHARGE': ChargeStatus.DROPPED,
    'OPEN': ChargeStatus.PENDING,
    'OTHER': None,
    'OTHER SEE NOTES': None,
    'OTHER W EXPLANATION': None,
    'PAROLE PROBATION REINSTATED': ChargeStatus.SENTENCED,
    'PAROLE PROBATION REVOKED': ChargeStatus.SENTENCED,
    'PENDING': ChargeStatus.PENDING,
    'PENDIGN ARRIGNMENT': ChargeStatus.PENDING,
    'PENDING CASE': ChargeStatus.PENDING,
    'PENDING SEE COMMENTS BELOW': ChargeStatus.PENDING,
    'PRESENTENCED': ChargeStatus.PRETRIAL,
    'PRE TRIAL': ChargeStatus.PRETRIAL,
    'PRETRIAL RELEASE': ChargeStatus.PRETRIAL,
    'PRETRIAL': ChargeStatus.PRETRIAL,
    'PROB REVOKED': ChargeStatus.SENTENCED,
    'PROBATION': ChargeStatus.SENTENCED,
    'PROBATION REVOCATION': ChargeStatus.SENTENCED,
    'PROBATION REVOKED': ChargeStatus.SENTENCED,
    'PLEAD': ChargeStatus.SENTENCED,
    'RELEASE': ChargeStatus.DROPPED,
    'RELEASE PER JUDGE': ChargeStatus.DROPPED,
    'RELEASED BY COURT': ChargeStatus.DROPPED,
    'RELEASED BY JUDGE': ChargeStatus.DROPPED,
    'RELEASED PER JUDGE': ChargeStatus.DROPPED,
    'RELEASED FROM CUSTODY': ChargeStatus.DROPPED,
    'RELEASED TIME SERVED': ChargeStatus.COMPLETED_SENTENCE,
    'REPORT IN': ChargeStatus.SENTENCED,
    'REVOKED': ChargeStatus.SENTENCED,
    'SAFE KEEPING': ChargeStatus.PENDING,
    'SENTENCE SERVED': ChargeStatus.COMPLETED_SENTENCE,
    'SENTENCED': ChargeStatus.SENTENCED,
    'SENTENCED STATE YEARS': ChargeStatus.SENTENCED,
    'SENTENCED TO PROBATION': ChargeStatus.SENTENCED,
    'SERVE OUT': ChargeStatus.SENTENCED,
    'SERVING TIME': ChargeStatus.SENTENCED,
    'SHOCK PROBATED': ChargeStatus.SENTENCED,
    'SUMMONS': ChargeStatus.PRETRIAL,
    'SUPERVISED PROBATION': ChargeStatus.SENTENCED,
    'TEMPORARY CUSTODY ORDER': ChargeStatus.PENDING,
    'TIME EXPIRED': ChargeStatus.COMPLETED_SENTENCE,
    'TIME SERVED': ChargeStatus.COMPLETED_SENTENCE,
    'TRUE BILL OF INDICTMENT': ChargeStatus.PRETRIAL,
    'UNDER SENTENCE': ChargeStatus.SENTENCED,
    'UNKNOWN': ChargeStatus.EXTERNAL_UNKNOWN,
    'UNSENTENCED': ChargeStatus.PRETRIAL,
    'WAITING FOR TRIAL': ChargeStatus.PRETRIAL,
    'WARRANT': ChargeStatus.PRETRIAL,
    'WARRANT SERVED': ChargeStatus.PRETRIAL,
    'WAVIER SIGNED': ChargeStatus.DROPPED,
    'WEEKENDER': ChargeStatus.SENTENCED,
    'WRIT OF HABEAS CORPUS': None,
    'WRONG PERSON BOOKED': ChargeStatus.DROPPED,
}
