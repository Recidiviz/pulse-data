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
from recidiviz.common.constants.mappable_enum import MappableEnum


class ChargeDegree(MappableEnum):
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    FIRST = enum_strings.degree_first
    SECOND = enum_strings.degree_second
    THIRD = enum_strings.degree_third

    @staticmethod
    def _get_default_map():
        return _CHARGE_DEGREE_MAP


class ChargeClass(MappableEnum):
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


class ChargeStatus(MappableEnum):
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


class CourtType(MappableEnum):
    CIRCUIT = enum_strings.court_type_circuit
    DISTRICT = enum_strings.court_type_district
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    OTHER = enum_strings.court_type_other
    SUPERIOR = enum_strings.court_type_superior

    @staticmethod
    def _get_default_map():
        return _COURT_TYPE_MAP


_CHARGE_DEGREE_MAP = {
    '1ST': ChargeDegree.FIRST,
    '2ND': ChargeDegree.SECOND,
    '3RD': ChargeDegree.THIRD,
    'FIRST': ChargeDegree.FIRST,
    'SECOND': ChargeDegree.SECOND,
    'THIRD': ChargeDegree.THIRD,
    'UNKNOWN': ChargeDegree.EXTERNAL_UNKNOWN,
}


_CHARGE_CLASS_MAP = {
    'CIVIL': ChargeClass.CIVIL,
    'FELONY': ChargeClass.FELONY,
    'F': ChargeClass.FELONY,
    'INFRACTION': ChargeClass.INFRACTION,
    'MISDEMEANOR': ChargeClass.MISDEMEANOR,
    'M': ChargeClass.MISDEMEANOR,
    'PAROLE VIOLATION': ChargeClass.PAROLE_VIOLATION,
    'PROBATION VIOLATION': ChargeClass.PROBATION_VIOLATION,
    'UNKNOWN': ChargeClass.EXTERNAL_UNKNOWN
}


_CHARGE_STATUS_MAP = {
    'ACQUITTED': ChargeStatus.ACQUITTED,
    'COMPLETED SENTENCE': ChargeStatus.COMPLETED_SENTENCE,
    'CONVICTED': ChargeStatus.CONVICTED,
    'DISMISSED BY DISTRICT ATTORNEY': ChargeStatus.DROPPED,
    'DROPPED': ChargeStatus.DROPPED,
    'NOT GUILTY': ChargeStatus.ACQUITTED,
    'PENDING': ChargeStatus.PENDING,
    'PRE-TRIAL': ChargeStatus.PRETRIAL,
    'PRETRIAL': ChargeStatus.PRETRIAL,
    'PROBATION': ChargeStatus.SENTENCED,
    'SENTENCED': ChargeStatus.SENTENCED,
    'TIME SERVED': ChargeStatus.COMPLETED_SENTENCE,
    'UNKNOWN': ChargeStatus.EXTERNAL_UNKNOWN,
}


_COURT_TYPE_MAP = {
    'CIRCUIT': CourtType.CIRCUIT,
    'DISTRICT': CourtType.DISTRICT,
    'CRIMINAL': CourtType.DISTRICT,
    'OTHER': CourtType.OTHER,
    'SUPERIOR': CourtType.SUPERIOR,
    'UNKNOWN': CourtType.EXTERNAL_UNKNOWN,
}
