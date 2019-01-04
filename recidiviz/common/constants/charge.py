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
    FIRST = enum_strings.degree_first
    SECOND = enum_strings.degree_second
    THIRD = enum_strings.degree_third

    @staticmethod
    def _get_default_map():
        return _CHARGE_DEGREE_MAP


class ChargeClass(MappableEnum):
    CIVIL = enum_strings.charge_class_civil
    EXTERNAL_UNKNOWN = enum_strings.charge_class_external_unknown
    FELONY = enum_strings.charge_class_felony
    MISDEMEANOR = enum_strings.charge_class_misdemeanor
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
    PENDING = enum_strings.charge_status_pending
    PRETRIAL = enum_strings.charge_status_pretrial
    SENTENCED = enum_strings.charge_status_sentenced

    @staticmethod
    def _get_default_map():
        return _CHARGE_STATUS_MAP


class CourtType(MappableEnum):
    CIRCUIT = enum_strings.court_type_circuit
    DISTRICT = enum_strings.court_type_district
    OTHER = enum_strings.court_type_other
    SUPERIOR = enum_strings.court_type_superior

    @staticmethod
    def _get_default_map():
        return _COURT_TYPE_MAP


_CHARGE_DEGREE_MAP = {
    'FIRST': ChargeDegree.FIRST,
    '1ST': ChargeDegree.FIRST,
    'SECOND': ChargeDegree.SECOND,
    '2ND': ChargeDegree.SECOND,
    'THIRD': ChargeDegree.THIRD,
    '3RD': ChargeDegree.THIRD,
}


_CHARGE_CLASS_MAP = {
    'CIVIL': ChargeClass.CIVIL,
    'FELONY': ChargeClass.FELONY,
    'MISDEMEANOR': ChargeClass.MISDEMEANOR,
    'PAROLE VIOLATION': ChargeClass.PAROLE_VIOLATION,
    'PROBATION VIOLATION': ChargeClass.PROBATION_VIOLATION,
    'UNKNOWN': ChargeClass.EXTERNAL_UNKNOWN
}


_CHARGE_STATUS_MAP = {
    'ACQUITTED': ChargeStatus.ACQUITTED,
    'COMPLETED SENTENCE': ChargeStatus.COMPLETED_SENTENCE,
    'CONVICTED': ChargeStatus.CONVICTED,
    'DROPPED': ChargeStatus.DROPPED,
    'PENDING': ChargeStatus.PENDING,
    'PRETRIAL': ChargeStatus.PRETRIAL,
    'SENTENCED': ChargeStatus.SENTENCED,
}


_COURT_TYPE_MAP = {
    'CIRCUIT': CourtType.CIRCUIT,
    'DISTRICT': CourtType.DISTRICT,
    'OTHER': CourtType.OTHER,
    'SUPERIOR': CourtType.SUPERIOR,
}
