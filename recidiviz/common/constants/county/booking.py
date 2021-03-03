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

"""Constants related to a booking entity."""

from recidiviz.common.constants.county import (
    enum_canonical_strings as county_enum_strings,
)
from recidiviz.common.constants import enum_canonical_strings as enum_strings
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


class AdmissionReason(EntityEnum, metaclass=EntityEnumMeta):
    ESCAPE = county_enum_strings.admission_reason_escape
    NEW_COMMITMENT = county_enum_strings.admission_reason_new_commitment
    PAROLE_VIOLATION = county_enum_strings.admission_reason_parole_violation
    PROBATION_VIOLATION = county_enum_strings.admission_reason_probation_violation
    SUPERVISION_VIOLATION_FOR_SEX_OFFENSE = (
        county_enum_strings.admission_reason_supervision_violation_for_sex_offense
    )
    TRANSFER = county_enum_strings.admission_reason_transfer

    @staticmethod
    def _get_default_map():
        return _ADMISSION_REASON_MAP


class Classification(EntityEnum, metaclass=EntityEnumMeta):
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    HIGH = county_enum_strings.classification_high
    LOW = county_enum_strings.classification_low
    MAXIMUM = county_enum_strings.classification_maximum
    MEDIUM = county_enum_strings.classification_medium
    MINIMUM = county_enum_strings.classification_minimum
    WORK_RELEASE = county_enum_strings.classification_work_release

    @staticmethod
    def _get_default_map():
        return _CLASSIFICATION_MAP


class CustodyStatus(EntityEnum, metaclass=EntityEnumMeta):
    ESCAPED = county_enum_strings.custody_status_escaped
    HELD_ELSEWHERE = county_enum_strings.custody_status_elsewhere
    IN_CUSTODY = county_enum_strings.custody_status_in_custody
    RELEASED = county_enum_strings.custody_status_released
    INFERRED_RELEASE = county_enum_strings.custody_status_inferred_release
    PRESENT_WITHOUT_INFO = enum_strings.present_without_info
    REMOVED_WITHOUT_INFO = enum_strings.removed_without_info

    @staticmethod
    def _get_default_map():
        return _CUSTODY_STATUS_MAP

    @staticmethod
    def get_released_statuses():
        return [
            CustodyStatus.RELEASED,
            CustodyStatus.INFERRED_RELEASE,
            CustodyStatus.REMOVED_WITHOUT_INFO,
        ]

    @staticmethod
    def get_raw_released_statuses():
        return [cs.value for cs in CustodyStatus.get_released_statuses()]


class ReleaseReason(EntityEnum, metaclass=EntityEnumMeta):
    ACQUITTAL = county_enum_strings.release_reason_acquittal
    BOND = county_enum_strings.release_reason_bond
    CASE_DISMISSED = county_enum_strings.release_reason_case_dismissed
    DEATH = county_enum_strings.release_reason_death
    ESCAPE = county_enum_strings.release_reason_escape
    EXPIRATION_OF_SENTENCE = county_enum_strings.release_reason_expiration
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    OWN_RECOGNIZANCE = county_enum_strings.release_reason_recognizance
    PAROLE = county_enum_strings.release_reason_parole
    PROBATION = county_enum_strings.release_reason_probation
    TRANSFER = county_enum_strings.release_reason_transfer

    @staticmethod
    def _get_default_map():
        return _RELEASE_REASON_MAP


# MappableEnum.parse will strip punctuation and separate tokens with a single
# space. Add mappings here using a single space between words and numbers.
# For example, `N/A` can be written as `N A` and `(10%)` can be written as `10`.
_ADMISSION_REASON_MAP = {
    "BAIL MITTIMUS": AdmissionReason.NEW_COMMITMENT,
    "BOND SURRENDER": AdmissionReason.NEW_COMMITMENT,
    "COURT ORDER": AdmissionReason.NEW_COMMITMENT,
    "ESCAPE": AdmissionReason.ESCAPE,
    "GOVERNORS WARRANT": AdmissionReason.NEW_COMMITMENT,
    "NEW COMMITMENT": AdmissionReason.NEW_COMMITMENT,
    "PAROLE VIOLATION": AdmissionReason.PAROLE_VIOLATION,
    "PENDING SENTENCE": AdmissionReason.NEW_COMMITMENT,
    "PROBATION VIOLATION": AdmissionReason.PROBATION_VIOLATION,
    "SENTENCE MITTIMUS": AdmissionReason.NEW_COMMITMENT,
    "TRANSFER": AdmissionReason.TRANSFER,
}

# MappableEnum.parse will strip punctuation and separate tokens with a single
# space. Add mappings here using a single space between words and numbers.
# For example, `N/A` can be written as `N A` and `(10%)` can be written as `10`.
_CLASSIFICATION_MAP = {
    "HIGH": Classification.HIGH,
    "LOW": Classification.LOW,
    "MAXIMUM": Classification.MAXIMUM,
    "MEDIUM": Classification.MEDIUM,
    "MINIMUM": Classification.MINIMUM,
    "UNKNOWN": Classification.EXTERNAL_UNKNOWN,
    "WORK RELEASE": Classification.WORK_RELEASE,
}

# MappableEnum.parse will strip punctuation and separate tokens with a single
# space. Add mappings here using a single space between words and numbers.
# For example, `N/A` can be written as `N A` and `(10%)` can be written as `10`.
_CUSTODY_STATUS_MAP = {
    "CURRENTLY BOOKED": CustodyStatus.IN_CUSTODY,
    "DISCHARGED": CustodyStatus.RELEASED,
    "ESCAPED": CustodyStatus.ESCAPED,
    "HELD ELSEWHERE": CustodyStatus.HELD_ELSEWHERE,
    "IN CUSTODY": CustodyStatus.IN_CUSTODY,
    "IN JAIL": CustodyStatus.IN_CUSTODY,
    "OUT TO COURT": CustodyStatus.HELD_ELSEWHERE,
    "RELEASED": CustodyStatus.RELEASED,
    "TEMP RELEASE": CustodyStatus.RELEASED,
}

# MappableEnum.parse will strip punctuation and separate tokens with a single
# space. Add mappings here using a single space between words and numbers.
# For example, `N/A` can be written as `N A` and `(10%)` can be written as `10`.
_RELEASE_REASON_MAP = {
    "ACQUITTAL": ReleaseReason.ACQUITTAL,
    "BALANCE OF SENTENCE SUSPENDED": ReleaseReason.EXPIRATION_OF_SENTENCE,
    "BOND": ReleaseReason.BOND,
    "CASE DISMISSED": ReleaseReason.CASE_DISMISSED,
    "DEATH": ReleaseReason.DEATH,
    "ESCAPE": ReleaseReason.ESCAPE,
    "ERROR": None,
    "EXPIRATION OF SENTENCE": ReleaseReason.EXPIRATION_OF_SENTENCE,
    "MID HUDSON PSYCH HOSPITAL": ReleaseReason.TRANSFER,
    "NEVER RETURNED FROM COURT": ReleaseReason.ESCAPE,
    "NG NOT GUILTY": ReleaseReason.ACQUITTAL,
    "OTHER": None,
    "OWN RECOGNIZANCE": ReleaseReason.OWN_RECOGNIZANCE,
    "PAROLE": ReleaseReason.PAROLE,
    "PROBATION": ReleaseReason.PROBATION,
    "SENTENCE EXPIRATION": ReleaseReason.EXPIRATION_OF_SENTENCE,
    "TRANSFER": ReleaseReason.TRANSFER,
    "UNKNOWN": ReleaseReason.EXTERNAL_UNKNOWN,
    "WARRANT": ReleaseReason.TRANSFER,
    "WRONG PERSON": ReleaseReason.CASE_DISMISSED,
}
