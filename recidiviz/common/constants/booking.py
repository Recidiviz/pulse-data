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

"""Constants related to a booking entity."""

import recidiviz.common.constants.enum_canonical_strings as enum_strings


class Classification:
    HIGH = enum_strings.classification_high
    LOW = enum_strings.classification_low
    MAXIMUM = enum_strings.classification_maximum
    MEDIUM = enum_strings.classification_medium
    MINIMUM = enum_strings.classification_minimum
    WORK_RELEASE = enum_strings.classification_work_release

    @staticmethod
    def from_str(label):
        return _CLASSIFICATION_MAP[label.upper()]


class CustodyStatus:
    ESCAPED = enum_strings.custody_status_escaped
    HELD_ELSEWHERE = enum_strings.custody_status_elsewhere
    IN_CUSTODY = enum_strings.custody_status_in_custody
    RELEASED = enum_strings.custody_status_released

    @staticmethod
    def from_str(label):
        return _CUSTODY_STATUS_MAP[label.upper()]


class ReleaseReason:
    BOND = enum_strings.release_reason_bond
    DEATH = enum_strings.release_reason_death
    ESCAPE = enum_strings.release_reason_escape
    EXPIRATION_OF_SENTENCE = enum_strings.release_reason_expiration
    INFERRED_RELEASE = enum_strings.release_reason_inferred
    OWN_RECOGNIZANCE = enum_strings.release_reason_recognizance
    PAROLE = enum_strings.release_reason_parole
    PROBATION = enum_strings.release_reason_probation
    TRANSFER = enum_strings.release_reason_transfer

    @staticmethod
    def from_str(label):
        return _RELEASE_REASON_MAP[label.upper()]


_CLASSIFICATION_MAP = {
    'HIGH': Classification.HIGH,
    'LOW': Classification.LOW,
    'MAXIMUM': Classification.MAXIMUM,
    'MEDIUM': Classification.MEDIUM,
    'MINIMUM': Classification.MINIMUM,
    'WORK RELEASE': Classification.WORK_RELEASE
}


_CUSTODY_STATUS_MAP = {
    'ESCAPED': CustodyStatus.ESCAPED,
    'HELD ELSEWHERE': CustodyStatus.HELD_ELSEWHERE,
    'IN CUSTODY': CustodyStatus.IN_CUSTODY,
    'RELEASED': CustodyStatus.RELEASED,
    'DISCHARGED': CustodyStatus.RELEASED,
}


_RELEASE_REASON_MAP = {
    'BOND': ReleaseReason.BOND,
    'DEATH': ReleaseReason.DEATH,
    'ESCAPE': ReleaseReason.ESCAPE,
    'EXPIRATION OF SENTENCE': ReleaseReason.EXPIRATION_OF_SENTENCE,
    'OWN RECOGNIZANCE': ReleaseReason.OWN_RECOGNIZANCE,
    'PAROLE': ReleaseReason.PAROLE,
    'PROBATION': ReleaseReason.PROBATION,
    'TRANSFER': ReleaseReason.TRANSFER,
}
