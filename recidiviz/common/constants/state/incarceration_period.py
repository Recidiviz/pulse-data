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

"""Constants related to a IncarcerationPeriod."""

import recidiviz.common.constants.enum_canonical_strings as enum_strings
import recidiviz.common.constants.state.enum_canonical_strings as \
    state_enum_strings
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


class IncarcerationPeriodStatus(EntityEnum, metaclass=EntityEnumMeta):
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    IN_CUSTODY = state_enum_strings.incarceration_period_status_in_custody
    NOT_IN_CUSTODY = \
        state_enum_strings.incarceration_period_status_not_in_custody

    @staticmethod
    def _get_default_map():
        return _INCARCERATION_PERIOD_STATUS_MAP


class IncarcerationFacilitySecurityLevel(EntityEnum, metaclass=EntityEnumMeta):
    MAXIMUM = state_enum_strings.incarceration_facility_security_level_maximum
    MEDIUM = state_enum_strings.incarceration_facility_security_level_medium
    MINIMUM = state_enum_strings.incarceration_facility_security_level_minimum

    @staticmethod
    def _get_default_map():
        return _INCARCERATION_FACILITY_SECURITY_LEVEL_MAP


class IncarcerationPeriodAdmissionReason(EntityEnum, metaclass=EntityEnumMeta):

    NEW_ADMISSION = \
        state_enum_strings.incarceration_period_admission_reason_new_admission
    PAROLE_REVOCATION = state_enum_strings. \
        incarceration_period_admission_reason_parole_revocation
    PROBATION_REVOCATION = state_enum_strings. \
        incarceration_period_admission_reason_probation_revocation
    RETURN_FROM_ESCAPE = state_enum_strings.\
        incarceration_period_admission_reason_return_from_escape
    TRANSFER = state_enum_strings.incarceration_period_admission_reason_transfer

    @staticmethod
    def _get_default_map():
        return _INCARCERATION_PERIOD_ADMISSION_REASON_MAP


class IncarcerationPeriodTerminationReason(EntityEnum,
                                           metaclass=EntityEnumMeta):
    DEATH = state_enum_strings.incarceration_period_termination_reason_death
    ESCAPED = state_enum_strings.incarceration_period_termination_reason_escaped
    RELEASED = \
        state_enum_strings.incarceration_period_termination_reason_released
    TRANSFER = \
        state_enum_strings.incarceration_period_termination_reason_transfer

    @staticmethod
    def _get_default_map():
        return _INCARCERATION_PERIOD_TERMINATION_REASON_MAP


class IncarcerationPeriodReleaseType(EntityEnum, metaclass=EntityEnumMeta):
    # This release type corresponds to any release into some sort of
    # supervision.
    CONDITIONAL = \
        state_enum_strings.incarceration_period_release_type_conditional
    SENTENCE_SERVED = \
        state_enum_strings.incarceration_period_release_type_sentence_served

    @staticmethod
    def _get_default_map():
        return _INCARCERATION_PERIOD_RELEASE_TYPE_MAP


_INCARCERATION_FACILITY_SECURITY_LEVEL_MAP = {
    'MAXIMUM': IncarcerationFacilitySecurityLevel.MAXIMUM,
    'MAX': IncarcerationFacilitySecurityLevel.MAXIMUM,
    'MEDIUM': IncarcerationFacilitySecurityLevel.MEDIUM,
    'MED': IncarcerationFacilitySecurityLevel.MEDIUM,
    'MINIMUM': IncarcerationFacilitySecurityLevel.MINIMUM,
    'MIN': IncarcerationFacilitySecurityLevel.MINIMUM,
}


_INCARCERATION_PERIOD_RELEASE_TYPE_MAP = {
    'CONDITIONAL': IncarcerationPeriodReleaseType.CONDITIONAL,
    'CONDITIONAL RELEASE': IncarcerationPeriodReleaseType.CONDITIONAL,
    'EARNED TIME': IncarcerationPeriodReleaseType.SENTENCE_SERVED,
    'TIME EARNED': IncarcerationPeriodReleaseType.SENTENCE_SERVED,
    'GOOD TIME': IncarcerationPeriodReleaseType.SENTENCE_SERVED,
    'PAROLE': IncarcerationPeriodReleaseType.CONDITIONAL,
    'RELEASE TO PAROLE': IncarcerationPeriodReleaseType.CONDITIONAL,

    # TODO(1697): Should this actually be SENTENCE_SERVED? The probation is now
    #  a new stacked sentence that is being served?
    'PROBATION': IncarcerationPeriodReleaseType.CONDITIONAL,
    'RELEASE TO PROBATION': IncarcerationPeriodReleaseType.CONDITIONAL,
    'SERVED': IncarcerationPeriodReleaseType.SENTENCE_SERVED,
    'TIME SERVED': IncarcerationPeriodReleaseType.SENTENCE_SERVED,
    'EXPIRED': IncarcerationPeriodReleaseType.SENTENCE_SERVED,
}


_INCARCERATION_PERIOD_STATUS_MAP = {
    'CUSTODY': IncarcerationPeriodStatus.IN_CUSTODY,
    'IN CUSTODY': IncarcerationPeriodStatus.IN_CUSTODY,

    # TODO(1697): What strings correspond to
    #  IncarcerationPeriodStatus.NOT_IN_CUSTODY?
}

_INCARCERATION_PERIOD_ADMISSION_REASON_MAP = {
    'NEW ADMISSION': IncarcerationPeriodAdmissionReason.NEW_ADMISSION,
    'PAROLE REVOCATION': IncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
    'PROBATION REVOCATION':
        IncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
    'RETURN FROM ESCAPE': IncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE,
    'TRANSFER': IncarcerationPeriodAdmissionReason.TRANSFER
}

_INCARCERATION_PERIOD_TERMINATION_REASON_MAP = {
    'DEATH': IncarcerationPeriodTerminationReason.DEATH,
    'ESCAPED': IncarcerationPeriodTerminationReason.ESCAPED,
    'ESCAPE': IncarcerationPeriodTerminationReason.ESCAPED,
    'RELEASED': IncarcerationPeriodTerminationReason.RELEASED,

    # TODO(1697): Is this actually what these mean?
    'HELD ELSEWHERE': IncarcerationPeriodTerminationReason.TRANSFER,
    'HOLD': IncarcerationPeriodTerminationReason.TRANSFER,
}
