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

"""Constants related to a StateIncarcerationPeriod."""

import recidiviz.common.constants.enum_canonical_strings as enum_strings
import recidiviz.common.constants.state.enum_canonical_strings as \
    state_enum_strings
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


class StateIncarcerationPeriodStatus(EntityEnum, metaclass=EntityEnumMeta):
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    IN_CUSTODY = state_enum_strings.state_incarceration_period_status_in_custody
    NOT_IN_CUSTODY = \
        state_enum_strings.state_incarceration_period_status_not_in_custody
    PRESENT_WITHOUT_INFO = enum_strings.present_without_info

    @staticmethod
    def _get_default_map():
        return _STATE_INCARCERATION_PERIOD_STATUS_MAP


class StateIncarcerationFacilitySecurityLevel(EntityEnum,
                                              metaclass=EntityEnumMeta):
    MAXIMUM = \
        state_enum_strings.state_incarceration_facility_security_level_maximum
    MEDIUM = \
        state_enum_strings.state_incarceration_facility_security_level_medium
    MINIMUM = \
        state_enum_strings.state_incarceration_facility_security_level_minimum

    @staticmethod
    def _get_default_map():
        return _STATE_INCARCERATION_FACILITY_SECURITY_LEVEL_MAP


class StateIncarcerationPeriodAdmissionReason(EntityEnum,
                                              metaclass=EntityEnumMeta):

    NEW_ADMISSION = \
        state_enum_strings.\
        state_incarceration_period_admission_reason_new_admission
    PAROLE_REVOCATION = state_enum_strings. \
        state_incarceration_period_admission_reason_parole_revocation
    PROBATION_REVOCATION = state_enum_strings. \
        state_incarceration_period_admission_reason_probation_revocation
    RETURN_FROM_ESCAPE = state_enum_strings. \
        state_incarceration_period_admission_reason_return_from_escape
    TRANSFER = \
        state_enum_strings.state_incarceration_period_admission_reason_transfer

    @staticmethod
    def _get_default_map():
        return _STATE_INCARCERATION_PERIOD_ADMISSION_REASON_MAP


class StateIncarcerationPeriodReleaseReason(EntityEnum,
                                            metaclass=EntityEnumMeta):
    # This release type corresponds to any release into some sort of
    # supervision.
    CONDITIONAL_RELEASE = \
        state_enum_strings.\
        state_incarceration_period_release_reason_conditional_release
    DEATH = state_enum_strings.state_incarceration_period_release_reason_death
    ESCAPE = state_enum_strings.state_incarceration_period_release_reason_escape
    SENTENCE_SERVED = \
        state_enum_strings.\
        state_incarceration_period_release_reason_sentence_served
    TRANSFER = \
        state_enum_strings.state_incarceration_period_release_reason_transfer

    @staticmethod
    def _get_default_map():
        return _STATE_INCARCERATION_PERIOD_RELEASE_REASON_MAP


_STATE_INCARCERATION_FACILITY_SECURITY_LEVEL_MAP = {
    'MAXIMUM': StateIncarcerationFacilitySecurityLevel.MAXIMUM,
    'MAX': StateIncarcerationFacilitySecurityLevel.MAXIMUM,
    'MEDIUM': StateIncarcerationFacilitySecurityLevel.MEDIUM,
    'MED': StateIncarcerationFacilitySecurityLevel.MEDIUM,
    'MINIMUM': StateIncarcerationFacilitySecurityLevel.MINIMUM,
    'MIN': StateIncarcerationFacilitySecurityLevel.MINIMUM,
}


_STATE_INCARCERATION_PERIOD_STATUS_MAP = {
    'CUSTODY': StateIncarcerationPeriodStatus.IN_CUSTODY,
    'IN CUSTODY': StateIncarcerationPeriodStatus.IN_CUSTODY,
    'NOT IN CUSTODY': StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
}

_STATE_INCARCERATION_PERIOD_ADMISSION_REASON_MAP = {
    'NEW ADMISSION': StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
    'PAROLE REVOCATION':
        StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
    'PROBATION REVOCATION':
        StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
    'RETURN FROM ESCAPE':
        StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE,
    'TRANSFER': StateIncarcerationPeriodAdmissionReason.TRANSFER
}

_STATE_INCARCERATION_PERIOD_RELEASE_REASON_MAP = {
    'CONDITIONAL': StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
    'CONDITIONAL RELEASE':
        StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
    'DEATH': StateIncarcerationPeriodReleaseReason.DEATH,
    'EARNED TIME': StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
    'ESCAPED': StateIncarcerationPeriodReleaseReason.ESCAPE,
    'ESCAPE': StateIncarcerationPeriodReleaseReason.ESCAPE,
    'EXPIRED': StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
    'GOOD TIME': StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,

    # TODO(1697): Is this actually what these mean?
    'HELD ELSEWHERE': StateIncarcerationPeriodReleaseReason.TRANSFER,
    'HOLD': StateIncarcerationPeriodReleaseReason.TRANSFER,

    'PAROLE': StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,

    # TODO(1697): Should this actually be SENTENCE_SERVED? The probation is now
    #  a new stacked sentence that is being served?
    'PROBATION': StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
    'RELEASE TO PAROLE':
        StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,

    # TODO(1697): Should this actually be SENTENCE_SERVED? The probation is now
    #  a new stacked sentence that is being served?
    'RELEASE TO PROBATION':
        StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
    'RELEASED': StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
    'SERVED': StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
    'TIME EARNED': StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
    'TIME SERVED': StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
}
