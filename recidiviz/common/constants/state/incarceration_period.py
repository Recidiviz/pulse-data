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


class IncarcerationPeriodReleaseReason(EntityEnum,
                                       metaclass=EntityEnumMeta):
    # This release type corresponds to any release into some sort of
    # supervision.
    CONDITIONAL_RELEASE = \
        state_enum_strings.\
        incarceration_period_release_reason_conditional_release
    DEATH = state_enum_strings.incarceration_period_release_reason_death
    ESCAPE = state_enum_strings.incarceration_period_release_reason_escape
    SENTENCE_SERVED = \
        state_enum_strings.incarceration_period_release_reason_sentence_served
    TRANSFER = \
        state_enum_strings.incarceration_period_release_reason_transfer

    @staticmethod
    def _get_default_map():
        return _INCARCERATION_PERIOD_RELEASE_REASON_MAP


_INCARCERATION_FACILITY_SECURITY_LEVEL_MAP = {
    'MAXIMUM': IncarcerationFacilitySecurityLevel.MAXIMUM,
    'MAX': IncarcerationFacilitySecurityLevel.MAXIMUM,
    'MEDIUM': IncarcerationFacilitySecurityLevel.MEDIUM,
    'MED': IncarcerationFacilitySecurityLevel.MEDIUM,
    'MINIMUM': IncarcerationFacilitySecurityLevel.MINIMUM,
    'MIN': IncarcerationFacilitySecurityLevel.MINIMUM,
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

_INCARCERATION_PERIOD_RELEASE_REASON_MAP = {
    'CONDITIONAL': IncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
    'CONDITIONAL RELEASE': IncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
    'DEATH': IncarcerationPeriodReleaseReason.DEATH,
    'EARNED TIME': IncarcerationPeriodReleaseReason.SENTENCE_SERVED,
    'ESCAPED': IncarcerationPeriodReleaseReason.ESCAPE,
    'ESCAPE': IncarcerationPeriodReleaseReason.ESCAPE,
    'EXPIRED': IncarcerationPeriodReleaseReason.SENTENCE_SERVED,
    'GOOD TIME': IncarcerationPeriodReleaseReason.SENTENCE_SERVED,

    # TODO(1697): Is this actually what these mean?
    'HELD ELSEWHERE': IncarcerationPeriodReleaseReason.TRANSFER,
    'HOLD': IncarcerationPeriodReleaseReason.TRANSFER,

    'PAROLE': IncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,

    # TODO(1697): Should this actually be SENTENCE_SERVED? The probation is now
    #  a new stacked sentence that is being served?
    'PROBATION': IncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
    'RELEASE TO PAROLE': IncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,

    # TODO(1697): Should this actually be SENTENCE_SERVED? The probation is now
    #  a new stacked sentence that is being served?
    'RELEASE TO PROBATION':
        IncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
    'RELEASED': IncarcerationPeriodReleaseReason.SENTENCE_SERVED,
    'SERVED': IncarcerationPeriodReleaseReason.SENTENCE_SERVED,
    'TIME EARNED': IncarcerationPeriodReleaseReason.SENTENCE_SERVED,
    'TIME SERVED': IncarcerationPeriodReleaseReason.SENTENCE_SERVED,
}
