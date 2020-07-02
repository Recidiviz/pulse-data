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
from typing import Optional

import recidiviz.common.constants.enum_canonical_strings as enum_strings
import recidiviz.common.constants.state.enum_canonical_strings as \
    state_enum_strings
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


class StateIncarcerationPeriodStatus(EntityEnum, metaclass=EntityEnumMeta):
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    IN_CUSTODY = state_enum_strings.state_incarceration_period_status_in_custody
    NOT_IN_CUSTODY = state_enum_strings.state_incarceration_period_status_not_in_custody
    PRESENT_WITHOUT_INFO = enum_strings.present_without_info

    @staticmethod
    def _get_default_map():
        return _STATE_INCARCERATION_PERIOD_STATUS_MAP


class StateIncarcerationFacilitySecurityLevel(EntityEnum,
                                              metaclass=EntityEnumMeta):
    MAXIMUM = state_enum_strings.state_incarceration_facility_security_level_maximum
    MEDIUM = state_enum_strings.state_incarceration_facility_security_level_medium
    MINIMUM = state_enum_strings.state_incarceration_facility_security_level_minimum

    @staticmethod
    def _get_default_map():
        return _STATE_INCARCERATION_FACILITY_SECURITY_LEVEL_MAP


class StateIncarcerationPeriodAdmissionReason(EntityEnum,
                                              metaclass=EntityEnumMeta):
    """Reasons for admission to a period of incarceration."""
    ADMITTED_IN_ERROR = state_enum_strings.state_incarceration_period_admission_reason_admitted_in_error
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    INTERNAL_UNKNOWN = enum_strings.internal_unknown
    NEW_ADMISSION = state_enum_strings.state_incarceration_period_admission_reason_new_admission
    PAROLE_REVOCATION = state_enum_strings.state_incarceration_period_admission_reason_parole_revocation
    PROBATION_REVOCATION = state_enum_strings.state_incarceration_period_admission_reason_probation_revocation
    DUAL_REVOCATION = state_enum_strings.state_incarceration_period_admission_reason_dual_revocation
    RETURN_FROM_ERRONEOUS_RELEASE = \
        state_enum_strings.state_incarceration_period_admission_reason_return_from_erroneous_release
    RETURN_FROM_ESCAPE = state_enum_strings.state_incarceration_period_admission_reason_return_from_escape
    RETURN_FROM_SUPERVISION = state_enum_strings.state_incarceration_period_admission_reason_return_from_supervision
    TEMPORARY_CUSTODY = state_enum_strings.state_incarceration_period_admission_reason_temporary_custody
    TRANSFER = state_enum_strings.state_incarceration_period_admission_reason_transfer
    TRANSFERRED_FROM_OUT_OF_STATE = \
        state_enum_strings.state_incarceration_period_admission_reason_transferred_from_out_of_state

    @staticmethod
    def _get_default_map():
        return _STATE_INCARCERATION_PERIOD_ADMISSION_REASON_MAP


class StateIncarcerationPeriodReleaseReason(EntityEnum,
                                            metaclass=EntityEnumMeta):
    """Reasons for release from a period of incarceration."""
    COMMUTED = state_enum_strings.state_incarceration_period_release_reason_commuted
    COMPASSIONATE = state_enum_strings.state_incarceration_period_release_reason_compassionate
    # This release type corresponds to any release into some sort of
    # supervision.
    CONDITIONAL_RELEASE = state_enum_strings.state_incarceration_period_release_reason_conditional_release
    COURT_ORDER = state_enum_strings.state_incarceration_period_release_reason_court_order
    DEATH = state_enum_strings.state_incarceration_period_release_reason_death
    ESCAPE = state_enum_strings.state_incarceration_period_release_reason_escape
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    EXECUTION = state_enum_strings.state_incarceration_period_release_reason_execution
    INTERNAL_UNKNOWN = enum_strings.internal_unknown
    PARDONED = state_enum_strings.state_incarceration_period_release_reason_pardoned
    RELEASED_FROM_ERRONEOUS_ADMISSION = \
        state_enum_strings.state_incarceration_period_release_reason_released_from_erroneous_admission
    RELEASED_FROM_TEMPORARY_CUSTODY = \
        state_enum_strings.state_incarceration_period_release_reason_released_from_temporary_custody
    RELEASED_IN_ERROR = state_enum_strings.state_incarceration_period_release_reason_released_in_error
    SENTENCE_SERVED = state_enum_strings.state_incarceration_period_release_reason_sentence_served
    TRANSFER = state_enum_strings.state_incarceration_period_release_reason_transfer
    TRANSFERRED_OUT_OF_STATE = state_enum_strings.state_incarceration_period_release_reason_transferred_out_of_state
    VACATED = state_enum_strings.state_incarceration_period_release_reason_vacated

    @staticmethod
    def _get_default_map():
        return _STATE_INCARCERATION_PERIOD_RELEASE_REASON_MAP


# TODO(3275): Update enum name to `StatePurposeForIncarceration` now that there is a 'GENERAL' option
class StateSpecializedPurposeForIncarceration(EntityEnum,
                                              metaclass=EntityEnumMeta):
    """Specialized purposes for a period of incarceration"""
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    GENERAL = state_enum_strings.state_specialized_purpose_for_incarceration_general
    INTERNAL_UNKNOWN = enum_strings.internal_unknown
    PAROLE_BOARD_HOLD = state_enum_strings.state_specialized_purpose_for_incarceration_parole_board_hold
    SHOCK_INCARCERATION = state_enum_strings.state_specialized_purpose_for_incarceration_shock_incarceration
    TREATMENT_IN_PRISON = state_enum_strings.state_specialized_purpose_for_incarceration_treatment_in_prison

    @staticmethod
    def _get_default_map():
        return _STATE_SPECIALIZED_PURPOSE_FOR_INCARCERATION_MAP


def is_revocation_admission(admission_reason: Optional[StateIncarcerationPeriodAdmissionReason]) -> bool:
    """Determines if the provided admission_reason is a type of revocation admission."""
    if not admission_reason:
        return False
    revocation_types = [
        StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
        StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
        StateIncarcerationPeriodAdmissionReason.DUAL_REVOCATION,
        # This is sometimes, but not always, a revocation admission. Handled by state-specific revocation logic.
        StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION
    ]
    non_revocation_types = [
        StateIncarcerationPeriodAdmissionReason.ADMITTED_IN_ERROR,
        StateIncarcerationPeriodAdmissionReason.EXTERNAL_UNKNOWN,
        StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
        StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ERRONEOUS_RELEASE,
        StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE,
        StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
        StateIncarcerationPeriodAdmissionReason.TRANSFER,
        StateIncarcerationPeriodAdmissionReason.TRANSFERRED_FROM_OUT_OF_STATE]
    if admission_reason in revocation_types:
        return True
    if admission_reason in non_revocation_types:
        return False
    raise ValueError(f"Unexpected StateIncarcerationPeriodAdmissionReason {admission_reason}.")


_STATE_INCARCERATION_FACILITY_SECURITY_LEVEL_MAP = {
    'MAXIMUM': StateIncarcerationFacilitySecurityLevel.MAXIMUM,
    'MAX': StateIncarcerationFacilitySecurityLevel.MAXIMUM,
    'MEDIUM': StateIncarcerationFacilitySecurityLevel.MEDIUM,
    'MED': StateIncarcerationFacilitySecurityLevel.MEDIUM,
    'MINIMUM': StateIncarcerationFacilitySecurityLevel.MINIMUM,
    'MIN': StateIncarcerationFacilitySecurityLevel.MINIMUM,
}


_STATE_INCARCERATION_PERIOD_STATUS_MAP = {
    'EXTERNAL UNKNOWN': StateIncarcerationPeriodStatus.EXTERNAL_UNKNOWN,
    'IN CUSTODY': StateIncarcerationPeriodStatus.IN_CUSTODY,
    'NOT IN CUSTODY': StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
    'PRESENT WITHOUT INFO': StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
}

_STATE_INCARCERATION_PERIOD_ADMISSION_REASON_MAP = {
    'ADM ERROR': StateIncarcerationPeriodAdmissionReason.ADMITTED_IN_ERROR,
    'ADMITTED IN ERROR': StateIncarcerationPeriodAdmissionReason.ADMITTED_IN_ERROR,
    'DUAL REVOCATION': StateIncarcerationPeriodAdmissionReason.DUAL_REVOCATION,
    'ERROR': StateIncarcerationPeriodAdmissionReason.ADMITTED_IN_ERROR,
    'EXTERNAL UNKNOWN': StateIncarcerationPeriodAdmissionReason.EXTERNAL_UNKNOWN,
    'INTERNAL UNKNOWN': StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
    'ADMN': StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
    'NEW ADMISSION': StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
    'PAROLE REVOCATION': StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
    'PV': StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
    'PRB': StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
    'PROBATION REVOCATION': StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
    'REC': StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE,
    'RECA': StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE,
    'RETURN FROM ESCAPE': StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE,
    'RETURN FROM ERRONEOUS RELEASE': StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ERRONEOUS_RELEASE,
    'RETURN FROM SUPERVISION': StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION,
    'COURT': StateIncarcerationPeriodAdmissionReason.TRANSFER,
    'CRT': StateIncarcerationPeriodAdmissionReason.TRANSFER,
    'DETOX': StateIncarcerationPeriodAdmissionReason.TRANSFER,
    'HOSPITAL': StateIncarcerationPeriodAdmissionReason.TRANSFER,
    'MED': StateIncarcerationPeriodAdmissionReason.TRANSFER,
    'MEDICAL': StateIncarcerationPeriodAdmissionReason.TRANSFER,
    'RETURN FROM MEDICAL': StateIncarcerationPeriodAdmissionReason.TRANSFER,
    'TEMPORARY CUSTODY': StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
    'TRANSFER': StateIncarcerationPeriodAdmissionReason.TRANSFER,
    'TRANSFERRED FROM OUT OF STATE': StateIncarcerationPeriodAdmissionReason.TRANSFERRED_FROM_OUT_OF_STATE,
}

_STATE_INCARCERATION_PERIOD_RELEASE_REASON_MAP = {
    'CMM': StateIncarcerationPeriodReleaseReason.COMMUTED,
    'COMMUTED': StateIncarcerationPeriodReleaseReason.COMMUTED,
    'COMMUTATION': StateIncarcerationPeriodReleaseReason.COMMUTED,
    'COMPASSIONATE': StateIncarcerationPeriodReleaseReason.COMPASSIONATE,
    'CONDITIONAL': StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
    'CONDITIONAL RELEASE': StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
    'COURT ORDER': StateIncarcerationPeriodReleaseReason.COURT_ORDER,
    'DEATH': StateIncarcerationPeriodReleaseReason.DEATH,
    'DECE': StateIncarcerationPeriodReleaseReason.DEATH,
    'DECEASED': StateIncarcerationPeriodReleaseReason.DEATH,
    'EARNED TIME': StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
    'ESC': StateIncarcerationPeriodReleaseReason.ESCAPE,
    'ESCP': StateIncarcerationPeriodReleaseReason.ESCAPE,
    'ESCAPED': StateIncarcerationPeriodReleaseReason.ESCAPE,
    'ESCAPE': StateIncarcerationPeriodReleaseReason.ESCAPE,
    'EXECUTION': StateIncarcerationPeriodReleaseReason.EXECUTION,
    'EXPIRED': StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
    'EXTERNAL UNKNOWN': StateIncarcerationPeriodReleaseReason.EXTERNAL_UNKNOWN,
    'GOOD TIME': StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
    'INTERNAL UNKNOWN': StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
    'PARDONED': StateIncarcerationPeriodReleaseReason.PARDONED,
    'PAROLE': StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
    'PARL': StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
    'RPAR': StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
    'RPRB': StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
    'SUPL': StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
    'PROBATION': StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
    'RELEASE TO PAROLE': StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
    'RELEASED FROM ERRONEOUS ADMISSION': StateIncarcerationPeriodReleaseReason.RELEASED_FROM_ERRONEOUS_ADMISSION,
    'RELEASED FROM TEMPORARY CUSTODY': StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
    'RELEASE TO PROBATION': StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
    'ERR': StateIncarcerationPeriodReleaseReason.RELEASED_IN_ERROR,
    'ERROR': StateIncarcerationPeriodReleaseReason.RELEASED_IN_ERROR,
    'RELEASED IN ERROR': StateIncarcerationPeriodReleaseReason.RELEASED_IN_ERROR,
    'RELEASED': StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
    'SERVED': StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
    'TIME EARNED': StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
    'TIME SERVED': StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
    'SENTENCE SERVED': StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
    'COURT': StateIncarcerationPeriodReleaseReason.TRANSFER,
    'CRT': StateIncarcerationPeriodReleaseReason.TRANSFER,
    'DETOX': StateIncarcerationPeriodReleaseReason.TRANSFER,
    'HOSPITAL': StateIncarcerationPeriodReleaseReason.TRANSFER,
    'MED': StateIncarcerationPeriodReleaseReason.TRANSFER,
    'MEDICAL': StateIncarcerationPeriodReleaseReason.TRANSFER,
    'TRANSFER': StateIncarcerationPeriodReleaseReason.TRANSFER,
    'TRN': StateIncarcerationPeriodReleaseReason.TRANSFER,
    'TRANSFERRED OUT OF STATE': StateIncarcerationPeriodReleaseReason.TRANSFERRED_OUT_OF_STATE,
    'VACATED': StateIncarcerationPeriodReleaseReason.VACATED,
}

_STATE_SPECIALIZED_PURPOSE_FOR_INCARCERATION_MAP = {
    'EXTERNAL UNKNOWN': StateSpecializedPurposeForIncarceration.EXTERNAL_UNKNOWN,
    'INTERNAL UNKNOWN': StateSpecializedPurposeForIncarceration.INTERNAL_UNKNOWN,
    'GENERAL': StateSpecializedPurposeForIncarceration.GENERAL,
    'PAROLE BOARD HOLD': StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
    'SHOCK INCARCERATION': StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
    'TREATMENT IN PRISON': StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
}
