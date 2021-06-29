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
from enum import unique
from typing import Dict, Optional

import recidiviz.common.constants.enum_canonical_strings as enum_strings
import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


@unique
class StateIncarcerationPeriodStatus(EntityEnum, metaclass=EntityEnumMeta):
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    IN_CUSTODY = state_enum_strings.state_incarceration_period_status_in_custody
    NOT_IN_CUSTODY = state_enum_strings.state_incarceration_period_status_not_in_custody
    PRESENT_WITHOUT_INFO = enum_strings.present_without_info

    @staticmethod
    def _get_default_map() -> Dict[str, "StateIncarcerationPeriodStatus"]:
        return _STATE_INCARCERATION_PERIOD_STATUS_MAP


@unique
class StateIncarcerationFacilitySecurityLevel(EntityEnum, metaclass=EntityEnumMeta):
    MAXIMUM = state_enum_strings.state_incarceration_facility_security_level_maximum
    MEDIUM = state_enum_strings.state_incarceration_facility_security_level_medium
    MINIMUM = state_enum_strings.state_incarceration_facility_security_level_minimum

    @staticmethod
    def _get_default_map() -> Dict[str, "StateIncarcerationFacilitySecurityLevel"]:
        return _STATE_INCARCERATION_FACILITY_SECURITY_LEVEL_MAP


@unique
class StateIncarcerationPeriodAdmissionReason(EntityEnum, metaclass=EntityEnumMeta):
    """Reasons for admission to a period of incarceration."""

    ADMITTED_IN_ERROR = (
        state_enum_strings.state_incarceration_period_admission_reason_admitted_in_error
    )
    # This is an ingest-only enum, and should only be used as a placeholder if we are unable
    # to determine whether this is a sanction, revocation, or temporary custody admission.
    ADMITTED_FROM_SUPERVISION = (
        state_enum_strings.state_incarceration_period_admission_reason_admitted_from_supervision
    )
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    INTERNAL_UNKNOWN = enum_strings.internal_unknown
    NEW_ADMISSION = (
        state_enum_strings.state_incarceration_period_admission_reason_new_admission
    )
    PAROLE_REVOCATION = (
        state_enum_strings.state_incarceration_period_admission_reason_parole_revocation
    )
    PROBATION_REVOCATION = (
        state_enum_strings.state_incarceration_period_admission_reason_probation_revocation
    )
    DUAL_REVOCATION = (
        state_enum_strings.state_incarceration_period_admission_reason_dual_revocation
    )
    RETURN_FROM_ERRONEOUS_RELEASE = (
        state_enum_strings.state_incarceration_period_admission_reason_return_from_erroneous_release
    )
    RETURN_FROM_ESCAPE = (
        state_enum_strings.state_incarceration_period_admission_reason_return_from_escape
    )
    SANCTION_ADMISSION = (
        state_enum_strings.state_incarceration_period_admission_reason_sanction_admission
    )
    STATUS_CHANGE = (
        state_enum_strings.state_incarceration_period_admission_reason_status_change
    )
    TEMPORARY_CUSTODY = (
        state_enum_strings.state_incarceration_period_admission_reason_temporary_custody
    )
    TRANSFER = state_enum_strings.state_incarceration_period_admission_reason_transfer
    TRANSFERRED_FROM_OUT_OF_STATE = (
        state_enum_strings.state_incarceration_period_admission_reason_transferred_from_out_of_state
    )

    @staticmethod
    def _get_default_map() -> Dict[str, "StateIncarcerationPeriodAdmissionReason"]:
        return _STATE_INCARCERATION_PERIOD_ADMISSION_REASON_MAP


@unique
class StateIncarcerationPeriodReleaseReason(EntityEnum, metaclass=EntityEnumMeta):
    """Reasons for release from a period of incarceration."""

    COMMUTED = state_enum_strings.state_incarceration_period_release_reason_commuted
    COMPASSIONATE = (
        state_enum_strings.state_incarceration_period_release_reason_compassionate
    )
    # This release type corresponds to a release through the process of being granted
    # parole by the parole board
    CONDITIONAL_RELEASE = (
        state_enum_strings.state_incarceration_period_release_reason_conditional_release
    )
    COURT_ORDER = (
        state_enum_strings.state_incarceration_period_release_reason_court_order
    )
    DEATH = state_enum_strings.state_incarceration_period_release_reason_death
    ESCAPE = state_enum_strings.state_incarceration_period_release_reason_escape
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    EXECUTION = state_enum_strings.state_incarceration_period_release_reason_execution
    INTERNAL_UNKNOWN = enum_strings.internal_unknown
    PARDONED = state_enum_strings.state_incarceration_period_release_reason_pardoned
    RELEASED_FROM_ERRONEOUS_ADMISSION = (
        state_enum_strings.state_incarceration_period_release_reason_released_from_erroneous_admission
    )
    RELEASED_FROM_TEMPORARY_CUSTODY = (
        state_enum_strings.state_incarceration_period_release_reason_released_from_temporary_custody
    )
    RELEASED_IN_ERROR = (
        state_enum_strings.state_incarceration_period_release_reason_released_in_error
    )
    # This release type corresponds to any release onto some sort of supervision that
    # doesn't qualify as a CONDITIONAL_RELEASE (also used if we cannot determine what
    # type of release to supervision a release is)
    RELEASED_TO_SUPERVISION = (
        state_enum_strings.state_incarceration_period_release_reason_released_to_supervision
    )
    SENTENCE_SERVED = (
        state_enum_strings.state_incarceration_period_release_reason_sentence_served
    )
    STATUS_CHANGE = (
        state_enum_strings.state_incarceration_period_release_reason_status_change
    )
    TRANSFER = state_enum_strings.state_incarceration_period_release_reason_transfer
    TRANSFER_OUT_OF_STATE = (
        state_enum_strings.state_incarceration_period_release_reason_transfer_out_of_state
    )
    VACATED = state_enum_strings.state_incarceration_period_release_reason_vacated

    @staticmethod
    def _get_default_map() -> Dict[str, "StateIncarcerationPeriodReleaseReason"]:
        return _STATE_INCARCERATION_PERIOD_RELEASE_REASON_MAP


# TODO(#3275): Update enum name to `StatePurposeForIncarceration` now that there is a 'GENERAL' option
@unique
class StateSpecializedPurposeForIncarceration(EntityEnum, metaclass=EntityEnumMeta):
    """Specialized purposes for a period of incarceration"""

    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    GENERAL = state_enum_strings.state_specialized_purpose_for_incarceration_general
    INTERNAL_UNKNOWN = enum_strings.internal_unknown
    PAROLE_BOARD_HOLD = (
        state_enum_strings.state_specialized_purpose_for_incarceration_parole_board_hold
    )
    SHOCK_INCARCERATION = (
        state_enum_strings.state_specialized_purpose_for_incarceration_shock_incarceration
    )
    TREATMENT_IN_PRISON = (
        state_enum_strings.state_specialized_purpose_for_incarceration_treatment_in_prison
    )
    TEMPORARY_CUSTODY = (
        state_enum_strings.state_specialized_purpose_for_incarceration_temporary_custody
    )

    @staticmethod
    def _get_default_map() -> Dict[str, "StateSpecializedPurposeForIncarceration"]:
        return _STATE_SPECIALIZED_PURPOSE_FOR_INCARCERATION_MAP


def is_commitment_from_supervision(
    admission_reason: Optional[StateIncarcerationPeriodAdmissionReason],
) -> bool:
    """Determines if the provided admission_reason represents a type of commitment from
    supervision due to a sanction or revocation."""
    if not admission_reason:
        return False
    revocation_types = [
        StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
        StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
        StateIncarcerationPeriodAdmissionReason.DUAL_REVOCATION,
        StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
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
        StateIncarcerationPeriodAdmissionReason.TRANSFERRED_FROM_OUT_OF_STATE,
        StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
    ]
    if admission_reason in revocation_types:
        return True
    if admission_reason in non_revocation_types:
        return False
    if (
        admission_reason
        == StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION
    ):
        # TODO(#7070): Once we have guaranteed ADMITTED_FROM_SUPERVISION is an
        #  ingest-only enum, and can expect to not see it after pre-processing,
        #  raise an error here.
        return False
    raise ValueError(
        f"Unexpected StateIncarcerationPeriodAdmissionReason {admission_reason}."
    )


def is_official_admission(
    admission_reason: Optional[StateIncarcerationPeriodAdmissionReason],
) -> bool:
    """Returns whether or not the |admission_reason| is considered an official start of incarceration, i.e. the root
    cause for being admitted to prison at all, not transfers or other unknown statuses resulting in facility changes."""
    if not admission_reason:
        return False

    # An incarceration period that has one of these admission reasons indicates the official start of incarceration
    official_admission_types = [
        StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
        StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
        StateIncarcerationPeriodAdmissionReason.DUAL_REVOCATION,
        StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
        StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
        StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
        StateIncarcerationPeriodAdmissionReason.TRANSFERRED_FROM_OUT_OF_STATE,
    ]

    non_official_admission_types = [
        StateIncarcerationPeriodAdmissionReason.ADMITTED_IN_ERROR,
        StateIncarcerationPeriodAdmissionReason.EXTERNAL_UNKNOWN,
        StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
        StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ERRONEOUS_RELEASE,
        StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE,
        StateIncarcerationPeriodAdmissionReason.TRANSFER,
        StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
    ]

    if admission_reason in official_admission_types:
        return True
    if admission_reason in non_official_admission_types:
        return False

    raise ValueError(
        f"Unsupported StateSupervisionPeriodAdmissionReason value: {admission_reason}"
    )


def is_official_release(
    release_reason: Optional[StateIncarcerationPeriodReleaseReason],
) -> bool:
    """Returns whether or not the |release_reason| is considered an official end of incarceration, i.e. a release that
    terminates the continuous period of time spent incarcerated for a specific reason, not transfers or other unknown
    statuses resulting in facility changes."""
    if not release_reason:
        return False

    # An incarceration period that has one of these release reasons indicates the official end of that period of
    # incarceration
    official_release_types = [
        StateIncarcerationPeriodReleaseReason.COMMUTED,
        StateIncarcerationPeriodReleaseReason.COMPASSIONATE,
        StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        StateIncarcerationPeriodReleaseReason.DEATH,
        StateIncarcerationPeriodReleaseReason.EXECUTION,
        StateIncarcerationPeriodReleaseReason.PARDONED,
        StateIncarcerationPeriodReleaseReason.RELEASED_FROM_ERRONEOUS_ADMISSION,
        StateIncarcerationPeriodReleaseReason.RELEASED_TO_SUPERVISION,
        # Someone may be released from temporary custody and immediately admitted to full custody. This is considered
        # an official release because it is an end to the period of temporary custody.
        StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        # Transfers out of state are classified as official releases because the custodial authority is changing.
        StateIncarcerationPeriodReleaseReason.TRANSFER_OUT_OF_STATE,
        StateIncarcerationPeriodReleaseReason.VACATED,
    ]

    non_official_release_types = [
        StateIncarcerationPeriodReleaseReason.COURT_ORDER,
        StateIncarcerationPeriodReleaseReason.ESCAPE,
        StateIncarcerationPeriodReleaseReason.EXTERNAL_UNKNOWN,
        StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
        StateIncarcerationPeriodReleaseReason.RELEASED_IN_ERROR,
        StateIncarcerationPeriodReleaseReason.TRANSFER,
        StateIncarcerationPeriodReleaseReason.STATUS_CHANGE,
    ]

    if release_reason in official_release_types:
        return True
    if release_reason in non_official_release_types:
        return False

    raise ValueError(
        f"Unsupported StateSupervisionPeriodReleaseReason value: {release_reason}"
    )


def release_reason_overrides_released_from_temporary_custody(
    release_reason: Optional[StateIncarcerationPeriodReleaseReason],
) -> bool:
    """RELEASED_FROM_TEMPORARY_CUSTODY is the expected release_reason for all periods of
    incarceration for which the admission_reason is TEMPORARY_CUSTODY. In certain
    cases, the release_reason contains more specific information about why the person
    was released from the period of temporary custody. This function returns whether
    we want to prioritize the existing release_reason over the standard
    RELEASED_FROM_TEMPORARY_CUSTODY.
    """
    # We want to prioritize these release reasons over RELEASED_FROM_TEMPORARY_CUSTODY
    prioritized_release_types = [
        StateIncarcerationPeriodReleaseReason.COMMUTED,
        StateIncarcerationPeriodReleaseReason.COURT_ORDER,
        StateIncarcerationPeriodReleaseReason.COMPASSIONATE,
        StateIncarcerationPeriodReleaseReason.DEATH,
        StateIncarcerationPeriodReleaseReason.ESCAPE,
        StateIncarcerationPeriodReleaseReason.EXECUTION,
        StateIncarcerationPeriodReleaseReason.PARDONED,
        StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        StateIncarcerationPeriodReleaseReason.TRANSFER_OUT_OF_STATE,
        StateIncarcerationPeriodReleaseReason.VACATED,
    ]

    # If someone is being released from a period of TEMPORARY_CUSTODY or
    # PAROLE_BOARD_HOLD, we'd rather have the RELEASED_FROM_TEMPORARY_CUSTODY
    # release_reason over the following values
    non_prioritized_release_types = [
        StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        StateIncarcerationPeriodReleaseReason.EXTERNAL_UNKNOWN,
        StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
        StateIncarcerationPeriodReleaseReason.RELEASED_IN_ERROR,
        StateIncarcerationPeriodReleaseReason.RELEASED_FROM_ERRONEOUS_ADMISSION,
        StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        StateIncarcerationPeriodReleaseReason.RELEASED_TO_SUPERVISION,
        StateIncarcerationPeriodReleaseReason.STATUS_CHANGE,
        StateIncarcerationPeriodReleaseReason.TRANSFER,
    ]

    if release_reason in prioritized_release_types:
        return True
    if release_reason in non_prioritized_release_types:
        return False

    raise ValueError(
        f"Unsupported StateSupervisionPeriodReleaseReason value: {release_reason}"
    )


# Commitment from supervision admissions for the following purposes of incarceration
# should always be classified as SANCTION_ADMISSION
SANCTION_ADMISSION_PURPOSE_FOR_INCARCERATION_VALUES = [
    StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
    StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
]


_STATE_INCARCERATION_FACILITY_SECURITY_LEVEL_MAP = {
    "MAXIMUM": StateIncarcerationFacilitySecurityLevel.MAXIMUM,
    "MAX": StateIncarcerationFacilitySecurityLevel.MAXIMUM,
    "MEDIUM": StateIncarcerationFacilitySecurityLevel.MEDIUM,
    "MED": StateIncarcerationFacilitySecurityLevel.MEDIUM,
    "MINIMUM": StateIncarcerationFacilitySecurityLevel.MINIMUM,
    "MIN": StateIncarcerationFacilitySecurityLevel.MINIMUM,
}


_STATE_INCARCERATION_PERIOD_STATUS_MAP = {
    "EXTERNAL UNKNOWN": StateIncarcerationPeriodStatus.EXTERNAL_UNKNOWN,
    "IN CUSTODY": StateIncarcerationPeriodStatus.IN_CUSTODY,
    "NOT IN CUSTODY": StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
    "PRESENT WITHOUT INFO": StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
}

_STATE_INCARCERATION_PERIOD_ADMISSION_REASON_MAP = {
    "ADMITTED IN ERROR": StateIncarcerationPeriodAdmissionReason.ADMITTED_IN_ERROR,
    "ADMITTED FROM SUPERVISION": StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
    "DUAL REVOCATION": StateIncarcerationPeriodAdmissionReason.DUAL_REVOCATION,
    "ERROR": StateIncarcerationPeriodAdmissionReason.ADMITTED_IN_ERROR,
    "EXTERNAL UNKNOWN": StateIncarcerationPeriodAdmissionReason.EXTERNAL_UNKNOWN,
    "INTERNAL UNKNOWN": StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
    "NEW ADMISSION": StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
    "PAROLE REVOCATION": StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
    "PROBATION REVOCATION": StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
    "RETURN FROM ESCAPE": StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE,
    "RETURN FROM ERRONEOUS RELEASE": StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ERRONEOUS_RELEASE,
    "COURT": StateIncarcerationPeriodAdmissionReason.TRANSFER,
    "HOSPITAL": StateIncarcerationPeriodAdmissionReason.TRANSFER,
    "MEDICAL": StateIncarcerationPeriodAdmissionReason.TRANSFER,
    "RETURN FROM MEDICAL": StateIncarcerationPeriodAdmissionReason.TRANSFER,
    "TEMPORARY CUSTODY": StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
    "TRANSFER": StateIncarcerationPeriodAdmissionReason.TRANSFER,
    "TRANSFERRED FROM OUT OF STATE": StateIncarcerationPeriodAdmissionReason.TRANSFERRED_FROM_OUT_OF_STATE,
    "SANCTION ADMISSION": StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
    "STATUS CHANGE": StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
}

_STATE_INCARCERATION_PERIOD_RELEASE_REASON_MAP = {
    "COMMUTED": StateIncarcerationPeriodReleaseReason.COMMUTED,
    "COMMUTATION": StateIncarcerationPeriodReleaseReason.COMMUTED,
    "COMPASSIONATE": StateIncarcerationPeriodReleaseReason.COMPASSIONATE,
    "CONDITIONAL": StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
    "CONDITIONAL RELEASE": StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
    "COURT ORDER": StateIncarcerationPeriodReleaseReason.COURT_ORDER,
    "DEATH": StateIncarcerationPeriodReleaseReason.DEATH,
    "DECEASED": StateIncarcerationPeriodReleaseReason.DEATH,
    "EARNED TIME": StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
    "ESCAPED": StateIncarcerationPeriodReleaseReason.ESCAPE,
    "ESCAPE": StateIncarcerationPeriodReleaseReason.ESCAPE,
    "EXECUTION": StateIncarcerationPeriodReleaseReason.EXECUTION,
    "EXPIRED": StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
    "EXTERNAL UNKNOWN": StateIncarcerationPeriodReleaseReason.EXTERNAL_UNKNOWN,
    "GOOD TIME": StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
    "INTERNAL UNKNOWN": StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
    "PARDONED": StateIncarcerationPeriodReleaseReason.PARDONED,
    "PAROLE": StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
    "PROBATION": StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
    "RELEASE TO PAROLE": StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
    "RELEASED FROM ERRONEOUS ADMISSION": StateIncarcerationPeriodReleaseReason.RELEASED_FROM_ERRONEOUS_ADMISSION,
    "RELEASED FROM TEMPORARY CUSTODY": StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
    "ERROR": StateIncarcerationPeriodReleaseReason.RELEASED_IN_ERROR,
    "RELEASED IN ERROR": StateIncarcerationPeriodReleaseReason.RELEASED_IN_ERROR,
    "RELEASED TO SUPERVISION": StateIncarcerationPeriodReleaseReason.RELEASED_TO_SUPERVISION,
    "RELEASED": StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
    "SERVED": StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
    "TIME EARNED": StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
    "TIME SERVED": StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
    "SENTENCE SERVED": StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
    "COURT": StateIncarcerationPeriodReleaseReason.TRANSFER,
    "HOSPITAL": StateIncarcerationPeriodReleaseReason.TRANSFER,
    "MEDICAL": StateIncarcerationPeriodReleaseReason.TRANSFER,
    "TRANSFER": StateIncarcerationPeriodReleaseReason.TRANSFER,
    "TRANSFER OUT OF STATE": StateIncarcerationPeriodReleaseReason.TRANSFER_OUT_OF_STATE,
    "VACATED": StateIncarcerationPeriodReleaseReason.VACATED,
    "STATUS CHANGE": StateIncarcerationPeriodReleaseReason.STATUS_CHANGE,
}

_STATE_SPECIALIZED_PURPOSE_FOR_INCARCERATION_MAP = {
    "EXTERNAL UNKNOWN": StateSpecializedPurposeForIncarceration.EXTERNAL_UNKNOWN,
    "INTERNAL UNKNOWN": StateSpecializedPurposeForIncarceration.INTERNAL_UNKNOWN,
    "GENERAL": StateSpecializedPurposeForIncarceration.GENERAL,
    "PAROLE BOARD HOLD": StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
    "SHOCK INCARCERATION": StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
    "TREATMENT IN PRISON": StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
    "TEMPORARY CUSTODY": StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
}
