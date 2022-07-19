# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""US_ID specific enum helper methods.

TODO(#8900): This file should become empty and be deleted when we have fully migrated
 this state to new ingest mappings version.
"""
from typing import Optional

from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.str_field_utils import sorted_list_from_str
from recidiviz.ingest.direct.regions.us_id.us_id_constants import (
    COMMUTED_LOCATION_NAMES,
    DECEASED_LOCATION_NAMES,
    DEPORTED_LOCATION_NAME,
    DISMISSED_LOCATION_NAME,
    EARLY_DISCHARGE_LOCATION_NAME,
    FEDERAL_CUSTODY_LOCATION_CODE,
    FUGITIVE_FACILITY_TYPE,
    HISTORY_FACILITY_TYPE,
    INCARCERATION_FACILITY_TYPE,
    INTERSTATE_FACILITY_CODE,
    JAIL_FACILITY_CODES,
    OTHER_FACILITY_TYPE,
    PARDONED_LOCATION_NAMES,
    PAROLE_COMMISSION_CODE,
    SUPERVISION_FACILITY_TYPE,
)


def supervision_admission_reason_mapper(
    label: str,
) -> Optional[StateSupervisionPeriodAdmissionReason]:
    if (
        label == HISTORY_FACILITY_TYPE
    ):  # Coming from history (person had completed sentences in past)
        return StateSupervisionPeriodAdmissionReason.COURT_SENTENCE
    if (
        label == SUPERVISION_FACILITY_TYPE
    ):  # Coming from probation/parole within the state
        return StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE
    # TODO(#3506): Clarify when OTHER_FACILITY_TYPE is used.
    if label in (
        INCARCERATION_FACILITY_TYPE,
        OTHER_FACILITY_TYPE,
    ):  # Coming from incarceration.
        return StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION
    if label == FUGITIVE_FACILITY_TYPE:  # Coming from absconsion.
        return StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION
    if label in (DEPORTED_LOCATION_NAME, INTERSTATE_FACILITY_CODE):
        return StateSupervisionPeriodAdmissionReason.TRANSFER_FROM_OTHER_JURISDICTION
    return None


def supervision_termination_reason_mapper(
    label: str,
) -> Optional[StateSupervisionPeriodTerminationReason]:
    # TODO(#9383): Update to split on a dash instead of spaces when we migrate the view
    #  that uses this function to ingest mappings v2.
    if label.startswith(
        f"{HISTORY_FACILITY_TYPE} "
    ):  # Going to history (completed all sentences)
        return _supervision_history_termination_reason_mapper(label)
    if label == SUPERVISION_FACILITY_TYPE:  # Going to probation/parole within the state
        return StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE
    # TODO(#3506): Clarify when OTHER_FACILITY_TYPE is used.
    if label in (
        INCARCERATION_FACILITY_TYPE,
        OTHER_FACILITY_TYPE,
    ):  # Going to incarceration.
        return StateSupervisionPeriodTerminationReason.ADMITTED_TO_INCARCERATION
    if label == FUGITIVE_FACILITY_TYPE:  # End of absconsion period
        return StateSupervisionPeriodTerminationReason.ABSCONSION
    if label in (DEPORTED_LOCATION_NAME, INTERSTATE_FACILITY_CODE):
        return StateSupervisionPeriodTerminationReason.TRANSFER_TO_OTHER_JURISDICTION

    return None


def _supervision_history_termination_reason_mapper(
    label: str,
) -> Optional[StateSupervisionPeriodTerminationReason]:
    # TODO(#9383): Update to split on a dash instead of spaces when we migrate the view
    #  that uses this function to ingest mappings v2.
    _history_facility_typ, location_name = label.split(" ", 1)
    if location_name in COMMUTED_LOCATION_NAMES:
        return StateSupervisionPeriodTerminationReason.COMMUTED
    if location_name == EARLY_DISCHARGE_LOCATION_NAME:
        return StateSupervisionPeriodTerminationReason.DISCHARGE
    if location_name in DECEASED_LOCATION_NAMES:
        return StateSupervisionPeriodTerminationReason.DEATH
    if location_name == DISMISSED_LOCATION_NAME:
        return StateSupervisionPeriodTerminationReason.VACATED
    if location_name in PARDONED_LOCATION_NAMES:
        return StateSupervisionPeriodTerminationReason.PARDONED

    # TODO(#4587): Consider breaking this out further.
    # Default to expiration if we cannot further identify why the person was released from IDOC custody.
    return StateSupervisionPeriodTerminationReason.EXPIRATION


def incarceration_admission_reason_mapper(
    label: str,
) -> Optional[StateIncarcerationPeriodAdmissionReason]:
    if (
        label == HISTORY_FACILITY_TYPE
    ):  # Coming from history (person had completed sentences in past)
        return StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION
    if (
        label == SUPERVISION_FACILITY_TYPE
    ):  # Coming from probation/parole within the state
        return StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION
    # TODO(#3506): Clarify when OTHER_FACILITY_TYPE is used.
    if label in (
        INCARCERATION_FACILITY_TYPE,
        OTHER_FACILITY_TYPE,
    ):  # Coming from incarceration.
        return StateIncarcerationPeriodAdmissionReason.TRANSFER
    if label == FUGITIVE_FACILITY_TYPE:  # Coming from absconsion.
        return StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE
    return None


def incarceration_release_reason_mapper(
    label: str,
) -> Optional[StateIncarcerationPeriodReleaseReason]:
    # TODO(#9389): Update to split on a dash instead of spaces when we migrate the view
    #  that uses this function to ingest mappings v2.
    if label.startswith(
        f"{HISTORY_FACILITY_TYPE} "
    ):  # Going to history (completed all sentences)
        return _incarceration_history_release_reason_mapper(label)
    if label == SUPERVISION_FACILITY_TYPE:  # Going to probation/parole within the state
        return StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE
    # TODO(#3506): Clarify when OTHER_FACILITY_TYPE is used.
    if label in (
        INCARCERATION_FACILITY_TYPE,
        OTHER_FACILITY_TYPE,
    ):  # Going to incarceration.
        return StateIncarcerationPeriodReleaseReason.TRANSFER
    if label == FUGITIVE_FACILITY_TYPE:  # Going to absconsion.
        return StateIncarcerationPeriodReleaseReason.ESCAPE
    return None


def _incarceration_history_release_reason_mapper(
    label: str,
) -> Optional[StateIncarcerationPeriodReleaseReason]:
    # TODO(#9389): Update to split on a dash instead of spaces when we migrate the view
    #  that uses this function to ingest mappings v2.
    _history_facility_typ, location_name = label.split(" ", 1)
    if location_name in COMMUTED_LOCATION_NAMES:
        return StateIncarcerationPeriodReleaseReason.COMMUTED
    if location_name in DECEASED_LOCATION_NAMES:
        return StateIncarcerationPeriodReleaseReason.DEATH

    # Default to sentence served if we cannot further identify why the person was released from IDOC custody.
    return StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED


def purpose_for_incarceration_mapper(
    label: str,
) -> Optional[StateSpecializedPurposeForIncarceration]:
    """Parses status information from the 'offstat' table into potential purposes for incarceration. Ranking of priority
    is taken from Idaho itself (the 'statstrt' table in the us_id_raw_data dataset).
    """
    statuses = sorted_list_from_str(label, delimiter=" ")
    if "TM" in statuses:  # Termer
        return StateSpecializedPurposeForIncarceration.GENERAL
    if "RJ" in statuses:  # Rider
        return StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON
    if (
        "NO" in statuses
    ):  # Non Idaho commitment TODO(#3518): Consider adding as specialized purpose.
        return StateSpecializedPurposeForIncarceration.INTERNAL_UNKNOWN
    if "PV" in statuses:  # Parole board hold
        return StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD
    if (
        "IP" in statuses
    ):  # Institutional Probation   TODO(#3518): Understand what this is.
        return StateSpecializedPurposeForIncarceration.INTERNAL_UNKNOWN
    if (
        "CV" in statuses
    ):  # Civil commitment      TODO(#3518): Consider adding a specialized purpose for this
        return StateSpecializedPurposeForIncarceration.INTERNAL_UNKNOWN
    if "CH" in statuses:  # Courtesy Hold         TODO(#3518): Understand what this is
        return StateSpecializedPurposeForIncarceration.INTERNAL_UNKNOWN
    if (
        "PB" in statuses
    ):  # Probation -- happens VERY infrequently (occurs as a data error from ID)
        return StateSpecializedPurposeForIncarceration.INTERNAL_UNKNOWN
    if (
        "PR" in statuses
    ):  # Parole -- happens VERY infrequently (occurs as a data error from ID)
        return StateSpecializedPurposeForIncarceration.INTERNAL_UNKNOWN
    return None


def supervision_period_supervision_type_mapper(
    label: str,
) -> Optional[StateSupervisionPeriodSupervisionType]:
    """Parses status information from the 'offstat' table into potential supervision types. Ranking of priority
    is taken from Idaho itself (the 'statstrt' table in the us_id_raw_data dataset).
    """
    statuses = sorted_list_from_str(label, delimiter=" ")
    if "PR" in statuses and "PB" in statuses:  # Parole and Probation
        return StateSupervisionPeriodSupervisionType.DUAL
    if "PR" in statuses:  # Parole
        return StateSupervisionPeriodSupervisionType.PAROLE
    if "PB" in statuses:  # Probation
        return StateSupervisionPeriodSupervisionType.PROBATION
    if "PS" in statuses:  # Pre sentence investigation
        return StateSupervisionPeriodSupervisionType.INVESTIGATION
    if (
        "PA" in statuses
    ):  # Pardon applicant     TODO(#3506): Get more info from ID. Filter these people out entirely?
        return StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN
    if (
        "PF" in statuses
    ):  # Firearm applicant    TODO(#3506): Get more info from ID. Filter these people out entirely?
        return StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN
    if "CR" in statuses:  # Rider (no longer used).
        return StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN
    if "BW" in statuses:  # Bench Warrant
        return StateSupervisionPeriodSupervisionType.BENCH_WARRANT
    if "CP" in statuses:  # Court Probation
        return StateSupervisionPeriodSupervisionType.INFORMAL_PROBATION

    # Note: These in general shouldn't be showing up since Termer and parole violators
    # are more incarceration types and not supervision types. We have this as a
    # fallback here to handle erroneous instances that we've seen.
    if "PV" in statuses:  # Parole violator
        return StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN
    if "TM" in statuses:  # Termer
        return StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN
    if "RJ" in statuses:  # Rider
        return StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN

    return None


def is_jail_facility(facility: str) -> bool:
    return facility in JAIL_FACILITY_CODES or "SHERIFF" in facility


def custodial_authority_mapper(
    custodial_authority_code: str,
) -> StateCustodialAuthority:
    """Parses supervision facility and location information to determine the custodial authority on the supervision
    period."""
    # Interstate and parole commission facilities mean some non-ID entity is supervising the person.
    if custodial_authority_code in (INTERSTATE_FACILITY_CODE, PAROLE_COMMISSION_CODE):
        return StateCustodialAuthority.OTHER_STATE
    if custodial_authority_code == DEPORTED_LOCATION_NAME:
        return StateCustodialAuthority.OTHER_COUNTRY
    if custodial_authority_code == FEDERAL_CUSTODY_LOCATION_CODE:
        return StateCustodialAuthority.FEDERAL

    return StateCustodialAuthority.SUPERVISION_AUTHORITY
