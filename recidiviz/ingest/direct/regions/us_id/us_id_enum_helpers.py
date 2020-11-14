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
"""US_ID specific enum helper methods."""
from typing import Optional

from recidiviz.common.constants.state.state_incarceration_period import StateIncarcerationPeriodAdmissionReason, \
    StateIncarcerationPeriodReleaseReason, StateSpecializedPurposeForIncarceration
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodAdmissionReason, \
    StateSupervisionPeriodTerminationReason, StateSupervisionPeriodSupervisionType
from recidiviz.common.str_field_utils import sorted_list_from_str
from recidiviz.ingest.direct.regions.us_id.us_id_constants import JAIL_FACILITY_CODES, DEPORTED_LOCATION_NAME, \
    INTERSTATE_FACILITY_CODE


def supervision_admission_reason_mapper(label: str) -> Optional[StateSupervisionPeriodAdmissionReason]:
    if label == 'H':    # Coming from history (person had completed sentences in past)
        return StateSupervisionPeriodAdmissionReason.COURT_SENTENCE
    if label == 'P':    # Coming from probation/parole within the state
        return StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE
    if label in ('I', 'O'):  # Coming from incarceration. TODO(#3506): Clarify when 'O' is used.
        return StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE
    if label == 'F':    # Coming from absconsion.
        return StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION
    if label in (DEPORTED_LOCATION_NAME, INTERSTATE_FACILITY_CODE):
        return StateSupervisionPeriodAdmissionReason.TRANSFER_OUT_OF_STATE
    return None


def supervision_termination_reason_mapper(label: str) -> Optional[StateSupervisionPeriodTerminationReason]:
    if label == 'H':    # Going to history (completed all sentences)
        return StateSupervisionPeriodTerminationReason.DISCHARGE
    if label == 'P':    # Going to probation/parole within the state
        return StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE
    if label in ('I', 'O'):     # Going to incarceration. TODO(#3506): Clarify when 'O' is used.
        return StateSupervisionPeriodTerminationReason.RETURN_TO_INCARCERATION
    if label == 'F':    # End of absconsion period
        return StateSupervisionPeriodTerminationReason.ABSCONSION
    if label in (DEPORTED_LOCATION_NAME, INTERSTATE_FACILITY_CODE):
        return StateSupervisionPeriodTerminationReason.TRANSFER_OUT_OF_STATE

    return None


def incarceration_admission_reason_mapper(label: str) -> Optional[StateIncarcerationPeriodAdmissionReason]:
    if label == 'H':    # Coming from history (person had completed sentences in past)
        return StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION
    if label == 'P':    # Coming from probation/parole within the state
        return StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION
    if label in ('I', 'O'):     # Coming from incarceration. TODO(#3506): Clarify when 'O' is used.
        return StateIncarcerationPeriodAdmissionReason.TRANSFER
    if label == 'F':    # Coming from absconsion.
        return StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE
    return None


def incarceration_release_reason_mapper(label: str) -> Optional[StateIncarcerationPeriodReleaseReason]:
    if label == 'H':    # Going to history (completed all sentences)
        return StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED
    if label == 'P':    # Going to probation/parole within the state
        return StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE
    if label in ('I', 'O'):     # Going to incarceration. TODO(#3506): Clarify when 'O' is used.
        return StateIncarcerationPeriodReleaseReason.TRANSFER
    if label == 'F':    # Going to absconsion.
        return StateIncarcerationPeriodReleaseReason.ESCAPE
    return None


def purpose_for_incarceration_mapper(label: str) -> Optional[StateSpecializedPurposeForIncarceration]:
    """Parses status information from the 'offstat' table into potential purposes for incarceration. Ranking of priority
    is taken from Idaho itself (the 'statstrt' table in the us_id_raw_data dataset).
    """
    statuses = sorted_list_from_str(label, delimiter=' ')
    if 'TM' in statuses:    # Termer
        return StateSpecializedPurposeForIncarceration.GENERAL
    if 'RJ' in statuses:    # Rider
        return StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON
    if 'NO' in statuses:    # Non Idaho commitment TODO(#3518): Consider adding as specialized purpose.
        return StateSpecializedPurposeForIncarceration.INTERNAL_UNKNOWN
    if 'PV' in statuses:    # Parole board hold
        return StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD
    if 'IP' in statuses:    # Institutional Probation   TODO(#3518): Understand what this is.
        return StateSpecializedPurposeForIncarceration.INTERNAL_UNKNOWN
    if 'CV' in statuses:    # Civil commitment      TODO(#3518): Consider adding a specialized purpose for this
        return StateSpecializedPurposeForIncarceration.INTERNAL_UNKNOWN
    if 'CH' in statuses:    # Courtesy Hold         TODO(#3518): Understand what this is
        return StateSpecializedPurposeForIncarceration.INTERNAL_UNKNOWN
    if 'PB' in statuses:    # Probation -- happens VERY infrequently (occurs as a data error from ID)
        return StateSpecializedPurposeForIncarceration.INTERNAL_UNKNOWN
    return None


def supervision_period_supervision_type_mapper(label: str) -> Optional[StateSupervisionPeriodSupervisionType]:
    """Parses status information from the 'offstat' table into potential supervision types. Ranking of priority
    is taken from Idaho itself (the 'statstrt' table in the us_id_raw_data dataset).
    """
    statuses = sorted_list_from_str(label, delimiter=' ')
    if 'PR' in statuses and 'PB' in statuses:   # Parole and Probation
        return StateSupervisionPeriodSupervisionType.DUAL
    if 'PR' in statuses:    # Parole
        return StateSupervisionPeriodSupervisionType.PAROLE
    if 'PB' in statuses:    # Probation
        return StateSupervisionPeriodSupervisionType.PROBATION
    if 'PS' in statuses:    # Pre sentence investigation
        return StateSupervisionPeriodSupervisionType.INVESTIGATION
    if 'PA' in statuses:    # Pardon applicant     TODO(#3506): Get more info from ID. Filter these people out entirely?
        return StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN
    if 'PF' in statuses:    # Firearm applicant    TODO(#3506): Get more info from ID. Filter these people out entirely?
        return StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN
    if 'CR' in statuses:    # Rider (no longer used).
        return StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN
    if 'BW' in statuses:    # Bench Warrant
        return StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN
    if 'CP' in statuses:    # Court Probation
        return StateSupervisionPeriodSupervisionType.INFORMAL_PROBATION
    return None


def is_jail_facility(facility: str) -> bool:
    return facility in JAIL_FACILITY_CODES or 'SHERIFF' in facility
