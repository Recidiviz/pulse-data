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
    StateIncarcerationPeriodReleaseReason
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodAdmissionReason, \
    StateSupervisionPeriodTerminationReason


def supervision_admission_reason_mapper(label: str) -> Optional[StateSupervisionPeriodAdmissionReason]:
    if label == 'H':    # Coming from history (person had completed sentences in past)
        return StateSupervisionPeriodAdmissionReason.COURT_SENTENCE
    if label == 'P':    # Coming from probation/parole within the state
        return StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE
    if label in ('I', 'O'):  # Coming from incarceration. TODO(2999): Clarify when 'O' is used.
        return StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE
    if label == 'F':    # Coming from absconsion.
        return StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION
    return None


def supervision_termination_reason_mapper(label: str) -> Optional[StateSupervisionPeriodTerminationReason]:
    if label == 'H':    # Going to history (completed all sentences)
        return StateSupervisionPeriodTerminationReason.DISCHARGE
    if label == 'P':    # Going to probation/parole within the state
        return StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE
    # TODO(2999): Should we have a special reason for when someone goes to parole violation?
    if label in ('I', 'O'):     # Going to incarceration. TODO(2999): Clarify when 'O' is used.
        return StateSupervisionPeriodTerminationReason.REVOCATION
    if label == 'F':    # End of absconsion period
        return StateSupervisionPeriodTerminationReason.ABSCONSION
    return None


def incarceration_admission_reason_mapper(label: str) -> Optional[StateIncarcerationPeriodAdmissionReason]:
    if label == 'H':    # Coming from history (person had completed sentences in past)
        return StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION
    if label == 'P':    # Coming from probation/parole within the state
        return StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION
    if label in ('I', 'O'):     # Coming from incarceration. TODO(2999): Clarify when 'O' is used.
        return StateIncarcerationPeriodAdmissionReason.TRANSFER
    if label == 'F':    # Coming from absconsion.
        return StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE
    return None


def incarceration_release_reason_mapper(label: str) -> Optional[StateIncarcerationPeriodReleaseReason]:
    if label == 'H':    # Going to history (completed all sentences)
        return StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED
    if label == 'P':    # Going to probation/parole within the state
        return StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE
    if label in ('I', 'O'):     # Going to incarceration. TODO(2999): Clarify when 'O' is used.
        return StateIncarcerationPeriodReleaseReason.TRANSFER
    if label == 'F':    # Going to absconsion.
        return StateIncarcerationPeriodReleaseReason.ESCAPE
    return None
