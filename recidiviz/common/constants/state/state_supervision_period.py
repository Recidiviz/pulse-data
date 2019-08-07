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

"""Constants related to a StateSupervisionPeriod."""
from typing import Dict

import recidiviz.common.constants.enum_canonical_strings as enum_strings
import recidiviz.common.constants.state.enum_canonical_strings as \
    state_enum_strings
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


class StateSupervisionPeriodAdmissionReason(EntityEnum,
                                            metaclass=EntityEnumMeta):
    # This reason indicates the person has been released from an incarceration
    # period into a supervision period (i.e. parole).
    CONDITIONAL_RELEASE = \
        state_enum_strings.\
        state_supervision_period_admission_reason_conditional_release
    COURT_SENTENCE = \
        state_enum_strings.\
        state_supervision_period_admission_reason_court_sentence
    RETURN_FROM_ABSCONSION = \
        state_enum_strings.\
        state_supervision_period_admission_reason_return_from_absconsion
    RETURN_FROM_SUSPENSION = \
        state_enum_strings.\
        state_supervision_period_admission_reason_return_from_suspension

    @staticmethod
    def _get_default_map():
        return _STATE_SUPERVISION_ADMISSION_TYPE_MAP


class StateSupervisionPeriodStatus(EntityEnum, metaclass=EntityEnumMeta):
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    PRESENT_WITHOUT_INFO = enum_strings.present_without_info
    TERMINATED = state_enum_strings.state_supervision_period_status_terminated
    UNDER_SUPERVISION = \
        state_enum_strings.state_supervision_period_status_under_supervision

    @staticmethod
    def _get_default_map():
        return _STATE_SUPERVISION_STATUS_MAP


class StateSupervisionLevel(EntityEnum, metaclass=EntityEnumMeta):
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    PRESENT_WITHOUT_INFO = enum_strings.present_without_info
    MINIMUM = \
        state_enum_strings.state_supervision_period_supervision_level_minimum
    MEDIUM = \
        state_enum_strings.state_supervision_period_supervision_level_medium
    MAXIMUM = \
        state_enum_strings.state_supervision_period_supervision_level_maximum
    DIVERSION = \
        state_enum_strings.state_supervision_period_supervision_level_diversion
    INTERSTATE_COMPACT = \
        state_enum_strings.\
        state_supervision_period_supervision_level_interstate_compact

    @staticmethod
    def _get_default_map():
        return _STATE_SUPERVISION_LEVEL_MAP


class StateSupervisionPeriodTerminationReason(EntityEnum,
                                              metaclass=EntityEnumMeta):
    ABSCONSION = state_enum_strings.\
        state_supervision_period_termination_reason_absconsion
    DISCHARGE = \
        state_enum_strings.state_supervision_period_termination_reason_discharge
    REVOCATION = \
        state_enum_strings.\
        state_supervision_period_termination_reason_revocation
    SUSPENSION = \
        state_enum_strings.\
        state_supervision_period_termination_reason_suspension

    @staticmethod
    def _get_default_map():
        return _STATE_SUPERVISION_PERIOD_TERMINATION_REASON_MAP


_STATE_SUPERVISION_ADMISSION_TYPE_MAP = {
    'CONDITIONAL RELEASE':
        StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
    'COURT SENTENCE': StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
    'SENTENCE': StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
    'RETURN FROM ABSCOND':
        StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION,
    'RETURN FROM ABSCONSION':
        StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION,
    'ABSCONDED': StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION,
    'SUSPENDED': StateSupervisionPeriodAdmissionReason.RETURN_FROM_SUSPENSION,
}


_STATE_SUPERVISION_STATUS_MAP = {
    'TERMINATED': StateSupervisionPeriodStatus.TERMINATED,
    'RELEASED': StateSupervisionPeriodStatus.TERMINATED,
    'UNDER SUPERVISION': StateSupervisionPeriodStatus.UNDER_SUPERVISION,
    'SUPERVISED': StateSupervisionPeriodStatus.UNDER_SUPERVISION
}

_STATE_SUPERVISION_LEVEL_MAP: Dict[str, StateSupervisionLevel] = {
    'MINIMUM': StateSupervisionLevel.MINIMUM,
    'MIN': StateSupervisionLevel.MINIMUM,
    'MEDIUM': StateSupervisionLevel.MEDIUM,
    'MED': StateSupervisionLevel.MEDIUM,
    'MAXIMUM': StateSupervisionLevel.MAXIMUM,
    'MAX': StateSupervisionLevel.MAXIMUM,
    'DIVERSION': StateSupervisionLevel.DIVERSION,
    'INTERSTATE COMPACT': StateSupervisionLevel.INTERSTATE_COMPACT,
    'INTERSTATE': StateSupervisionLevel.INTERSTATE_COMPACT,
}

_STATE_SUPERVISION_PERIOD_TERMINATION_REASON_MAP = {
    'ABSCOND': StateSupervisionPeriodTerminationReason.ABSCONSION,
    'ABSCONDED': StateSupervisionPeriodTerminationReason.ABSCONSION,
    'DISCHARGE': StateSupervisionPeriodTerminationReason.DISCHARGE,
    'DISCHARGED': StateSupervisionPeriodTerminationReason.DISCHARGE,
    'REVOCATION': StateSupervisionPeriodTerminationReason.REVOCATION,
    'REVOKED': StateSupervisionPeriodTerminationReason.REVOCATION,
    'SUSPENDED': StateSupervisionPeriodAdmissionReason.RETURN_FROM_SUSPENSION,
}
