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

"""Constants related to a SupervisionPeriod."""
from typing import Dict

import recidiviz.common.constants.enum_canonical_strings as enum_strings
import recidiviz.common.constants.state.enum_canonical_strings as \
    state_enum_strings
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


class SupervisionPeriodAdmissionReason(EntityEnum, metaclass=EntityEnumMeta):
    # This reason indicates the person has been released from an incarceration
    # period into a supervision period (i.e. parole).
    CONDITIONAL_RELEASE = \
        state_enum_strings.supervision_period_admission_type_conditional_release
    COURT_SENTENCE = \
        state_enum_strings.supervision_period_admission_type_court_sentence
    RETURN_FROM_ABSCONSION = \
        state_enum_strings.\
        supervision_period_admission_type_return_from_absconsion
    RETURN_FROM_SUSPENSION = \
        state_enum_strings.\
        supervision_period_admission_type_return_from_suspension

    @staticmethod
    def _get_default_map():
        return _SUPERVISION_ADMISSION_TYPE_MAP


class SupervisionPeriodStatus(EntityEnum, metaclass=EntityEnumMeta):
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    TERMINATED = state_enum_strings.supervision_period_status_terminated
    UNDER_SUPERVISION = \
        state_enum_strings.supervision_period_status_under_supervision

    @staticmethod
    def _get_default_map():
        return _SUPERVISION_STATUS_MAP


class SupervisionLevel(EntityEnum, metaclass=EntityEnumMeta):
    # TODO(1697): Add values here

    @staticmethod
    def _get_default_map():
        return _SUPERVISION_LEVEL_MAP


class SupervisionPeriodTerminationReason(EntityEnum, metaclass=EntityEnumMeta):
    ABSCONSION = state_enum_strings.\
        supervision_period_termination_reason_absconsion
    DISCHARGE = \
        state_enum_strings.supervision_period_termination_reason_discharge
    REVOCATION = \
        state_enum_strings.supervision_period_termination_reason_revocation
    SUSPENSION = \
        state_enum_strings.supervision_period_termination_reason_suspension

    @staticmethod
    def _get_default_map():
        return _SUPERVISION_PERIOD_TERMINATION_REASON_MAP


_SUPERVISION_ADMISSION_TYPE_MAP = {
    'COURT SENTENCE': SupervisionPeriodAdmissionReason.COURT_SENTENCE,
    'SENTENCE': SupervisionPeriodAdmissionReason.COURT_SENTENCE,
    'RETURN FROM ABSCOND':
        SupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION,
    'RETURN FROM ABSCONSION':
        SupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION,
    'ABSCONDED': SupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION,
    'SUSPENDED': SupervisionPeriodAdmissionReason.RETURN_FROM_SUSPENSION,
}


_SUPERVISION_STATUS_MAP = {
    'TERMINATED': SupervisionPeriodStatus.TERMINATED,
    'RELEASED': SupervisionPeriodStatus.TERMINATED,
    'UNDER SUPERVISION': SupervisionPeriodStatus.UNDER_SUPERVISION,
    'SUPERVISED': SupervisionPeriodStatus.UNDER_SUPERVISION
}

_SUPERVISION_LEVEL_MAP: Dict[str, SupervisionLevel] = {
    # TODO(1697): Add values here
}

_SUPERVISION_PERIOD_TERMINATION_REASON_MAP = {
    'ABSCOND': SupervisionPeriodTerminationReason.ABSCONSION,
    'ABSCONDED': SupervisionPeriodTerminationReason.ABSCONSION,
    'DISCHARGE': SupervisionPeriodTerminationReason.DISCHARGE,
    'DISCHARGED': SupervisionPeriodTerminationReason.DISCHARGE,
    'REVOCATION': SupervisionPeriodTerminationReason.REVOCATION,
    'REVOKED': SupervisionPeriodTerminationReason.REVOCATION,
    'SUSPENDED': SupervisionPeriodAdmissionReason.RETURN_FROM_SUSPENSION,
}
