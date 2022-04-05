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

"""Constants related to a StateProgramAssignment."""

import recidiviz.common.constants.state.enum_canonical_strings as \
    state_enum_strings
from recidiviz.common.constants import enum_canonical_strings
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


class StateProgramAssignmentParticipationStatus(
        EntityEnum, metaclass=EntityEnumMeta):
    DENIED = state_enum_strings.\
        state_program_assignment_participation_status_denied
    DISCHARGED = state_enum_strings.\
        state_program_assignment_participation_status_discharged
    EXTERNAL_UNKNOWN = enum_canonical_strings.external_unknown
    IN_PROGRESS = state_enum_strings.\
        state_program_assignment_participation_status_in_progress
    PENDING = state_enum_strings.\
        state_program_assignment_participation_status_pending
    PRESENT_WITHOUT_INFO = enum_canonical_strings.present_without_info

    @staticmethod
    def _get_default_map():
        return _STATE_PROGRAM_ASSIGNMENT_PARTICIPATION_STATUS_MAP


_STATE_PROGRAM_ASSIGNMENT_PARTICIPATION_STATUS_MAP = {
    'DENIED': StateProgramAssignmentParticipationStatus.DENIED,
    'DISCHARGED': StateProgramAssignmentParticipationStatus.DISCHARGED,
    'IN PROGRESS': StateProgramAssignmentParticipationStatus.IN_PROGRESS,
    'PENDING': StateProgramAssignmentParticipationStatus.PENDING,
}


class StateProgramAssignmentDischargeReason(
        EntityEnum, metaclass=EntityEnumMeta):
    ABSCONDED = state_enum_strings.\
        state_program_assignment_discharge_reason_absconded
    ADVERSE_TERMINATION = state_enum_strings.\
        state_program_assignment_discharge_reason_adverse_termination
    COMPLETED = state_enum_strings.\
        state_program_assignment_discharge_reason_completed
    EXTERNAL_UNKNOWN = enum_canonical_strings.external_unknown
    MOVED = state_enum_strings.state_program_assignment_discharge_reason_moved
    OPTED_OUT = state_enum_strings.\
        state_program_assignment_discharge_reason_opted_out
    PROGRAM_TRANSFER = state_enum_strings. \
        state_program_assignment_discharge_reason_program_transfer
    REINCARCERATED = state_enum_strings. \
        state_program_assignment_discharge_reason_reincarcerated

    @staticmethod
    def _get_default_map():
        return _STATE_PROGRAM_ASSIGNMENT_DISCHARGE_REASON_MAP


_STATE_PROGRAM_ASSIGNMENT_DISCHARGE_REASON_MAP = {
    'ABSCONDED': StateProgramAssignmentDischargeReason.ABSCONDED,
    'ADVERSE TERMINATION':
        StateProgramAssignmentDischargeReason.ADVERSE_TERMINATION,
    'COMPLETED': StateProgramAssignmentDischargeReason.COMPLETED,
    'MOVED': StateProgramAssignmentDischargeReason.MOVED,
    'OPTED OUT': StateProgramAssignmentDischargeReason.OPTED_OUT,
    'PROGRAM TRANSFER': StateProgramAssignmentDischargeReason.PROGRAM_TRANSFER,
    'REINCARCERATED': StateProgramAssignmentDischargeReason.REINCARCERATED,
}
