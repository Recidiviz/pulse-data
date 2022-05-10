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
from enum import unique
from typing import Dict

import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum


# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
@unique
class StateProgramAssignmentParticipationStatus(StateEntityEnum):
    DENIED = state_enum_strings.state_program_assignment_participation_status_denied
    DISCHARGED = (
        state_enum_strings.state_program_assignment_participation_status_discharged
    )
    IN_PROGRESS = (
        state_enum_strings.state_program_assignment_participation_status_in_progress
    )
    PENDING = state_enum_strings.state_program_assignment_participation_status_pending
    REFUSED = state_enum_strings.state_program_assignment_participation_status_refused
    PRESENT_WITHOUT_INFO = state_enum_strings.present_without_info
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, "StateProgramAssignmentParticipationStatus"]:
        return _STATE_PROGRAM_ASSIGNMENT_PARTICIPATION_STATUS_MAP

    @classmethod
    def get_enum_description(cls) -> str:
        return "The status of a person’s participation in a program."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_PROGRAM_ASSIGNMENT_PARTICIPATION_VALUE_DESCRIPTIONS


_STATE_PROGRAM_ASSIGNMENT_PARTICIPATION_VALUE_DESCRIPTIONS: Dict[
    StateEntityEnum, str
] = {
    StateProgramAssignmentParticipationStatus.DENIED: "Used when the referral for the "
    "person to participate in the program has been denied.",
    StateProgramAssignmentParticipationStatus.DISCHARGED: "Used when a person has been "
    "discharged from the program.",
    StateProgramAssignmentParticipationStatus.IN_PROGRESS: "Used when a person is "
    "actively participating in the program.",
    StateProgramAssignmentParticipationStatus.PENDING: "Used when the referral for "
    "the person to participate in the program is pending approval.",
    StateProgramAssignmentParticipationStatus.REFUSED: "Used when the person has "
    "refused to participate in the program.",
}

_STATE_PROGRAM_ASSIGNMENT_PARTICIPATION_STATUS_MAP = {
    "DENIED": StateProgramAssignmentParticipationStatus.DENIED,
    "DISCHARGED": StateProgramAssignmentParticipationStatus.DISCHARGED,
    "EXTERNAL UNKNOWN": StateProgramAssignmentParticipationStatus.EXTERNAL_UNKNOWN,
    "IN PROGRESS": StateProgramAssignmentParticipationStatus.IN_PROGRESS,
    "PENDING": StateProgramAssignmentParticipationStatus.PENDING,
    "PRESENT WITHOUT INFO": StateProgramAssignmentParticipationStatus.PRESENT_WITHOUT_INFO,
    "REFUSED": StateProgramAssignmentParticipationStatus.REFUSED,
    "INTERNAL UNKNOWN": StateProgramAssignmentParticipationStatus.INTERNAL_UNKNOWN,
}
