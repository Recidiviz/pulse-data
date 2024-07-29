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


@unique
class StateProgramAssignmentParticipationStatus(StateEntityEnum):
    """The program assignment participation status of a person."""

    DECEASED = state_enum_strings.state_program_assignment_participation_status_deceased
    DENIED = state_enum_strings.state_program_assignment_participation_status_denied
    DISCHARGED_SUCCESSFUL = (
        state_enum_strings.state_program_assignment_participation_status_discharged_successful
    )
    DISCHARGED_SUCCESSFUL_WITH_DISCRETION = (
        state_enum_strings.state_program_assignment_participation_status_discharged_successful_with_discretion
    )
    DISCHARGED_UNSUCCESSFUL = (
        state_enum_strings.state_program_assignment_participation_status_discharged_unsuccessful
    )
    DISCHARGED_OTHER = (
        state_enum_strings.state_program_assignment_participation_status_discharged_other
    )
    DISCHARGED_UNKNOWN = (
        state_enum_strings.state_program_assignment_participation_status_discharged_unknown
    )
    IN_PROGRESS = (
        state_enum_strings.state_program_assignment_participation_status_in_progress
    )
    PENDING = state_enum_strings.state_program_assignment_participation_status_pending
    REFUSED = state_enum_strings.state_program_assignment_participation_status_refused
    PRESENT_WITHOUT_INFO = state_enum_strings.present_without_info
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return "The status of a person’s participation in a program."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_PROGRAM_ASSIGNMENT_PARTICIPATION_VALUE_DESCRIPTIONS


_STATE_PROGRAM_ASSIGNMENT_PARTICIPATION_VALUE_DESCRIPTIONS: Dict[
    StateEntityEnum, str
] = {
    StateProgramAssignmentParticipationStatus.DECEASED: "Used when the person does not "
    "complete the program due to being deceased.",
    StateProgramAssignmentParticipationStatus.DENIED: "Used when the referral for the "
    "person to participate in the program has been denied.",
    StateProgramAssignmentParticipationStatus.DISCHARGED_SUCCESSFUL: "Used when a person "
    "successfully completes the program.",
    StateProgramAssignmentParticipationStatus.DISCHARGED_SUCCESSFUL_WITH_DISCRETION: "Used when a person has most likely completed the program successfully but it will"
    "be at the discretion of a person (e.g. a parole officer) to decide. For example, 'Max Benefit' in US_OR.",
    StateProgramAssignmentParticipationStatus.DISCHARGED_UNSUCCESSFUL: "Used when a person "
    "fails the program.",
    StateProgramAssignmentParticipationStatus.DISCHARGED_OTHER: "Used when a person stops"
    "participating in a program for external circumstances that do not indicate that"
    "they have either succeeded or failed. For example, if they transfer to a new location"
    "that doesn't offer the program or the program is discontinued.",
    StateProgramAssignmentParticipationStatus.DISCHARGED_UNKNOWN: "Used when a person"
    "has been discharged from the program and we have no information about whether it"
    "was a successful discharge or not.",
    StateProgramAssignmentParticipationStatus.IN_PROGRESS: "Used when a person is "
    "actively participating in the program.",
    StateProgramAssignmentParticipationStatus.PENDING: "Used when the referral for "
    "the person to participate in the program is pending approval.",
    StateProgramAssignmentParticipationStatus.REFUSED: "Used when the person has "
    "refused to participate in the program.",
}
