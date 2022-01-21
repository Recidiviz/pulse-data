# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
# pylint: disable=protected-access
"""Tests for program_assignments_normalization_manager.py."""
import datetime
import unittest
from typing import List

from recidiviz.calculator.pipeline.utils.entity_normalization.program_assignment_normalization_manager import (
    ProgramAssignmentNormalizationManager,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_program_assignment_normalization_delegate import (
    UsXxProgramAssignmentNormalizationDelegate,
)
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentDischargeReason,
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.persistence.entity.state.entities import StateProgramAssignment


class TestPrepareProgramAssignmentsForCalculations(unittest.TestCase):
    """State-agnostic tests for pre-processing that happens to all program assignments
    regardless of state (dropping null dates, sorting, and merging)."""

    def setUp(self) -> None:
        self.state_code = "US_XX"
        self.delegate = UsXxProgramAssignmentNormalizationDelegate()

    def _normalized_program_assignments_for_calculations(
        self, program_assignments: List[StateProgramAssignment]
    ) -> List[StateProgramAssignment]:
        entity_normalization_manager = ProgramAssignmentNormalizationManager(
            program_assignments=program_assignments,
            normalization_delegate=self.delegate,
        )
        return (
            entity_normalization_manager.normalized_program_assignments_for_calculations()
        )

    def test_default_filtered_program_assignments_null_dates(self) -> None:
        null_dates = StateProgramAssignment.new_with_defaults(
            state_code=self.state_code,
            participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
        )
        pg_1 = StateProgramAssignment.new_with_defaults(
            state_code=self.state_code,
            participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
            referral_date=datetime.date(2000, 1, 1),
        )
        pg_2 = StateProgramAssignment.new_with_defaults(
            state_code=self.state_code,
            participation_status=StateProgramAssignmentParticipationStatus.DISCHARGED,
            discharge_reason=StateProgramAssignmentDischargeReason.COMPLETED,
            discharge_date=datetime.date(2001, 2, 3),
        )
        program_assignments = [null_dates, pg_1, pg_2]

        normalized_assignments = self._normalized_program_assignments_for_calculations(
            program_assignments=program_assignments
        )

        self.assertEqual([pg_1, pg_2], normalized_assignments)

    def test_default_sorted_program_assignments_by_date(self) -> None:
        program_assignments = [
            StateProgramAssignment.new_with_defaults(
                state_code=self.state_code,
                participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
                referral_date=datetime.date(2000, 1, 1),
            ),
            StateProgramAssignment.new_with_defaults(
                state_code=self.state_code,
                participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
                start_date=datetime.date(2000, 3, 4),
            ),
            StateProgramAssignment.new_with_defaults(
                state_code=self.state_code,
                participation_status=StateProgramAssignmentParticipationStatus.DISCHARGED,
                discharge_reason=StateProgramAssignmentDischargeReason.COMPLETED,
                discharge_date=datetime.date(2000, 2, 3),
            ),
            StateProgramAssignment.new_with_defaults(
                state_code=self.state_code,
                participation_status=StateProgramAssignmentParticipationStatus.DISCHARGED,
                discharge_reason=StateProgramAssignmentDischargeReason.COMPLETED,
                referral_date=datetime.date(2000, 1, 2),
                start_date=datetime.date(2000, 2, 1),
                discharge_date=datetime.date(2000, 5, 1),
            ),
        ]
        normalized_assignments = self._normalized_program_assignments_for_calculations(
            program_assignments=program_assignments
        )
        self.assertEqual(
            [
                program_assignments[0],
                program_assignments[3],
                program_assignments[2],
                program_assignments[1],
            ],
            normalized_assignments,
        )
