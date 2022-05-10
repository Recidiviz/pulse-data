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
"""Tests the US_ID-specific aspects of when the state-specific delegate is used in
the ProgramAssignmentNormalizationManager."""
import datetime
import unittest
from typing import List

import attr

from recidiviz.calculator.pipeline.normalization.utils.normalization_managers.program_assignment_normalization_manager import (
    ProgramAssignmentNormalizationManager,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_program_assignment_normalization_delegate import (
    UsIdProgramAssignmentNormalizationDelegate,
)
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import StateProgramAssignment


class TestPrepareProgramAssignmentsForCalculations(unittest.TestCase):
    """Tests the US_ID-specific aspects of the
    normalized_program_assignments_and_additional_attributes function on the
    ProgramAssignmentNormalizationManager when a
    UsIdProgramAssignmentNormalizationDelegate is provided."""

    def setUp(self) -> None:
        self.state_code = StateCode.US_ID.value
        self.delegate = UsIdProgramAssignmentNormalizationDelegate()

    def _normalized_program_assignments_for_calculations(
        self, program_assignments: List[StateProgramAssignment]
    ) -> List[StateProgramAssignment]:
        entity_normalization_manager = ProgramAssignmentNormalizationManager(
            program_assignments=program_assignments,
            normalization_delegate=self.delegate,
        )

        (
            processed_program_assignments,
            _,
        ) = (
            entity_normalization_manager.normalized_program_assignments_and_additional_attributes()
        )

        return processed_program_assignments

    def test_prepare_program_assignments_for_calculations_us_id_merged_program_assignments(
        self,
    ) -> None:
        null_dates = StateProgramAssignment.new_with_defaults(
            state_code=self.state_code,
            participation_status=StateProgramAssignmentParticipationStatus.EXTERNAL_UNKNOWN,
        )
        pg_1 = StateProgramAssignment.new_with_defaults(
            state_code=self.state_code,
            participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
            referral_date=datetime.date(2000, 1, 1),
            start_date=datetime.date(2000, 1, 1),
        )
        pg_2 = StateProgramAssignment.new_with_defaults(
            state_code=self.state_code,
            participation_status=StateProgramAssignmentParticipationStatus.DISCHARGED,
            discharge_date=datetime.date(2000, 2, 1),
        )
        pg_3 = StateProgramAssignment.new_with_defaults(
            state_code=self.state_code,
            participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
            referral_date=datetime.date(2000, 4, 1),
            start_date=datetime.date(2000, 4, 1),
        )
        pg_4 = StateProgramAssignment.new_with_defaults(
            state_code=self.state_code,
            participation_status=StateProgramAssignmentParticipationStatus.EXTERNAL_UNKNOWN,
            discharge_date=datetime.date(2000, 3, 1),
        )
        normalized_assignments = self._normalized_program_assignments_for_calculations(
            program_assignments=[
                null_dates,
                pg_1,
                pg_2,
                pg_3,
                pg_4,
            ]
        )
        merged = attr.evolve(
            pg_1,
            participation_status=pg_2.participation_status,
            discharge_date=pg_2.discharge_date,
        )
        self.assertEqual([merged, pg_4, pg_3], normalized_assignments)
