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
"""US_ID implementation of the program assignment delegate"""

from typing import List

import attr

from recidiviz.calculator.pipeline.utils.program_assignment_pre_processing_manager import (
    StateSpecificProgramAssignmentPreProcessingDelegate,
)
from recidiviz.persistence.entity.state.entities import StateProgramAssignment


class UsIdProgramAssignmentPreProcessingDelegate(
    StateSpecificProgramAssignmentPreProcessingDelegate
):
    """US_ID implementation of the program assignment delegate"""

    def merge_program_assignments(
        self, program_assignments: List[StateProgramAssignment]
    ) -> List[StateProgramAssignment]:
        """In US_ID, StateProgramAssignment events are ingested with either a start +
        referral date or a discharge date. This method merges adjacent program assignment
        rows (already sorted by date fields) in which the first entity has a start date
        and the second entity has a discharge date into one program assignment."""
        updated_program_assignments: List[StateProgramAssignment] = []
        idx = 0
        while idx < len(program_assignments):
            if (
                idx < len(program_assignments) - 1
                and program_assignments[idx].start_date is not None
                and program_assignments[idx + 1].discharge_date is not None
            ):
                current_assignment = program_assignments[idx]
                next_assignment = program_assignments[idx + 1]
                merged_assignment = attr.evolve(
                    current_assignment,
                    discharge_date=next_assignment.discharge_date,
                    discharge_reason=next_assignment.discharge_reason,
                    discharge_reason_raw_text=next_assignment.discharge_reason_raw_text,
                    participation_status=next_assignment.participation_status,
                    participation_status_raw_text=next_assignment.participation_status_raw_text,
                )
                updated_program_assignments.append(merged_assignment)
                idx += 2
            else:
                updated_program_assignments.append(program_assignments[idx])
                idx += 1
        return updated_program_assignments
