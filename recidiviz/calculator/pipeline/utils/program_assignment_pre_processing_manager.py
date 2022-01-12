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
"""Contains the logic for a ProgramAssignmentPreProcessingManager that manages the
pre-processing of StateProgramAssignment entities in the calculation
pipelines."""
import datetime
from copy import deepcopy
from typing import List

from recidiviz.persistence.entity.state.entities import StateProgramAssignment


class StateSpecificProgramAssignmentPreProcessingDelegate:
    """Interface for state-specific decisions involved in pre-processing program assignments
    for calculations."""

    def merge_program_assignments(
        self, program_assignments: List[StateProgramAssignment]
    ) -> List[StateProgramAssignment]:
        """Contains state-specific logic for merging program assignments together.

        Default behavior is to return the |program_assignments| unchanged."""
        return program_assignments


class ProgramAssignmentPreProcessingManager:
    """Interface for generalized and state-specific pre-processing of
    StateProgramAssignments for use in calculations."""

    def __init__(
        self,
        program_assignments: List[StateProgramAssignment],
        pre_processing_delegate: StateSpecificProgramAssignmentPreProcessingDelegate,
    ) -> None:
        self._program_assignments = deepcopy(program_assignments)
        self.pre_processing_delegate = pre_processing_delegate

    def pre_processed_program_assignments_for_calculations(
        self,
    ) -> List[StateProgramAssignment]:
        """Performs pre-processing on program assignments. Filters out responses
        with null dates, sorts assignments by `referral_date`, `start_date`,
        `discharge_date`, merges appropriate assignments (if the state delegate
        says we should), and returns the list of sorted, pre-processed
        StateProgramAssignments."""
        assignments_for_pre_processing = deepcopy(self._program_assignments)
        filtered_assignments = self._drop_assignments_with_missing_dates(
            assignments_for_pre_processing
        )
        sorted_assignments = self._sorted_program_assignments(filtered_assignments)
        merged_assignments = self.pre_processing_delegate.merge_program_assignments(
            sorted_assignments
        )
        return merged_assignments

    def _drop_assignments_with_missing_dates(
        self, program_assignments: List[StateProgramAssignment]
    ) -> List[StateProgramAssignment]:
        """Filters out assignments with all null dates."""
        filtered_assignments = [
            assignment
            for assignment in program_assignments
            if assignment.referral_date is not None
            or assignment.start_date is not None
            or assignment.discharge_date is not None
        ]
        return filtered_assignments

    def _sorted_program_assignments(
        self, program_assignments: List[StateProgramAssignment]
    ) -> List[StateProgramAssignment]:
        """Sorts the program assignments first by referral date, then by start date,
        then by discharge date, depending on whichever is present."""
        program_assignments.sort(
            key=lambda b: b.referral_date
            or b.start_date
            or b.discharge_date
            or datetime.date.min
        )
        return program_assignments
