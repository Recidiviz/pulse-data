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
"""Contains the logic for a ProgramAssignmentNormalizationManager that manages the
normalization of StateProgramAssignment entities in the calculation
pipelines."""
import datetime
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple, Type

from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.normalized_entities_utils import (
    AdditionalAttributesMap,
    get_shared_additional_attributes_map_for_entities,
    merge_additional_attributes_maps,
)
from recidiviz.persistence.entity.state.entities import StateProgramAssignment
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateProgramAssignment,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.entity_normalization_manager import (
    EntityNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalized_entity_conversion_utils import (
    convert_entity_trees_to_normalized_versions,
)


class ProgramAssignmentNormalizationManager(EntityNormalizationManager):
    """Interface for generalized and state-specific normalization of
    StateProgramAssignments for use in calculations."""

    def __init__(
        self,
        program_assignments: List[StateProgramAssignment],
        staff_external_id_to_staff_id: Dict[Tuple[str, str], int],
    ) -> None:
        self._program_assignments = deepcopy(program_assignments)
        self._normalized_program_assignments_and_additional_attributes: Optional[
            Tuple[List[StateProgramAssignment], AdditionalAttributesMap]
        ] = None
        self.staff_external_id_to_staff_id = staff_external_id_to_staff_id

    def get_normalized_program_assignments(
        self,
    ) -> list[NormalizedStateProgramAssignment]:
        (
            processed_program_assignments,
            additional_pa_attributes,
        ) = self.normalized_program_assignments_and_additional_attributes()
        return convert_entity_trees_to_normalized_versions(
            processed_program_assignments,
            NormalizedStateProgramAssignment,
            additional_pa_attributes,
        )

    def normalized_program_assignments_and_additional_attributes(
        self,
    ) -> Tuple[List[StateProgramAssignment], AdditionalAttributesMap]:
        """Performs normalization on program assignments. Filters out responses
        with null dates, sorts assignments by `referral_date`, `start_date`,
        `discharge_date`, merges appropriate assignments (if the state delegate
        says we should), and returns the list of sorted, normalized
        StateProgramAssignments."""
        if not self._normalized_program_assignments_and_additional_attributes:
            assignments_for_normalization = deepcopy(self._program_assignments)
            filtered_assignments = self._drop_assignments_with_missing_dates(
                assignments_for_normalization
            )
            sorted_assignments = self._sorted_program_assignments(filtered_assignments)

            self._normalized_program_assignments_and_additional_attributes = (
                sorted_assignments,
                self.additional_attributes_map_for_normalized_pas(
                    program_assignments=sorted_assignments
                ),
            )

        return self._normalized_program_assignments_and_additional_attributes

    @staticmethod
    def normalized_entity_classes() -> List[Type[Entity]]:
        return [StateProgramAssignment]

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
            # TODO(#31377): This sorting is non-deterministic when, for example, two
            #  program assignments have the same referral_date.
            key=lambda b: b.referral_date
            or b.start_date
            or b.discharge_date
            or datetime.date.min
        )
        return program_assignments

    def additional_attributes_map_for_normalized_pas(
        self,
        program_assignments: List[StateProgramAssignment],
    ) -> AdditionalAttributesMap:
        """Get additional attributes for each StateProgramAssignment."""
        shared_additional_attributes_map = (
            get_shared_additional_attributes_map_for_entities(
                entities=program_assignments
            )
        )
        program_assignments_additional_attributes_map: Dict[
            str, Dict[int, Dict[str, Any]]
        ] = {StateProgramAssignment.__name__: {}}

        for program_assignment in program_assignments:
            if not program_assignment.program_assignment_id:
                raise ValueError(
                    "Expected non-null program_assignment_id values"
                    f"at this point. Found {program_assignment}."
                )
            referring_staff_id = None
            if program_assignment.referring_staff_external_id:
                if not program_assignment.referring_staff_external_id_type:
                    raise ValueError(
                        f"Found no referring_staff_external_id_type for referring_staff_external_id "
                        f"{program_assignment.referring_staff_external_id} on person "
                        f"{program_assignment.person}"
                    )
                referring_staff_id = self.staff_external_id_to_staff_id[
                    (
                        program_assignment.referring_staff_external_id,
                        program_assignment.referring_staff_external_id_type,
                    )
                ]
            program_assignments_additional_attributes_map[
                StateProgramAssignment.__name__
            ][program_assignment.program_assignment_id] = {
                "referring_staff_id": referring_staff_id,
            }
        return merge_additional_attributes_maps(
            [
                shared_additional_attributes_map,
                program_assignments_additional_attributes_map,
            ]
        )
