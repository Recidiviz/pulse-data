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
"""Entity normalizer for normalizing all entities with configured normalization
processes."""
from typing import List

from recidiviz.calculator.pipeline.normalization.base_entity_normalizer import (
    BaseEntityNormalizer,
    EntityNormalizerContext,
    EntityNormalizerResult,
)
from recidiviz.calculator.pipeline.normalization.utils.entity_normalization_manager_utils import (
    normalized_periods_for_calculations,
    normalized_violation_responses_from_processed_versions,
)
from recidiviz.calculator.pipeline.normalization.utils.normalization_managers.incarceration_period_normalization_manager import (
    StateSpecificIncarcerationNormalizationDelegate,
)
from recidiviz.calculator.pipeline.normalization.utils.normalization_managers.program_assignment_normalization_manager import (
    ProgramAssignmentNormalizationManager,
    StateSpecificProgramAssignmentNormalizationDelegate,
)
from recidiviz.calculator.pipeline.normalization.utils.normalization_managers.supervision_period_normalization_manager import (
    StateSpecificSupervisionNormalizationDelegate,
)
from recidiviz.calculator.pipeline.normalization.utils.normalization_managers.supervision_violation_responses_normalization_manager import (
    StateSpecificViolationResponseNormalizationDelegate,
    ViolationResponseNormalizationManager,
)
from recidiviz.calculator.pipeline.normalization.utils.normalized_entities_utils import (
    merge_additional_attributes_maps,
)
from recidiviz.persistence.entity.entity_utils import CoreEntityFieldIndex
from recidiviz.persistence.entity.state.entities import (
    StateAssessment,
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StateProgramAssignment,
    StateSupervisionPeriod,
    StateSupervisionSentence,
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
)


class ComprehensiveEntityNormalizer(BaseEntityNormalizer):
    """Entity normalizer class for the normalization pipeline that normalizes all
    entities with configured normalization processes."""

    def __init__(self) -> None:
        self.field_index = CoreEntityFieldIndex()

    def normalize_entities(
        self,
        person_id: int,
        normalizer_args: EntityNormalizerContext,
    ) -> EntityNormalizerResult:
        """Normalizes all entities with corresponding normalization managers.

        Note: does not normalize all of the entities that are required by
        normalization. E.g. StateSupervisionSentences are required to normalize
        StateSupervisionPeriod entities, but are not themselves normalized.

        Returns a dictionary mapping the entity class name to the list of normalized
        entities, as well as a map of additional attributes that should be persisted
        to the normalized entity tables.
        """
        return self._normalize_entities(
            person_id=person_id,
            ip_normalization_delegate=normalizer_args[
                StateSpecificIncarcerationNormalizationDelegate.__name__
            ],
            sp_normalization_delegate=normalizer_args[
                StateSpecificSupervisionNormalizationDelegate.__name__
            ],
            violation_response_normalization_delegate=normalizer_args[
                StateSpecificViolationResponseNormalizationDelegate.__name__
            ],
            program_assignment_normalization_delegate=normalizer_args[
                StateSpecificProgramAssignmentNormalizationDelegate.__name__
            ],
            incarceration_periods=normalizer_args[StateIncarcerationPeriod.__name__],
            incarceration_sentences=normalizer_args[
                StateIncarcerationSentence.__name__
            ],
            supervision_sentences=normalizer_args[StateSupervisionSentence.__name__],
            supervision_periods=normalizer_args[StateSupervisionPeriod.__name__],
            violation_responses=normalizer_args[
                StateSupervisionViolationResponse.__name__
            ],
            program_assignments=normalizer_args[StateProgramAssignment.__name__],
            assessments=normalizer_args[StateAssessment.__name__],
        )

    def _normalize_entities(
        self,
        person_id: int,
        ip_normalization_delegate: StateSpecificIncarcerationNormalizationDelegate,
        sp_normalization_delegate: StateSpecificSupervisionNormalizationDelegate,
        violation_response_normalization_delegate: StateSpecificViolationResponseNormalizationDelegate,
        program_assignment_normalization_delegate: StateSpecificProgramAssignmentNormalizationDelegate,
        incarceration_periods: List[StateIncarcerationPeriod],
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_sentences: List[StateSupervisionSentence],
        supervision_periods: List[StateSupervisionPeriod],
        violation_responses: List[StateSupervisionViolationResponse],
        program_assignments: List[StateProgramAssignment],
        assessments: List[StateAssessment],
    ) -> EntityNormalizerResult:
        """Normalizes all entities with corresponding normalization managers."""
        processed_entities = all_normalized_entities(
            person_id=person_id,
            ip_normalization_delegate=ip_normalization_delegate,
            sp_normalization_delegate=sp_normalization_delegate,
            violation_response_normalization_delegate=violation_response_normalization_delegate,
            program_assignment_normalization_delegate=program_assignment_normalization_delegate,
            incarceration_periods=incarceration_periods,
            supervision_periods=supervision_periods,
            violation_responses=violation_responses,
            program_assignments=program_assignments,
            incarceration_sentences=incarceration_sentences,
            supervision_sentences=supervision_sentences,
            assessments=assessments,
            field_index=self.field_index,
        )

        return processed_entities


def all_normalized_entities(
    person_id: int,
    ip_normalization_delegate: StateSpecificIncarcerationNormalizationDelegate,
    sp_normalization_delegate: StateSpecificSupervisionNormalizationDelegate,
    violation_response_normalization_delegate: StateSpecificViolationResponseNormalizationDelegate,
    program_assignment_normalization_delegate: StateSpecificProgramAssignmentNormalizationDelegate,
    incarceration_periods: List[StateIncarcerationPeriod],
    supervision_periods: List[StateSupervisionPeriod],
    violation_responses: List[StateSupervisionViolationResponse],
    program_assignments: List[StateProgramAssignment],
    incarceration_sentences: List[StateIncarcerationSentence],
    supervision_sentences: List[StateSupervisionSentence],
    assessments: List[StateAssessment],
    field_index: CoreEntityFieldIndex,
) -> EntityNormalizerResult:
    """Normalizes all entities that have corresponding comprehensive managers.

    Returns a dictionary mapping the entity class name to the list of normalized
    entities.
    """
    violation_response_manager = ViolationResponseNormalizationManager(
        person_id=person_id,
        delegate=violation_response_normalization_delegate,
        violation_responses=violation_responses,
    )

    (
        processed_violation_responses,
        additional_vr_attributes,
    ) = violation_response_manager.normalized_violation_responses_for_calculations()

    normalized_violation_responses = (
        normalized_violation_responses_from_processed_versions(
            processed_violation_responses=processed_violation_responses,
            additional_vr_attributes=additional_vr_attributes,
            field_index=field_index,
        )
    )

    program_assignment_manager = ProgramAssignmentNormalizationManager(
        program_assignments,
        program_assignment_normalization_delegate,
    )

    (
        processed_program_assignments,
        additional_pa_attributes,
    ) = (
        program_assignment_manager.normalized_program_assignments_and_additional_attributes()
    )

    (
        (processed_incarceration_periods, additional_ip_attributes),
        (processed_supervision_periods, additional_sp_attributes),
    ) = normalized_periods_for_calculations(
        person_id=person_id,
        ip_normalization_delegate=ip_normalization_delegate,
        sp_normalization_delegate=sp_normalization_delegate,
        incarceration_periods=incarceration_periods,
        supervision_periods=supervision_periods,
        normalized_violation_responses=normalized_violation_responses,
        field_index=field_index,
        incarceration_sentences=incarceration_sentences,
        supervision_sentences=supervision_sentences,
        assessments=assessments,
    )

    additional_attributes_map = merge_additional_attributes_maps(
        # We don't expect any overlapping entity types in this list, but we just merge
        # so that we can return one unified map.
        additional_attributes_maps=[
            additional_ip_attributes,
            additional_sp_attributes,
            additional_pa_attributes,
            additional_vr_attributes,
        ]
    )

    distinct_processed_violations: List[StateSupervisionViolation] = []

    # We return the StateSupervisionViolation entities as the top-level entity being
    # normalized, since they are the root of the entity tree that contains all
    # violation information. This allows us to not have to de-duplicate
    # NormalizedStateSupervisionViolation entities, which would happen if we sent in
    # the StateSupervisionViolationResponse entities as the top-level entity,
    # since multiple responses can hang off of the same StateSupervisionViolation.
    for response in processed_violation_responses:
        if (
            response.supervision_violation
            and response.supervision_violation not in distinct_processed_violations
        ):
            distinct_processed_violations.append(response.supervision_violation)

    return (
        {
            StateIncarcerationPeriod.__name__: processed_incarceration_periods,
            StateSupervisionPeriod.__name__: processed_supervision_periods,
            StateSupervisionViolation.__name__: distinct_processed_violations,
            StateProgramAssignment.__name__: processed_program_assignments,
        },
        additional_attributes_map,
    )
