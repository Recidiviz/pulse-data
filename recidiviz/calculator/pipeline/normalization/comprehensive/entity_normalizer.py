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
from typing import Dict, List, Sequence

from recidiviz.calculator.pipeline.normalization.base_entity_normalizer import (
    BaseEntityNormalizer,
    EntityNormalizerContext,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.entity_normalization_manager_utils import (
    entity_normalization_managers_for_periods,
    normalized_program_assignments_for_calculations,
    normalized_violation_responses_for_calculations,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.incarceration_period_normalization_manager import (
    IncarcerationPeriodNormalizationManager,
    StateSpecificIncarcerationNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_entities import (
    NormalizedStateEntity,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.program_assignment_normalization_manager import (
    ProgramAssignmentNormalizationManager,
    StateSpecificProgramAssignmentNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.supervision_period_normalization_manager import (
    StateSpecificSupervisionNormalizationDelegate,
    SupervisionPeriodNormalizationManager,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.supervision_violation_responses_normalization_manager import (
    StateSpecificViolationResponseNormalizationDelegate,
    ViolationResponseNormalizationManager,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_incarceration_delegate import (
    StateSpecificIncarcerationDelegate,
)
from recidiviz.persistence.entity.entity_utils import CoreEntityFieldIndex
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StateProgramAssignment,
    StateSupervisionPeriod,
    StateSupervisionSentence,
    StateSupervisionViolationResponse,
)


class ComprehensiveEntityNormalizer(BaseEntityNormalizer):
    """Entity normalizer class for the normalization pipeline that normalizes all
    entities with configured normalization processes."""

    def __init__(self) -> None:
        self.field_index = CoreEntityFieldIndex()

    def normalize_entities(
        self,
        normalizer_args: EntityNormalizerContext,
    ) -> Dict[str, Sequence[NormalizedStateEntity]]:
        """Normalizes all entities with corresponding normalization managers.

        Returns a dictionary mapping the entity class name to the list of normalized
        entities.
        """
        return self._normalize_entities(
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
            incarceration_delegate=normalizer_args[
                StateSpecificIncarcerationDelegate.__name__
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
        )

    def _normalize_entities(
        self,
        ip_normalization_delegate: StateSpecificIncarcerationNormalizationDelegate,
        sp_normalization_delegate: StateSpecificSupervisionNormalizationDelegate,
        violation_response_normalization_delegate: StateSpecificViolationResponseNormalizationDelegate,
        program_assignment_normalization_delegate: StateSpecificProgramAssignmentNormalizationDelegate,
        incarceration_delegate: StateSpecificIncarcerationDelegate,
        incarceration_periods: List[StateIncarcerationPeriod],
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_sentences: List[StateSupervisionSentence],
        supervision_periods: List[StateSupervisionPeriod],
        violation_responses: List[StateSupervisionViolationResponse],
        program_assignments: List[StateProgramAssignment],
    ) -> Dict[str, Sequence[NormalizedStateEntity]]:
        """Normalizes all entities with corresponding normalization managers."""
        processed_entities = all_normalized_entities(
            ip_normalization_delegate=ip_normalization_delegate,
            sp_normalization_delegate=sp_normalization_delegate,
            violation_response_normalization_delegate=violation_response_normalization_delegate,
            program_assignment_normalization_delegate=program_assignment_normalization_delegate,
            incarceration_delegate=incarceration_delegate,
            field_index=self.field_index,
            incarceration_periods=incarceration_periods,
            supervision_periods=supervision_periods,
            violation_responses=violation_responses,
            program_assignments=program_assignments,
            incarceration_sentences=incarceration_sentences,
            supervision_sentences=supervision_sentences,
        )

        return processed_entities


def all_normalized_entities(
    ip_normalization_delegate: StateSpecificIncarcerationNormalizationDelegate,
    sp_normalization_delegate: StateSpecificSupervisionNormalizationDelegate,
    violation_response_normalization_delegate: StateSpecificViolationResponseNormalizationDelegate,
    program_assignment_normalization_delegate: StateSpecificProgramAssignmentNormalizationDelegate,
    incarceration_delegate: StateSpecificIncarcerationDelegate,
    incarceration_periods: List[StateIncarcerationPeriod],
    supervision_periods: List[StateSupervisionPeriod],
    violation_responses: List[StateSupervisionViolationResponse],
    program_assignments: List[StateProgramAssignment],
    incarceration_sentences: List[StateIncarcerationSentence],
    supervision_sentences: List[StateSupervisionSentence],
    field_index: CoreEntityFieldIndex,
) -> Dict[str, Sequence[NormalizedStateEntity]]:
    """Normalizes all entities that have corresponding comprehensive managers.

    Returns a dictionary mapping the entity class name to the list of normalized
    entities.
    """
    processed_violation_responses = normalized_violation_responses_for_calculations(
        violation_response_normalization_delegate=violation_response_normalization_delegate,
        violation_responses=violation_responses,
    )

    processed_program_assignments = normalized_program_assignments_for_calculations(
        program_assignment_normalization_delegate=program_assignment_normalization_delegate,
        program_assignments=program_assignments,
    )

    (
        ip_normalization_manager,
        sp_normalization_manager,
    ) = entity_normalization_managers_for_periods(
        ip_normalization_delegate=ip_normalization_delegate,
        sp_normalization_delegate=sp_normalization_delegate,
        incarceration_delegate=incarceration_delegate,
        incarceration_periods=incarceration_periods,
        supervision_periods=supervision_periods,
        # TODO(#10729): Send in the NormalizedStateSupervisionViolationResponse entities
        normalized_violation_responses=processed_violation_responses,
        field_index=field_index,
        incarceration_sentences=incarceration_sentences,
        supervision_sentences=supervision_sentences,
    )

    if not ip_normalization_manager:
        raise ValueError(
            "Expected instantiated "
            "IncarcerationPeriodNormalizationManager. Found None."
        )

    if not sp_normalization_manager:
        raise ValueError(
            "Expected instantiated "
            "SupervisionPeriodNormalizationManager. Found None."
        )

    # TODO(#10727): Move collapsing of transfers to later
    ip_index = (
        ip_normalization_manager.normalized_incarceration_period_index_for_calculations(
            collapse_transfers=False, overwrite_facility_information_in_transfers=False
        )
    )

    processed_ips = ip_index.incarceration_periods

    # TODO(#10729): Move these conversions to the end of the normalization processes
    normalized_ips = (
        IncarcerationPeriodNormalizationManager.convert_ips_to_normalized_ips(
            processed_ips,
            ip_index.ip_id_to_pfi_subtype,
        )
    )

    normalized_sps = SupervisionPeriodNormalizationManager.convert_sps_to_normalized_sps(
        sp_normalization_manager.normalized_supervision_period_index_for_calculations().supervision_periods
    )

    normalized_vrs = (
        ViolationResponseNormalizationManager.convert_vrs_to_normalized_vrs(
            processed_violation_responses
        )
    )

    normalized_program_assignments = (
        ProgramAssignmentNormalizationManager.convert_pas_to_normalized_pas(
            processed_program_assignments
        )
    )

    return {
        StateIncarcerationPeriod.__name__: normalized_ips,
        StateSupervisionPeriod.__name__: normalized_sps,
        StateSupervisionViolationResponse.__name__: normalized_vrs,
        StateProgramAssignment.__name__: normalized_program_assignments,
    }
