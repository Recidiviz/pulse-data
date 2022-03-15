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
"""The comprehensive normalization calculation pipeline. See
recidiviz/tools/run_sandbox_calculation_pipeline.py for details on how to launch a
local run.
"""
from typing import List, Type

from recidiviz.calculator.pipeline.base_pipeline import PipelineConfig
from recidiviz.calculator.pipeline.normalization.base_entity_normalizer import (
    BaseEntityNormalizer,
)
from recidiviz.calculator.pipeline.normalization.base_normalization_pipeline import (
    NormalizationPipelineRunDelegate,
)
from recidiviz.calculator.pipeline.normalization.comprehensive import entity_normalizer
from recidiviz.calculator.pipeline.pipeline_type import (
    COMPREHENSIVE_NORMALIZATION_PIPELINE_NAME,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.entity_normalization_manager import (
    EntityNormalizationManager,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.incarceration_period_normalization_manager import (
    IncarcerationPeriodNormalizationManager,
    StateSpecificIncarcerationNormalizationDelegate,
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
from recidiviz.calculator.query.state.views.reference.us_mo_sentence_statuses import (
    US_MO_SENTENCE_STATUSES_VIEW_NAME,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state import entities


class ComprehensiveNormalizationPipelineRunDelegate(NormalizationPipelineRunDelegate):
    """Defines the entity normalization pipeline that normalizes all entities with
    configured normalization processes."""

    @classmethod
    def pipeline_config(cls) -> PipelineConfig:
        return PipelineConfig(
            pipeline_name=COMPREHENSIVE_NORMALIZATION_PIPELINE_NAME,
            # Note: This is a list of all of the entities that are required to
            # perform entity normalization on all entities with normalization
            # processes. This is *not* the list of entities that are normalized by
            # this pipeline. See the normalized_entity_classes attribute of each of
            # the EntityNormalizationManagers in the
            # required_entity_normalization_managers below to see all entities
            # normalized by this pipeline.
            required_entities=[
                entities.StateSupervisionSentence,
                entities.StateIncarcerationSentence,
                entities.StateIncarcerationPeriod,
                entities.StateSupervisionPeriod,
                entities.StateSupervisionCaseTypeEntry,
                entities.StateSupervisionViolation,
                entities.StateSupervisionViolationTypeEntry,
                entities.StateSupervisionViolatedConditionEntry,
                entities.StateSupervisionViolationResponse,
                entities.StateSupervisionViolationResponseDecisionEntry,
                entities.StateProgramAssignment,
            ],
            required_reference_tables=[],
            state_specific_required_delegates=[
                StateSpecificIncarcerationNormalizationDelegate,
                StateSpecificSupervisionNormalizationDelegate,
                StateSpecificViolationResponseNormalizationDelegate,
                StateSpecificProgramAssignmentNormalizationDelegate,
            ],
            state_specific_required_reference_tables={
                # We need to bring in the US_MO sentence status table to do
                # do state-specific processing of the sentences for normalizing
                # supervision periods.
                StateCode.US_MO: [US_MO_SENTENCE_STATUSES_VIEW_NAME]
            },
        )

    @classmethod
    def entity_normalizer(cls) -> BaseEntityNormalizer:
        return entity_normalizer.ComprehensiveEntityNormalizer()

    @classmethod
    def required_entity_normalization_managers(
        cls,
    ) -> List[Type[EntityNormalizationManager]]:
        return [
            IncarcerationPeriodNormalizationManager,
            ProgramAssignmentNormalizationManager,
            SupervisionPeriodNormalizationManager,
            ViolationResponseNormalizationManager,
        ]
