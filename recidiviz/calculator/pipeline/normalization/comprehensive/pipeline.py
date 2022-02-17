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
import apache_beam as beam

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
from recidiviz.calculator.pipeline.utils.entity_normalization.incarceration_period_normalization_manager import (
    StateSpecificIncarcerationNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.program_assignment_normalization_manager import (
    StateSpecificProgramAssignmentNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.supervision_period_normalization_manager import (
    StateSpecificSupervisionNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.supervision_violation_responses_normalization_manager import (
    StateSpecificViolationResponseNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_incarceration_delegate import (
    StateSpecificIncarcerationDelegate,
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
                StateSpecificIncarcerationDelegate,
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

    def write_output(self, pipeline: beam.Pipeline) -> None:
        pass
