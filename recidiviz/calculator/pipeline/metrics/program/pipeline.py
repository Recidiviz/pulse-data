# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""The program metric calculation pipeline. See recidiviz/tools/run_sandbox_calculation_pipeline.py
for details on how to launch a local run.
"""
from recidiviz.calculator.pipeline.base_pipeline import PipelineConfig
from recidiviz.calculator.pipeline.metrics.base_identifier import BaseIdentifier
from recidiviz.calculator.pipeline.metrics.base_metric_pipeline import (
    MetricPipelineRunDelegate,
)
from recidiviz.calculator.pipeline.metrics.base_metric_producer import (
    BaseMetricProducer,
)
from recidiviz.calculator.pipeline.metrics.program import identifier, metric_producer
from recidiviz.calculator.pipeline.pipeline_type import PROGRAM_METRICS_PIPELINE_NAME
from recidiviz.calculator.pipeline.utils.entity_normalization import normalized_entities
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)
from recidiviz.calculator.query.state.views.reference.supervision_period_to_agent_association import (
    SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_NAME,
)
from recidiviz.persistence.entity.state import entities


class ProgramMetricsPipelineRunDelegate(MetricPipelineRunDelegate):
    """Defines the program metric calculation pipeline."""

    @classmethod
    def pipeline_config(cls) -> PipelineConfig:
        return PipelineConfig(
            pipeline_name=PROGRAM_METRICS_PIPELINE_NAME,
            required_entities=[
                entities.StatePerson,
                entities.StatePersonRace,
                entities.StatePersonEthnicity,
                entities.StateAssessment,
                normalized_entities.NormalizedStateProgramAssignment,
                normalized_entities.NormalizedStateSupervisionPeriod,
            ],
            required_reference_tables=[
                SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_NAME
            ],
            state_specific_required_delegates=[
                StateSpecificSupervisionDelegate,
            ],
            state_specific_required_reference_tables={},
        )

    @classmethod
    def identifier(cls) -> BaseIdentifier:
        return identifier.ProgramIdentifier()

    @classmethod
    def metric_producer(cls) -> BaseMetricProducer:
        return metric_producer.ProgramMetricProducer()

    @classmethod
    def include_calculation_limit_args(cls) -> bool:
        return True
