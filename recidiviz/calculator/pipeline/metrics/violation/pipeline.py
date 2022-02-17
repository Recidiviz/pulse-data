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
"""The violations metric calculation pipeline. See recidiviz/tools/run_sandbox_calculation_pipeline.py
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
from recidiviz.calculator.pipeline.metrics.violation import identifier, metric_producer
from recidiviz.calculator.pipeline.pipeline_type import VIOLATION_METRICS_PIPELINE_NAME
from recidiviz.calculator.pipeline.utils.entity_normalization.supervision_violation_responses_normalization_manager import (
    StateSpecificViolationResponseNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_violations_delegate import (
    StateSpecificViolationDelegate,
)
from recidiviz.persistence.entity.state import entities


class ViolationMetricsPipelineRunDelegate(MetricPipelineRunDelegate):
    """Defines the violation metric calculation pipeline."""

    @classmethod
    def pipeline_config(cls) -> PipelineConfig:
        return PipelineConfig(
            pipeline_name=VIOLATION_METRICS_PIPELINE_NAME,
            required_entities=[
                entities.StatePerson,
                entities.StatePersonRace,
                entities.StatePersonEthnicity,
                entities.StateSupervisionViolation,
                entities.StateSupervisionViolationTypeEntry,
                entities.StateSupervisionViolatedConditionEntry,
                entities.StateSupervisionViolationResponse,
                entities.StateSupervisionViolationResponseDecisionEntry,
            ],
            required_reference_tables=[],
            state_specific_required_delegates=[
                StateSpecificViolationResponseNormalizationDelegate,
                StateSpecificViolationDelegate,
            ],
            state_specific_required_reference_tables={},
        )

    @classmethod
    def identifier(cls) -> BaseIdentifier:
        return identifier.ViolationIdentifier()

    @classmethod
    def metric_producer(cls) -> BaseMetricProducer:
        return metric_producer.ViolationMetricProducer()

    @classmethod
    def include_calculation_limit_args(cls) -> bool:
        return True
