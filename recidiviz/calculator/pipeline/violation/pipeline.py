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
"""The violations calculation pipeline. See recidiviz/tools/run_sandbox_calculation_pipeline.py
for details on how to launch a local run.
"""
from recidiviz.calculator.pipeline.base_pipeline import BasePipeline, PipelineConfig
from recidiviz.calculator.pipeline.pipeline_type import PipelineType
from recidiviz.calculator.pipeline.violation import identifier, metric_producer
from recidiviz.persistence.entity.state import entities


class ViolationPipeline(BasePipeline):
    """Defines the violation calculation pipeline."""

    def __init__(self) -> None:
        self.pipeline_config = PipelineConfig(
            pipeline_type=PipelineType.VIOLATION,
            identifier=identifier.ViolationIdentifier(),
            metric_producer=metric_producer.ViolationMetricProducer(),
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
            state_specific_required_reference_tables={},
        )
        self.include_calculation_limit_args = True
