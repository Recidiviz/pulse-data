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
"""The program metric calculation pipeline. See recidiviz/tools/calculator/run_sandbox_calculation_pipeline.py
for details on how to launch a local run.
"""
from typing import Dict, List, Type, Union

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state import entities, normalized_entities
from recidiviz.persistence.entity.state.normalized_state_entity import (
    NormalizedStateEntity,
)
from recidiviz.pipelines.metrics.base_identifier import BaseIdentifier
from recidiviz.pipelines.metrics.base_metric_pipeline import MetricPipeline
from recidiviz.pipelines.metrics.base_metric_producer import BaseMetricProducer
from recidiviz.pipelines.metrics.program import identifier, metric_producer
from recidiviz.pipelines.utils.state_utils.state_specific_delegate import (
    StateSpecificDelegate,
)
from recidiviz.pipelines.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)


class ProgramMetricsPipeline(MetricPipeline):
    """Defines the program metric calculation pipeline."""

    @classmethod
    def required_entities(
        cls,
    ) -> List[Union[Type[Entity], Type[NormalizedStateEntity]]]:
        return [
            entities.StatePerson,
            entities.StatePersonRace,
            entities.StatePersonEthnicity,
            normalized_entities.NormalizedStateAssessment,
            normalized_entities.NormalizedStateProgramAssignment,
            normalized_entities.NormalizedStateSupervisionPeriod,
        ]

    @classmethod
    def required_reference_tables(cls) -> List[str]:
        return []

    @classmethod
    def state_specific_required_delegates(cls) -> List[Type[StateSpecificDelegate]]:
        return [
            StateSpecificSupervisionDelegate,
        ]

    @classmethod
    def state_specific_required_reference_tables(cls) -> Dict[StateCode, List[str]]:
        return {}

    @classmethod
    def pipeline_name(cls) -> str:
        return "PROGRAM_METRICS"

    @classmethod
    def identifier(cls) -> BaseIdentifier:
        return identifier.ProgramIdentifier()

    @classmethod
    def metric_producer(cls) -> BaseMetricProducer:
        return metric_producer.ProgramMetricProducer()

    @classmethod
    def include_calculation_limit_args(cls) -> bool:
        return True
