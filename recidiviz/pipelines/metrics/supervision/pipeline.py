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
"""The supervision metric calculation pipeline. See recidiviz/tools/calculator/run_sandbox_calculation_pipeline.py
for details on how to launch a local run.
"""
from typing import List, Type

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.pipelines.metrics.base_identifier import BaseIdentifier
from recidiviz.pipelines.metrics.base_metric_pipeline import MetricPipeline
from recidiviz.pipelines.metrics.base_metric_producer import BaseMetricProducer
from recidiviz.pipelines.metrics.supervision import identifier, metric_producer


class SupervisionMetricsPipeline(MetricPipeline):
    """Defines the supervision metric calculation pipeline."""

    @classmethod
    def required_entities(cls) -> List[Type[Entity]]:
        return [
            normalized_entities.NormalizedStatePerson,
            normalized_entities.NormalizedStatePersonRace,
            normalized_entities.NormalizedStatePersonExternalId,
            normalized_entities.NormalizedStateSupervisionContact,
            normalized_entities.NormalizedStateAssessment,
            normalized_entities.NormalizedStateIncarcerationPeriod,
            normalized_entities.NormalizedStateSupervisionPeriod,
            normalized_entities.NormalizedStateSupervisionCaseTypeEntry,
            normalized_entities.NormalizedStateSupervisionViolation,
            normalized_entities.NormalizedStateSupervisionViolationTypeEntry,
            normalized_entities.NormalizedStateSupervisionViolatedConditionEntry,
            normalized_entities.NormalizedStateSupervisionViolationResponse,
            normalized_entities.NormalizedStateSupervisionViolationResponseDecisionEntry,
        ]

    @classmethod
    def pipeline_name(cls) -> str:
        return "SUPERVISION_METRICS"

    @classmethod
    def identifier(cls, state_code: StateCode) -> BaseIdentifier:
        return identifier.SupervisionIdentifier(state_code)

    @classmethod
    def metric_producer(cls) -> BaseMetricProducer:
        return metric_producer.SupervisionMetricProducer()

    @classmethod
    def include_calculation_limit_args(cls) -> bool:
        return True
