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
"""The incarceration metric calculation pipeline. See recidiviz/tools/calculator/run_sandbox_calculation_pipeline.py
for details on how to launch a local run.
"""
from __future__ import absolute_import

from typing import Dict, List, Type, Union

from recidiviz.calculator.query.state.views.reference.persons_to_recent_county_of_residence import (
    PERSONS_TO_RECENT_COUNTY_OF_RESIDENCE_VIEW_NAME,
)
from recidiviz.calculator.query.state.views.reference.supervision_location_ids_to_names import (
    SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_NAME,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state import entities, normalized_entities
from recidiviz.persistence.entity.state.normalized_entities import NormalizedStateEntity
from recidiviz.pipelines.metrics.base_identifier import BaseIdentifier
from recidiviz.pipelines.metrics.base_metric_pipeline import MetricPipeline
from recidiviz.pipelines.metrics.base_metric_producer import BaseMetricProducer
from recidiviz.pipelines.metrics.incarceration import identifier, metric_producer
from recidiviz.pipelines.utils.state_utils.state_specific_commitment_from_supervision_delegate import (
    StateSpecificCommitmentFromSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.state_specific_delegate import (
    StateSpecificDelegate,
)
from recidiviz.pipelines.utils.state_utils.state_specific_incarceration_delegate import (
    StateSpecificIncarcerationDelegate,
)
from recidiviz.pipelines.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.state_specific_violations_delegate import (
    StateSpecificViolationDelegate,
)


class IncarcerationMetricsPipeline(MetricPipeline):
    """Defines the incarceration metric calculation pipeline."""

    @classmethod
    def required_entities(
        cls,
    ) -> List[Union[Type[Entity], Type[NormalizedStateEntity]]]:
        return [
            entities.StatePerson,
            entities.StatePersonRace,
            entities.StatePersonEthnicity,
            entities.StatePersonExternalId,
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
    def required_reference_tables(cls) -> List[str]:
        return [
            PERSONS_TO_RECENT_COUNTY_OF_RESIDENCE_VIEW_NAME,
        ]

    @classmethod
    def required_state_based_reference_tables(cls) -> List[str]:
        return [SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_NAME]

    @classmethod
    def state_specific_required_delegates(cls) -> List[Type[StateSpecificDelegate]]:
        return [
            StateSpecificCommitmentFromSupervisionDelegate,
            StateSpecificIncarcerationDelegate,
            StateSpecificSupervisionDelegate,
            StateSpecificViolationDelegate,
        ]

    @classmethod
    def state_specific_required_reference_tables(cls) -> Dict[StateCode, List[str]]:
        return {}

    @classmethod
    def pipeline_name(cls) -> str:
        return "INCARCERATION_METRICS"

    @classmethod
    def identifier(cls) -> BaseIdentifier:
        return identifier.IncarcerationIdentifier()

    @classmethod
    def metric_producer(cls) -> BaseMetricProducer:
        return metric_producer.IncarcerationMetricProducer()

    @classmethod
    def include_calculation_limit_args(cls) -> bool:
        return True
