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
"""The supervision metric calculation pipeline. See recidiviz/tools/run_sandbox_calculation_pipeline.py
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
from recidiviz.calculator.pipeline.metrics.supervision import (
    identifier,
    metric_producer,
)
from recidiviz.calculator.pipeline.normalization.utils import normalized_entities
from recidiviz.calculator.pipeline.pipeline_type import (
    SUPERVISION_METRICS_PIPELINE_NAME,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_incarceration_delegate import (
    StateSpecificIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_violations_delegate import (
    StateSpecificViolationDelegate,
)
from recidiviz.calculator.query.state.views.reference.supervision_location_ids_to_names import (
    SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_NAME,
)
from recidiviz.calculator.query.state.views.reference.supervision_period_judicial_district_association import (
    SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_NAME,
)
from recidiviz.calculator.query.state.views.reference.supervision_period_to_agent_association import (
    SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_NAME,
)
from recidiviz.calculator.query.state.views.reference.us_mo_sentence_statuses import (
    US_MO_SENTENCE_STATUSES_VIEW_NAME,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state import entities


class SupervisionMetricsPipelineRunDelegate(MetricPipelineRunDelegate):
    """Defines the supervision metric calculation pipeline."""

    @classmethod
    def pipeline_config(cls) -> PipelineConfig:
        return PipelineConfig(
            pipeline_name=SUPERVISION_METRICS_PIPELINE_NAME,
            required_entities=[
                entities.StatePerson,
                entities.StatePersonRace,
                entities.StatePersonEthnicity,
                entities.StatePersonExternalId,
                entities.StateAssessment,
                entities.StateSupervisionContact,
                entities.StateSupervisionSentence,
                entities.StateIncarcerationSentence,
                normalized_entities.NormalizedStateIncarcerationPeriod,
                normalized_entities.NormalizedStateSupervisionPeriod,
                normalized_entities.NormalizedStateSupervisionCaseTypeEntry,
                normalized_entities.NormalizedStateSupervisionViolation,
                normalized_entities.NormalizedStateSupervisionViolationTypeEntry,
                normalized_entities.NormalizedStateSupervisionViolatedConditionEntry,
                normalized_entities.NormalizedStateSupervisionViolationResponse,
                normalized_entities.NormalizedStateSupervisionViolationResponseDecisionEntry,
            ],
            required_reference_tables=[
                SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_NAME,
                SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_NAME,
            ],
            required_state_based_reference_tables=[
                SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_NAME
            ],
            state_specific_required_delegates=[
                StateSpecificIncarcerationDelegate,
                StateSpecificSupervisionDelegate,
                StateSpecificViolationDelegate,
            ],
            state_specific_required_reference_tables={
                # We need to bring in the US_MO sentence status table to load
                # state-specific versions of sentences
                StateCode.US_MO: [US_MO_SENTENCE_STATUSES_VIEW_NAME]
            },
        )

    @classmethod
    def identifier(cls) -> BaseIdentifier:
        return identifier.SupervisionIdentifier()

    @classmethod
    def metric_producer(cls) -> BaseMetricProducer:
        return metric_producer.SupervisionMetricProducer()

    @classmethod
    def include_calculation_limit_args(cls) -> bool:
        return True
