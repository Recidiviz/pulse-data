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
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

import apache_beam as beam
from apache_beam.pvalue import AsList

from recidiviz.calculator.pipeline.base_pipeline import (
    BasePipeline,
    ClassifyEvents,
    GetMetrics,
    PipelineConfig,
)
from recidiviz.calculator.pipeline.pipeline_type import PipelineType
from recidiviz.calculator.pipeline.utils.extractor_utils import (
    ExtractDataForPipeline,
    ImportTable,
)
from recidiviz.calculator.pipeline.utils.person_utils import (
    BuildPersonMetadata,
    ExtractPersonEventsMetadata,
)
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
        )
        self.include_calculation_limit_args = True

    def execute_pipeline(
        self,
        pipeline: beam.Pipeline,
        all_pipeline_options: Dict[str, Any],
        state_code: str,
        input_dataset: str,
        reference_dataset: str,
        static_reference_dataset: str,
        metric_types: List[str],
        person_id_filter_set: Optional[Set[int]],
        calculation_month_count: int = -1,
        calculation_end_month: Optional[str] = None,
    ) -> beam.Pipeline:
        # TODO(#2769): Remove this once required_entities is non-optional
        if not self.pipeline_config.required_entities:
            raise ValueError("Must set required_entities arg on PipelineConfig.")

        # Get all required entities and reference data
        pipeline_data = pipeline | "Load required data" >> ExtractDataForPipeline(
            state_code=state_code,
            dataset=input_dataset,
            reference_dataset=reference_dataset,
            required_entity_classes=self.pipeline_config.required_entities,
            required_reference_tables=self.pipeline_config.required_reference_tables,
            unifying_class=entities.StatePerson,
            unifying_id_field_filter_set=person_id_filter_set,
        )

        person_violation_events = pipeline_data | "Get ViolationEvents" >> beam.ParDo(
            ClassifyEvents(), identifier=self.pipeline_config.identifier
        )

        state_race_ethnicity_population_counts = (
            pipeline
            | "Load state_race_ethnicity_population_counts"
            >> ImportTable(
                dataset_id=static_reference_dataset,
                table_id="state_race_ethnicity_population_counts",
                state_code_filter=state_code,
            )
        )

        person_metadata = (
            pipeline_data
            | "Build the person_metadata dictionary"
            >> beam.ParDo(
                BuildPersonMetadata(),
                state_race_ethnicity_population_counts=AsList(
                    state_race_ethnicity_population_counts
                ),
            )
        )

        person_violation_events_with_metadata = (
            {
                "person_events": person_violation_events,
                "person_metadata": person_metadata,
            }
            | "Group ViolationEvents with person-level metadata" >> beam.CoGroupByKey()
            | "Organize StatePerson, PersonMetadata and ViolationEvents for calculations"
            >> beam.ParDo(ExtractPersonEventsMetadata())
        )

        metric_types_set = set(metric_types)
        job_timestamp = datetime.now().strftime("%Y-%m-%d_%H_%M_%S.%f")
        all_pipeline_options["job_timestamp"] = job_timestamp

        # Get violation metrics
        violation_metrics = (
            person_violation_events_with_metadata
            | "Get Violation Metrics"
            >> GetMetrics(
                pipeline_options=all_pipeline_options,
                pipeline_config=self.pipeline_config,
                metric_types_to_include=metric_types_set,
                calculation_end_month=calculation_end_month,
                calculation_month_count=calculation_month_count,
            )
        )

        return violation_metrics
