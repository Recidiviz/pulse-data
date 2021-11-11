# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""The program calculation pipeline. See recidiviz/tools/run_sandbox_calculation_pipeline.py
for details on how to launch a local run.
"""
import datetime
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
from recidiviz.calculator.pipeline.program import identifier, metric_producer
from recidiviz.calculator.pipeline.utils.extractor_utils import (
    ExtractDataForPipeline,
    ImportTable,
)
from recidiviz.calculator.pipeline.utils.person_utils import (
    BuildPersonMetadata,
    ExtractPersonEventsMetadata,
)
from recidiviz.calculator.query.state.views.reference.supervision_period_to_agent_association import (
    SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_NAME,
)
from recidiviz.persistence.entity.state import entities


class ProgramPipeline(BasePipeline):
    """Defines the program calculation pipeline."""

    def __init__(self) -> None:
        self.pipeline_config = PipelineConfig(
            pipeline_type=PipelineType.PROGRAM,
            identifier=identifier.ProgramIdentifier(),
            metric_producer=metric_producer.ProgramMetricProducer(),
            required_entities=[
                entities.StatePerson,
                entities.StatePersonRace,
                entities.StatePersonEthnicity,
                entities.StateProgramAssignment,
                entities.StateAssessment,
                entities.StateSupervisionPeriod,
            ],
            required_reference_tables=[
                SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_NAME
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
            required_entity_classes=self.pipeline_config.required_entities,
            unifying_class=entities.StatePerson,
            reference_dataset=reference_dataset,
            required_reference_tables=self.pipeline_config.required_reference_tables,
            unifying_id_field_filter_set=person_id_filter_set,
        )

        state_race_ethnicity_population_counts = (
            pipeline
            | "Load state_race_ethnicity_population_counts"
            >> ImportTable(
                dataset_id=static_reference_dataset,
                table_id="state_race_ethnicity_population_counts",
                state_code_filter=state_code,
                unifying_id_filter_set=None,
            )
        )

        # Identify ProgramEvents from the StatePerson's StateProgramAssignments
        person_program_events = pipeline_data | "Identify program events" >> beam.ParDo(
            ClassifyEvents(), identifier=self.pipeline_config.identifier
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

        person_program_events_with_metadata = (
            {"person_events": person_program_events, "person_metadata": person_metadata}
            | "Group ProgramEvents with person-level metadata" >> beam.CoGroupByKey()
            | "Organize StatePerson, PersonMetadata and ProgramEvents for calculations"
            >> beam.ParDo(ExtractPersonEventsMetadata())
        )

        # Add timestamp for local jobs
        job_timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H_%M_%S.%f")
        all_pipeline_options["job_timestamp"] = job_timestamp

        # Get the type of metric to calculate
        metric_types_set = set(metric_types)

        # Get program metrics
        program_metrics = (
            person_program_events_with_metadata
            | "Get Program Metrics"
            >> GetMetrics(
                pipeline_options=all_pipeline_options,
                pipeline_config=self.pipeline_config,
                metric_types_to_include=metric_types_set,
                calculation_end_month=calculation_end_month,
                calculation_month_count=calculation_month_count,
            )
        )

        return program_metrics
