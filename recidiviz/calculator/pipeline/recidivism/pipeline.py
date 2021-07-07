# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""The recidivism calculation pipeline. See recidiviz/tools/run_sandbox_calculation_pipeline.py
for details on how to launch a local run.
"""

from __future__ import absolute_import

import datetime
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import apache_beam as beam
from apache_beam.pvalue import AsList
from apache_beam.typehints import with_input_types, with_output_types
from more_itertools import one

from recidiviz.calculator.pipeline.base_pipeline import (
    BasePipeline,
    ClassifyEvents,
    GetMetrics,
    PipelineConfig,
)
from recidiviz.calculator.pipeline.pipeline_type import PipelineType
from recidiviz.calculator.pipeline.recidivism import identifier, metric_producer
from recidiviz.calculator.pipeline.recidivism.events import ReleaseEvent
from recidiviz.calculator.pipeline.utils.beam_utils import (
    ImportTable,
    ImportTableAsKVTuples,
)
from recidiviz.calculator.pipeline.utils.extractor_utils import BuildRootEntity
from recidiviz.calculator.pipeline.utils.person_utils import (
    BuildPersonMetadata,
    PersonMetadata,
)
from recidiviz.calculator.query.state.views.reference.persons_to_recent_county_of_residence import (
    PERSONS_TO_RECENT_COUNTY_OF_RESIDENCE_VIEW_NAME,
)
from recidiviz.persistence.entity.state import entities


@with_input_types(beam.typehints.Tuple[int, Dict[str, Iterable[Any]]])
@with_output_types(
    beam.typehints.Tuple[entities.StatePerson, Dict[int, ReleaseEvent], PersonMetadata]
)
class ExtractPersonReleaseEventsMetadata(beam.DoFn):
    # pylint: disable=arguments-differ
    def process(
        self, element: Tuple[int, Dict[str, Iterable[Any]]]
    ) -> Iterable[Tuple[entities.StatePerson, List[ReleaseEvent], PersonMetadata]]:
        """Extracts the StatePerson, dict of release years and ReleaseEvents, and PersonMetadata for use in the
        calculator step of the pipeline.

        Note: This is a pipeline-specific version of the ExtractPersonEventsMetadata DoFn in utils/beam_utils.py
        """
        _, element_data = element

        person_events = element_data.get("person_events")
        person_metadata_group = element_data.get("person_metadata")

        # If there isn't a person associated with this person_id_person, continue
        if person_events and person_metadata_group:
            person, events = one(person_events)
            person_metadata = one(person_metadata_group)

            yield person, events, person_metadata

    def to_runner_api_parameter(self, unused_context: Any) -> None:
        pass  # Passing unused abstract method.


class RecidivismPipeline(BasePipeline):
    """Defines the recidivism calculation pipeline."""

    def __init__(self) -> None:
        self.pipeline_config = PipelineConfig(
            pipeline_type=PipelineType.RECIDIVISM,
            identifier=identifier.RecidivismIdentifier(),
            metric_producer=metric_producer.RecidivismMetricProducer(),
        )
        self.include_calculation_limit_args = False

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
        persons = pipeline | "Load Persons" >> BuildRootEntity(
            dataset=input_dataset,
            root_entity_class=entities.StatePerson,
            unifying_id_field=entities.StatePerson.get_class_id_name(),
            build_related_entities=True,
            unifying_id_field_filter_set=person_id_filter_set,
            state_code=state_code,
        )

        # Get StateIncarcerationPeriods
        incarceration_periods = (
            pipeline
            | "Load IncarcerationPeriods"
            >> BuildRootEntity(
                dataset=input_dataset,
                root_entity_class=entities.StateIncarcerationPeriod,
                unifying_id_field=entities.StatePerson.get_class_id_name(),
                build_related_entities=False,
                unifying_id_field_filter_set=person_id_filter_set,
                state_code=state_code,
            )
        )

        # Get StateSupervisionPeriods
        supervision_periods = pipeline | "Load SupervisionPeriods" >> BuildRootEntity(
            dataset=input_dataset,
            root_entity_class=entities.StateSupervisionPeriod,
            unifying_id_field=entities.StatePerson.get_class_id_name(),
            build_related_entities=False,
            unifying_id_field_filter_set=person_id_filter_set,
            state_code=state_code,
        )

        # Bring in the table that associates people and their county of residence
        person_id_to_county_kv = (
            pipeline
            | "Load person_id_to_county_kv"
            >> ImportTableAsKVTuples(
                dataset_id=reference_dataset,
                table_id=PERSONS_TO_RECENT_COUNTY_OF_RESIDENCE_VIEW_NAME,
                table_key="person_id",
                state_code_filter=state_code,
                person_id_filter_set=person_id_filter_set,
            )
        )

        # Group each StatePerson with their StateIncarcerationPeriods
        person_entities = {
            "person": persons,
            "incarceration_periods": incarceration_periods,
            "supervision_periods": supervision_periods,
            "persons_to_recent_county_of_residence": person_id_to_county_kv,
        } | "Group StatePerson to StateIncarcerationPeriods" >> beam.CoGroupByKey()

        state_race_ethnicity_population_counts = (
            pipeline
            | "Load state_race_ethnicity_population_counts"
            >> ImportTable(
                dataset_id=static_reference_dataset,
                table_id="state_race_ethnicity_population_counts",
                state_code_filter=state_code,
                person_id_filter_set=None,
            )
        )

        # Identify ReleaseEvents events from the StatePerson's StateIncarcerationPeriods
        person_release_events = person_entities | "ClassifyReleaseEvents" >> beam.ParDo(
            ClassifyEvents(), identifier=self.pipeline_config.identifier
        )

        person_metadata = (
            persons
            | "Build the person_metadata dictionary"
            >> beam.ParDo(
                BuildPersonMetadata(),
                state_race_ethnicity_population_counts=AsList(
                    state_race_ethnicity_population_counts
                ),
            )
        )

        person_release_events_with_metadata = (
            {"person_events": person_release_events, "person_metadata": person_metadata}
            | "Group ReleaseEvents with person-level metadata" >> beam.CoGroupByKey()
            | "Organize StatePerson, PersonMetadata and ReleaseEvents for calculations"
            >> beam.ParDo(ExtractPersonReleaseEventsMetadata())
        )

        # Add timestamp for local jobs
        job_timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H_%M_%S.%f")
        all_pipeline_options["job_timestamp"] = job_timestamp

        # Get the type of metric to calculate
        metric_types_set = set(metric_types)

        # Get recidivism metrics
        recidivism_metrics = (
            person_release_events_with_metadata
            | "Get Recidivism Metrics"
            >> GetMetrics(
                pipeline_options=all_pipeline_options,
                pipeline_config=self.pipeline_config,
                metric_types_to_include=metric_types_set,
                calculation_end_month=calculation_end_month,
                calculation_month_count=calculation_month_count,
            )
        )
        return recidivism_metrics
