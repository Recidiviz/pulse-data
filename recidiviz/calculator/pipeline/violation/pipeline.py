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
import logging
from datetime import datetime
from typing import Any, Dict, Generator, Iterable, List, Optional, Set, Tuple, cast

import apache_beam as beam
from apache_beam.pvalue import AsList
from apache_beam.runners.pipeline_context import PipelineContext
from apache_beam.typehints.decorators import with_input_types, with_output_types

from recidiviz.calculator.pipeline.base_pipeline import BasePipeline, job_id
from recidiviz.calculator.pipeline.utils.beam_utils import ImportTable
from recidiviz.calculator.pipeline.utils.entity_hydration_utils import (
    SetViolationResponsesOntoViolations,
)
from recidiviz.calculator.pipeline.utils.event_utils import IdentifierEvent
from recidiviz.calculator.pipeline.utils.execution_utils import (
    person_and_kwargs_for_identifier,
)
from recidiviz.calculator.pipeline.utils.extractor_utils import BuildRootEntity
from recidiviz.calculator.pipeline.utils.person_utils import (
    BuildPersonMetadata,
    ExtractPersonEventsMetadata,
    PersonMetadata,
)
from recidiviz.calculator.pipeline.violation import identifier, metric_producer
from recidiviz.calculator.pipeline.violation.metrics import (
    ViolationMetric,
    ViolationMetricType,
)
from recidiviz.calculator.pipeline.violation.violation_event import ViolationEvent
from recidiviz.persistence.entity.state import entities


@with_input_types(beam.typehints.Tuple[int, Dict[str, Iterable[Any]]])
@with_output_types(
    beam.typehints.Tuple[int, Tuple[entities.StatePerson, List[ViolationEvent]]]
)
class ClassifyViolationEvents(beam.DoFn):
    """Classifies violation events based on various attributes."""

    # pylint: disable=arguments-differ
    def process(
        self, element: Tuple[int, Dict[str, Iterable[Any]]]
    ) -> Generator[
        Tuple[Optional[int], Tuple[entities.StatePerson, List[ViolationEvent]]],
        None,
        None,
    ]:
        """Identifies various events related to violation relevant to calculations."""
        _, person_entities = element

        person, kwargs = person_and_kwargs_for_identifier(person_entities)

        violation_events = identifier.find_violation_events(**kwargs)

        if not violation_events:
            logging.info(
                "No valid violation events for person with id: %d. Excluding them from the calculations.",
                person.person_id,
            )
        else:
            yield person.person_id, (person, violation_events)

    def to_runner_api_parameter(
        self, _unused_context: PipelineContext
    ) -> Tuple[str, Any]:
        pass


@with_input_types(
    beam.typehints.Tuple[entities.StatePerson, List[IdentifierEvent], PersonMetadata],
    beam.typehints.Optional[str],
    beam.typehints.Optional[int],
    beam.typehints.Dict[ViolationMetricType, bool],
    beam.typehints.Dict[str, Any],
)
@with_output_types(ViolationMetric)
class ProduceViolationMetrics(beam.DoFn):
    """Produces violation metrics."""

    # pylint: disable=arguments-differ
    def process(
        self,
        element: Tuple[entities.StatePerson, List[ViolationEvent], PersonMetadata],
        calculation_end_month: Optional[str],
        calculation_month_count: int,
        metric_inclusions: Dict[ViolationMetricType, bool],
        pipeline_options: Dict[str, str],
    ) -> Generator[ViolationMetric, None, None]:
        """Produces various ViolationMetrics.
        Sends the metric_producer the StatePerson entity and their corresponding ViolationEvents for all metrics.
        Args:
            element: Dictionary containing the person, ViolationEvents, and person_metadata
            calculation_end_month: The year and month of the last month for which metrics should be calculated.
            calculation_month_count: The number of months to limit the monthly calculation output to.
            metric_inclusions: A dictionary where the keys are each ViolationMetricType, and the values are boolean
                values for whether or not to include that metric type in the calculations
            pipeline_options: A dictionary storing configuration details for the pipeline.
        Yields:
            Each violation metric.
        """
        person, violation_events, person_metadata = element

        pipeline_job_id = job_id(pipeline_options)

        violation_events = cast(List[ViolationEvent], violation_events)

        metrics = metric_producer.produce_violation_metrics(
            person,
            violation_events,
            metric_inclusions,
            calculation_end_month,
            calculation_month_count,
            person_metadata,
            pipeline_job_id,
        )

        for metric in metrics:
            yield metric

    def to_runner_api_parameter(
        self, _unused_context: PipelineContext
    ) -> Tuple[str, Any]:
        pass


@with_input_types(
    beam.typehints.Tuple[entities.StatePerson, List[IdentifierEvent], PersonMetadata]
)
@with_output_types(ViolationMetric)
class GetViolationMetrics(beam.PTransform):
    """Transforms a StatePerson and their ViolationEvents into ViolationMetrics."""

    def __init__(
        self,
        pipeline_options: Dict[str, str],
        metric_types: Set[str],
        calculation_month_count: int,
        calculation_end_month: Optional[str] = None,
    ) -> None:
        super().__init__()
        self._pipeline_options = pipeline_options
        self._calculation_end_month = calculation_end_month
        self._calculation_month_count = calculation_month_count

        month_count_string = (
            str(calculation_month_count) if calculation_month_count != -1 else "all"
        )
        end_month_string = (
            calculation_end_month if calculation_end_month else "the current month"
        )
        logging.info(
            "Producing metric output for %s month(s) up to %s",
            month_count_string,
            end_month_string,
        )

        self._metric_inclusions: Dict[ViolationMetricType, bool] = {}

        for metric_option in ViolationMetricType:
            if metric_option.value in metric_types or "ALL" in metric_types:
                self._metric_inclusions[metric_option] = True
                logging.info("Producing %s metrics", metric_option.value)
            else:
                self._metric_inclusions[metric_option] = False

    def expand(self, input_or_inputs: List[Any]) -> List[ViolationMetric]:
        violation_metrics = input_or_inputs | "Produce ViolationMetrics" >> beam.ParDo(
            ProduceViolationMetrics(),
            self._calculation_end_month,
            self._calculation_month_count,
            self._metric_inclusions,
            self._pipeline_options,
        )

        return violation_metrics


class ViolationPipeline(BasePipeline):
    """Defines the violation calculation pipeline."""

    def __init__(self) -> None:
        self.name = "violation"
        self.metric_type_class = ViolationMetricType  # type: ignore
        self.metric_class = ViolationMetric  # type: ignore
        self.include_calculation_limit_args = True

    def execute_pipeline(
        self,
        pipeline: beam.Pipeline,
        all_pipeline_options: Dict[str, Any],
        state_code: str,
        input_dataset: str,
        _reference_dataset: str,
        static_reference_dataset: str,
        metric_types: List[str],
        person_id_filter_set: Optional[Set[int]],
        calculation_month_count: int = -1,
        calculation_end_month: Optional[str] = None,
    ) -> beam.Pipeline:
        # Get StatePersons
        persons = pipeline | "Load Persons" >> BuildRootEntity(
            dataset=input_dataset,
            root_entity_class=entities.StatePerson,
            unifying_id_field=entities.StatePerson.get_class_id_name(),
            build_related_entities=True,
            unifying_id_field_filter_set=person_id_filter_set,
            state_code=state_code,
        )

        # Get StateSupervisionViolations
        supervision_violations = (
            pipeline
            | "Load SupervisionViolations"
            >> BuildRootEntity(
                dataset=input_dataset,
                root_entity_class=entities.StateSupervisionViolation,
                unifying_id_field=entities.StatePerson.get_class_id_name(),
                build_related_entities=True,
                unifying_id_field_filter_set=person_id_filter_set,
                state_code=state_code,
            )
        )

        # Get StateSupervisionViolationResponses
        supervision_violation_responses = (
            pipeline
            | "Load SupervisionViolationResponses"
            >> BuildRootEntity(
                dataset=input_dataset,
                root_entity_class=entities.StateSupervisionViolationResponse,
                unifying_id_field=entities.StatePerson.get_class_id_name(),
                build_related_entities=True,
                unifying_id_field_filter_set=person_id_filter_set,
                state_code=state_code,
            )
        )

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

        # Group StateSupervisionViolationResponses and StateSupervisionViolations by person_id
        supervision_violations_and_responses = (
            {
                "violations": supervision_violations,
                "violation_responses": supervision_violation_responses,
            }
            | "Group StateSupervisionViolationResponses to StateSupervisionViolations"
            >> beam.CoGroupByKey()
        )

        violations_with_hydrated_violation_responses = (
            supervision_violations_and_responses
            | "Set hydrated StateSupervisionViolationResponses on the StateSupervisionViolations"
            >> beam.ParDo(SetViolationResponsesOntoViolations())
        )

        person_entities = {
            "person": persons,
            "violations": violations_with_hydrated_violation_responses,
        } | "Group StatePerson to violation entities" >> beam.CoGroupByKey()

        person_violation_events = person_entities | "Get ViolationEvents" >> beam.ParDo(
            ClassifyViolationEvents()
        )

        person_metadata = (
            persons
            | "Build the person_metadata dictionary"
            >> beam.ParDo(
                BuildPersonMetadata(), AsList(state_race_ethnicity_population_counts)
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
            >> GetViolationMetrics(
                pipeline_options=all_pipeline_options,
                metric_types=metric_types_set,
                calculation_end_month=calculation_end_month,
                calculation_month_count=calculation_month_count,
            )
        )

        return violation_metrics
