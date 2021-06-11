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
import logging
from typing import Any, Dict, Generator, Iterable, List, Optional, Set, Tuple, cast

import apache_beam as beam
from apache_beam.pvalue import AsList
from apache_beam.typehints import with_input_types, with_output_types

from recidiviz.calculator.pipeline.base_pipeline import BasePipeline, job_id
from recidiviz.calculator.pipeline.program import identifier, metric_producer
from recidiviz.calculator.pipeline.program.metrics import (
    ProgramMetric,
    ProgramMetricType,
)
from recidiviz.calculator.pipeline.program.program_event import ProgramEvent
from recidiviz.calculator.pipeline.utils.beam_utils import (
    ImportTable,
    ImportTableAsKVTuples,
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
from recidiviz.calculator.query.state.views.reference.supervision_period_to_agent_association import (
    SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_NAME,
)
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.entity.state.entities import StatePerson


@with_input_types(
    beam.typehints.Tuple[entities.StatePerson, List[IdentifierEvent], PersonMetadata]
)
@with_output_types(ProgramMetric)
class GetProgramMetrics(beam.PTransform):
    """Transforms a StatePerson and their ProgramEvents into ProgramMetrics."""

    def __init__(
        self,
        pipeline_options: Dict[str, Any],
        metric_types: Set[str],
        calculation_month_count: int,
        calculation_end_month: Optional[str] = None,
    ):
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

        self._metric_inclusions: Dict[ProgramMetricType, bool] = {}

        for metric_option in ProgramMetricType:
            if metric_option.value in metric_types or "ALL" in metric_types:
                self._metric_inclusions[metric_option] = True
                logging.info("Producing %s metrics", metric_option.value)
            else:
                self._metric_inclusions[metric_option] = False

    def expand(self, input_or_inputs: List[Any]) -> List[ProgramMetric]:
        # Produce ProgramMetrics from a StatePerson and their ProgramEvents
        program_metrics = input_or_inputs | "Produce ProgramMetrics" >> beam.ParDo(
            ProduceProgramMetrics(),
            self._calculation_end_month,
            self._calculation_month_count,
            self._metric_inclusions,
            self._pipeline_options,
        )

        return program_metrics


@with_input_types(beam.typehints.Tuple[int, Dict[str, Any]])
@with_output_types(
    beam.typehints.Tuple[int, Tuple[entities.StatePerson, List[ProgramEvent]]]
)
class ClassifyProgramAssignments(beam.DoFn):
    """Classifies program assignments as program events, such as referrals to a program."""

    # pylint: disable=arguments-differ
    def process(
        self, element: Tuple[int, Dict[str, Iterable[Any]]]
    ) -> Generator[
        Tuple[Optional[int], Tuple[StatePerson, List[ProgramEvent]]], None, None
    ]:
        """Identifies instances of referrals to a program."""
        _, person_entities = element

        person, kwargs = person_and_kwargs_for_identifier(person_entities)

        # Find the ProgramEvents from the StateProgramAssignments
        program_events = identifier.find_program_events(**kwargs)

        if not program_events:
            logging.info(
                "No valid program events for person with id: %d. Excluding them from the "
                "calculations.",
                person.person_id,
            )
        else:
            yield person.person_id, (person, program_events)

    def to_runner_api_parameter(self, unused_context: Any) -> None:
        pass  # Passing unused abstract method.


@with_input_types(
    beam.typehints.Tuple[entities.StatePerson, List[IdentifierEvent], PersonMetadata],
    beam.typehints.Optional[str],
    beam.typehints.Optional[int],
    beam.typehints.Dict[ProgramMetricType, bool],
    beam.typehints.Dict[str, Any],
)
@with_output_types(ProgramMetric)
class ProduceProgramMetrics(beam.DoFn):
    """Produces ProgramMetrics."""

    # pylint: disable=arguments-differ
    def process(
        self,
        element: Tuple[StatePerson, List[ProgramEvent], PersonMetadata],
        calculation_end_month: Optional[str],
        calculation_month_count: int,
        metric_inclusions: Dict[ProgramMetricType, bool],
        pipeline_options: Dict[str, str],
    ) -> Generator[ProgramMetric, None, None]:
        """Produces various ProgramMetrics.

        Sends the metric producer the StatePerson entity and their corresponding ProgramEvents for mapping all program
        metrics.

        Args:
            element: Tuple containing a StatePerson and their ProgramEvents
            calculation_end_month: The year and month of the last month for which metrics should be calculated.
            calculation_month_count: The number of months to limit the monthly calculation output to.
            metric_inclusions: A dictionary where the keys are each ProgramMetricType, and the values are boolean
                flags for whether or not to include that metric type in the calculations
            pipeline_options: A dictionary storing configuration details for the pipeline.
        Yields:
            Each ProgramMetric.
        """
        person, program_events, person_metadata = element

        pipeline_job_id = job_id(pipeline_options)

        # Assert all events are of type IncarcerationEvent
        program_events = cast(List[ProgramEvent], program_events)

        # Produce program metrics for this person and their program events
        metrics = metric_producer.produce_program_metrics(
            person=person,
            program_events=program_events,
            metric_inclusions=metric_inclusions,
            calculation_end_month=calculation_end_month,
            calculation_month_count=calculation_month_count,
            person_metadata=person_metadata,
            pipeline_job_id=pipeline_job_id,
        )

        # Return each of the ProgramMetrics
        for metric in metrics:
            yield metric

    def to_runner_api_parameter(self, unused_context: Any) -> None:
        pass  # Passing unused abstract method.


class ProgramPipeline(BasePipeline):
    """Defines the program calculation pipeline."""

    def __init__(self) -> None:
        self.name = "program"
        self.metric_type_class = ProgramMetricType  # type: ignore
        self.metric_class = ProgramMetric  # type: ignore
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
        # Get StatePersons
        persons = pipeline | "Load Persons" >> BuildRootEntity(
            dataset=input_dataset,
            root_entity_class=entities.StatePerson,
            unifying_id_field=entities.StatePerson.get_class_id_name(),
            build_related_entities=True,
            unifying_id_field_filter_set=person_id_filter_set,
            state_code=state_code,
        )

        # Get StateProgramAssignments
        program_assignments = pipeline | "Load Program Assignments" >> BuildRootEntity(
            dataset=input_dataset,
            root_entity_class=entities.StateProgramAssignment,
            unifying_id_field=entities.StatePerson.get_class_id_name(),
            build_related_entities=True,
            unifying_id_field_filter_set=person_id_filter_set,
            state_code=state_code,
        )

        # Get StateAssessments
        assessments = pipeline | "Load Assessments" >> BuildRootEntity(
            dataset=input_dataset,
            root_entity_class=entities.StateAssessment,
            unifying_id_field=entities.StatePerson.get_class_id_name(),
            build_related_entities=False,
            unifying_id_field_filter_set=person_id_filter_set,
            state_code=state_code,
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

        supervision_period_to_agent_associations_as_kv = (
            pipeline
            | "Load supervision_period_to_agent_associations_as_kv"
            >> ImportTableAsKVTuples(
                dataset_id=reference_dataset,
                table_id=SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_NAME,
                table_key="person_id",
                state_code_filter=state_code,
                person_id_filter_set=person_id_filter_set,
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

        # Group each StatePerson with their other entities
        persons_entities = {
            "person": persons,
            "program_assignments": program_assignments,
            "assessments": assessments,
            "supervision_periods": supervision_periods,
            "supervision_period_to_agent_association": supervision_period_to_agent_associations_as_kv,
        } | "Group StatePerson to StateProgramAssignments and" >> beam.CoGroupByKey()

        # Identify ProgramEvents from the StatePerson's StateProgramAssignments
        person_program_events = persons_entities | beam.ParDo(
            ClassifyProgramAssignments()
        )

        person_metadata = (
            persons
            | "Build the person_metadata dictionary"
            >> beam.ParDo(
                BuildPersonMetadata(), AsList(state_race_ethnicity_population_counts)
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
            >> GetProgramMetrics(
                pipeline_options=all_pipeline_options,
                metric_types=metric_types_set,
                calculation_end_month=calculation_end_month,
                calculation_month_count=calculation_month_count,
            )
        )

        return program_metrics
