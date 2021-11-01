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
"""Abstract class for all calculation pipelines."""
import abc
import argparse
import logging
from typing import (
    Any,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
)

import apache_beam as beam
import attr
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.runners.pipeline_context import PipelineContext
from apache_beam.typehints.decorators import with_input_types, with_output_types

from recidiviz.calculator.dataflow_config import (
    DATAFLOW_METRICS_TO_TABLES,
    DATAFLOW_TABLES_TO_METRIC_TYPES,
)
from recidiviz.calculator.pipeline.base_identifier import BaseIdentifier
from recidiviz.calculator.pipeline.base_metric_producer import BaseMetricProducer
from recidiviz.calculator.pipeline.pipeline_type import PipelineType
from recidiviz.calculator.pipeline.utils.beam_utils import (
    RecidivizMetricWritableDict,
    WriteAppendToBigQuery,
)
from recidiviz.calculator.pipeline.utils.event_utils import IdentifierEvent
from recidiviz.calculator.pipeline.utils.execution_utils import (
    get_job_id,
    person_and_kwargs_for_identifier,
)
from recidiviz.calculator.pipeline.utils.metric_utils import (
    RecidivizMetric,
    RecidivizMetricType,
)
from recidiviz.calculator.pipeline.utils.person_utils import PersonMetadata
from recidiviz.calculator.pipeline.utils.pipeline_args_utils import (
    add_shared_pipeline_arguments,
)
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.state import entities
from recidiviz.utils import environment

# Cached job_id value
_job_id = None


def job_id(pipeline_options: Dict[str, str]) -> str:
    global _job_id
    if not _job_id:
        _job_id = get_job_id(pipeline_options)
    return _job_id


@environment.test_only
def clear_job_id() -> None:
    global _job_id
    _job_id = None


@attr.s(frozen=True)
class PipelineConfig:
    """Configuration needed for a calculation pipeline."""

    # The type of the pipeline, usually matches the top-level package and is used to identify which pipeline to run in sandbox runs
    pipeline_type: PipelineType = attr.ib()

    # The identifier used to identify specific events for metrics
    identifier: BaseIdentifier = attr.ib()

    # The metric producer transforming events to metrics
    metric_producer: BaseMetricProducer = attr.ib()


@with_input_types(
    beam.typehints.Tuple[
        entities.StatePerson,
        Union[Dict[int, IdentifierEvent], List[IdentifierEvent]],
        PersonMetadata,
    ],
    beam.typehints.Optional[PipelineConfig],
    beam.typehints.Dict[RecidivizMetricType, bool],
    beam.typehints.Dict[str, Any],
    beam.typehints.Optional[str],
    beam.typehints.Optional[int],
)
@with_output_types(RecidivizMetric)
class ProduceMetrics(beam.DoFn):
    """A DoFn that produces metrics given a StatePerson, metadata and associated events."""

    # pylint: disable=arguments-differ
    def process(
        self,
        element: Tuple[
            entities.StatePerson,
            Union[Dict[int, IdentifierEvent], List[IdentifierEvent]],
            PersonMetadata,
        ],
        pipeline_config: PipelineConfig,
        metric_inclusions: Dict[RecidivizMetricType, bool],
        pipeline_options: Dict[str, str],
        calculation_end_month: Optional[str] = None,
        calculation_month_count: int = -1,
    ) -> Generator[RecidivizMetric, None, None]:
        """Produces various metrics.
        Sends the metric_producer the StatePerson entity and their corresponding events for mapping all metrics.
        Args:
            element: Dictionary containing the person, events, and person_metadata
            calculation_end_month: The year and month of the last month for which metrics should be calculated.
            calculation_month_count: The number of months to limit the monthly calculation output to.
            metric_inclusions: A dictionary where the keys are each metric type, and the values are boolean
                values for whether or not to include that metric type in the calculations
            pipeline_options: A dictionary storing configuration details for the pipeline.
        Yields:
            Each supervision metric."""
        person, events, person_metadata = element

        pipeline_job_id = job_id(pipeline_options)

        metrics = pipeline_config.metric_producer.produce_metrics(
            person=person,
            identifier_events=events,
            metric_inclusions=metric_inclusions,
            person_metadata=person_metadata,
            pipeline_type=pipeline_config.pipeline_type,
            pipeline_job_id=pipeline_job_id,
            calculation_end_month=calculation_end_month,
            calculation_month_count=calculation_month_count,
        )

        for metric in metrics:
            yield metric

    def to_runner_api_parameter(
        self, _unused_context: PipelineContext
    ) -> Tuple[str, Any]:
        pass


@with_input_types(
    beam.typehints.Tuple[
        entities.StatePerson,
        Union[Dict[int, IdentifierEvent], List[IdentifierEvent]],
        PersonMetadata,
    ],
)
@with_output_types(RecidivizMetric)
class GetMetrics(beam.PTransform):
    """A PTransform that transforms a StatePerson and their corresponding events to corresponding metrics."""

    def __init__(
        self,
        pipeline_options: Dict[str, str],
        pipeline_config: PipelineConfig,
        metric_types_to_include: Set[str],
        calculation_month_count: int = -1,
        calculation_end_month: Optional[str] = None,
    ) -> None:
        super().__init__()
        self._pipeline_config = pipeline_config
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

        self._metric_inclusions: Dict[RecidivizMetricType, bool] = {}

        for (
            metric_option
        ) in pipeline_config.metric_producer.metric_class.metric_type_cls:
            if (
                metric_option.value in metric_types_to_include
                or "ALL" in metric_types_to_include
            ):
                self._metric_inclusions[metric_option] = True
                logging.info("Producing %s metrics", metric_option.value)
            else:
                self._metric_inclusions[metric_option] = False

    def expand(self, input_or_inputs: List[Any]) -> List[RecidivizMetric]:
        metrics = input_or_inputs | "Produce Metrics" >> beam.ParDo(
            ProduceMetrics(),
            self._pipeline_config,
            self._metric_inclusions,
            self._pipeline_options,
            self._calculation_end_month,
            self._calculation_month_count,
        )

        return metrics


@with_input_types(
    beam.typehints.Tuple[int, Dict[str, Iterable[Any]]],
    beam.typehints.Optional[BaseIdentifier],
)
@with_output_types(
    beam.typehints.Tuple[
        Optional[int],
        beam.typehints.Tuple[entities.StatePerson, List[IdentifierEvent]],
    ]
)
class ClassifyEvents(beam.DoFn):
    """Classifies an event according to multiple types of measurement."""

    # pylint: disable=arguments-differ
    def process(
        self, element: Tuple[int, Dict[str, Iterable[Any]]], identifier: BaseIdentifier
    ) -> Generator[
        Tuple[Optional[int], Tuple[entities.StatePerson, List[IdentifierEvent]]],
        None,
        None,
    ]:
        """Identifies various events relevant to calculations."""
        _, person_entities = element

        person, kwargs = person_and_kwargs_for_identifier(person_entities)

        events = identifier.find_events(person, kwargs)

        if not events:
            logging.info(
                "No valid events for person with id: %d. Excluding them from the calculations.",
                person.person_id,
            )
        else:
            yield person.person_id, (person, events)

    def to_runner_api_parameter(
        self, _unused_context: PipelineContext
    ) -> Tuple[str, Any]:
        pass


@attr.s
class BasePipeline(abc.ABC):
    """A base class defining a calculation pipeline."""

    pipeline_config: PipelineConfig = attr.ib()
    include_calculation_limit_args: bool = attr.ib(default=False)

    @abc.abstractmethod
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
        """Define the specific pipeline steps here and returns the last step of the pipeline as the output before writing metrics."""

    @property
    def _metric_type_values(self) -> List[str]:
        return [
            metric_type.value
            for metric_type in self.pipeline_config.metric_producer.metric_class.metric_type_cls
        ]

    @property
    def _metric_subclasses(self) -> Set[Type[RecidivizMetric]]:
        subclasses = set()
        path = [self.pipeline_config.metric_producer.metric_class]
        while path:
            parent = path.pop()
            for child in parent.__subclasses__():
                if child not in subclasses:
                    subclasses.add(child)
                    path.append(child)
        return subclasses

    def get_arg_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()

        add_shared_pipeline_arguments(parser, self.include_calculation_limit_args)

        metric_type_options: List[str] = self._metric_type_values
        metric_type_options.append("ALL")

        parser.add_argument(
            "--metric_types",
            dest="metric_types",
            type=str,
            nargs="+",
            choices=metric_type_options,
            help="A list of the types of metric to calculate.",
            default={"ALL"},
        )

        return parser

    def write_to_metrics(
        self, metrics_output_pipeline: beam.Pipeline, output: str
    ) -> None:
        """Takes the output from metrics_output_pipeline and writes it into a beam.pvalue.TaggedOutput
        dictionary-like object. This is then written to appropriate BigQuery tables under the appropriate
        Dataflow metrics table and namespace.

        Each metric type is a tag in the TaggedOutput and is accessed individually to be written to a
        separate table in BigQuery."""
        writable_metrics = (
            metrics_output_pipeline
            | "Convert to dict to be written to BQ"
            >> beam.ParDo(RecidivizMetricWritableDict()).with_outputs(
                *self._metric_type_values
            )
        )

        for metric_subclass in self._metric_subclasses:
            table_id = DATAFLOW_METRICS_TO_TABLES[metric_subclass]
            metric_type = DATAFLOW_TABLES_TO_METRIC_TYPES[table_id]
            _ = writable_metrics.__getattr__(
                metric_type.value
            ) | f"Write {metric_type.value} metrics to BQ table: {table_id}" >> WriteAppendToBigQuery(
                output_table=table_id, output_dataset=output
            )

    def run(
        self,
        apache_beam_pipeline_options: PipelineOptions,
        data_input: str,
        reference_view_input: str,
        static_reference_input: str,
        output: str,
        metric_types: List[str],
        state_code: str,
        person_filter_ids: Optional[List[int]],
        calculation_month_count: int = -1,
        calculation_end_month: Optional[str] = None,
    ) -> None:
        """Runs the designated pipeline."""

        # Workaround to load SQLAlchemy objects at start of pipeline. This is necessary because the BuildRootEntity
        # function tries to access attributes of relationship properties on the SQLAlchemy room_schema_class before they
        # have been loaded. However, if *any* SQLAlchemy objects have been instantiated, then the relationship properties
        # are loaded and their attributes can be successfully accessed.
        _ = schema.StatePerson()

        apache_beam_pipeline_options.view_as(SetupOptions).save_main_session = True

        # Get pipeline job details
        all_pipeline_options = apache_beam_pipeline_options.get_all_options()
        project_id = all_pipeline_options["project"]

        if project_id is None:
            raise ValueError(
                f"No project set in pipeline options: {all_pipeline_options}"
            )

        if state_code is None:
            raise ValueError("No state_code set for pipeline")

        input_dataset = project_id + "." + data_input
        reference_dataset = project_id + "." + reference_view_input
        static_reference_dataset = project_id + "." + static_reference_input

        person_id_filter_set = set(person_filter_ids) if person_filter_ids else None

        with beam.Pipeline(options=apache_beam_pipeline_options) as p:
            metrics = self.execute_pipeline(
                p,
                all_pipeline_options,
                state_code,
                input_dataset,
                reference_dataset,
                static_reference_dataset,
                metric_types,
                person_id_filter_set,
                calculation_month_count,
                calculation_end_month,
            )

            if person_id_filter_set:
                logging.warning(
                    "Non-empty person filter set - returning before writing metrics."
                )
                return

            self.write_to_metrics(metrics, output)
