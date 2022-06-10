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
"""Classes for running all metric calculation pipelines."""
import abc
import argparse
import logging
from typing import (
    Any,
    Dict,
    Generator,
    Generic,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    Union,
)

import apache_beam as beam
import attr
from apache_beam.pvalue import AsList, PBegin
from apache_beam.runners.pipeline_context import PipelineContext
from apache_beam.typehints import with_input_types, with_output_types
from apache_beam.typehints.decorators import with_input_types, with_output_types

from recidiviz.calculator.dataflow_config import (
    DATAFLOW_METRICS_TO_TABLES,
    DATAFLOW_TABLES_TO_METRIC_TYPES,
)
from recidiviz.calculator.pipeline.base_pipeline import (
    PipelineConfig,
    PipelineJobArgs,
    PipelineRunDelegate,
)
from recidiviz.calculator.pipeline.metrics.base_identifier import BaseIdentifier
from recidiviz.calculator.pipeline.metrics.base_metric_producer import (
    BaseMetricProducer,
)
from recidiviz.calculator.pipeline.metrics.utils.metric_utils import (
    PersonMetadata,
    RecidivizMetric,
    RecidivizMetricType,
    RecidivizMetricTypeT,
    json_serializable_list_value_handler,
)
from recidiviz.calculator.pipeline.utils.beam_utils.bigquery_io_utils import (
    WriteToBigQuery,
    json_serializable_dict,
)
from recidiviz.calculator.pipeline.utils.beam_utils.extractor_utils import ImportTable
from recidiviz.calculator.pipeline.utils.beam_utils.person_utils import (
    PERSON_EVENTS_KEY,
    PERSON_METADATA_KEY,
    BuildPersonMetadata,
    ExtractPersonEventsMetadata,
)
from recidiviz.calculator.pipeline.utils.execution_utils import (
    TableRow,
    calculation_end_month_arg,
    calculation_month_count_arg,
    get_job_id,
    person_and_kwargs_for_identifier,
)
from recidiviz.calculator.pipeline.utils.identifier_models import IdentifierResult
from recidiviz.calculator.pipeline.utils.state_utils.state_calculation_config_manager import (
    get_required_state_specific_delegates,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_delegate import (
    StateSpecificDelegate,
)
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_DATASET,
    STATIC_REFERENCE_TABLES_DATASET,
)
from recidiviz.calculator.query.state.state_specific_query_strings import (
    STATE_RACE_ETHNICITY_POPULATION_TABLE_NAME,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state import entities
from recidiviz.utils import environment

# Cached job_id value
_job_id = None

ALL_MONTHS_IN_OUTPUT: int = -1


def job_id(project_id: str, region: str, job_name: str) -> str:
    global _job_id
    if not _job_id:
        _job_id = get_job_id(project_id, region, job_name)
    return _job_id


@environment.test_only
def clear_job_id() -> None:
    global _job_id
    _job_id = None


@attr.s(frozen=True)
class MetricPipelineJobArgs(Generic[RecidivizMetricTypeT], PipelineJobArgs):
    """Stores information about the metric calculation pipeline job being run."""

    # The name of the pipeline job
    job_name: str = attr.ib()

    # Which GCP region the job is being run in
    region: str = attr.ib()

    # Which dataset to query from for static reference tables
    static_reference_dataset: str = attr.ib()

    # Which metrics should included in the output of the pipeline
    metric_inclusions: Dict[RecidivizMetricTypeT, bool] = attr.ib()

    # How many months of output metrics should be written to BigQuery
    calculation_month_count: int = attr.ib()

    # An optional string in the format YYYY-MM specifying the last month for which
    # metrics should be calculated. If unset, defaults to the current month.
    calculation_end_month: Optional[str] = attr.ib()


class MetricPipelineRunDelegate(PipelineRunDelegate[MetricPipelineJobArgs]):
    """Delegate for running a metric pipeline."""

    @classmethod
    @abc.abstractmethod
    def pipeline_config(cls) -> PipelineConfig:
        pass

    @classmethod
    @abc.abstractmethod
    def identifier(cls) -> BaseIdentifier:
        """Returns the identifier for this pipeline."""

    @classmethod
    @abc.abstractmethod
    def metric_producer(cls) -> BaseMetricProducer:
        """Returns the metric producer for this pipeline."""

    @classmethod
    @abc.abstractmethod
    def include_calculation_limit_args(cls) -> bool:
        """Whether or not to include the args relevant to limiting calculation metric
        output to a specific set of months. Should be overwritten by subclasses."""

    @classmethod
    def default_output_dataset(cls, state_code: str) -> str:
        return DATAFLOW_METRICS_DATASET

    @classmethod
    def add_pipeline_job_args_to_parser(
        cls,
        parser: argparse.ArgumentParser,
    ) -> None:
        """Adds argument configs to the |parser| for the calculation pipeline args."""
        super().add_pipeline_job_args_to_parser(parser)

        parser.add_argument(
            "--static_reference_input",
            type=str,
            help="BigQuery static reference table dataset to query.",
            default=STATIC_REFERENCE_TABLES_DATASET,
        )

        metric_type_options: List[str] = cls._metric_type_values()
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

        if cls.include_calculation_limit_args():
            # Only for pipelines that may receive these arguments
            parser.add_argument(
                "--calculation_month_count",
                dest="calculation_month_count",
                type=calculation_month_count_arg,
                help="The number of months (including this one) to limit the monthly "
                "calculation output to. If set to -1, does not limit the "
                "calculations.",
                default=1,
            )

            parser.add_argument(
                "--calculation_end_month",
                dest="calculation_end_month",
                type=calculation_end_month_arg,
                help="The year and month, formatted in YYYY-MM, specifying the last month "
                "for which metrics should be calculated. If unset, defaults to the "
                "current month. Cannot be a month in the future.",
            )

    @classmethod
    def _build_pipeline_job_args(
        cls,
        parser: argparse.ArgumentParser,
        argv: List[str],
    ) -> MetricPipelineJobArgs:
        """Builds the MetricPipelineJobArgs object from the provided args."""
        base_pipeline_args = cls._get_base_pipeline_job_args(parser, argv)

        # Re-parse the args to get the ones relevant to the CalculationPipelineJobArgs
        (
            known_args,
            _,
        ) = parser.parse_known_args(argv)

        all_beam_options = (
            base_pipeline_args.apache_beam_pipeline_options.get_all_options()
        )

        static_reference_dataset = known_args.static_reference_input

        metric_types = set(known_args.metric_types)
        metric_inclusions: Dict[RecidivizMetricType, bool] = {}

        for metric_option in cls.metric_producer().metric_class.metric_type_cls:
            if metric_option.value in metric_types or "ALL" in metric_types:
                metric_inclusions[metric_option] = True
                logging.info("Producing %s metrics", metric_option.value)
            else:
                metric_inclusions[metric_option] = False

        calculation_end_month_value = (
            known_args.calculation_end_month
            if cls.include_calculation_limit_args()
            else None
        )
        calculation_end_month = (
            str(calculation_end_month_value) if calculation_end_month_value else None
        )

        calculation_month_count = int(
            known_args.calculation_month_count
            if cls.include_calculation_limit_args()
            else ALL_MONTHS_IN_OUTPUT
        )

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

        return MetricPipelineJobArgs(
            state_code=base_pipeline_args.state_code,
            project_id=base_pipeline_args.project_id,
            input_dataset=base_pipeline_args.input_dataset,
            normalized_input_dataset=base_pipeline_args.normalized_input_dataset,
            reference_dataset=base_pipeline_args.reference_dataset,
            output_dataset=base_pipeline_args.output_dataset,
            apache_beam_pipeline_options=base_pipeline_args.apache_beam_pipeline_options,
            static_reference_dataset=static_reference_dataset,
            person_id_filter_set=base_pipeline_args.person_id_filter_set,
            metric_inclusions=metric_inclusions,
            region=str(all_beam_options["region"]),
            job_name=str(all_beam_options["job_name"]),
            calculation_end_month=calculation_end_month,
            calculation_month_count=calculation_month_count,
        )

    def _validate_pipeline_config(self) -> None:
        """Validates the contents of the PipelineConfig."""
        # All of these entities are required for all metric calculation pipelines
        default_entities: Set[Type[Entity]] = {
            entities.StatePerson,
            entities.StatePersonRace,
            entities.StatePersonEthnicity,
        }

        if self.pipeline_config().required_entities:
            missing_default_entities = default_entities.difference(
                set(self.pipeline_config().required_entities)
            )
            if missing_default_entities:
                raise ValueError(
                    "PipelineConfig.required_entities must include the "
                    f"following default entity types: ["
                    f"{default_entities}]. Missing: {missing_default_entities}"
                )

    def run_data_transforms(
        self, p: PBegin, pipeline_data: beam.Pipeline
    ) -> beam.Pipeline:
        """Runs the data transforms of a metric calculation pipeline. First, classifies the
        events relevant to the type of pipeline running. Then, converts those events
        into metrics. Returns thee metrics to be written to BigQuery."""
        person_events = pipeline_data | "Get Events" >> beam.ParDo(
            ClassifyResults(),
            state_code=self.pipeline_job_args.state_code,
            identifier=self.identifier(),
            pipeline_config=self.pipeline_config(),
        )

        state_race_ethnicity_population_counts = (
            p
            | "Load state_race_ethnicity_population_counts"
            >> ImportTable(
                project_id=self.pipeline_job_args.project_id,
                dataset_id=self.pipeline_job_args.static_reference_dataset,
                table_id=STATE_RACE_ETHNICITY_POPULATION_TABLE_NAME,
                state_code_filter=self.pipeline_job_args.state_code,
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

        person_events_with_metadata = (
            {PERSON_EVENTS_KEY: person_events, PERSON_METADATA_KEY: person_metadata}
            | "Group events with person-level metadata" >> beam.CoGroupByKey()
            | "Organize StatePerson, PersonMetadata and events for calculations"
            >> beam.ParDo(ExtractPersonEventsMetadata())
        )

        # Return the metrics
        return person_events_with_metadata | "Get Metrics" >> GetMetrics(
            pipeline_job_args=self.pipeline_job_args,
            metric_producer=self.metric_producer(),
            pipeline_name=self.pipeline_config().pipeline_name,
        )

    def write_output(self, pipeline: beam.Pipeline) -> None:
        """Takes the output from the pipeline and writes it into a
        beam.pvalue.TaggedOutput dictionary-like object. This is then written to
        appropriate BigQuery tables under the appropriate Dataflow metrics table and
        namespace.

        Each metric type is a tag in the TaggedOutput and is accessed individually to
        be written to a separate table in BigQuery."""
        writable_metrics = (
            pipeline
            | "Convert to dict to be written to BQ"
            >> beam.ParDo(RecidivizMetricWritableDict()).with_outputs(
                *self._metric_type_values()
            )
        )

        for metric_subclass in self._metric_subclasses:
            table_id = DATAFLOW_METRICS_TO_TABLES[metric_subclass]
            metric_type = DATAFLOW_TABLES_TO_METRIC_TYPES[table_id]
            _ = getattr(
                writable_metrics, metric_type.value
            ) | f"Write {metric_type.value} metrics to BQ table: {table_id}" >> WriteToBigQuery(
                output_table=table_id,
                output_dataset=self.pipeline_job_args.output_dataset,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )

    @classmethod
    def _metric_type_values(cls) -> List[str]:
        return [
            metric_type.value
            for metric_type in cls.metric_producer().metric_class.metric_type_cls
        ]

    @property
    def _metric_subclasses(self) -> Set[Type[RecidivizMetric]]:
        subclasses = set()
        path = [self.metric_producer().metric_class]
        while path:
            parent = path.pop()
            for child in parent.__subclasses__():
                if child not in subclasses:
                    subclasses.add(child)
                    path.append(child)
        return subclasses


@with_input_types(
    beam.typehints.Tuple[
        entities.StatePerson,
        Union[Dict[int, IdentifierResult], List[IdentifierResult]],
        PersonMetadata,
    ],
    beam.typehints.Optional[MetricPipelineJobArgs],
    beam.typehints.Optional[BaseMetricProducer],
    beam.typehints.Optional[str],
)
@with_output_types(RecidivizMetric)
class ProduceMetrics(beam.DoFn):
    """A DoFn that produces metrics given a StatePerson, metadata and associated events."""

    # pylint: disable=arguments-differ
    def process(
        self,
        element: Tuple[
            entities.StatePerson,
            Union[Dict[int, IdentifierResult], List[IdentifierResult]],
            PersonMetadata,
        ],
        pipeline_job_args: MetricPipelineJobArgs,
        metric_producer: BaseMetricProducer,
        pipeline_name: str,
    ) -> Generator[RecidivizMetric, None, None]:
        """Produces various metrics.
        Sends the metric_producer the StatePerson entity and their corresponding events for mapping all metrics.
        Args:
            element: Dictionary containing the person, events, and person_metadata
            pipeline_job_args: Object storing information about the calculation
                pipeline job currently running
            metric_producer: The metric producer to call to produce metrics
            pipeline_name: The name of pipeline being run

        Yields:
            Each metric."""
        person, events, person_metadata = element

        pipeline_job_id = job_id(
            project_id=pipeline_job_args.project_id,
            region=pipeline_job_args.region,
            job_name=pipeline_job_args.job_name,
        )

        metrics = metric_producer.produce_metrics(
            person=person,
            identifier_events=events,
            metric_inclusions=pipeline_job_args.metric_inclusions,
            person_metadata=person_metadata,
            pipeline_name=pipeline_name,
            pipeline_job_id=pipeline_job_id,
            calculation_end_month=pipeline_job_args.calculation_end_month,
            calculation_month_count=pipeline_job_args.calculation_month_count,
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
        Union[Dict[int, IdentifierResult], List[IdentifierResult]],
        PersonMetadata,
    ],
)
@with_output_types(RecidivizMetric)
class GetMetrics(beam.PTransform):
    """A PTransform that transforms a StatePerson and their corresponding events to
    corresponding metrics."""

    def __init__(
        self,
        pipeline_job_args: MetricPipelineJobArgs,
        metric_producer: BaseMetricProducer,
        pipeline_name: str,
    ) -> None:
        super().__init__()
        self._pipeline_job_args = pipeline_job_args
        self._metric_producer = metric_producer
        self._pipeline_name = pipeline_name

    def expand(self, input_or_inputs: List[Any]) -> List[RecidivizMetric]:
        metrics = input_or_inputs | "Produce Metrics" >> beam.ParDo(
            ProduceMetrics(),
            self._pipeline_job_args,
            self._metric_producer,
            self._pipeline_name,
        )

        return metrics


@with_input_types(
    beam.typehints.Tuple[int, Dict[str, Iterable[Any]]],
    str,
    BaseIdentifier,
    PipelineConfig,
)
@with_output_types(
    beam.typehints.Tuple[
        int,
        beam.typehints.Tuple[entities.StatePerson, List[IdentifierResult]],
    ]
)
class ClassifyResults(beam.DoFn):
    """Classifies a result according to multiple types of measurement."""

    # pylint: disable=arguments-differ
    def process(
        self,
        element: Tuple[int, Dict[str, Iterable[Any]]],
        state_code: str,
        identifier: BaseIdentifier,
        pipeline_config: PipelineConfig,
    ) -> Generator[
        Tuple[int, Tuple[entities.StatePerson, List[IdentifierResult]]],
        None,
        None,
    ]:
        """Identifies various events or spans relevant to calculations."""
        _, person_entities = element

        person, entity_kwargs = person_and_kwargs_for_identifier(person_entities)

        required_delegates = get_required_state_specific_delegates(
            state_code=state_code,
            required_delegates=pipeline_config.state_specific_required_delegates,
            entity_kwargs=entity_kwargs,
        )

        all_kwargs: Dict[
            str, Union[Sequence[Entity], List[TableRow], StateSpecificDelegate]
        ] = {
            **entity_kwargs,
            **required_delegates,
        }

        results = identifier.identify(person, all_kwargs)

        if results:
            person_id = person.person_id
            if person_id is None:
                raise ValueError("Found unexpected null person_id.")
            yield person_id, (person, results)

    def to_runner_api_parameter(
        self, _unused_context: PipelineContext
    ) -> Tuple[str, Any]:
        pass


@with_input_types(RecidivizMetric)
@with_output_types(beam.typehints.Dict[str, Any])
class RecidivizMetricWritableDict(beam.DoFn):
    """Builds a dictionary in the format necessary to write the output to BigQuery."""

    # pylint: disable=arguments-differ
    def process(
        self, element: RecidivizMetric
    ) -> Generator[Dict[str, Any], None, None]:
        """The beam.io.WriteToBigQuery transform requires elements to be in dictionary
        form, where the values are in formats as required by BigQuery I/O connector.

        For a list of required formats, see the "Data types" section of:
            https://beam.apache.org/documentation/io/built-in/google-bigquery/

        Args:
            element: A RecidivizMetric

        Yields:
            A dictionary representation of the RecidivizMetric in the format
                Dict[str, Any] so that it can be written to BigQuery using
                beam.io.WriteToBigQuery.
        """
        element_dict = json_serializable_dict(
            element.__dict__, json_serializable_list_value_handler
        )

        if isinstance(element, RecidivizMetric):
            yield beam.pvalue.TaggedOutput(element.metric_type.value, element_dict)
        else:
            raise ValueError(
                "Attempting to convert an object that is not a RecidivizMetric into a "
                "writable dict for BigQuery."
            )

    def to_runner_api_parameter(
        self, _unused_context: PipelineContext
    ) -> Tuple[str, Any]:
        pass
