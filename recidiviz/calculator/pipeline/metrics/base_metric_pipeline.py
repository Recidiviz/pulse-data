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
import logging
from typing import (
    Any,
    Dict,
    Generator,
    Iterable,
    List,
    Sequence,
    Set,
    Tuple,
    Type,
    Union,
)

import apache_beam as beam
from apache_beam.pvalue import AsList, PBegin
from apache_beam.typehints.decorators import with_input_types, with_output_types

from recidiviz.calculator.dataflow_config import (
    DATAFLOW_METRICS_TO_TABLES,
    DATAFLOW_TABLES_TO_METRIC_TYPES,
)
from recidiviz.calculator.pipeline.base_pipeline import (
    PipelineConfig,
    PipelineRunDelegate,
)
from recidiviz.calculator.pipeline.metrics.base_identifier import BaseIdentifier
from recidiviz.calculator.pipeline.metrics.base_metric_producer import (
    BaseMetricProducer,
)
from recidiviz.calculator.pipeline.metrics.pipeline_parameters import (
    MetricsPipelineParameters,
)
from recidiviz.calculator.pipeline.metrics.utils.metric_utils import (
    PersonMetadata,
    RecidivizMetric,
    RecidivizMetricType,
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
    get_job_id,
    person_and_kwargs_for_identifier,
)
from recidiviz.calculator.pipeline.utils.identifier_models import IdentifierResult
from recidiviz.calculator.pipeline.utils.state_utils.state_calculation_config_manager import (
    get_required_state_specific_delegates,
    get_required_state_specific_metrics_producer_delegates,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_delegate import (
    StateSpecificDelegate,
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


class MetricPipelineRunDelegate(PipelineRunDelegate[MetricsPipelineParameters]):
    """Delegate for running a metric pipeline."""

    @classmethod
    def parameters_type(cls) -> Type[MetricsPipelineParameters]:
        return MetricsPipelineParameters

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
            state_code=self.pipeline_parameters.state_code,
            identifier=self.identifier(),
            pipeline_config=self.pipeline_config(),
        )

        state_race_ethnicity_population_counts = (
            p
            | "Load state_race_ethnicity_population_counts"
            >> ImportTable(
                project_id=self.pipeline_parameters.project,
                dataset_id=self.pipeline_parameters.static_reference_input,
                table_id=STATE_RACE_ETHNICITY_POPULATION_TABLE_NAME,
                state_code_filter=self.pipeline_parameters.state_code,
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

        metrics = person_events_with_metadata | "Produce Metrics" >> beam.ParDo(
            ProduceMetrics(),
            project_id=self.pipeline_parameters.project,
            region=self.pipeline_parameters.region,
            job_name=self.pipeline_parameters.job_name,
            state_code=self.pipeline_parameters.state_code,
            metric_types_str=self.pipeline_parameters.metric_types,
            calculation_month_count=self.pipeline_parameters.calculation_month_count,
            metric_producer=self.metric_producer(),
        )

        # Return the metrics
        return metrics

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
                output_dataset=self.pipeline_parameters.output,
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
    beam.typehints.Optional[str],
    beam.typehints.Optional[str],
    beam.typehints.Optional[str],
    beam.typehints.Optional[str],
    beam.typehints.Optional[str],
    beam.typehints.Optional[int],
    beam.typehints.Optional[BaseMetricProducer],
)
@with_output_types(RecidivizMetric)
class ProduceMetrics(beam.DoFn):
    """A DoFn that produces metrics given a StatePerson, metadata and associated events."""

    # Silence `Method 'process_batch' is abstract in class 'DoFn' but is not overridden (abstract-method)`
    # pylint: disable=W0223

    # pylint: disable=arguments-differ
    def process(
        self,
        element: Tuple[
            entities.StatePerson,
            Union[Dict[int, IdentifierResult], List[IdentifierResult]],
            PersonMetadata,
        ],
        project_id: str,
        region: str,
        job_name: str,
        state_code: str,
        metric_types_str: str,
        calculation_month_count: int,
        metric_producer: BaseMetricProducer,
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
        person, results, person_metadata = element
        pipeline_job_id = job_id(
            project_id=project_id,
            region=region,
            job_name=job_name,
        )

        metrics_producer_delegates = (
            get_required_state_specific_metrics_producer_delegates(
                state_code,
                set(metric_producer.metrics_producer_delegate_classes.values()),
            )
        )

        metric_types = set(metric_types_str.split(" "))
        metric_inclusions: Dict[RecidivizMetricType, bool] = {}

        for metric_option in metric_producer.metric_class.metric_type_cls:
            if metric_option.value in metric_types or "ALL" in metric_types:
                metric_inclusions[metric_option] = True
                logging.info("Producing %s metrics", metric_option.value)
            else:
                metric_inclusions[metric_option] = False

        metrics = metric_producer.produce_metrics(
            person=person,
            identifier_results=results,
            metric_inclusions=metric_inclusions,
            person_metadata=person_metadata,
            pipeline_job_id=pipeline_job_id,
            calculation_month_count=calculation_month_count,
            metrics_producer_delegates=metrics_producer_delegates,
        )

        for metric in metrics:
            yield metric


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

    # Silence `Method 'process_batch' is abstract in class 'DoFn' but is not overridden (abstract-method)`
    # pylint: disable=W0223

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


@with_input_types(RecidivizMetric)
@with_output_types(beam.typehints.Dict[str, Any])
class RecidivizMetricWritableDict(beam.DoFn):
    """Builds a dictionary in the format necessary to write the output to BigQuery."""

    # Silence `Method 'process_batch' is abstract in class 'DoFn' but is not overridden (abstract-method)`
    # pylint: disable=W0223

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
