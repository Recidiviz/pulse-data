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
from typing import Any, Dict, Generator, Iterable, List, Set, Tuple, Type, Union

import apache_beam as beam
from apache_beam import Pipeline
from apache_beam.typehints.decorators import with_input_types, with_output_types

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_query_provider import StateFilteredQueryProvider
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.serialization import json_serializable_dict
from recidiviz.persistence.entity.state.normalized_entities import NormalizedStatePerson
from recidiviz.pipelines.base_pipeline import BasePipeline
from recidiviz.pipelines.dataflow_config import (
    DATAFLOW_METRICS_TO_TABLES,
    DATAFLOW_TABLES_TO_METRIC_TYPES,
)
from recidiviz.pipelines.metrics.base_identifier import BaseIdentifier
from recidiviz.pipelines.metrics.base_metric_producer import BaseMetricProducer
from recidiviz.pipelines.metrics.pipeline_parameters import MetricsPipelineParameters
from recidiviz.pipelines.metrics.utils.metric_utils import (
    RecidivizMetric,
    RecidivizMetricType,
    json_serializable_list_value_handler,
)
from recidiviz.pipelines.utils.beam_utils.bigquery_io_utils import WriteToBigQuery
from recidiviz.pipelines.utils.beam_utils.extractor_utils import (
    ExtractRootEntityDataForPipeline,
)
from recidiviz.pipelines.utils.execution_utils import (
    get_job_id,
    person_and_kwargs_for_identifier,
)
from recidiviz.pipelines.utils.identifier_models import IdentifierResult
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


class MetricPipeline(
    BasePipeline[MetricsPipelineParameters],
):
    """Delegate for running a metric pipeline."""

    @classmethod
    def parameters_type(cls) -> Type[MetricsPipelineParameters]:
        return MetricsPipelineParameters

    @classmethod
    @abc.abstractmethod
    def required_entities(cls) -> List[Type[Entity]]:
        """Returns the required entities for this pipeline."""

    @classmethod
    def all_input_reference_query_providers(
        cls, state_code: StateCode, address_overrides: BigQueryAddressOverrides | None
    ) -> Dict[str, StateFilteredQueryProvider]:
        return {}

    @classmethod
    @abc.abstractmethod
    def identifier(cls, state_code: StateCode) -> BaseIdentifier:
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

    def run_pipeline(self, p: Pipeline) -> None:
        pipeline_parameters = self.pipeline_parameters
        state_code = StateCode(pipeline_parameters.state_code.upper())
        person_id_filter_set = (
            {
                int(person_id)
                for person_id in pipeline_parameters.person_filter_ids.split(" ")
            }
            if pipeline_parameters.person_filter_ids
            else None
        )

        metric_types = self.parse_metric_types(self.pipeline_parameters.metric_types)

        pipeline_data = (
            p
            | "Load required person-level data"
            >> ExtractRootEntityDataForPipeline(
                state_code=state_code,
                project_id=self.pipeline_parameters.project,
                entities_dataset=self.pipeline_parameters.normalized_input,
                required_entity_classes=self.required_entities(),
                reference_data_queries_by_name={},
                root_entity_cls=NormalizedStatePerson,
                root_entity_id_filter_set=person_id_filter_set,
                resource_labels=self.pipeline_parameters.resource_labels,
            )
        )

        person_events = pipeline_data | "Get Events" >> beam.ParDo(
            ClassifyResults(),
            identifier=self.identifier(state_code),
            included_result_classes=self.metric_producer().included_result_classes(
                metric_types
            ),
        )

        metrics = (
            person_events
            | "Produce Metrics"
            >> beam.ParDo(
                ProduceMetrics(),
                project_id=self.pipeline_parameters.project,
                region=self.pipeline_parameters.region,
                job_name=self.pipeline_parameters.job_name,
                metric_types=metric_types,
                calculation_month_count=self.pipeline_parameters.calculation_month_count,
                metric_producer=self.metric_producer(),
            )
            | "Convert to dict to be written to BQ"
            >> beam.ParDo(RecidivizMetricWritableDict()).with_outputs(
                *self._metric_type_values()
            )
        )

        for metric_subclass in self._metric_subclasses:
            table_id = DATAFLOW_METRICS_TO_TABLES[metric_subclass]
            metric_type = DATAFLOW_TABLES_TO_METRIC_TYPES[table_id]
            if metric_type in metric_types:
                _ = getattr(
                    metrics, metric_type.value
                ) | f"Write {metric_type.value} metrics to BQ table: {table_id}" >> WriteToBigQuery(
                    output_table=table_id,
                    output_dataset=self.pipeline_parameters.output,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                )

    @classmethod
    def parse_metric_types(cls, metric_types_str: str) -> Set[RecidivizMetricType]:
        metric_types = set(metric_types_str.split(" "))
        metric_inclusions: Set[RecidivizMetricType] = set()

        for metric_option in cls.metric_producer().metric_class.metric_type_cls:
            if metric_option.value in metric_types or "ALL" in metric_types:
                metric_inclusions.add(metric_option)

        return metric_inclusions

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
        NormalizedStatePerson,
        Union[Dict[int, Iterable[IdentifierResult]], Iterable[IdentifierResult]],
    ],
    beam.typehints.Optional[str],
    beam.typehints.Optional[str],
    beam.typehints.Optional[str],
    beam.typehints.Optional[Set[RecidivizMetricType]],
    beam.typehints.Optional[int],
    beam.typehints.Optional[BaseMetricProducer],
)
@with_output_types(RecidivizMetric)
class ProduceMetrics(beam.DoFn):
    """A DoFn that produces metrics given a NormalizedStatePerson, metadata and associated events."""

    # Silence `Method 'process_batch' is abstract in class 'DoFn' but is not overridden (abstract-method)`
    # pylint: disable=W0223

    # pylint: disable=arguments-differ
    def process(
        self,
        element: Tuple[
            NormalizedStatePerson,
            Union[Dict[int, Iterable[IdentifierResult]], Iterable[IdentifierResult]],
        ],
        project_id: str,
        region: str,
        job_name: str,
        metric_types: Set[RecidivizMetricType],
        calculation_month_count: int,
        metric_producer: BaseMetricProducer,
    ) -> Generator[RecidivizMetric, None, None]:
        """Produces various metrics.
        Sends the metric_producer the NormalizedStatePerson entity and their corresponding events for mapping all metrics.
        Args:
            element: Dictionary containing the person and events
            pipeline_job_args: Object storing information about the calculation
                pipeline job currently running
            metric_producer: The metric producer to call to produce metrics
            pipeline_name: The name of pipeline being run

        Yields:
            Each metric."""
        person, results = element
        pipeline_job_id = job_id(
            project_id=project_id,
            region=region,
            job_name=job_name,
        )

        metrics = metric_producer.produce_metrics(
            person=person,
            identifier_results=results,
            metric_inclusions=metric_types,
            pipeline_job_id=pipeline_job_id,
            calculation_month_count=calculation_month_count,
        )
        yield from metrics


@with_input_types(
    beam.typehints.Tuple[int, Dict[str, Iterable[Any]]],
    BaseIdentifier,
    Set[Type[IdentifierResult]],
)
@with_output_types(
    beam.typehints.Tuple[
        NormalizedStatePerson,
        Union[Dict[int, Iterable[IdentifierResult]], Iterable[IdentifierResult]],
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
        identifier: BaseIdentifier,
        included_result_classes: Set[Type[IdentifierResult]],
    ) -> Generator[
        Tuple[
            NormalizedStatePerson,
            Union[Dict[int, Iterable[IdentifierResult]], Iterable[IdentifierResult]],
        ],
        None,
        None,
    ]:
        """Identifies various events or spans relevant to calculations."""
        _, person_entities = element

        person, all_kwargs = person_and_kwargs_for_identifier(person_entities)

        results = identifier.identify(
            person,
            identifier_context=all_kwargs,
            included_result_classes=included_result_classes,
        )

        if results:
            yield person, results


@with_input_types(RecidivizMetric)
@with_output_types(beam.typehints.Dict[str, Any])
class RecidivizMetricWritableDict(beam.DoFn):
    """Builds a dictionary in the format necessary to write the output to BigQuery."""

    # Silence `Method 'process_batch' is abstract in class 'DoFn' but is not overridden (abstract-method)`
    # pylint: disable=W0223

    # pylint: disable=arguments-differ
    def process(
        self, element: RecidivizMetric
    ) -> Generator[beam.pvalue.TaggedOutput, None, None]:
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
