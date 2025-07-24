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
"""Base class for a metric producer for a metric calculation pipeline."""

import abc
from typing import Dict, Generic, List, Set, Type, TypeVar

import attr

from recidiviz.persistence.entity.state.normalized_entities import NormalizedStatePerson
from recidiviz.pipelines.dataflow_config import (
    DATAFLOW_METRICS_TO_TABLES,
    DATAFLOW_TABLES_TO_METRIC_TYPES,
)
from recidiviz.pipelines.metrics.utils.calculator_utils import (
    produce_standard_event_metrics,
)
from recidiviz.pipelines.metrics.utils.metric_utils import (
    RecidivizMetric,
    RecidivizMetricType,
)
from recidiviz.pipelines.utils.identifier_models import IdentifierResult

IdentifierResultT = TypeVar("IdentifierResultT", bound=IdentifierResult)
IdentifierResultsT = TypeVar("IdentifierResultsT")
RecidivizMetricTypeT = TypeVar("RecidivizMetricTypeT", bound=RecidivizMetricType)
RecidivizMetricT = TypeVar("RecidivizMetricT", bound=RecidivizMetric)


@attr.s
class BaseMetricProducer(
    abc.ABC,
    Generic[
        IdentifierResultT, IdentifierResultsT, RecidivizMetricTypeT, RecidivizMetricT
    ],
):
    """Base class for a metric producer for a metric calculation pipeline."""

    metric_class: Type[RecidivizMetricT] = attr.ib()

    @property
    @abc.abstractmethod
    def result_class_to_metric_classes_mapping(
        self,
    ) -> Dict[Type[IdentifierResultT], List[Type[RecidivizMetricT]]]:
        """A mapping of the result class to the metric classes that are produced by that result."""

    def result_class_to_included_metric_classes(
        self,
        included_metric_types: Set[RecidivizMetricTypeT],
    ) -> Dict[Type[IdentifierResultT], List[Type[RecidivizMetricT]]]:
        return {
            result_class: [
                metric_class
                for metric_class in metric_classes
                if metric_type_for_metric_class(metric_class) in included_metric_types
            ]
            for result_class, metric_classes in self.result_class_to_metric_classes_mapping.items()
        }

    def included_result_classes(
        self, included_metric_types: Set[RecidivizMetricTypeT]
    ) -> Set[Type[IdentifierResultT]]:
        return {
            result_class
            for result_class, included_metric_classes in self.result_class_to_included_metric_classes(
                included_metric_types
            ).items()
            if len(included_metric_classes)
        }

    def produce_metrics(
        self,
        person: NormalizedStatePerson,
        identifier_results: IdentifierResultsT,
        metric_inclusions: Set[RecidivizMetricTypeT],
        pipeline_job_id: str,
        calculation_month_count: int = -1,
    ) -> List[RecidivizMetricT]:
        """Transforms the events and a NormalizedStatePerson into RecidivizMetrics.
        Args:
            person: the NormalizedStatePerson
            identifier_results: A list of IdentifierResults for the given NormalizedStatePerson.
            metric_inclusions: A dictionary where the keys are each Metric type and the values are boolean
                flags for whether or not to include that metric type in the calculations.
            pipeline_job_id: The job_id of the pipeline that is currently running.
            calculation_month_count: The number of months (including the month of the calculation_month_upper_bound) to
                limit the monthly calculation output to. If set to -1, does not limit the calculations.
        Returns:
            A list of RecidivizMetrics
        """
        metrics = produce_standard_event_metrics(
            person=person,
            identifier_results=identifier_results,  # type: ignore
            calculation_month_count=calculation_month_count,
            event_to_metric_classes=self.result_class_to_included_metric_classes(
                metric_inclusions
            ),  # type: ignore
            pipeline_job_id=pipeline_job_id,
        )

        metrics_of_class: List[RecidivizMetricT] = []

        for metric in metrics:
            if not isinstance(metric, self.metric_class):
                raise ValueError(
                    f"Unexpected metric type {type(metric)}."
                    f" All metrics should be of type {self.metric_class}."
                )
            metrics_of_class.append(metric)

        return metrics_of_class


# TODO(#28513): Delete this method once we can use the properties on the metrics.
def metric_type_for_metric_class(
    metric_class: Type[RecidivizMetric[RecidivizMetricTypeT]],
) -> RecidivizMetricTypeT:
    """Returns the RecidivizMetricType corresponding to the given RecidivizMetric class."""
    metric_table = DATAFLOW_METRICS_TO_TABLES[metric_class]
    metric_type = DATAFLOW_TABLES_TO_METRIC_TYPES[metric_table]
    if not isinstance(metric_type, metric_class.metric_type_cls):
        raise ValueError(
            f"Found incorrect metric type [{metric_type}], expected value of type "
            f"[{metric_class.metric_type_cls}]"
        )
    return metric_type
