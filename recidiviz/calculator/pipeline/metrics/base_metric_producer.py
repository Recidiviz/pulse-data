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
from typing import Dict, Generic, List, Optional, Type, TypeVar

import attr

from recidiviz.calculator.pipeline.metrics.utils.calculator_utils import (
    produce_standard_event_metrics,
)
from recidiviz.calculator.pipeline.metrics.utils.metric_utils import (
    PersonMetadata,
    RecidivizMetric,
    RecidivizMetricType,
)
from recidiviz.calculator.pipeline.utils.identifier_models import IdentifierResult
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_metrics_producer_delegate import (
    StateSpecificMetricsProducerDelegate,
)
from recidiviz.persistence.entity.state.entities import StatePerson

IdentifierResultT = TypeVar("IdentifierResultT")
RecidivizMetricTypeT = TypeVar("RecidivizMetricTypeT", bound=RecidivizMetricType)
RecidivizMetricT = TypeVar("RecidivizMetricT", bound=RecidivizMetric)


@attr.s
class BaseMetricProducer(
    abc.ABC, Generic[IdentifierResultT, RecidivizMetricTypeT, RecidivizMetricT]
):
    """Base class for a metric producer for a metric calculation pipeline."""

    metric_class: Type[RecidivizMetricT] = attr.ib()
    event_to_metric_classes: Dict[
        Type[IdentifierResult], List[Type[RecidivizMetricT]]
    ] = attr.ib()
    metrics_producer_delegate_classes: Dict[
        Type[RecidivizMetricT], Type[StateSpecificMetricsProducerDelegate]
    ] = attr.ib()

    def produce_metrics(
        self,
        person: StatePerson,
        identifier_results: IdentifierResultT,
        metric_inclusions: Dict[RecidivizMetricTypeT, bool],
        person_metadata: PersonMetadata,
        pipeline_job_id: str,
        metrics_producer_delegates: Dict[str, StateSpecificMetricsProducerDelegate],
        calculation_end_month: Optional[str] = None,
        calculation_month_count: int = -1,
    ) -> List[RecidivizMetricT]:
        """Transforms the events and a StatePerson into RecidivizMetrics.
        Args:
            person: the StatePerson
            identifier_results: A list of IdentifierResults for the given StatePerson.
            metric_inclusions: A dictionary where the keys are each Metric type and the values are boolean
                flags for whether or not to include that metric type in the calculations.
            calculation_end_month: The year and month in YYYY-MM format of the last month for which metrics should be
                calculated. If unset, ends with the current month.
            calculation_month_count: The number of months (including the month of the calculation_month_upper_bound) to
                limit the monthly calculation output to. If set to -1, does not limit the calculations.
            person_metadata: Contains information about the StatePerson that is necessary for the metrics.
            pipeline_job_id: The job_id of the pipeline that is currently running.
            pipeline_name: The name of the pipeline producing these metrics
        Returns:
            A list of RecidivizMetrics
        """
        metrics_producer_delegate_class = self.metrics_producer_delegate_classes.get(
            self.metric_class
        )
        metrics_producer_delegate = (
            metrics_producer_delegates.get(metrics_producer_delegate_class.__name__)
            if metrics_producer_delegate_class
            else None
        )

        metrics = produce_standard_event_metrics(
            person=person,
            identifier_results=identifier_results,  # type: ignore
            metric_inclusions=metric_inclusions,
            calculation_end_month=calculation_end_month,
            calculation_month_count=calculation_month_count,
            person_metadata=person_metadata,
            event_to_metric_classes=self.event_to_metric_classes,  # type: ignore
            pipeline_job_id=pipeline_job_id,
            metrics_producer_delegate=metrics_producer_delegate,
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
