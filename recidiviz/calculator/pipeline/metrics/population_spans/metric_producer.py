# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Calculates PopulationSpanMetrics from PopulationSpans.

This contains the core logic for calculating population span metrics on a person-by-person
basis. It transforms Spans into PopulationSpanMetrics."""
from typing import Dict, List, Optional, Sequence, Type

from recidiviz.calculator.pipeline.metrics.base_metric_producer import (
    BaseMetricProducer,
)
from recidiviz.calculator.pipeline.metrics.population_spans.metrics import (
    IncarcerationPopulationSpanMetric,
    PopulationSpanMetric,
    PopulationSpanMetricType,
)
from recidiviz.calculator.pipeline.metrics.population_spans.spans import (
    IncarcerationPopulationSpan,
)
from recidiviz.calculator.pipeline.metrics.utils.calculator_utils import (
    produce_standard_span_metrics,
)
from recidiviz.calculator.pipeline.metrics.utils.metric_utils import (
    PersonMetadata,
    RecidivizMetric,
)
from recidiviz.calculator.pipeline.utils.identifier_models import Span
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_incarceration_metrics_producer_delegate import (
    StateSpecificIncarcerationMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_metrics_producer_delegate import (
    StateSpecificMetricsProducerDelegate,
)
from recidiviz.persistence.entity.state.entities import StatePerson


class PopulationSpanMetricProducer(
    BaseMetricProducer[Sequence[Span], PopulationSpanMetricType, PopulationSpanMetric]
):
    """Calculates PopulationSpanMetrics from Spans."""

    def __init__(self) -> None:
        # TODO(python/mypy#5374): Remove the ignore type when abstract class assignments are supported.
        self.metric_class = PopulationSpanMetric  # type: ignore
        self.event_to_metric_classes = {
            IncarcerationPopulationSpan: [IncarcerationPopulationSpanMetric],
        }
        self.metrics_producer_delegate_classes = {
            IncarcerationPopulationSpanMetric: StateSpecificIncarcerationMetricsProducerDelegate
        }

    def produce_metrics(
        self,
        person: StatePerson,
        identifier_results: Sequence[Span],
        metric_inclusions: Dict[PopulationSpanMetricType, bool],
        person_metadata: PersonMetadata,
        pipeline_job_id: str,
        metrics_producer_delegates: Dict[str, StateSpecificMetricsProducerDelegate],
        calculation_end_month: Optional[str] = None,
        calculation_month_count: int = -1,
    ) -> List[PopulationSpanMetric]:
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
        Returns:
            A list of RecidivizMetrics
        """
        metric_classes_to_producer_delegates: Dict[
            Type[RecidivizMetric[PopulationSpanMetricType]],
            Optional[StateSpecificMetricsProducerDelegate],
        ] = {}
        for (
            metric_class,
            metric_producer_delegate_class,
        ) in self.metrics_producer_delegate_classes.items():
            metric_classes_to_producer_delegates[
                metric_class
            ] = metrics_producer_delegates.get(metric_producer_delegate_class.__name__)

        metrics = produce_standard_span_metrics(
            person=person,
            identifier_results=identifier_results,  # type: ignore
            metric_inclusions=metric_inclusions,
            person_metadata=person_metadata,
            event_to_metric_classes=self.event_to_metric_classes,  # type: ignore
            pipeline_job_id=pipeline_job_id,
            metric_classes_to_producer_delegates=metric_classes_to_producer_delegates,
        )

        metrics_of_class: List[PopulationSpanMetric] = []

        for metric in metrics:
            if not isinstance(metric, self.metric_class):
                raise ValueError(
                    f"Unexpected metric type {type(metric)}."
                    f" All metrics should be of type {self.metric_class}."
                )
            metrics_of_class.append(metric)

        return metrics_of_class
