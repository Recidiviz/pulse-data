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
"""Calculates IncarcerationMetrics from IncarcerationEvents.

This contains the core logic for calculating incarceration metrics on a person-by-person
basis. It transforms IncarcerationEvents into IncarcerationMetrics.
"""
from typing import Dict, List, Optional, Sequence

from recidiviz.calculator.pipeline.metrics.base_metric_producer import (
    BaseMetricProducer,
)
from recidiviz.calculator.pipeline.metrics.incarceration.events import (
    IncarcerationCommitmentFromSupervisionAdmissionEvent,
    IncarcerationEvent,
    IncarcerationReleaseEvent,
    IncarcerationStandardAdmissionEvent,
    IncarcerationStayEvent,
)
from recidiviz.calculator.pipeline.metrics.incarceration.metrics import (
    IncarcerationAdmissionMetric,
    IncarcerationCommitmentFromSupervisionMetric,
    IncarcerationMetric,
    IncarcerationMetricType,
    IncarcerationPopulationMetric,
    IncarcerationReleaseMetric,
)
from recidiviz.calculator.pipeline.metrics.utils.calculator_utils import (
    age_at_date,
    build_metric,
    get_calculation_month_lower_bound_date,
    get_calculation_month_upper_bound_date,
    include_in_output,
    metric_type_for_metric_class,
)
from recidiviz.calculator.pipeline.metrics.utils.metric_utils import PersonMetadata
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_incarceration_metrics_producer_delegate import (
    StateSpecificIncarcerationMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_metrics_producer_delegate import (
    StateSpecificMetricsProducerDelegate,
)
from recidiviz.persistence.entity.state.entities import StatePerson


class IncarcerationMetricProducer(
    BaseMetricProducer[
        Sequence[IncarcerationEvent], IncarcerationMetricType, IncarcerationMetric
    ]
):
    """Calculates IncarcerationMetrics from IncarcerationEvents."""

    def __init__(self) -> None:
        # TODO(python/mypy#5374): Remove the ignore type when abstract class assignments are supported.
        self.metric_class = IncarcerationMetric  # type: ignore
        self.event_to_metric_classes = {
            IncarcerationStandardAdmissionEvent: [IncarcerationAdmissionMetric],
            IncarcerationCommitmentFromSupervisionAdmissionEvent: [
                IncarcerationAdmissionMetric,
                IncarcerationCommitmentFromSupervisionMetric,
            ],
            IncarcerationStayEvent: [IncarcerationPopulationMetric],
            IncarcerationReleaseEvent: [IncarcerationReleaseMetric],
        }
        self.metrics_producer_delegate_classes = {
            IncarcerationMetric: StateSpecificIncarcerationMetricsProducerDelegate
        }

    # TODO(#15541) Remove customized logic once population metrics stop being populated.
    def produce_metrics(
        self,
        person: StatePerson,
        identifier_results: Sequence[IncarcerationEvent],
        metric_inclusions: Dict[IncarcerationMetricType, bool],
        person_metadata: PersonMetadata,
        pipeline_job_id: str,
        metrics_producer_delegates: Dict[str, StateSpecificMetricsProducerDelegate],
        calculation_end_month: Optional[str] = None,
        calculation_month_count: int = -1,
    ) -> List[IncarcerationMetric]:
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
        metrics: List[IncarcerationMetric] = []

        calculation_month_upper_bound = get_calculation_month_upper_bound_date(
            calculation_end_month
        )

        calculation_month_lower_bound = get_calculation_month_lower_bound_date(
            calculation_month_upper_bound, calculation_month_count
        )

        capped_calculation_month_lower_bound = (
            get_calculation_month_lower_bound_date(calculation_month_upper_bound, 240)
            if calculation_month_count == -1
            else calculation_month_lower_bound
        )

        for event in identifier_results:
            event_date = event.event_date
            event_year = event.event_date.year
            event_month = event.event_date.month

            if not include_in_output(
                event_year,
                event_month,
                calculation_month_upper_bound,
                calculation_month_lower_bound,
            ) or (
                isinstance(event, IncarcerationStayEvent)
                and not include_in_output(
                    event_year,
                    event_month,
                    calculation_month_upper_bound,
                    capped_calculation_month_lower_bound,
                )
            ):
                continue

            metric_classes = self.event_to_metric_classes[type(event)]

            for metric_class in metric_classes:
                metric_type = metric_type_for_metric_class(metric_class)

                if metric_inclusions.get(metric_type):
                    metric = build_metric(
                        result=event,
                        metric_class=metric_class,
                        person=person,
                        person_age=age_at_date(person, event_date),
                        person_metadata=person_metadata,
                        pipeline_job_id=pipeline_job_id,
                        additional_attributes={
                            "year": event_date.year,
                            "month": event_date.month,
                        },
                        metrics_producer_delegate=metrics_producer_delegate,
                    )

                    if not isinstance(metric, IncarcerationMetric):
                        raise ValueError(
                            f"Unexpected metric type {type(metric)}. "
                            "All metrics should be SupervisionMetric."
                        )

                    metrics.append(metric)

        return metrics
