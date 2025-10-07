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
"""Produces SupervisionMetrics from SupervisionEvents.

This contains the core logic for calculating supervision metrics on a person-by-person
basis. It transforms SupervisionEvents into SupervisionMetrics.
"""
from operator import attrgetter
from typing import Dict, List, Set, Type

from recidiviz.persistence.entity.state.normalized_entities import NormalizedStatePerson
from recidiviz.pipelines.metrics.base_metric_producer import (
    BaseMetricProducer,
    metric_type_for_metric_class,
)
from recidiviz.pipelines.metrics.supervision.events import (
    SupervisionEvent,
    SupervisionPopulationEvent,
    SupervisionStartEvent,
    SupervisionTerminationEvent,
)
from recidiviz.pipelines.metrics.supervision.metrics import (
    SupervisionCaseComplianceMetric,
    SupervisionMetric,
    SupervisionMetricType,
    SupervisionOutOfStatePopulationMetric,
    SupervisionPopulationMetric,
    SupervisionStartMetric,
    SupervisionTerminationMetric,
)
from recidiviz.pipelines.metrics.utils.calculator_utils import (
    age_at_date,
    build_metric,
    get_calculation_month_lower_bound_date,
    get_calculation_month_upper_bound_date,
    include_in_output,
)


class SupervisionMetricProducer(
    BaseMetricProducer[
        SupervisionEvent,
        List[SupervisionEvent],
        SupervisionMetricType,
        SupervisionMetric,
    ]
):
    """Produces SupervisionMetrics from SupervisionEvents."""

    def __init__(self) -> None:
        # TODO(python/mypy#5374): Remove the ignore type when abstract class assignments are supported.
        self.metric_class = SupervisionMetric  # type: ignore

    @property
    def result_class_to_metric_classes_mapping(
        self,
    ) -> Dict[Type[SupervisionEvent], List[Type[SupervisionMetric]]]:
        return {
            SupervisionPopulationEvent: [
                SupervisionCaseComplianceMetric,
                SupervisionPopulationMetric,
                SupervisionOutOfStatePopulationMetric,
            ],
            SupervisionStartEvent: [SupervisionStartMetric],
            SupervisionTerminationEvent: [SupervisionTerminationMetric],
        }

    def produce_metrics(
        self,
        person: NormalizedStatePerson,
        identifier_results: List[SupervisionEvent],
        metric_inclusions: Set[SupervisionMetricType],
        pipeline_job_id: str,
        calculation_month_count: int = -1,
    ) -> List[SupervisionMetric]:
        """Transforms SupervisionEvents and a NormalizedStatePerson into SupervisionMetrics.

        Takes in a NormalizedStatePerson and all of their SupervisionEvents and returns an array
        of SupervisionMetrics.

        Args:
            person: the NormalizedStatePerson
            identifier_events: A list of SupervisionEvents for the given NormalizedStatePerson.
            metric_inclusions: A dictionary where the keys are each SupervisionMetricType, and the values are boolean
                    flags for whether or not to include that metric type in the calculations
            calculation_month_count: The number of months to limit the monthly calculation output to.
                    If set to -1, does not limit the calculations.
            pipeline_job_id: The job_id of the pipeline that is currently running.

        Returns:
            A list of SupervisionMetrics.
        """
        metrics: List[SupervisionMetric] = []

        identifier_results.sort(key=attrgetter("year", "month"))

        calculation_month_upper_bound = get_calculation_month_upper_bound_date()
        calculation_month_lower_bound = get_calculation_month_lower_bound_date(
            calculation_month_upper_bound, calculation_month_count
        )
        event_to_metric_classes = self.result_class_to_included_metric_classes(
            metric_inclusions
        )

        for event in identifier_results:
            event_date = event.event_date

            if (
                isinstance(
                    event,
                    SupervisionPopulationEvent,
                )
                and event.case_compliance
            ):
                event_date = event.case_compliance.date_of_evaluation

            event_year = event_date.year
            event_month = event_date.month

            if not include_in_output(
                event_year,
                event_month,
                calculation_month_upper_bound,
                calculation_month_lower_bound,
            ):
                continue

            metric_classes = event_to_metric_classes[type(event)]
            if not metric_classes:
                raise ValueError(
                    f"No included metric classes for event of type {type(event)}"
                )

            for metric_class in event_to_metric_classes[type(event)]:
                if self.include_event_in_metric(event, metric_class):
                    metric = build_metric(
                        result=event,
                        metric_class=metric_class,
                        person=person,
                        person_age=age_at_date(person, event_date),
                        pipeline_job_id=pipeline_job_id,
                        additional_attributes={
                            "year": event_date.year,
                            "month": event_date.month,
                        },
                    )

                    if not isinstance(metric, SupervisionMetric):
                        raise ValueError(
                            f"Unexpected metric type {type(metric)}. "
                            "All metrics should be SupervisionMetric."
                        )

                    metrics.append(metric)

        return metrics

    def include_event_in_metric(
        self,
        event: SupervisionEvent,
        metric_class: Type[SupervisionMetric],
    ) -> bool:
        """Returns whether the given event should contribute to metrics of the given metric_type."""
        metric_type = metric_type_for_metric_class(metric_class)
        if metric_type is SupervisionMetricType.SUPERVISION_COMPLIANCE:
            return (
                isinstance(
                    event,
                    SupervisionPopulationEvent,
                )
                and event.case_compliance is not None
            )
        if metric_type is SupervisionMetricType.SUPERVISION_OUT_OF_STATE_POPULATION:
            return (
                isinstance(
                    event,
                    SupervisionPopulationEvent,
                )
                and event.supervision_out_of_state
            )
        if metric_type is SupervisionMetricType.SUPERVISION_POPULATION:
            return (
                isinstance(
                    event,
                    SupervisionPopulationEvent,
                )
                and not event.supervision_out_of_state
            )
        if metric_type in (
            SupervisionMetricType.SUPERVISION_START,
            SupervisionMetricType.SUPERVISION_TERMINATION,
        ):
            return True

        raise ValueError(f"Unhandled metric type {metric_class.metric_type}")
