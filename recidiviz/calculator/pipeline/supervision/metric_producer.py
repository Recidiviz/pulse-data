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
"""Produces SupervisionMetrics from SupervisionTimeBuckets.

This contains the core logic for calculating supervision metrics on a person-by-person
basis. It transforms SupervisionTimeBuckets into SupervisionMetrics.
"""
from operator import attrgetter
from typing import Any, Dict, List, Optional, Type

from recidiviz.calculator.pipeline.base_metric_producer import BaseMetricProducer
from recidiviz.calculator.pipeline.pipeline_type import PipelineType
from recidiviz.calculator.pipeline.supervision.events import (
    NonRevocationReturnSupervisionTimeBucket,
    ProjectedSupervisionCompletionBucket,
    SupervisionStartBucket,
    SupervisionTerminationBucket,
    SupervisionTimeBucket,
)
from recidiviz.calculator.pipeline.supervision.metrics import (
    SuccessfulSupervisionSentenceDaysServedMetric,
    SupervisionCaseComplianceMetric,
    SupervisionDowngradeMetric,
    SupervisionMetric,
    SupervisionMetricType,
    SupervisionOutOfStatePopulationMetric,
    SupervisionPopulationMetric,
    SupervisionStartMetric,
    SupervisionSuccessMetric,
    SupervisionTerminationMetric,
)
from recidiviz.calculator.pipeline.utils.calculator_utils import (
    build_metric,
    get_calculation_month_lower_bound_date,
    get_calculation_month_upper_bound_date,
    include_in_output,
)
from recidiviz.calculator.pipeline.utils.person_utils import PersonMetadata
from recidiviz.calculator.pipeline.utils.state_utils.state_calculation_config_manager import (
    supervision_period_is_out_of_state,
)
from recidiviz.persistence.entity.state.entities import StatePerson


class SupervisionMetricProducer(
    BaseMetricProducer[
        List[SupervisionTimeBucket], SupervisionMetricType, SupervisionMetric
    ]
):
    """Produces SupervisionMetrics from SupervisionTimeBuckets."""

    def __init__(self) -> None:
        # TODO(python/mypy#5374): Remove the ignore type when abstract class assignments are supported.
        self.metric_class = SupervisionMetric  # type: ignore
        self.event_to_metric_classes = {}
        self.event_to_metric_types = {
            NonRevocationReturnSupervisionTimeBucket: [
                SupervisionMetricType.SUPERVISION_COMPLIANCE,
                SupervisionMetricType.SUPERVISION_POPULATION,
                SupervisionMetricType.SUPERVISION_OUT_OF_STATE_POPULATION,
                SupervisionMetricType.SUPERVISION_DOWNGRADE,
            ],
            ProjectedSupervisionCompletionBucket: [
                SupervisionMetricType.SUPERVISION_SUCCESS,
                SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED,
            ],
            SupervisionStartBucket: [SupervisionMetricType.SUPERVISION_START],
            SupervisionTerminationBucket: [
                SupervisionMetricType.SUPERVISION_TERMINATION
            ],
        }
        self.metric_type_to_class: Dict[
            SupervisionMetricType, Type[SupervisionMetric]
        ] = {
            SupervisionMetricType.SUPERVISION_COMPLIANCE: SupervisionCaseComplianceMetric,
            SupervisionMetricType.SUPERVISION_DOWNGRADE: SupervisionDowngradeMetric,
            SupervisionMetricType.SUPERVISION_OUT_OF_STATE_POPULATION: SupervisionOutOfStatePopulationMetric,
            SupervisionMetricType.SUPERVISION_POPULATION: SupervisionPopulationMetric,
            SupervisionMetricType.SUPERVISION_START: SupervisionStartMetric,
            SupervisionMetricType.SUPERVISION_SUCCESS: SupervisionSuccessMetric,
            SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED: SuccessfulSupervisionSentenceDaysServedMetric,
            SupervisionMetricType.SUPERVISION_TERMINATION: SupervisionTerminationMetric,
        }

    def produce_metrics(
        self,
        person: StatePerson,
        identifier_events: List[SupervisionTimeBucket],
        metric_inclusions: Dict[SupervisionMetricType, bool],
        person_metadata: PersonMetadata,
        pipeline_type: PipelineType,
        pipeline_job_id: str,
        calculation_end_month: Optional[str] = None,
        calculation_month_count: int = -1,
    ) -> List[SupervisionMetric]:
        """Transforms SupervisionTimeBuckets and a StatePerson into SuperviisonMetrics.

        Takes in a StatePerson and all of her SupervisionTimeBuckets and returns an array
        of SupervisionMetrics.

        Args:
            person: the StatePerson
            supervision_time_buckets: A list of SupervisionTimeBuckets for the given StatePerson.
            metric_inclusions: A dictionary where the keys are each SupervisionMetricType, and the values are boolean
                    flags for whether or not to include that metric type in the calculations
            calculation_end_month: The year and month in YYYY-MM format of the last month for which metrics should be
                calculated. If unset, ends with the current month.
            calculation_month_count: The number of months (including the month of the calculation_end_month) to
                limit the monthly calculation output to. If set to -1, does not limit the calculations.
            person_metadata: Contains information about the StatePerson that is necessary for the metrics.
            pipeline_job_id: The job_id of the pipeline that is currently running.

        Returns:
            A list of SupervisionMetrics.
        """
        metrics: List[SupervisionMetric] = []

        identifier_events.sort(key=attrgetter("year", "month"))

        calculation_month_upper_bound = get_calculation_month_upper_bound_date(
            calculation_end_month
        )
        calculation_month_lower_bound = get_calculation_month_lower_bound_date(
            calculation_month_upper_bound, calculation_month_count
        )

        for supervision_time_bucket in identifier_events:
            event_date = supervision_time_bucket.event_date

            if (
                isinstance(
                    supervision_time_bucket,
                    NonRevocationReturnSupervisionTimeBucket,
                )
                and supervision_time_bucket.case_compliance
            ):
                event_date = supervision_time_bucket.case_compliance.date_of_evaluation

            event_year = event_date.year
            event_month = event_date.month

            if not include_in_output(
                event_year,
                event_month,
                calculation_month_upper_bound,
                calculation_month_lower_bound,
            ):
                continue

            applicable_metric_types = self.event_to_metric_types.get(
                type(supervision_time_bucket)
            )

            if not applicable_metric_types:
                raise ValueError(
                    "No metric types mapped to supervision_time_bucket of type {}".format(
                        type(supervision_time_bucket)
                    )
                )

            for metric_type in applicable_metric_types:
                if not metric_inclusions[metric_type]:
                    continue

                metric_class = self.metric_type_to_class.get(metric_type)

                if not metric_class:
                    raise ValueError(
                        "No metric class for metric type {}".format(metric_type)
                    )

                if self.include_event_in_metric(supervision_time_bucket, metric_type):
                    additional_attributes: Dict[str, Any] = {}

                    if (
                        isinstance(
                            supervision_time_bucket,
                            ProjectedSupervisionCompletionBucket,
                        )
                        and metric_type
                        == SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED
                    ):
                        additional_attributes[
                            "days_served"
                        ] = supervision_time_bucket.sentence_days_served

                    metric = build_metric(
                        pipeline=pipeline_type.value.lower(),
                        event=supervision_time_bucket,
                        metric_class=metric_class,
                        person=person,
                        event_date=event_date,
                        person_metadata=person_metadata,
                        pipeline_job_id=pipeline_job_id,
                        additional_attributes=additional_attributes,
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
        supervision_time_bucket: SupervisionTimeBucket,
        metric_type: SupervisionMetricType,
    ) -> bool:
        """Returns whether the given supervision_time_bucket should contribute to metrics of the given metric_type."""
        if metric_type == SupervisionMetricType.SUPERVISION_COMPLIANCE:
            return (
                isinstance(
                    supervision_time_bucket,
                    NonRevocationReturnSupervisionTimeBucket,
                )
                and supervision_time_bucket.case_compliance is not None
            )
        if metric_type == SupervisionMetricType.SUPERVISION_DOWNGRADE:
            return (
                isinstance(
                    supervision_time_bucket,
                    NonRevocationReturnSupervisionTimeBucket,
                )
                and supervision_time_bucket.supervision_level_downgrade_occurred
            )
        if metric_type == SupervisionMetricType.SUPERVISION_OUT_OF_STATE_POPULATION:
            return (
                isinstance(
                    supervision_time_bucket,
                    (NonRevocationReturnSupervisionTimeBucket,),
                )
                and supervision_period_is_out_of_state(supervision_time_bucket)
            )
        if metric_type == SupervisionMetricType.SUPERVISION_POPULATION:
            return (
                isinstance(
                    supervision_time_bucket,
                    (NonRevocationReturnSupervisionTimeBucket,),
                )
                and not supervision_period_is_out_of_state(supervision_time_bucket)
            )
        if (
            metric_type
            == SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED
        ):
            return (
                isinstance(
                    supervision_time_bucket, ProjectedSupervisionCompletionBucket
                )
                and supervision_time_bucket.successful_completion
                # Only include successful sentences where the person was not incarcerated during the sentence
                # in this metric
                and not supervision_time_bucket.incarcerated_during_sentence
                # Only include this event in this metric if there is a recorded number of days served
                and supervision_time_bucket.sentence_days_served is not None
            )
        if metric_type in (
            SupervisionMetricType.SUPERVISION_START,
            SupervisionMetricType.SUPERVISION_SUCCESS,
            SupervisionMetricType.SUPERVISION_TERMINATION,
        ):
            return True

        raise ValueError(f"Unhandled metric type {metric_type}")
