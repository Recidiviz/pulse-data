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
"""Calculates supervision metrics from supervision time buckets.

This contains the core logic for calculating supervision metrics on a person-by-person basis. It transforms
SupervisionTimeBuckets into supervision metrics, key-value pairs where the key represents all of the dimensions
represented in the data point, and the value represents an indicator of whether the person should contribute to that
metric.
"""
from operator import attrgetter
from typing import Dict, List, Tuple, Any, Optional, Type

from recidiviz.calculator.pipeline.supervision.supervision_time_bucket import \
    SupervisionTimeBucket, RevocationReturnSupervisionTimeBucket, ProjectedSupervisionCompletionBucket, \
    NonRevocationReturnSupervisionTimeBucket, SupervisionTerminationBucket, SupervisionStartBucket
from recidiviz.calculator.pipeline.utils.calculator_utils import \
    augmented_combo_for_calculations, include_in_output, \
    get_calculation_month_lower_bound_date, get_calculation_month_upper_bound_date, characteristics_dict_builder
from recidiviz.calculator.pipeline.supervision.metrics import \
    SupervisionMetricType, SupervisionSuccessMetric, SupervisionMetric, SupervisionPopulationMetric, \
    SupervisionRevocationMetric, SupervisionTerminationMetric, SupervisionCaseComplianceMetric, \
    SuccessfulSupervisionSentenceDaysServedMetric, SupervisionRevocationAnalysisMetric, SupervisionDowngradeMetric,\
    SupervisionStartMetric, SupervisionOutOfStatePopulationMetric
from recidiviz.calculator.pipeline.utils.person_utils import PersonMetadata
from recidiviz.calculator.pipeline.utils.state_utils.state_calculation_config_manager import \
    supervision_period_is_out_of_state
from recidiviz.persistence.entity.state.entities import StatePerson

BUCKET_TO_METRIC_TYPES: Dict[Type[SupervisionTimeBucket], List[SupervisionMetricType]] = {
    NonRevocationReturnSupervisionTimeBucket: [SupervisionMetricType.SUPERVISION_COMPLIANCE,
                                               SupervisionMetricType.SUPERVISION_POPULATION,
                                               SupervisionMetricType.SUPERVISION_OUT_OF_STATE_POPULATION,
                                               SupervisionMetricType.SUPERVISION_DOWNGRADE],
    ProjectedSupervisionCompletionBucket: [SupervisionMetricType.SUPERVISION_SUCCESS,
                                           SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED],
    RevocationReturnSupervisionTimeBucket: [SupervisionMetricType.SUPERVISION_POPULATION,
                                            SupervisionMetricType.SUPERVISION_OUT_OF_STATE_POPULATION,
                                            SupervisionMetricType.SUPERVISION_REVOCATION,
                                            SupervisionMetricType.SUPERVISION_REVOCATION_ANALYSIS],
    SupervisionStartBucket: [SupervisionMetricType.SUPERVISION_START],
    SupervisionTerminationBucket: [SupervisionMetricType.SUPERVISION_TERMINATION]
}

METRIC_TYPE_TO_CLASS: Dict[SupervisionMetricType, Type[SupervisionMetric]] = {
    SupervisionMetricType.SUPERVISION_COMPLIANCE: SupervisionCaseComplianceMetric,
    SupervisionMetricType.SUPERVISION_DOWNGRADE: SupervisionDowngradeMetric,
    SupervisionMetricType.SUPERVISION_OUT_OF_STATE_POPULATION: SupervisionOutOfStatePopulationMetric,
    SupervisionMetricType.SUPERVISION_POPULATION: SupervisionPopulationMetric,
    SupervisionMetricType.SUPERVISION_REVOCATION: SupervisionRevocationMetric,
    SupervisionMetricType.SUPERVISION_REVOCATION_ANALYSIS: SupervisionRevocationAnalysisMetric,
    SupervisionMetricType.SUPERVISION_START: SupervisionStartMetric,
    SupervisionMetricType.SUPERVISION_SUCCESS: SupervisionSuccessMetric,
    SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED:
        SuccessfulSupervisionSentenceDaysServedMetric,
    SupervisionMetricType.SUPERVISION_TERMINATION: SupervisionTerminationMetric,
}


def map_supervision_combinations(person: StatePerson,
                                 supervision_time_buckets: List[SupervisionTimeBucket],
                                 metric_inclusions: Dict[SupervisionMetricType, bool],
                                 calculation_end_month: Optional[str],
                                 calculation_month_count: int,
                                 person_metadata: PersonMetadata) -> List[Tuple[Dict[str, Any], Any]]:
    """Transforms SupervisionTimeBuckets and a StatePerson into metric combinations.

    Takes in a StatePerson and all of her SupervisionTimeBuckets and returns an array of "supervision combinations".
    These are key-value pairs where the key represents a specific metric and the value represents whether or not
    the person should be counted as a positive instance of that metric.

    This translates a particular time on supervision into many different supervision population metrics.

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
    Returns:
        A list of key-value tuples representing specific metric combinations and the value corresponding to that metric.
    """
    metrics: List[Tuple[Dict[str, Any], Any]] = []

    supervision_time_buckets.sort(key=attrgetter('year', 'month'))

    calculation_month_upper_bound = get_calculation_month_upper_bound_date(calculation_end_month)
    calculation_month_lower_bound = get_calculation_month_lower_bound_date(
        calculation_month_upper_bound, calculation_month_count)

    for supervision_time_bucket in supervision_time_buckets:
        event_date = supervision_time_bucket.event_date

        if (isinstance(supervision_time_bucket, NonRevocationReturnSupervisionTimeBucket)
                and supervision_time_bucket.case_compliance):
            event_date = supervision_time_bucket.case_compliance.date_of_evaluation

        event_year = event_date.year
        event_month = event_date.month

        if not include_in_output(event_year, event_month, calculation_month_upper_bound, calculation_month_lower_bound):
            continue

        applicable_metric_types = BUCKET_TO_METRIC_TYPES.get(type(supervision_time_bucket))

        if not applicable_metric_types:
            raise ValueError(
                'No metric types mapped to supervision_time_bucket of type {}'.format(type(supervision_time_bucket)))

        for metric_type in applicable_metric_types:
            if not metric_inclusions[metric_type]:
                continue

            metric_class = METRIC_TYPE_TO_CLASS.get(metric_type)

            if not metric_class:
                raise ValueError('No metric class for metric type {}'.format(metric_type))

            if include_event_in_metric(supervision_time_bucket, metric_type):
                characteristic_combo = characteristics_dict_builder(pipeline='supervision',
                                                                    event=supervision_time_bucket,
                                                                    metric_class=metric_class,
                                                                    person=person,
                                                                    event_date=event_date,
                                                                    person_metadata=person_metadata)

                metric_combo = augmented_combo_for_calculations(
                    characteristic_combo, supervision_time_bucket.state_code,
                    metric_type, event_year, event_month)

                value = value_for_metric_combo(supervision_time_bucket, metric_type)

                metrics.append((metric_combo, value))

    return metrics


def include_event_in_metric(supervision_time_bucket, metric_type):
    """Returns whether the given supervision_time_bucket should contribute to metrics of the given metric_type."""
    if metric_type == SupervisionMetricType.SUPERVISION_COMPLIANCE:
        return (isinstance(supervision_time_bucket, NonRevocationReturnSupervisionTimeBucket)
                and supervision_time_bucket.case_compliance)
    if metric_type == SupervisionMetricType.SUPERVISION_DOWNGRADE:
        return (isinstance(supervision_time_bucket, NonRevocationReturnSupervisionTimeBucket)
                and supervision_time_bucket.supervision_level_downgrade_occurred)
    if metric_type == SupervisionMetricType.SUPERVISION_OUT_OF_STATE_POPULATION:
        return (isinstance(supervision_time_bucket,
                           (NonRevocationReturnSupervisionTimeBucket, RevocationReturnSupervisionTimeBucket))
                and supervision_period_is_out_of_state(supervision_time_bucket))
    if metric_type == SupervisionMetricType.SUPERVISION_POPULATION:
        return (isinstance(supervision_time_bucket,
                           (NonRevocationReturnSupervisionTimeBucket, RevocationReturnSupervisionTimeBucket))
                and not supervision_period_is_out_of_state(supervision_time_bucket))
    if metric_type == SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED:
        return (isinstance(supervision_time_bucket, ProjectedSupervisionCompletionBucket)
                and supervision_time_bucket.successful_completion
                # Only include successful sentences where the person was not incarcerated during the sentence
                # in this metric
                and not supervision_time_bucket.incarcerated_during_sentence
                # Only include this event in this metric if there is a recorded number of days served
                and supervision_time_bucket.sentence_days_served is not None)
    if metric_type in (
            SupervisionMetricType.SUPERVISION_REVOCATION,
            SupervisionMetricType.SUPERVISION_REVOCATION_ANALYSIS,
            SupervisionMetricType.SUPERVISION_START,
            SupervisionMetricType.SUPERVISION_SUCCESS,
            SupervisionMetricType.SUPERVISION_TERMINATION
    ):
        return True

    raise ValueError(f"Unhandled metric type {metric_type}")


def value_for_metric_combo(
        supervision_time_bucket: SupervisionTimeBucket,
        metric_type: SupervisionMetricType) -> int:
    """Returns the value that should be associated with the metric of metric_type produced by this
     supervision_time_bucket.

        Args:
            supervision_time_bucket: The SupervisionTimeBucket from which the combination was derived
            metric_type: The type of metric being tracked by this combo

        Returns:
            An integer value that will be associated with this metric.
        """
    if isinstance(supervision_time_bucket, ProjectedSupervisionCompletionBucket):
        if metric_type == SupervisionMetricType.SUPERVISION_SUCCESS:
            # Set 1 for successful completion, 0 for unsuccessful completion
            return 1 if supervision_time_bucket.successful_completion else 0
        if metric_type == SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED:
            return supervision_time_bucket.sentence_days_served

        raise ValueError(f"Unsupported metric type {metric_type} for ProjectedSupervisionCompletionBucket.")

    # The default value for all combos is 1
    return 1
