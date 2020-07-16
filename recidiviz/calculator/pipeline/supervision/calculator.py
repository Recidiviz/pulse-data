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
from collections import defaultdict
from datetime import date
from operator import attrgetter
from typing import Dict, List, Tuple, Any, Optional

from recidiviz.calculator.pipeline.supervision.supervision_time_bucket import \
    SupervisionTimeBucket, RevocationReturnSupervisionTimeBucket, ProjectedSupervisionCompletionBucket, \
    NonRevocationReturnSupervisionTimeBucket, SupervisionTerminationBucket
from recidiviz.calculator.pipeline.utils.calculator_utils import \
    augmented_combo_for_calculations, relevant_metric_periods, \
    augment_combination, include_in_historical_metrics, \
    get_calculation_month_lower_bound_date, characteristics_with_person_id_fields, add_demographic_characteristics, \
    get_calculation_month_upper_bound_date
from recidiviz.calculator.pipeline.utils.assessment_utils import \
    assessment_score_bucket, include_assessment_in_metric
from recidiviz.calculator.pipeline.supervision.metrics import \
    SupervisionMetricType
from recidiviz.calculator.pipeline.utils.metric_utils import \
    MetricMethodologyType
from recidiviz.calculator.pipeline.utils.state_utils.state_calculation_config_manager import \
    supervision_types_distinct_for_state
from recidiviz.persistence.entity.state.entities import StatePerson


def map_supervision_combinations(person: StatePerson,
                                 supervision_time_buckets: List[SupervisionTimeBucket],
                                 metric_inclusions: Dict[SupervisionMetricType, bool],
                                 calculation_end_month: Optional[str],
                                 calculation_month_count: int) -> List[Tuple[Dict[str, Any], Any]]:
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
    Returns:
        A list of key-value tuples representing specific metric combinations and the value corresponding to that metric.
    """
    metrics: List[Tuple[Dict[str, Any], Any]] = []
    periods_and_buckets: Dict[int, List[SupervisionTimeBucket]] = defaultdict()

    supervision_time_buckets.sort(key=attrgetter('year', 'month'))

    calculation_month_upper_bound = get_calculation_month_upper_bound_date(calculation_end_month)

    # If the calculations include the current month, then we will calculate person-based metrics for each metric
    # period in METRIC_PERIOD_MONTHS ending with the current month
    include_metric_period_output = calculation_month_upper_bound == get_calculation_month_upper_bound_date(
        date.today().strftime('%Y-%m'))

    if include_metric_period_output:
        periods_and_buckets = _classify_buckets_by_relevant_metric_periods(supervision_time_buckets,
                                                                           calculation_month_upper_bound)

    calculation_month_lower_bound = get_calculation_month_lower_bound_date(
        calculation_month_upper_bound, calculation_month_count)

    for supervision_time_bucket in supervision_time_buckets:
        if isinstance(supervision_time_bucket, ProjectedSupervisionCompletionBucket):
            if metric_inclusions.get(SupervisionMetricType.SUCCESS):
                characteristic_combo_success = characteristics_dict(
                    person, supervision_time_bucket, SupervisionMetricType.SUCCESS)

                supervision_success_metrics = map_metric_combinations(
                    characteristic_combo_success, supervision_time_bucket,
                    calculation_month_upper_bound, calculation_month_lower_bound,
                    supervision_time_buckets, periods_and_buckets,
                    SupervisionMetricType.SUCCESS, include_metric_period_output)

                metrics.extend(supervision_success_metrics)

            if metric_inclusions.get(SupervisionMetricType.SUCCESSFUL_SENTENCE_DAYS_SERVED) \
                    and supervision_time_bucket.successful_completion \
                    and not supervision_time_bucket.incarcerated_during_sentence:
                # Only include successful sentences where the person was not incarcerated during the sentence in this
                # metric
                characteristic_combo_successful_sentence_length = characteristics_dict(
                    person, supervision_time_bucket, SupervisionMetricType.SUCCESSFUL_SENTENCE_DAYS_SERVED
                )

                successful_sentence_length_metrics = map_metric_combinations(
                    characteristic_combo_successful_sentence_length, supervision_time_bucket,
                    calculation_month_upper_bound, calculation_month_lower_bound,
                    supervision_time_buckets, periods_and_buckets,
                    SupervisionMetricType.SUCCESSFUL_SENTENCE_DAYS_SERVED, include_metric_period_output)

                metrics.extend(successful_sentence_length_metrics)

        elif isinstance(supervision_time_bucket, SupervisionTerminationBucket):
            if metric_inclusions.get(SupervisionMetricType.ASSESSMENT_CHANGE):
                characteristic_combo_assessment = characteristics_dict(
                    person, supervision_time_bucket, SupervisionMetricType.ASSESSMENT_CHANGE)

                assessment_change_metrics = map_metric_combinations(
                    characteristic_combo_assessment, supervision_time_bucket,
                    calculation_month_upper_bound, calculation_month_lower_bound,
                    supervision_time_buckets, periods_and_buckets,
                    SupervisionMetricType.ASSESSMENT_CHANGE, include_metric_period_output)

                metrics.extend(assessment_change_metrics)
        elif isinstance(supervision_time_bucket,
                        (NonRevocationReturnSupervisionTimeBucket, RevocationReturnSupervisionTimeBucket)):
            if metric_inclusions.get(SupervisionMetricType.POPULATION):
                characteristic_combo_population = characteristics_dict(
                    person, supervision_time_bucket, SupervisionMetricType.POPULATION)

                population_metrics = map_metric_combinations(
                    characteristic_combo_population, supervision_time_bucket,
                    calculation_month_upper_bound, calculation_month_lower_bound,
                    supervision_time_buckets, periods_and_buckets,
                    SupervisionMetricType.POPULATION, include_metric_period_output)

                metrics.extend(population_metrics)

            if (metric_inclusions.get(SupervisionMetricType.COMPLIANCE)
                    and isinstance(supervision_time_bucket, NonRevocationReturnSupervisionTimeBucket)
                    and supervision_time_bucket.case_compliance is not None):
                characteristic_combo_compliance = characteristics_dict(
                    person, supervision_time_bucket, SupervisionMetricType.COMPLIANCE)

                compliance_metrics = map_metric_combinations(
                    characteristic_combo_compliance, supervision_time_bucket,
                    calculation_month_upper_bound, calculation_month_lower_bound,
                    supervision_time_buckets, periods_and_buckets,
                    SupervisionMetricType.COMPLIANCE,
                    # The SupervisionCaseComplianceMetric metric is explicitly a daily metric
                    include_metric_period_output=False)

                metrics.extend(compliance_metrics)

            if isinstance(supervision_time_bucket, RevocationReturnSupervisionTimeBucket):
                if metric_inclusions.get(SupervisionMetricType.REVOCATION):
                    characteristic_combo_revocation = characteristics_dict(
                        person, supervision_time_bucket, SupervisionMetricType.REVOCATION)

                    revocation_metrics = map_metric_combinations(
                        characteristic_combo_revocation,
                        supervision_time_bucket,
                        calculation_month_upper_bound,
                        calculation_month_lower_bound,
                        supervision_time_buckets,
                        periods_and_buckets,
                        SupervisionMetricType.REVOCATION,
                        include_metric_period_output)

                    metrics.extend(revocation_metrics)

                if metric_inclusions.get(SupervisionMetricType.REVOCATION_ANALYSIS):
                    characteristic_combo_revocation_analysis = characteristics_dict(
                        person, supervision_time_bucket, SupervisionMetricType.REVOCATION_ANALYSIS)

                    revocation_analysis_metrics = map_metric_combinations(
                        characteristic_combo_revocation_analysis,
                        supervision_time_bucket,
                        calculation_month_upper_bound,
                        calculation_month_lower_bound,
                        supervision_time_buckets,
                        periods_and_buckets,
                        SupervisionMetricType.REVOCATION_ANALYSIS,
                        include_metric_period_output
                    )

                    metrics.extend(revocation_analysis_metrics)

                if (metric_inclusions.get(SupervisionMetricType.REVOCATION_VIOLATION_TYPE_ANALYSIS)
                        and supervision_time_bucket.violation_type_frequency_counter):
                    characteristic_combo_revocation_violation_type_analysis = characteristics_dict(
                        person, supervision_time_bucket, SupervisionMetricType.REVOCATION_VIOLATION_TYPE_ANALYSIS)

                    revocation_violation_type_analysis_metrics = get_revocation_violation_type_analysis_metrics(
                        supervision_time_bucket, characteristic_combo_revocation_violation_type_analysis,
                        calculation_month_upper_bound, calculation_month_lower_bound,
                        supervision_time_buckets, periods_and_buckets,
                        include_metric_period_output
                    )

                    metrics.extend(revocation_violation_type_analysis_metrics)
        else:
            raise ValueError(f"Bucket is of unexpected SupervisionTimeBucket type: {supervision_time_bucket}")

    return metrics


def characteristics_dict(person: StatePerson,
                         supervision_time_bucket: SupervisionTimeBucket,
                         metric_type: SupervisionMetricType) -> Dict[str, Any]:
    """Builds a dictionary that describes the characteristics of the person and supervision_time_bucket.

    Args:
        person: the StatePerson we are picking characteristics from
        supervision_time_bucket: the SupervisionTimeBucket we are picking characteristics from
        metric_type: The SupervisionMetricType provided determines which fields should be added to the characteristics
            dictionary

    Returns:
        A dictionary populated with all relevant characteristics.
    """
    characteristics: Dict[str, Any] = {}

    include_revocation_dimensions = _include_revocation_dimensions_for_metric(metric_type)
    include_assessment_dimensions = _include_assessment_dimensions_for_metric(metric_type)
    include_demographic_dimensions = _include_demographic_dimensions_for_metric(metric_type)
    include_person_level_dimensions = _include_person_level_dimensions_for_metric(metric_type)

    if (metric_type == SupervisionMetricType.POPULATION and
            isinstance(supervision_time_bucket, (RevocationReturnSupervisionTimeBucket,
                                                 NonRevocationReturnSupervisionTimeBucket))):
        if supervision_time_bucket.most_severe_violation_type:
            characteristics['most_severe_violation_type'] = supervision_time_bucket.most_severe_violation_type
        if supervision_time_bucket.most_severe_violation_type_subtype:
            characteristics['most_severe_violation_type_subtype'] = \
                supervision_time_bucket.most_severe_violation_type_subtype
        if supervision_time_bucket.response_count is not None:
            characteristics['response_count'] = supervision_time_bucket.response_count

    if include_revocation_dimensions and \
            isinstance(supervision_time_bucket,
                       RevocationReturnSupervisionTimeBucket):
        if supervision_time_bucket.revocation_type:
            characteristics['revocation_type'] = supervision_time_bucket.revocation_type

        if supervision_time_bucket.source_violation_type:
            characteristics['source_violation_type'] = supervision_time_bucket.source_violation_type

        if metric_type in [SupervisionMetricType.REVOCATION_ANALYSIS,
                           SupervisionMetricType.REVOCATION_VIOLATION_TYPE_ANALYSIS]:
            if supervision_time_bucket.most_severe_violation_type:
                characteristics['most_severe_violation_type'] = supervision_time_bucket.most_severe_violation_type

            if supervision_time_bucket.most_severe_violation_type_subtype:
                characteristics['most_severe_violation_type_subtype'] = \
                    supervision_time_bucket.most_severe_violation_type_subtype

            if metric_type in [SupervisionMetricType.REVOCATION_ANALYSIS]:
                if supervision_time_bucket.most_severe_response_decision:
                    characteristics['most_severe_response_decision'] = \
                        supervision_time_bucket.most_severe_response_decision

            if supervision_time_bucket.response_count is not None:
                characteristics['response_count'] = supervision_time_bucket.response_count

    if isinstance(supervision_time_bucket, SupervisionTerminationBucket):
        if supervision_time_bucket.termination_reason:
            characteristics['termination_reason'] = supervision_time_bucket.termination_reason

    if (isinstance(supervision_time_bucket, NonRevocationReturnSupervisionTimeBucket)
            and metric_type == SupervisionMetricType.COMPLIANCE):
        if supervision_time_bucket.case_compliance:
            characteristics['assessment_count'] = supervision_time_bucket.case_compliance.assessment_count
            characteristics['assessment_up_to_date'] = supervision_time_bucket.case_compliance.assessment_up_to_date
            characteristics['face_to_face_count'] = supervision_time_bucket.case_compliance.face_to_face_count
            characteristics['face_to_face_frequency_sufficient'] = \
                supervision_time_bucket.case_compliance.face_to_face_frequency_sufficient

    if supervision_time_bucket.supervision_type:
        characteristics['supervision_type'] = supervision_time_bucket.supervision_type
    if supervision_time_bucket.case_type:
        characteristics['case_type'] = supervision_time_bucket.case_type
    if supervision_time_bucket.judicial_district_code:
        characteristics['judicial_district_code'] = supervision_time_bucket.judicial_district_code

    if not include_revocation_dimensions and supervision_time_bucket.supervision_level:
        characteristics['supervision_level'] = supervision_time_bucket.supervision_level

    if include_assessment_dimensions:
        # TODO(2853): Figure out more robust solution for not assessed people. Here we don't set assessment_type when
        #  someone is not assessed. This only works as desired because BQ doesn't rely on assessment_type at all.
        characteristics['assessment_score_bucket'] = 'NOT_ASSESSED'
        if supervision_time_bucket.assessment_score and supervision_time_bucket.assessment_type:
            assessment_bucket = assessment_score_bucket(
                supervision_time_bucket.assessment_score,
                supervision_time_bucket.assessment_level,
                supervision_time_bucket.assessment_type)

            if assessment_bucket and include_assessment_in_metric(
                    'supervision', supervision_time_bucket.state_code, supervision_time_bucket.assessment_type):
                characteristics['assessment_score_bucket'] = assessment_bucket
                characteristics['assessment_type'] = supervision_time_bucket.assessment_type
    if supervision_time_bucket.supervising_officer_external_id:
        characteristics['supervising_officer_external_id'] = supervision_time_bucket.supervising_officer_external_id

    if supervision_time_bucket.supervising_district_external_id:
        characteristics['supervising_district_external_id'] = supervision_time_bucket.supervising_district_external_id

    if isinstance(supervision_time_bucket, RevocationReturnSupervisionTimeBucket):
        event_date = supervision_time_bucket.revocation_admission_date

        if include_revocation_dimensions:
            characteristics['revocation_admission_date'] = supervision_time_bucket.revocation_admission_date
    elif isinstance(supervision_time_bucket, SupervisionTerminationBucket):
        event_date = supervision_time_bucket.termination_date
        characteristics['termination_date'] = supervision_time_bucket.termination_date
    elif (isinstance(supervision_time_bucket, NonRevocationReturnSupervisionTimeBucket)
          and metric_type == SupervisionMetricType.COMPLIANCE
          and supervision_time_bucket.case_compliance):
        event_date = supervision_time_bucket.case_compliance.date_of_evaluation
        characteristics['date_of_evaluation'] = event_date
    else:
        year = supervision_time_bucket.year
        month = supervision_time_bucket.month

        event_date = date(year, month, 1)

    if include_demographic_dimensions:
        characteristics = add_demographic_characteristics(characteristics, person, event_date)

    if include_person_level_dimensions:
        characteristics = characteristics_with_person_id_fields(characteristics, person, 'supervision')

        if not include_revocation_dimensions and supervision_time_bucket.supervision_level_raw_text:
            characteristics['supervision_level_raw_text'] = \
                supervision_time_bucket.supervision_level_raw_text

        if metric_type == SupervisionMetricType.POPULATION:
            if isinstance(supervision_time_bucket,
                          (RevocationReturnSupervisionTimeBucket, NonRevocationReturnSupervisionTimeBucket)):
                characteristics['is_on_supervision_last_day_of_month'] = \
                    supervision_time_bucket.is_on_supervision_last_day_of_month

        if metric_type == SupervisionMetricType.REVOCATION_ANALYSIS:
            if isinstance(supervision_time_bucket, RevocationReturnSupervisionTimeBucket) \
                    and supervision_time_bucket.violation_history_description:
                characteristics['violation_history_description'] = \
                    supervision_time_bucket.violation_history_description

    return characteristics


def map_metric_combinations(
        characteristic_combo: Dict[str, Any],
        supervision_time_bucket: SupervisionTimeBucket,
        calculation_month_upper_bound: date,
        calculation_month_lower_bound: Optional[date],
        all_supervision_time_buckets: List[SupervisionTimeBucket],
        periods_and_buckets: Dict[int, List[SupervisionTimeBucket]],
        metric_type: SupervisionMetricType,
        include_metric_period_output: bool) -> \
        List[Tuple[Dict[str, Any], Any]]:
    """Maps the given time bucket and characteristic combinations to a variety of metrics that track supervision
     population and revocation counts.

    Args:
        characteristic_combo: A dictionary describing the person and supervision_time_bucket.
        supervision_time_bucket: The time bucket on supervision from which the combination was derived.
        calculation_month_upper_bound: The year and month of the last month for which metrics should be calculated.
        calculation_month_lower_bound: The date of the first month to be included in the monthly calculations
        all_supervision_time_buckets: All of the person's SupervisionTimeBuckets
        periods_and_buckets: Dictionary mapping metric period month lengths to the SupervisionTimeBuckets that fall in
            that period.
        metric_type: The metric type to set on each combination.
        include_metric_period_output: Whether or not to include metrics for the various metric periods before the
            current month. If False, will still include metric_period_months = 1 for the current month.

    Returns:
        A list of key-value tuples representing specific metric combinations and the metric value corresponding to that
        metric.
    """
    metrics = []

    characteristic_combo['metric_type'] = metric_type

    if include_in_historical_metrics(
            supervision_time_bucket.year, supervision_time_bucket.month,
            calculation_month_upper_bound, calculation_month_lower_bound):
        # SupervisionCaseComplianceMetrics are point-in-time metrics for the date of the compliance evaluation, all
        # other SupervisionMetrics are metrics based on the month of the event
        is_daily_metric = metric_type == SupervisionMetricType.COMPLIANCE

        metrics.extend(combination_supervision_monthly_metrics(
            characteristic_combo, supervision_time_bucket,
            all_supervision_time_buckets, metric_type, is_daily_metric))

    if include_metric_period_output:
        metrics.extend(combination_supervision_metric_period_metrics(
            characteristic_combo,
            supervision_time_bucket,
            calculation_month_upper_bound,
            periods_and_buckets,
            metric_type
        ))

    return metrics


def get_revocation_violation_type_analysis_metrics(
        supervision_time_bucket: RevocationReturnSupervisionTimeBucket,
        characteristic_combo: Dict[str, Any],
        calculation_month_upper_bound: date,
        calculation_month_lower_bound: Optional[date],
        all_buckets_sorted: List[SupervisionTimeBucket],
        periods_and_buckets: Dict[int, List[SupervisionTimeBucket]],
        include_metric_period_output: bool) -> List[Tuple[Dict[str, Any], Any]]:
    """Produces metrics of the type REVOCATION_VIOLATION_TYPE_ANALYSIS. For each violation type list in the bucket's
    violation_type_frequency_counter, produces metrics for each violation type in the list, and one with a
    violation_count_type of 'VIOLATION' to keep track of the overall number of violations."""
    metrics = []
    if supervision_time_bucket.violation_type_frequency_counter:
        for violation_type_list in supervision_time_bucket.violation_type_frequency_counter:
            violation_type_augment_values = {'violation_count_type': 'VIOLATION'}

            violation_count_characteristic_combo = augment_combination(characteristic_combo,
                                                                       violation_type_augment_values)

            revocation_analysis_metrics_violation_count = map_metric_combinations(
                violation_count_characteristic_combo,
                supervision_time_bucket,
                calculation_month_upper_bound,
                calculation_month_lower_bound,
                all_buckets_sorted,
                periods_and_buckets,
                SupervisionMetricType.REVOCATION_VIOLATION_TYPE_ANALYSIS,
                include_metric_period_output
            )

            metrics.extend(revocation_analysis_metrics_violation_count)

            for violation_type_string in violation_type_list:

                violation_type_augment_values = {'violation_count_type': violation_type_string}

                violation_type_characteristic_combo = augment_combination(characteristic_combo,
                                                                          violation_type_augment_values)

                revocation_analysis_metrics_violation_type = map_metric_combinations(
                    violation_type_characteristic_combo,
                    supervision_time_bucket,
                    calculation_month_upper_bound,
                    calculation_month_lower_bound,
                    all_buckets_sorted,
                    periods_and_buckets,
                    SupervisionMetricType.REVOCATION_VIOLATION_TYPE_ANALYSIS,
                    include_metric_period_output
                )

                metrics.extend(revocation_analysis_metrics_violation_type)

    return metrics


def combination_supervision_monthly_metrics(
        combo: Dict[str, Any],
        supervision_time_bucket: SupervisionTimeBucket,
        all_supervision_time_buckets: List[SupervisionTimeBucket],
        metric_type: SupervisionMetricType,
        is_daily_metric: bool
) -> List[Tuple[Dict[str, Any], int]]:
    """Returns all unique supervision metrics for the given time bucket and combination for the month of the bucket.

    First, includes an event-based count for the month the SupervisionTimeBucket represents. If this bucket of
    supervision should be included in the person-based count for the month when the supervision occurred, adds those
    person-based metrics.

    Args:
        combo: A characteristic combination to convert into metrics
        supervision_time_bucket: The SupervisionTimeBucket from which the combination was derived
        all_supervision_time_buckets: All of this person's SupervisionTimeBuckets
        metric_type: The type of metric being tracked by this combo
        is_daily_metric:  If True, limits person-based counts to the date of the event. If False, limits person-based
            counts to the month of the event.

    Returns:
        A list of key-value tuples representing specific metric combination dictionaries and the the metric value
            corresponding to that metric.
    """
    metrics: List[Tuple[Dict[str, Any], int]] = []

    bucket_year = supervision_time_bucket.year
    bucket_month = supervision_time_bucket.month

    base_metric_period = 0 if is_daily_metric else 1

    # Add event-based combo for the base metric period of the month and year of the bucket
    event_based_same_bucket_combo = augmented_combo_for_calculations(
        combo, supervision_time_bucket.state_code,
        bucket_year, bucket_month,
        MetricMethodologyType.EVENT, base_metric_period)

    event_combo_value = None

    if isinstance(supervision_time_bucket, ProjectedSupervisionCompletionBucket):
        if metric_type == SupervisionMetricType.SUCCESS:
            # Set 1 for successful completion, 0 for unsuccessful completion
            event_combo_value = 1 if supervision_time_bucket.successful_completion else 0
        elif metric_type == SupervisionMetricType.SUCCESSFUL_SENTENCE_DAYS_SERVED:
            if supervision_time_bucket.sentence_days_served is not None:
                # Only include this combo if there is a recorded number of days served. Set the value as the number of
                # days served.
                event_combo_value = supervision_time_bucket.sentence_days_served
            else:
                # If there's no recorded days served on this completion bucket, don't include it in any of the
                # successful sentence days served metrics.
                pass
        else:
            raise ValueError(f"Unsupported metric type {metric_type} for ProjectedSupervisionCompletionBucket.")

    elif metric_type == SupervisionMetricType.ASSESSMENT_CHANGE and \
            isinstance(supervision_time_bucket, SupervisionTerminationBucket):
        if supervision_time_bucket.assessment_score_change is not None:
            # Only include this combo if there is an assessment score change associated with this termination. Set the
            # value as the assessment score change
            event_combo_value = supervision_time_bucket.assessment_score_change
        else:
            # The only metric relying on the SupervisionTerminationBuckets is the
            # TerminatedSupervisionAssessmentScoreChangeMetric. So, if there's no recorded assessment score change on
            # this termination, don't include it in any of the metrics.
            pass
    else:
        # The default value for all combos is 1
        event_combo_value = 1

    if event_combo_value is None:
        # If the event_combo_value is not set, then exclude this bucket from all metrics
        return metrics

    # TODO(2913): Exclude combos with a supervision_type of DUAL from event-based counts
    metrics.append((event_based_same_bucket_combo, event_combo_value))

    # Create the person-based combo for the base metric period of the month of the bucket
    person_based_same_bucket_combo = augmented_combo_for_calculations(
        combo, supervision_time_bucket.state_code,
        bucket_year, bucket_month,
        MetricMethodologyType.PERSON, base_metric_period
    )

    buckets_in_period: List[SupervisionTimeBucket] = []

    if metric_type == SupervisionMetricType.POPULATION:
        # Get all other supervision time buckets for the same month as this one
        buckets_in_period = [
            bucket for bucket in all_supervision_time_buckets
            if (isinstance(bucket, (RevocationReturnSupervisionTimeBucket, NonRevocationReturnSupervisionTimeBucket)))
            and bucket.year == bucket_year and
            bucket.month == bucket_month
        ]
    elif metric_type == SupervisionMetricType.REVOCATION:
        # Get all other revocation supervision buckets for the same month as this one
        buckets_in_period = [
            bucket for bucket in all_supervision_time_buckets
            if isinstance(bucket, RevocationReturnSupervisionTimeBucket)
            and bucket.year == bucket_year and
            bucket.month == bucket_month
        ]
    elif metric_type in (SupervisionMetricType.SUCCESS, SupervisionMetricType.SUCCESSFUL_SENTENCE_DAYS_SERVED):
        # Get all other projected completion buckets for the same month as this one
        buckets_in_period = [
            bucket for bucket in all_supervision_time_buckets
            if isinstance(bucket, ProjectedSupervisionCompletionBucket)
            and bucket.year == bucket_year and
            bucket.month == bucket_month
        ]
    elif metric_type == SupervisionMetricType.ASSESSMENT_CHANGE:
        # Get all other termination buckets for the same month as this one
        buckets_in_period = [
            bucket for bucket in all_supervision_time_buckets
            if isinstance(bucket, SupervisionTerminationBucket)
            and bucket.year == bucket_year and
            bucket.month == bucket_month
        ]
    elif metric_type in (SupervisionMetricType.REVOCATION_ANALYSIS,
                         SupervisionMetricType.REVOCATION_VIOLATION_TYPE_ANALYSIS):
        # Get all other revocation supervision buckets for the same month as this one
        buckets_in_period = [
            bucket for bucket in all_supervision_time_buckets
            if isinstance(bucket, RevocationReturnSupervisionTimeBucket)
            and bucket.year == bucket_year and
            bucket.month == bucket_month
        ]
    elif metric_type == SupervisionMetricType.COMPLIANCE:
        if supervision_time_bucket.case_compliance is None:
            raise ValueError("Attempting to calculate COMPLIANCE metrics on a SupervisionTimeBucket that has no"
                             "case_compliance set.")

        # Get all other NonRevocationReturnSupervisionTimeBucket buckets with a set case_compliance field
        buckets_in_period = [
            bucket for bucket in all_supervision_time_buckets
            if isinstance(bucket, NonRevocationReturnSupervisionTimeBucket)
            and bucket.case_compliance is not None
            and bucket.case_compliance.date_of_evaluation == supervision_time_bucket.case_compliance.date_of_evaluation
        ]

    if buckets_in_period and include_supervision_in_count(
            combo,
            supervision_time_bucket,
            buckets_in_period,
            metric_type):
        person_combo_value = _person_combo_value(combo, supervision_time_bucket, buckets_in_period, metric_type)

        # Include this event in the person-based count
        metrics.append((person_based_same_bucket_combo, person_combo_value))

    return metrics


def combination_supervision_metric_period_metrics(
        combo: Dict[str, Any],
        supervision_time_bucket: SupervisionTimeBucket,
        metric_period_end_date: date,
        periods_and_buckets: Dict[int, List[SupervisionTimeBucket]],
        metric_type: SupervisionMetricType) \
        -> List[Tuple[Dict[str, Any], int]]:
    """Returns all unique supervision metrics for the given time bucket and combination for each of the relevant
    metric_period_months.

    Returns metrics for each of the metric period lengths that this event falls into if this event should be included in
    the person-based count for that metric period length.

    Args:
        combo: A characteristic combination to convert into metrics
        supervision_time_bucket: The SupervisionTimeBucket from which the
            combination was derived
        metric_period_end_date: The day the metric periods end
        periods_and_buckets: Dictionary mapping metric period month lengths to
            the SupervisionTimeBuckets that fall in that period
        metric_type: The type of metric being tracked by this combo

    Returns:
        A list of key-value tuples representing specific metric combination dictionaries and the the metric value
            corresponding to that metric.
    """
    metrics: List[Tuple[Dict[str, Any], int]] = []

    period_end_year = metric_period_end_date.year
    period_end_month = metric_period_end_date.month

    for period_length, buckets_in_period in periods_and_buckets.items():
        if supervision_time_bucket in buckets_in_period:
            # This event falls within this metric period
            person_based_period_combo = augmented_combo_for_calculations(
                combo, supervision_time_bucket.state_code,
                period_end_year, period_end_month,
                MetricMethodologyType.PERSON, period_length
            )

            relevant_buckets_in_period: List[SupervisionTimeBucket] = []

            if metric_type == SupervisionMetricType.ASSESSMENT_CHANGE:
                # Get all other supervision time buckets for this period that should contribute to an assessment change
                # metric
                relevant_buckets_in_period = [
                    bucket for bucket in buckets_in_period
                    if (isinstance(bucket, SupervisionTerminationBucket))
                ]
            elif metric_type == SupervisionMetricType.POPULATION:
                # Get all other supervision time buckets for this period that should contribute to a population metric
                relevant_buckets_in_period = [
                    bucket for bucket in buckets_in_period
                    if (isinstance(bucket, (RevocationReturnSupervisionTimeBucket,
                                            NonRevocationReturnSupervisionTimeBucket)))
                ]
            elif metric_type in (SupervisionMetricType.REVOCATION,
                                 SupervisionMetricType.REVOCATION_ANALYSIS,
                                 SupervisionMetricType.REVOCATION_VIOLATION_TYPE_ANALYSIS):
                # Get all other revocation return time buckets for this period
                relevant_buckets_in_period = [
                    bucket for bucket in buckets_in_period
                    if isinstance(bucket, RevocationReturnSupervisionTimeBucket)
                ]
            elif metric_type in (SupervisionMetricType.SUCCESS, SupervisionMetricType.SUCCESSFUL_SENTENCE_DAYS_SERVED):
                # Get all other projected completion buckets in this period
                relevant_buckets_in_period = [
                    bucket for bucket in buckets_in_period
                    if (isinstance(bucket, ProjectedSupervisionCompletionBucket))
                ]

            if relevant_buckets_in_period and include_supervision_in_count(
                    combo,
                    supervision_time_bucket,
                    relevant_buckets_in_period,
                    metric_type):

                person_combo_value = _person_combo_value(
                    combo, supervision_time_bucket, relevant_buckets_in_period, metric_type
                )

                # Include this event in the person-based count
                metrics.append((person_based_period_combo, person_combo_value))

    return metrics


def include_supervision_in_count(combo: Dict[str, Any],
                                 supervision_time_bucket: SupervisionTimeBucket,
                                 all_buckets_in_period:
                                 List[SupervisionTimeBucket],
                                 metric_type: SupervisionMetricType) -> bool:
    """Determines whether the given supervision_time_bucket should be included in a person-based count given the other
    buckets in the period.

    If the combo has a value for the key 'supervision_type', this means that this will contribute to a metric that is
    specific to a given supervision type. For some states, the person-based count for this metric should only be with
    respect to other buckets that share the same supervision-type. If the combo is not for a supervision-type-specific
    metric, or the combo is for the person-level output, then the person-based count should take into account all
    buckets in the period.

    If the metric is of type POPULATION, and there are buckets that represent revocation in that period, then this
    bucket is included only if it is the last instance of revocation for the period. However, if none of the
    buckets represent revocation, then this bucket is included if it is the last bucket in the period. If the metric is
    of type REVOCATION, SUCCESS, or ASSESSMENT_CHANGE, then this bucket is included only if it is the last
    bucket in the period.

    If the metric is of type SUCCESSFUL_SENTENCE_DAYS_SERVED, then a bucket for this month is only included if all
    supervision sentences that were projected to complete in this period finished successfully and were not
    overlapped by any incarceration. If this is the case, then this bucket is included only if it is the longest
    sentence that ended in this time period.

    This function assumes that the SupervisionTimeBuckets in all_buckets_in_period are of the same type and that the
    list is sorted in ascending order by year and month.
    """
    # If supervision types are distinct for a given state, then a person who has events with different types of
    # supervision cannot contribute to counts for more than one type
    if supervision_types_distinct_for_state(supervision_time_bucket.state_code):
        supervision_type_specific_metric = False
    else:
        # If this combo specifies the supervision type (and it's not a person-level combo), then limit this inclusion
        # logic to only buckets of the same supervision type
        supervision_type_specific_metric = combo.get('supervision_type') is not None and combo.get('person_id') is None

    relevant_buckets = [
        bucket for bucket in all_buckets_in_period
        if not (supervision_type_specific_metric and bucket.supervision_type !=
                supervision_time_bucket.supervision_type)
    ]

    revocation_buckets = [
        bucket for bucket in relevant_buckets
        if isinstance(bucket, RevocationReturnSupervisionTimeBucket)
    ]

    # Sort by the revocation admission date
    revocation_buckets.sort(key=lambda b: b.revocation_admission_date)

    if metric_type == SupervisionMetricType.POPULATION:
        if revocation_buckets:
            # If there are SupervisionTimeBuckets that are of type RevocationReturnSupervisionTimeBucket, then we want
            # to include that bucket in the counts over any NonRevocationReturnSupervisionTimeBucket. This ensures that
            # the supervision information (supervision_type, district, officer, etc) for the revocation metrics will
            # have corresponding population instances
            return id(supervision_time_bucket) == id(revocation_buckets[-1])

        last_day_of_month_buckets = [
            b for b in relevant_buckets
            if isinstance(b, (RevocationReturnSupervisionTimeBucket, NonRevocationReturnSupervisionTimeBucket))
            and b.is_on_supervision_last_day_of_month]

        if last_day_of_month_buckets:
            return id(supervision_time_bucket) == id(last_day_of_month_buckets[-1])

        return id(supervision_time_bucket) == id(relevant_buckets[-1])

    if metric_type in (SupervisionMetricType.REVOCATION,
                       SupervisionMetricType.REVOCATION_ANALYSIS,
                       SupervisionMetricType.REVOCATION_VIOLATION_TYPE_ANALYSIS):
        return id(supervision_time_bucket) == id(revocation_buckets[-1])

    if metric_type in (SupervisionMetricType.SUCCESS,
                       SupervisionMetricType.ASSESSMENT_CHANGE,
                       SupervisionMetricType.COMPLIANCE):
        return id(supervision_time_bucket) == id(relevant_buckets[-1])

    if metric_type == SupervisionMetricType.SUCCESSFUL_SENTENCE_DAYS_SERVED:
        # If any supervision sentence that was projected to complete in this period didn't finish successfully
        # or was overlapped by incarceration, then do not count this month as a successful completion for the
        # successful sentence days served metric
        if any(isinstance(b, ProjectedSupervisionCompletionBucket) and (
                not b.successful_completion or b.incarcerated_during_sentence) for b in relevant_buckets):
            return False

        sentence_length_buckets = [
            bucket for bucket in relevant_buckets
            if isinstance(bucket, ProjectedSupervisionCompletionBucket)
            and bucket.sentence_days_served is not None
        ]

        sentence_length_buckets.sort(key=lambda b: b.sentence_days_served)

        # Use only the longest sentence that ended in this time period
        return id(supervision_time_bucket) == id(sentence_length_buckets[-1])

    raise ValueError(f"SupervisionMetricType {metric_type} not handled.")


def _person_combo_value(combo: Dict[str, Any],
                        supervision_time_bucket: SupervisionTimeBucket,
                        all_buckets_in_period: List[SupervisionTimeBucket],
                        metric_type: SupervisionMetricType) -> int:
    """Determines what the value should be for a person-based metric given the combo, the supervision_time_bucket,
    the buckets in the period, and the type of metric this combo will be contributing to.

    All values will be 1 for the POPULATION and REVOCATION count metrics, because the presence of a
    SupervisionTimeBucket for a given time bucket implies that the person was counted towards the supervision population
    for that time bucket, and possibly that the person was counted towards the revoked population for that same time
    bucket.

    The value for ASSESSMENT_CHANGE metrics will be the assessment_score_change on the given supervision_time_bucket.

    The value for SUCCESSFUL_SENTENCE_DAYS_SERVED metrics will be the sentence_days_served value on the given
    supervision_time_bucket.

    The value for the SUCCESS metrics is 1 for a successful completion, and 0 for an unsuccessful completion. For any
    combos that do not specify supervision type, the success value on this combo should only be 1 if all
    supervisions ending that month were successful. For any combos that do specify supervision type, the success value
    on this combo should only be 1 if all other supervisions of the same type ending in that month were successful.
    """
    person_combo_value = 1

    if isinstance(supervision_time_bucket, ProjectedSupervisionCompletionBucket):
        if metric_type == SupervisionMetricType.SUCCESSFUL_SENTENCE_DAYS_SERVED:
            return supervision_time_bucket.sentence_days_served

        if metric_type == SupervisionMetricType.SUCCESS:
            supervision_type_specific_metric = combo.get(
                'supervision_type') is not None

            # If the combination specifies the supervision type, then remove any buckets of other supervision types
            relevant_buckets = [
                bucket for bucket in all_buckets_in_period
                if not (supervision_type_specific_metric and bucket.supervision_type !=
                        supervision_time_bucket.supervision_type)
            ]

            for completion_bucket in relevant_buckets:
                if isinstance(completion_bucket, ProjectedSupervisionCompletionBucket) and not \
                        completion_bucket.successful_completion:
                    person_combo_value = 0
    elif metric_type == SupervisionMetricType.ASSESSMENT_CHANGE and \
            isinstance(supervision_time_bucket, SupervisionTerminationBucket):
        # This should always evaluate to true at this point
        if supervision_time_bucket.assessment_score_change is not None:
            person_combo_value = supervision_time_bucket.assessment_score_change

    return person_combo_value


def _classify_buckets_by_relevant_metric_periods(
        supervision_time_buckets: List[SupervisionTimeBucket],
        metric_period_end_date: date
) -> Dict[int, List[SupervisionTimeBucket]]:
    """Returns a dictionary mapping metric period month values to the corresponding relevant SupervisionTimeBuckets."""
    periods_and_buckets: Dict[int, List[SupervisionTimeBucket]] = defaultdict()

    # Organize the month buckets by the relevant metric periods
    for supervision_time_bucket in supervision_time_buckets:
        bucket_start_date = date(supervision_time_bucket.year, supervision_time_bucket.month, 1)

        relevant_periods = relevant_metric_periods(
            bucket_start_date,
            metric_period_end_date.year,
            metric_period_end_date.month)

        if relevant_periods:
            for period in relevant_periods:
                period_events = periods_and_buckets.get(period)

                if period_events:
                    period_events.append(supervision_time_bucket)
                else:
                    periods_and_buckets[period] = [supervision_time_bucket]

    return periods_and_buckets


def _include_revocation_dimensions_for_metric(metric_type: SupervisionMetricType) -> bool:
    """Returns whether revocation dimensions should be included in metrics of the given metric_type."""
    if metric_type in (
            SupervisionMetricType.REVOCATION,
            SupervisionMetricType.REVOCATION_ANALYSIS,
            SupervisionMetricType.REVOCATION_VIOLATION_TYPE_ANALYSIS
    ):
        return True

    if metric_type in (
            SupervisionMetricType.POPULATION,
            SupervisionMetricType.ASSESSMENT_CHANGE,
            SupervisionMetricType.SUCCESS,
            SupervisionMetricType.SUCCESSFUL_SENTENCE_DAYS_SERVED,
            SupervisionMetricType.COMPLIANCE
    ):
        return False

    raise ValueError(f"SupervisionMetricType {metric_type} not handled.")


def _include_assessment_dimensions_for_metric(metric_type: SupervisionMetricType) -> bool:
    """Returns whether assessment dimensions should be included in metrics of the given metric_type."""
    if metric_type in (
            SupervisionMetricType.REVOCATION,
            SupervisionMetricType.REVOCATION_ANALYSIS,
            SupervisionMetricType.POPULATION,
            SupervisionMetricType.ASSESSMENT_CHANGE,
            SupervisionMetricType.COMPLIANCE
    ):
        return True

    if metric_type in (
            SupervisionMetricType.REVOCATION_VIOLATION_TYPE_ANALYSIS,
            SupervisionMetricType.SUCCESS,
            SupervisionMetricType.SUCCESSFUL_SENTENCE_DAYS_SERVED
    ):
        return False

    raise ValueError(f"SupervisionMetricType {metric_type} not handled.")


def _include_demographic_dimensions_for_metric(metric_type: SupervisionMetricType) -> bool:
    """Returns whether demographic dimensions should be included in metrics of the given metric_type."""
    if metric_type in (
            SupervisionMetricType.REVOCATION,
            SupervisionMetricType.REVOCATION_ANALYSIS,
            SupervisionMetricType.POPULATION,
            SupervisionMetricType.ASSESSMENT_CHANGE,
            SupervisionMetricType.SUCCESS,
            SupervisionMetricType.SUCCESSFUL_SENTENCE_DAYS_SERVED,
            SupervisionMetricType.COMPLIANCE
    ):
        return True

    if metric_type in (
            SupervisionMetricType.REVOCATION_VIOLATION_TYPE_ANALYSIS,
    ):
        return False

    raise ValueError(f"SupervisionMetricType {metric_type} not handled.")


def _include_person_level_dimensions_for_metric(metric_type: SupervisionMetricType) -> bool:
    """Returns whether person-level dimensions should be included in metrics of the given metric_type."""
    if metric_type in (
            SupervisionMetricType.REVOCATION,
            SupervisionMetricType.REVOCATION_ANALYSIS,
            SupervisionMetricType.POPULATION,
            SupervisionMetricType.ASSESSMENT_CHANGE,
            SupervisionMetricType.SUCCESS,
            SupervisionMetricType.SUCCESSFUL_SENTENCE_DAYS_SERVED,
            SupervisionMetricType.COMPLIANCE
    ):
        return True

    if metric_type == SupervisionMetricType.REVOCATION_VIOLATION_TYPE_ANALYSIS:
        return False

    raise ValueError(f"SupervisionMetricType {metric_type} not handled.")
