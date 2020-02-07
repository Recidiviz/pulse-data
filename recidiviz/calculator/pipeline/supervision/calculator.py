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

This contains the core logic for calculating supervision metrics on a
person-by-person basis. It transforms SupervisionTimeBuckets into supervision
metrics, key-value pairs where the key represents all of the dimensions
represented in the data point, and the value represents an indicator of whether
the person should contribute to that metric.
"""
from collections import defaultdict
from datetime import date
from operator import attrgetter
from typing import Dict, List, Tuple, Any

from recidiviz.calculator.pipeline.supervision.supervision_time_bucket import \
    SupervisionTimeBucket, RevocationReturnSupervisionTimeBucket, \
    ProjectedSupervisionCompletionBucket, \
    NonRevocationReturnSupervisionTimeBucket, SupervisionTerminationBucket
from recidiviz.calculator.pipeline.utils.calculator_utils import age_at_date, \
    age_bucket, for_characteristics_races_ethnicities, for_characteristics, \
    augmented_combo_list, last_day_of_month, relevant_metric_periods, \
    person_external_id_to_include
from recidiviz.calculator.pipeline.utils.assessment_utils import \
    assessment_score_bucket, include_assessment_in_metric
from recidiviz.calculator.pipeline.supervision.metrics import \
    SupervisionMetricType
from recidiviz.calculator.pipeline.utils.metric_utils import \
    MetricMethodologyType
from recidiviz.persistence.entity.state.entities import StatePerson


def map_supervision_combinations(person: StatePerson,
                                 supervision_time_buckets:
                                 List[SupervisionTimeBucket],
                                 inclusions: Dict[str, bool]) \
        -> List[Tuple[Dict[str, Any], Any]]:
    """Transforms SupervisionTimeBuckets and a StatePerson into metric
    combinations.

    Takes in a StatePerson and all of her SupervisionTimeBuckets and returns an
    array of "supervision combinations". These are key-value pairs where the key
    represents a specific metric and the value represents whether or not
    the person should be counted as a positive instance of that metric.

    This translates a particular time on supervision into many different
    supervision population metrics. Each metric represents one of many possible
    combinations of characteristics being tracked for that event. For example,
    if a White male is on supervision, there is a metric that corresponds to
    White people, one to males, one to White males, one to all people, and more
    depending on other dimensions in the data.

    Args:
        person: the StatePerson
        supervision_time_buckets: A list of SupervisionTimeBuckets for the given
            StatePerson.
        inclusions: A dictionary containing the following keys that correspond
            to characteristic dimensions:
                - age_bucket
                - ethnicity
                - gender
                - race
            Where the values are boolean flags indicating whether to include
            the dimension in the calculations.
    Returns:
        A list of key-value tuples representing specific metric combinations and
        the value corresponding to that metric.
    """
    metrics: List[Tuple[Dict[str, Any], Any]] = []

    # We will calculate person-based metrics for each metric period in
    # METRIC_PERIOD_MONTHS ending with the current month
    metric_period_end_date = last_day_of_month(date.today())

    month_buckets = [
        bucket for bucket in supervision_time_buckets
        if bucket.month is not None
    ]

    year_buckets = [
        bucket for bucket in supervision_time_buckets
        if bucket.month is None
    ]

    month_buckets.sort(key=attrgetter('year', 'month'))
    year_buckets.sort(key=attrgetter('year'))

    all_buckets_sorted = month_buckets + year_buckets

    periods_and_buckets = _classify_buckets_by_relevant_metric_periods(
        all_buckets_sorted, metric_period_end_date
    )

    for supervision_time_bucket in all_buckets_sorted:
        characteristic_combos_population = \
            characteristic_combinations(
                person, supervision_time_bucket, inclusions)

        if isinstance(supervision_time_bucket,
                      ProjectedSupervisionCompletionBucket):
            if inclusions.get(SupervisionMetricType.SUCCESS.value):
                supervision_success_metrics = map_metric_combinations(
                    characteristic_combos_population, supervision_time_bucket,
                    metric_period_end_date, all_buckets_sorted,
                    periods_and_buckets,
                    SupervisionMetricType.SUCCESS)

                metrics.extend(supervision_success_metrics)
        elif isinstance(supervision_time_bucket,
                        SupervisionTerminationBucket):
            if inclusions.get(SupervisionMetricType.ASSESSMENT_CHANGE.value):
                assessment_change_metrics = map_metric_combinations(
                    characteristic_combos_population, supervision_time_bucket,
                    metric_period_end_date, all_buckets_sorted,
                    periods_and_buckets,
                    SupervisionMetricType.ASSESSMENT_CHANGE)

                metrics.extend(assessment_change_metrics)
        else:
            if inclusions.get(SupervisionMetricType.POPULATION.value):
                population_metrics = map_metric_combinations(
                    characteristic_combos_population, supervision_time_bucket,
                    metric_period_end_date, all_buckets_sorted,
                    periods_and_buckets,
                    SupervisionMetricType.POPULATION)

                metrics.extend(population_metrics)

            if inclusions.get(SupervisionMetricType.REVOCATION.value):
                characteristic_combos_revocation = \
                    characteristic_combinations(
                        person, supervision_time_bucket, inclusions,
                        with_revocation_dimensions=True)

                if isinstance(supervision_time_bucket,
                              RevocationReturnSupervisionTimeBucket):
                    revocation_metrics = map_metric_combinations(
                        characteristic_combos_revocation,
                        supervision_time_bucket,
                        metric_period_end_date,
                        all_buckets_sorted,
                        periods_and_buckets,
                        SupervisionMetricType.REVOCATION)

                    metrics.extend(revocation_metrics)

            if inclusions.get(SupervisionMetricType.REVOCATION_ANALYSIS.value) and \
                    isinstance(supervision_time_bucket,
                               RevocationReturnSupervisionTimeBucket):
                characteristic_combos_revocation_analysis = \
                    characteristic_combinations(
                        person, supervision_time_bucket, inclusions,
                        with_revocation_dimensions=True,
                        with_revocation_analysis_dimensions=True)

                revocation_analysis_metrics = map_metric_combinations(
                    characteristic_combos_revocation_analysis,
                    supervision_time_bucket,
                    metric_period_end_date,
                    all_buckets_sorted,
                    periods_and_buckets,
                    SupervisionMetricType.REVOCATION_ANALYSIS
                )

                metrics.extend(revocation_analysis_metrics)

    return metrics


def characteristic_combinations(person: StatePerson,
                                supervision_time_bucket: SupervisionTimeBucket,
                                inclusions: Dict[str, bool],
                                with_revocation_dimensions: bool = False,
                                with_revocation_analysis_dimensions:
                                bool = False) -> \
        List[Dict[str, Any]]:
    """Calculates all supervision metric combinations.

    Returns the list of all combinations of the metric characteristics, of all
    sizes, given the StatePerson and SupervisionTimeBucket. That is, this
    returns a list of dictionaries where each dictionary is a combination of 0
    to n unique elements of characteristics, where n is the number of keys in
    the given inclusions dictionary that are set to True + 1 dimension for the
    supervision type (which is always included).

    For each event, we need to calculate metrics across combinations of:
    MetricMethodologyType (Event-based, Person-based);
    Demographics (age, race, ethnicity, gender);

    Methodology is not included in the output here. It is added into augmented
    versions of these combinations later.

    Args:
        person: the StatePerson we are picking characteristics from
        supervision_time_bucket: the SupervisionTimeBucket we are picking
            characteristics from
        inclusions: A dictionary containing the following keys that correspond
            to characteristic dimensions:
                - age_bucket
                - ethnicity
                - gender
                - race
            Where the values are boolean flags indicating whether to include
            the dimension in the calculations.
        with_revocation_dimensions: Whether or not to include revocation-related
            dimensions, if relevant to the given month. Defaults to False.
        with_revocation_analysis_dimensions: Whether or not to include
            revocation-analysis-related dimensions, if relevant to the given
            month. Defaults to False.

    Returns:
        A list of dictionaries containing all unique combinations of
        characteristics.
    """

    characteristics: Dict[str, Any] = {}

    if with_revocation_dimensions and \
            isinstance(supervision_time_bucket,
                       RevocationReturnSupervisionTimeBucket):
        if supervision_time_bucket.revocation_type:
            characteristics['revocation_type'] = supervision_time_bucket.revocation_type

        if supervision_time_bucket.source_violation_type:
            characteristics['source_violation_type'] = supervision_time_bucket.source_violation_type

        if with_revocation_analysis_dimensions:
            if supervision_time_bucket.most_severe_violation_type:
                characteristics['most_severe_violation_type'] = supervision_time_bucket.most_severe_violation_type

            if supervision_time_bucket.most_severe_violation_type_subtype:
                characteristics['most_severe_violation_type_subtype'] = \
                    supervision_time_bucket.most_severe_violation_type_subtype

            if supervision_time_bucket.most_severe_response_decision:
                characteristics['most_severe_response_decision'] = supervision_time_bucket.most_severe_response_decision

            if supervision_time_bucket.response_count is not None:
                characteristics['response_count'] = supervision_time_bucket.response_count

    if isinstance(supervision_time_bucket, SupervisionTerminationBucket):
        if supervision_time_bucket.termination_reason:
            characteristics['termination_reason'] = supervision_time_bucket.termination_reason

    if supervision_time_bucket.supervision_type:
        characteristics['supervision_type'] = supervision_time_bucket.supervision_type
    if supervision_time_bucket.case_type:
        characteristics['case_type'] = supervision_time_bucket.case_type
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
    if inclusions.get('age_bucket'):
        year = supervision_time_bucket.year
        month = supervision_time_bucket.month

        if month is None:
            month = 1

        start_of_bucket = date(year, month, 1)
        entry_age = age_at_date(person, start_of_bucket)
        entry_age_bucket = age_bucket(entry_age)
        if entry_age_bucket is not None:
            characteristics['age_bucket'] = entry_age_bucket
    if inclusions.get('gender'):
        if person.gender is not None:
            characteristics['gender'] = person.gender

    if person.races or person.ethnicities:
        if inclusions.get('race'):
            races = person.races
        else:
            races = []

        if inclusions.get('ethnicity'):
            ethnicities = person.ethnicities
        else:
            ethnicities = []

        all_combinations = for_characteristics_races_ethnicities(races, ethnicities, characteristics)
    else:
        all_combinations = for_characteristics(characteristics)

    if with_revocation_analysis_dimensions:
        # Person external id is added to a characteristics combination
        # dictionary that has all fields set. We only want person-level
        # output that has all possible fields set.
        person_external_id = person_external_id_to_include(person)

        if person_external_id is not None:
            characteristics['person_external_id'] = person_external_id

            # Only include violation history descriptions on person-level
            # metrics
            if isinstance(supervision_time_bucket, RevocationReturnSupervisionTimeBucket) \
                    and supervision_time_bucket.violation_history_description:
                characteristics['violation_history_description'] = supervision_time_bucket.violation_history_description

            all_combinations.append(characteristics)

    return all_combinations


def map_metric_combinations(
        characteristic_combos: List[Dict[str, Any]],
        supervision_time_bucket: SupervisionTimeBucket,
        metric_period_end_date: date,
        all_supervision_time_buckets: List[SupervisionTimeBucket],
        periods_and_buckets: Dict[int, List[SupervisionTimeBucket]],
        metric_type: SupervisionMetricType) -> \
        List[Tuple[Dict[str, Any], Any]]:
    """Maps the given time bucket and characteristic combinations to a variety
    of metrics that track supervision population and revocation counts.

    Args:
        characteristic_combos: A list of dictionaries containing all unique
            combinations of characteristics.
        supervision_time_bucket: The time bucket on supervision from which
            the combination was derived.
        metric_period_end_date: The day the metric periods end
        all_supervision_time_buckets: All of the person's SupervisionTimeBuckets
        periods_and_buckets: Dictionary mapping metric period month lengths to
            the SupervisionTimeBuckets that fall in that period
        metric_type: The metric type to set on each combination

    Returns:
        A list of key-value tuples representing specific metric combinations and
        the metric value corresponding to that metric.
    """
    metrics = []

    for combo in characteristic_combos:
        combo['metric_type'] = metric_type.value

        if combo.get('person_external_id') is not None and \
                metric_type != SupervisionMetricType.REVOCATION_ANALYSIS:
            # Only output person-level metrics for the revocation analysis
            # metrics
            continue

        metrics.extend(combination_supervision_metrics(
            combo, supervision_time_bucket, metric_period_end_date,
            all_supervision_time_buckets, periods_and_buckets, metric_type))

    return metrics


def combination_supervision_metrics(
        combo: Dict[str, Any],
        supervision_time_bucket: SupervisionTimeBucket,
        metric_period_end_date: date,
        all_supervision_time_buckets: List[SupervisionTimeBucket],
        periods_and_buckets: Dict[int, List[SupervisionTimeBucket]],
        metric_type: SupervisionMetricType) \
        -> List[Tuple[Dict[str, Any], int]]:
    """Returns all unique supervision metrics for the given time bucket and
    combination.

    First, includes an event-based count for the month or year the
    SupervisionTimeBucket represents. If the bucket represents a month, then the
    metric period is 1 month. If the bucket represents a year, then the metric
    period is None. If this bucket of supervision should be included in the
    person-based count for the month when the supervision occurred, adds those
    person-based metrics. Finally, returns metrics for each of the metric period
    lengths that this event falls into if this event should be included in the
    person-based count for that metric period length.

    Args:
        combo: A characteristic combination to convert into metrics
        supervision_time_bucket: The SupervisionTimeBucket from which the
            combination was derived
        metric_period_end_date: The day the metric periods end
        periods_and_buckets: Dictionary mapping metric period month lengths to
            the SupervisionTimeBuckets that fall in that period
        all_supervision_time_buckets: All of this person's
            SupervisionTimeBuckets
        metric_type: The type of metric being tracked by this combo

    Returns:
        A list of key-value tuples representing specific metric combination
            dictionaries and the the metric value corresponding to that metric.
    """
    metrics: List[Tuple[Dict[str, Any], int]] = []

    bucket_year = supervision_time_bucket.year
    bucket_month = supervision_time_bucket.month

    # Don't set a metric_period_months value if this is a year bucket
    base_metric_period = 0 if bucket_month is None else 1

    # Add event-based combos for the base metric period of the month or year
    # of the bucket
    event_based_same_bucket_combos = augmented_combo_list(
        combo, supervision_time_bucket.state_code,
        bucket_year, bucket_month,
        MetricMethodologyType.EVENT, base_metric_period)

    # The default value for all combos is 1
    event_combo_value = 1

    if metric_type == SupervisionMetricType.SUCCESS and \
            isinstance(supervision_time_bucket,
                       ProjectedSupervisionCompletionBucket):
        # Set 1 for successful completion, 0 for unsuccessful completion
        event_combo_value = 1 if \
            supervision_time_bucket.successful_completion else 0
    elif metric_type == SupervisionMetricType.ASSESSMENT_CHANGE and \
            isinstance(supervision_time_bucket,
                       SupervisionTerminationBucket):
        if supervision_time_bucket.assessment_score_change is not None:
            # Only include this combo if there is an assessment score change
            # associated with this termination. Set the value as the
            # assessment score change
            event_combo_value = \
                supervision_time_bucket.assessment_score_change
        else:
            # The only metric relying on the SupervisionTerminationBuckets
            # is the TerminatedSupervisionAssessmentScoreChangeMetric. So,
            # if there's no recorded assessment score change on this
            # termination, don't include it in any of the metrics.
            return metrics

    if metric_type != SupervisionMetricType.REVOCATION_ANALYSIS or \
            combo.get('person_external_id') is None:
        # If this is a revocation analysis metric, only include it if it's not
        # a person-level metric
        for event_combo in event_based_same_bucket_combos:
            metrics.append((event_combo, event_combo_value))

    # Create the person-based combos for the base metric period of the month
    # or year of the bucket
    person_based_same_bucket_combos = augmented_combo_list(
        combo, supervision_time_bucket.state_code,
        bucket_year, bucket_month,
        MetricMethodologyType.PERSON, base_metric_period
    )

    buckets_in_period: List[SupervisionTimeBucket] = []

    if metric_type == SupervisionMetricType.POPULATION:
        # Get all other supervision time buckets for the same month as
        # this one
        buckets_in_period = [
            bucket for bucket in all_supervision_time_buckets
            if (isinstance(bucket,
                           (RevocationReturnSupervisionTimeBucket,
                            NonRevocationReturnSupervisionTimeBucket)))
            and bucket.year == bucket_year and
            bucket.month == bucket_month
        ]
    elif metric_type == SupervisionMetricType.REVOCATION:
        # Get all other revocation supervision buckets for the same month as
        # this one
        buckets_in_period = [
            bucket for bucket in all_supervision_time_buckets
            if isinstance(bucket,
                          RevocationReturnSupervisionTimeBucket)
            and bucket.year == bucket_year and
            bucket.month == bucket_month
        ]
    elif metric_type == SupervisionMetricType.SUCCESS:
        # Get all other projected completion buckets for the same month
        # as this one
        buckets_in_period = [
            bucket for bucket in all_supervision_time_buckets
            if isinstance(bucket,
                          ProjectedSupervisionCompletionBucket)
            and bucket.year == bucket_year and
            bucket.month == bucket_month
        ]
    elif metric_type == SupervisionMetricType.ASSESSMENT_CHANGE:
        # Get all other termination buckets for the same month as this
        # one
        buckets_in_period = [
            bucket for bucket in all_supervision_time_buckets
            if isinstance(bucket,
                          SupervisionTerminationBucket)
            and bucket.year == bucket_year and
            bucket.month == bucket_month
        ]
    elif metric_type == SupervisionMetricType.REVOCATION_ANALYSIS:
        # If this is a person-level metric, only include it if it falls in the
        # 1-month metric period
        if combo.get('person_external_id') is None or (
                supervision_time_bucket.year == metric_period_end_date.year and
                supervision_time_bucket.month == metric_period_end_date.month):
            # Get all other revocation supervision buckets for the same month as
            # this one
            buckets_in_period = [
                bucket for bucket in all_supervision_time_buckets
                if isinstance(bucket,
                              RevocationReturnSupervisionTimeBucket)
                and bucket.year == bucket_year and
                bucket.month == bucket_month
            ]

    if buckets_in_period and include_supervision_in_count(
            combo,
            supervision_time_bucket,
            buckets_in_period,
            metric_type):
        person_combo_value = _person_combo_value(
            combo, supervision_time_bucket, buckets_in_period, metric_type
        )

        # Include this event in the person-based count
        for person_combo in person_based_same_bucket_combos:
            metrics.append((person_combo, person_combo_value))

    period_end_year = metric_period_end_date.year
    period_end_month = metric_period_end_date.month

    for period_length, buckets_in_period in periods_and_buckets.items():
        if supervision_time_bucket in buckets_in_period:
            # This event falls within this metric period
            person_based_period_combos = augmented_combo_list(
                combo, supervision_time_bucket.state_code,
                period_end_year, period_end_month,
                MetricMethodologyType.PERSON, period_length
            )

            relevant_buckets_in_period: List[SupervisionTimeBucket] = []

            if metric_type == SupervisionMetricType.ASSESSMENT_CHANGE:
                # Get all other supervision time buckets for this period that
                # should contribute to an assessment change metric
                relevant_buckets_in_period = [
                    bucket for bucket in buckets_in_period
                    if (isinstance(bucket, SupervisionTerminationBucket))
                ]
            elif metric_type == SupervisionMetricType.POPULATION:
                # Get all other supervision time buckets for this period that
                # should contribute to a population metric
                relevant_buckets_in_period = [
                    bucket for bucket in buckets_in_period
                    if (isinstance(bucket,
                                   (RevocationReturnSupervisionTimeBucket,
                                    NonRevocationReturnSupervisionTimeBucket)))
                ]
            elif metric_type in (SupervisionMetricType.REVOCATION,
                                 SupervisionMetricType.REVOCATION_ANALYSIS):
                # Get all other supervision time buckets for this period that
                # should contribute to a revocation metric
                relevant_buckets_in_period = [
                    bucket for bucket in buckets_in_period
                    if isinstance(bucket,
                                  RevocationReturnSupervisionTimeBucket)
                ]
            elif metric_type == SupervisionMetricType.SUCCESS:
                # Get all other supervision time buckets for this period that
                # should contribute to a supervision success metric
                relevant_buckets_in_period = [
                    bucket for bucket in buckets_in_period
                    if (isinstance(bucket,
                                   ProjectedSupervisionCompletionBucket))
                ]

            if relevant_buckets_in_period and include_supervision_in_count(
                    combo,
                    supervision_time_bucket,
                    relevant_buckets_in_period,
                    metric_type):

                person_combo_value = _person_combo_value(
                    combo, supervision_time_bucket, relevant_buckets_in_period,
                    metric_type
                )

                # Include this event in the person-based count
                for person_combo in person_based_period_combos:
                    metrics.append((person_combo, person_combo_value))

    return metrics


def include_supervision_in_count(combo: Dict[str, Any],
                                 supervision_time_bucket: SupervisionTimeBucket,
                                 all_buckets_in_period:
                                 List[SupervisionTimeBucket],
                                 metric_type: SupervisionMetricType) -> bool:
    """Determines whether the given supervision_time_bucket should be included
    in a person-based count given the other buckets in the period.

    If the combo has a value for the key 'supervision_type', this means that
    this will contribute to a metric that is specific to a given supervision
    type. The person-based count for this metric should only be with respect
    to other buckets that share the same supervision-type. If the combo is not
    for a supervision-type-specific metric, then the person-based count should
    take into account all buckets in the period.

    If the metric is of type POPULATION, and there are buckets that represent
    revocation in that period, then this bucket is included only if it
    is the last instance of revocation for the period. However, if none of the
    buckets represent revocation, then this bucket is included if it is the last
    bucket in the period. If the metric is of type REVOCATION, SUCCESS, or
    ASSESSMENT_CHANGE, then this bucket is included only if it is the last
    bucket in the period.

    This function assumes that the SupervisionTimeBuckets in
    all_buckets_in_period are of the same type and that the list is sorted in
    ascending order by year and month.
    """
    supervision_type_specific_metric = combo.get('supervision_type') is not None

    # If the combination specifies the supervision type, then remove any
    # buckets of other supervision types
    relevant_buckets = [
        bucket for bucket in all_buckets_in_period
        if not (supervision_type_specific_metric and bucket.supervision_type !=
                supervision_time_bucket.supervision_type)
    ]

    if metric_type == SupervisionMetricType.POPULATION:
        revocation_buckets = [
            bucket for bucket in relevant_buckets
            if isinstance(bucket, RevocationReturnSupervisionTimeBucket)
        ]

        if revocation_buckets:
            # If there are SupervisionTimeBuckets that are of type
            # RevocationReturnSupervisionTimeBucket, then we want to include
            # that bucket in the counts over any
            # NonRevocationReturnSupervisionTimeBucket. This ensures that the
            # supervision information (supervision_type, district, officer, etc)
            # for the revocation metrics will have corresponding population
            # instances
            return id(supervision_time_bucket) == id(revocation_buckets[-1])

        return id(supervision_time_bucket) == id(relevant_buckets[-1])

    if metric_type in (SupervisionMetricType.REVOCATION,
                       SupervisionMetricType.REVOCATION_ANALYSIS,
                       SupervisionMetricType.SUCCESS,
                       SupervisionMetricType.ASSESSMENT_CHANGE):

        return id(supervision_time_bucket) == id(relevant_buckets[-1])

    return False


def _person_combo_value(combo: Dict[str, Any],
                        supervision_time_bucket: SupervisionTimeBucket,
                        all_buckets_in_period: List[SupervisionTimeBucket],
                        metric_type: SupervisionMetricType) -> int:
    """Determines what the value should be for a person-based metric given the
    combo, the supervision_time_bucket, the buckets in the period, and the
    type of metric this combo will be contributing to.

    All values will be 1 for the POPULATION and REVOCATION count metrics,
    because the presence of a SupervisionTimeBucket for a given time bucket
    implies that the person was counted towards the supervision population for
    that time bucket, and possibly that the person was counted towards the
    revoked population for that same time bucket.

    The value for ASSESSMENT_CHANGE metrics will be the assessment_score_change
    on the given supervision_time_bucket.

    The value for the SUCCESS metrics is 1 for a successful completion, and 0
    for an unsuccessful completion. For any combos that do not specify
    supervision type, the success value on this combo should only be 1 if all
    supervisions ending that month were successful. For any combos that do
    specify supervision type, the success value on this combo should only be 1
    if all other supervisions of the same type ending in that month were
    successful.
    """
    person_combo_value = 1

    if metric_type == SupervisionMetricType.SUCCESS and \
            isinstance(supervision_time_bucket,
                       ProjectedSupervisionCompletionBucket):

        supervision_type_specific_metric = combo.get(
            'supervision_type') is not None

        # If the combination specifies the supervision type, then remove any
        # buckets of other supervision types
        relevant_buckets = [
            bucket for bucket in all_buckets_in_period
            if
            not (supervision_type_specific_metric and bucket.supervision_type !=
                 supervision_time_bucket.supervision_type)
        ]

        for completion_bucket in relevant_buckets:
            if isinstance(completion_bucket,
                          ProjectedSupervisionCompletionBucket) and not \
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
    """Returns a dictionary mapping metric period month values to
    the corresponding relevant SupervisionTimeBuckets."""
    periods_and_buckets: Dict[int, List[SupervisionTimeBucket]] = defaultdict()

    # Organize the month buckets by the relevant metric periods
    for supervision_time_bucket in supervision_time_buckets:
        if supervision_time_bucket.month is not None:
            bucket_start_date = \
                date(supervision_time_bucket.year,
                     supervision_time_bucket.month,
                     1)

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
