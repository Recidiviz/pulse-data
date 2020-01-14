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
from copy import deepcopy
from datetime import date
from typing import Dict, List, Tuple, Any, Optional

from recidiviz.calculator.pipeline.supervision.supervision_time_bucket import \
    SupervisionTimeBucket, RevocationReturnSupervisionTimeBucket, \
    ProjectedSupervisionCompletionBucket
from recidiviz.calculator.pipeline.utils.calculator_utils import age_at_date, \
    age_bucket, for_characteristics_races_ethnicities, for_characteristics, \
    convert_event_based_to_person_based_metrics, assessment_score_bucket
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

    event_based_metrics: List[Tuple[Dict[str, Any], Any]] = []

    organized_projected_completion_buckets = \
        _organize_projected_completion_buckets_by_month(
            supervision_time_buckets)

    for supervision_time_bucket in supervision_time_buckets:
        characteristic_combos_population = \
            characteristic_combinations(
                person, supervision_time_bucket, inclusions)

        if isinstance(supervision_time_bucket,
                      ProjectedSupervisionCompletionBucket):
            supervision_success_metrics_event_based = map_metric_combinations(
                characteristic_combos_population, supervision_time_bucket,
                SupervisionMetricType.SUCCESS,
                organized_projected_completion_buckets)

            event_based_metrics.extend(supervision_success_metrics_event_based)
        else:
            characteristic_combos_revocation = \
                characteristic_combinations(
                    person, supervision_time_bucket, inclusions,
                    with_revocation_dimensions=True)

            population_metrics_event_based = map_metric_combinations(
                characteristic_combos_population, supervision_time_bucket,
                SupervisionMetricType.POPULATION)

            event_based_metrics.extend(population_metrics_event_based)

            if isinstance(supervision_time_bucket,
                          RevocationReturnSupervisionTimeBucket):
                revocation_metrics_event_based = map_metric_combinations(
                    characteristic_combos_revocation, supervision_time_bucket,
                    SupervisionMetricType.REVOCATION)

                event_based_metrics.extend(revocation_metrics_event_based)

    metrics.extend(event_based_metrics)

    # Convert the event-based population metrics to person-based
    person_based_metrics = convert_event_based_to_person_based_metrics(
        deepcopy(event_based_metrics)
    )

    metrics.extend(person_based_metrics)

    return metrics


def characteristic_combinations(person: StatePerson,
                                supervision_time_bucket: SupervisionTimeBucket,
                                inclusions: Dict[str, bool],
                                with_revocation_dimensions: bool = False) -> \
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

    Returns:
        A list of dictionaries containing all unique combinations of
        characteristics.
    """

    characteristics: Dict[str, Any] = {}

    if with_revocation_dimensions and \
            isinstance(supervision_time_bucket,
                       RevocationReturnSupervisionTimeBucket):
        if supervision_time_bucket.revocation_type:
            characteristics['revocation_type'] = \
                supervision_time_bucket.revocation_type

        if supervision_time_bucket.source_violation_type:
            characteristics['source_violation_type'] = \
                supervision_time_bucket.source_violation_type

    if supervision_time_bucket.supervision_type:
        characteristics['supervision_type'] = \
            supervision_time_bucket.supervision_type
    if supervision_time_bucket.assessment_score and \
            supervision_time_bucket.assessment_type:
        characteristics['assessment_score_bucket'] = \
            assessment_score_bucket(supervision_time_bucket.assessment_score,
                                    supervision_time_bucket.assessment_type)
        characteristics['assessment_type'] = \
            supervision_time_bucket.assessment_type
    if supervision_time_bucket.supervising_officer_external_id:
        characteristics['supervising_officer_external_id'] = \
            supervision_time_bucket.supervising_officer_external_id

    if supervision_time_bucket.supervising_district_external_id:
        characteristics['supervising_district_external_id'] = \
            supervision_time_bucket.supervising_district_external_id
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

        return for_characteristics_races_ethnicities(
            races, ethnicities, characteristics)

    return for_characteristics(characteristics)


def map_metric_combinations(
        characteristic_combos: List[Dict[str, Any]],
        supervision_time_bucket: SupervisionTimeBucket,
        metric_type: SupervisionMetricType,
        organized_projected_completion_buckets:
        Optional[Dict[Tuple[int, int],
                      List[ProjectedSupervisionCompletionBucket]]] = None) -> \
        List[Tuple[Dict[str, Any], Any]]:
    """Maps the given time bucket and characteristic combinations to a variety
    of metrics that track supervision population and revocation counts.

    All values will be 1 for these count metrics, because the presence of a
    SupervisionTimeBucket for a given time bucket implies that the person was
    counted towards the supervision population for that time bucket, and
    possibly that the person was counted towards the revoked population for
    that same time bucket.

    The value for the SUCCESS metrics is 1 for a successful completion, and 0
    for an unsuccessful completion. For any combos that do not specify
    supervision type, the success value on this combo should only be 1 if all
    supervisions ending that month were successful.

    Args:
        characteristic_combos: A list of dictionaries containing all unique
            combinations of characteristics.
        supervision_time_bucket: The time bucket on supervision from which
            the combination was derived.
        metric_type: The metric type to set on each combination
        organized_projected_completion_buckets: A dictionary that organizes the
            projected supervision completion buckets by year and month of
            projected completion

    Returns:
        A list of key-value tuples representing specific metric combinations and
        the metric value corresponding to that metric.
    """
    metrics = []

    year = supervision_time_bucket.year
    month = supervision_time_bucket.month
    state_code = supervision_time_bucket.state_code

    for combo in characteristic_combos:
        combo['metric_type'] = metric_type.value
        combo['state_code'] = state_code
        combo['year'] = year
        combo['month'] = month
        combo['methodology'] = MetricMethodologyType.EVENT

        if isinstance(supervision_time_bucket,
                      ProjectedSupervisionCompletionBucket) and \
                organized_projected_completion_buckets:
            # Note: Month should never be None for this kind of bucket
            if combo.get('supervision_type') is None and month is not None:
                # If this metric doesn't specify the supervision type, then
                # the success value on this combo should only be 1 if all
                # supervisions ending this month were successful
                completion_buckets_this_month = \
                    organized_projected_completion_buckets[(year, month)]

                success_value = 1
                for completion_bucket in completion_buckets_this_month:
                    if not completion_bucket.successful_completion:
                        success_value = 0

                metrics.append((combo, success_value))
            else:
                # Set 1 for successful completion, 0 for unsuccessful completion
                if supervision_time_bucket.successful_completion:
                    metrics.append((combo, 1))
                else:
                    metrics.append((combo, 0))
        else:
            metrics.append((combo, 1))

    return metrics


def _organize_projected_completion_buckets_by_month(
        supervision_time_buckets: List[SupervisionTimeBucket]) -> \
        Dict[Tuple[int, int], List[ProjectedSupervisionCompletionBucket]]:
    """Returns a dictionary of ProjectedSupervisionCompletionBuckets ordered
    by year and month of projected completion."""

    organized_buckets: \
        Dict[Tuple[int, int], List[ProjectedSupervisionCompletionBucket]] = {}

    projected_completion_buckets = [
        bucket for bucket in supervision_time_buckets if
        isinstance(bucket, ProjectedSupervisionCompletionBucket)
    ]

    for projected_completion_bucket in projected_completion_buckets:
        year = projected_completion_bucket.year
        month = projected_completion_bucket.month
        # Month should never be None for this kind of bucket
        if month is not None:
            if (year, month) in organized_buckets.keys():
                organized_buckets[(year, month)].append(
                    projected_completion_bucket)
            else:
                organized_buckets[(year, month)] = [projected_completion_bucket]

    return organized_buckets
