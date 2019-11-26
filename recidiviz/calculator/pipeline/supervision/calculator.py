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
"""Calculates supervision metrics from supervision months.

This contains the core logic for calculating supervision metrics on a
person-by-person basis. It transforms SupervisionMonths into supervision
metrics, key-value pairs where the key represents all of the dimensions
represented in the data point, and the value represents an indicator of whether
the person should contribute to that metric.
"""
import json
from copy import deepcopy
from datetime import date
from typing import Dict, List, Tuple, Any

from recidiviz.calculator.pipeline.supervision.supervision_month import \
    SupervisionMonth
from recidiviz.calculator.pipeline.utils.calculator_utils import age_at_date, \
    age_bucket, for_characteristics_races_ethnicities, for_characteristics
from recidiviz.calculator.pipeline.supervision.metrics import \
    SupervisionMetricType
from recidiviz.calculator.pipeline.utils.metric_utils import \
    MetricMethodologyType, json_serializable_metric_key
from recidiviz.persistence.entity.state.entities import StatePerson


def map_supervision_combinations(person: StatePerson,
                                 supervision_months: List[SupervisionMonth],
                                 inclusions: Dict[str, bool]) \
        -> List[Tuple[Dict[str, Any], Any]]:
    """Transforms SupervisionMonths and a StatePerson into metric combinations.

    Takes in a StatePerson and all of her SupervisionMonths and returns an array
    of "supervision combinations". These are key-value pairs where the key
    represents a specific metric and the value represents whether or not
    the person should be counted as a positive instance of that metric.

    This translates a particular month on supervision into many different
    supervision population metrics. Each metric represents one of many possible
    combinations of characteristics being tracked for that event. For example,
    if a White male is on supervision, there is a metric that corresponds to
    White people, one to males, one to White males, one to all people, and more
    depending on other dimensions in the data.

    Args:
        person: the StatePerson
        supervision_months: A list of SupervisionMonths for the given
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

    for supervision_month in supervision_months:
        characteristic_combos_population = \
            characteristic_combinations(person, supervision_month, inclusions)

        population_metrics_event_based = map_population_combinations(
            characteristic_combos_population, supervision_month
        )

        event_based_metrics.extend(population_metrics_event_based)

    metrics.extend(event_based_metrics)

    # Convert the event-based population metrics to person-based
    person_based_metrics = convert_event_based_to_person_based_metrics(
        deepcopy(event_based_metrics)
    )

    metrics.extend(person_based_metrics)

    return metrics


def characteristic_combinations(person: StatePerson,
                                supervision_month: SupervisionMonth,
                                inclusions: Dict[str, bool]) -> \
        List[Dict[str, Any]]:
    """Calculates all supervision metric combinations.

    Returns the list of all combinations of the metric characteristics, of all
    sizes, given the StatePerson and SupervisionMonth. That is, this returns a
    list of dictionaries where each dictionary is a combination of 0 to n unique
    elements of characteristics, where n is the number of keys in the given
    inclusions bucket that are set to True + 1 dimension for the supervision
    type (which is always included).

    For each event, we need to calculate metrics across combinations of:
    MetricMethodologyType (Event-based, Person-based);
    Demographics (age, race, ethnicity, gender);

    Methodology is not included in the output here. It is added into augmented
    versions of these combinations later.

    Args:
        person: the StatePerson we are picking characteristics from
        supervision_month: the SupervisionMonth we are picking characteristics
            from
        inclusions: A dictionary containing the following keys that correspond
            to characteristic dimensions:
                - age_bucket
                - ethnicity
                - gender
                - race
            Where the values are boolean flags indicating whether to include
            the dimension in the calculations.

    Returns:
        A list of dictionaries containing all unique combinations of
        characteristics.
    """

    characteristics: Dict[str, Any] = {}

    if supervision_month.supervision_type:
        characteristics['supervision_type'] = supervision_month.supervision_type
    if inclusions.get('age_bucket'):
        start_of_month = \
            date(supervision_month.year, supervision_month.month, 1)
        entry_age = age_at_date(person, start_of_month)
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


def map_population_combinations(
        characteristic_combos: List[Dict[str, Any]],
        supervision_month: SupervisionMonth) -> \
        List[Tuple[Dict[str, Any], Any]]:
    """Maps the given month and characteristic combinations to a variety of
    metrics that track supervision populations.

    All values will be 1 for population metrics, because the presence of a
    SupervisionMonth for a given month implies that the person was counted
    towards the supervision population for that month.

    Args:
        characteristic_combos: A list of dictionaries containing all unique
            combinations of characteristics.
        supervision_month: the month on supervision from which the combination
            was derived

    Returns:
        A list of key-value tuples representing specific metric combinations and
        the population value corresponding to that metric.
    """
    metrics = []

    year = supervision_month.year
    month = supervision_month.month
    state_code = supervision_month.state_code

    for combo in characteristic_combos:
        combo['metric_type'] = SupervisionMetricType.POPULATION.value
        combo['state_code'] = state_code
        combo['year'] = year
        combo['month'] = month
        combo['methodology'] = MetricMethodologyType.EVENT

        metrics.append((combo, 1))

    return metrics


def convert_event_based_to_person_based_metrics(
        metrics: List[Tuple[Dict[str, Any], Any]]) -> \
        List[Tuple[Dict[str, Any], Any]]:
    """
    Takes in a set of event-based metrics and converts them to be person-based
    by removing any duplicate metric dictionaries attributed to this person.

    By eliminating duplicate instances of metric keys, this person will only
    contribute a +1 to a metric once per metric for all person-based counts.
    """

    person_based_metrics_set = set()

    for metric, value in metrics:
        metric['methodology'] = MetricMethodologyType.PERSON
        # Converting the metric key to a JSON string so it is hashable
        serializable_dict = json_serializable_metric_key(metric)
        json_key = json.dumps(serializable_dict, sort_keys=True)
        # Add the metric to the set
        person_based_metrics_set.add((json_key, value))

    person_based_metrics: List[Tuple[Dict[str, Any], Any]] = []

    for json_metric, value in person_based_metrics_set:
        # Convert JSON string to dictionary
        dict_metric_key = json.loads(json_metric)

        person_based_metrics.append((dict_metric_key, 1))

    return person_based_metrics
