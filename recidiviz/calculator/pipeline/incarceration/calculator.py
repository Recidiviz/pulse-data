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
"""Calculates incarceration metrics from incarceration events.

This contains the core logic for calculating incarceration metrics on a
person-by-person basis. It transforms IncarcerationEvents into incarceration
metrics, key-value pairs where the key represents all of the dimensions
represented in the data point, and the value represents an indicator of whether
the person should contribute to that metric.
"""
from copy import deepcopy
from datetime import date
from typing import List, Dict, Tuple, Any, Type

from recidiviz.calculator.pipeline.incarceration.incarceration_event import \
    IncarcerationEvent, IncarcerationAdmissionEvent, IncarcerationReleaseEvent
from recidiviz.calculator.pipeline.incarceration.metrics import \
    IncarcerationMetricType
from recidiviz.calculator.pipeline.utils.calculator_utils import age_at_date, \
    age_bucket, for_characteristics_races_ethnicities, for_characteristics, \
    convert_event_based_to_person_based_metrics
from recidiviz.calculator.pipeline.utils.metric_utils import \
    MetricMethodologyType
from recidiviz.persistence.entity.state.entities import StatePerson


METRIC_TYPES: Dict[Type[IncarcerationEvent], IncarcerationMetricType] = {
    IncarcerationAdmissionEvent: IncarcerationMetricType.ADMISSION,
    IncarcerationReleaseEvent: IncarcerationMetricType.RELEASE
}


def map_incarceration_combinations(person: StatePerson,
                                   incarceration_events:
                                   List[IncarcerationEvent],
                                   inclusions: Dict[str, bool]) \
        -> List[Tuple[Dict[str, Any], Any]]:
    """Transforms IncarcerationEvents and a StatePerson into metric
    combinations.

    Takes in a StatePerson and all of their IncarcerationEvent and returns an
    array of "incarceration combinations". These are key-value pairs where the
    key represents a specific metric and the value represents whether or not
    the person should be counted as a positive instance of that metric.

    This translates a particular incarceration event, e.g. admission or release,
    into many different incarceration metrics. Each metric represents one of
    many possible combinations of characteristics being tracked for that event.
    For example, if a White male is admitted to prison, there is a metric that
    corresponds to White people, one to males, one to White males, one to all
    people, and more depending on other dimensions in the data.

    Args:
        person: the StatePerson
        incarceration_events: A list of IncarcerationEvents for the given
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

    for incarceration_event in incarceration_events:
        characteristic_combos = characteristic_combinations(
            person, incarceration_event, inclusions)

        metric_type = METRIC_TYPES.get(type(incarceration_event))
        if not metric_type:
            raise ValueError(
                'No metric type mapped to incarceration event '
                'of type {}'.format(type(incarceration_event)))

        incarceration_admission_metrics_event_based = \
            map_metric_combinations(
                characteristic_combos, incarceration_event,
                metric_type
            )
        event_based_metrics.extend(
            incarceration_admission_metrics_event_based)

    metrics.extend(event_based_metrics)

    # Convert the event-based incarceration metrics to person-based
    person_based_metrics = convert_event_based_to_person_based_metrics(
        deepcopy(event_based_metrics)
    )

    metrics.extend(person_based_metrics)

    return metrics


def characteristic_combinations(person: StatePerson,
                                incarceration_event: IncarcerationEvent,
                                inclusions: Dict[str, bool]) -> \
        List[Dict[str, Any]]:
    """Calculates all incarceration metric combinations.

    Returns the list of all combinations of the metric characteristics, of all
    sizes, given the StatePerson and IncarcerationEvent. That is, this
    returns a list of dictionaries where each dictionary is a combination of 0
    to n unique elements of characteristics, where n is the number of keys in
    the given inclusions dictionary that are set to True + the dimensions for
    the given type of event.

    For each event, we need to calculate metrics across combinations of:
    MetricMethodologyType (Event-based, Person-based);
    Demographics (age, race, ethnicity, gender);

    Methodology is not included in the output here. It is added into augmented
    versions of these combinations later.

    Args:
        person: the StatePerson we are picking characteristics from
        incarceration_event: the IncarcerationEvent we are picking
            characteristics from
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

    # Always include facility as a dimension
    if incarceration_event.facility:
        characteristics['facility'] = incarceration_event.facility

    if inclusions.get('age_bucket'):
        year = incarceration_event.year
        month = incarceration_event.month

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
        incarceration_event: IncarcerationEvent,
        metric_type: IncarcerationMetricType) -> \
        List[Tuple[Dict[str, Any], Any]]:
    """Maps the given time bucket and characteristic combinations to a variety
     of metrics that track incarceration admission and release counts.

     All values will be 1 for these count metrics, because the presence of an
     IncarcerationEvent for a given month implies that the person was
     counted towards the admission or release for that month.

     Args:
         characteristic_combos: A list of dictionaries containing all unique
             combinations of characteristics.
         incarceration_event: The incarceration event from which
             the combination was derived.
         metric_type: The metric type to set on each combination

     Returns:
         A list of key-value tuples representing specific metric combinations
        and the metric value corresponding to that metric.
     """

    metrics = []

    year = incarceration_event.year
    month = incarceration_event.month
    state_code = incarceration_event.state_code

    for combo in characteristic_combos:
        combo['metric_type'] = metric_type.value
        combo['state_code'] = state_code
        combo['year'] = year
        combo['month'] = month
        combo['methodology'] = MetricMethodologyType.EVENT

        if isinstance(incarceration_event, IncarcerationAdmissionEvent):
            combo['admission_reason'] = incarceration_event.admission_reason

        if isinstance(incarceration_event, IncarcerationReleaseEvent):
            combo['release_reason'] = incarceration_event.release_reason

        metrics.append((combo, 1))

    return metrics
