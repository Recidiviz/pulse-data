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

This contains the core logic for calculating incarceration metrics on a person-by-person basis. It transforms
IncarcerationEvents into incarceration metrics, key-value pairs where the key represents all of the dimensions
represented in the data point, and the value represents an indicator of whether the person should contribute to that
metric.
"""
from collections import defaultdict
from datetime import date
from typing import List, Dict, Tuple, Any, Type, Sequence, Optional

from recidiviz.calculator.pipeline.incarceration.incarceration_event import \
    IncarcerationEvent, IncarcerationAdmissionEvent,\
    IncarcerationReleaseEvent, IncarcerationStayEvent
from recidiviz.calculator.pipeline.incarceration.metrics import \
    IncarcerationMetricType
from recidiviz.calculator.pipeline.utils.calculator_utils import for_characteristics_races_ethnicities,\
    for_characteristics, last_day_of_month, relevant_metric_periods, augmented_combo_list, \
    get_calculation_month_lower_bound_date, include_in_monthly_metrics, characteristics_with_person_id_fields, \
    add_demographic_characteristics
from recidiviz.calculator.pipeline.utils.metric_utils import \
    MetricMethodologyType
from recidiviz.common.constants.state.state_incarceration_period import is_revocation_admission
from recidiviz.persistence.entity.state.entities import StatePerson


METRIC_TYPES: Dict[Type[IncarcerationEvent], IncarcerationMetricType] = {
    IncarcerationAdmissionEvent: IncarcerationMetricType.ADMISSION,
    IncarcerationStayEvent: IncarcerationMetricType.POPULATION,
    IncarcerationReleaseEvent: IncarcerationMetricType.RELEASE
}


def map_incarceration_combinations(person: StatePerson,
                                   incarceration_events:
                                   List[IncarcerationEvent],
                                   metric_inclusions: Dict[IncarcerationMetricType, bool],
                                   calculation_month_limit: int) -> List[Tuple[Dict[str, Any], Any]]:
    """Transforms IncarcerationEvents and a StatePerson into metric combinations.

    Takes in a StatePerson and all of their IncarcerationEvent and returns an array of "incarceration combinations".
    These are key-value pairs where the key represents a specific metric and the value represents whether or not
    the person should be counted as a positive instance of that metric.

    This translates a particular incarceration event, e.g. admission or release, into many different incarceration
    metrics. Each metric represents one of many possible combinations of characteristics being tracked for that event.
    For example, if a White male is admitted to prison, there is a metric that corresponds to White people, one to
    males, one to White males, one to all people, and more depending on other dimensions in the data.

    Args:
        person: the StatePerson
        incarceration_events: A list of IncarcerationEvents for the given StatePerson.
        metric_inclusions: A dictionary where the keys are each IncarcerationMetricType, and the values are boolean
            flags for whether or not to include that metric type in the calculations
        calculation_month_limit: The number of months (including this one) to limit the monthly calculation output to.
            If set to -1, does not limit the calculations.
    Returns:
        A list of key-value tuples representing specific metric combinations and the value corresponding to that metric.
    """
    metrics: List[Tuple[Dict[str, Any], Any]] = []

    periods_and_events: Dict[int, List[IncarcerationEvent]] = defaultdict()

    # We will calculate person-based metrics for each metric period in METRIC_PERIOD_MONTHS ending with the current
    # month
    metric_period_end_date = last_day_of_month(date.today())

    calculation_month_lower_bound = get_calculation_month_lower_bound_date(
        metric_period_end_date, calculation_month_limit)

    # Organize the events by the relevant metric periods
    for incarceration_event in incarceration_events:
        relevant_periods = relevant_metric_periods(
            incarceration_event.event_date,
            metric_period_end_date.year,
            metric_period_end_date.month)

        if relevant_periods:
            for period in relevant_periods:
                period_events = periods_and_events.get(period)

                if period_events:
                    period_events.append(incarceration_event)
                else:
                    periods_and_events[period] = [incarceration_event]

    for incarceration_event in incarceration_events:

        metric_type = METRIC_TYPES.get(type(incarceration_event))
        if not metric_type:
            raise ValueError(
                'No metric type mapped to incarceration event of type {}'.format(type(incarceration_event)))

        if metric_inclusions.get(metric_type):
            characteristic_combos = characteristic_combinations(person, incarceration_event, metric_type)

            metrics.extend(map_metric_combinations(
                characteristic_combos, incarceration_event,
                metric_period_end_date, calculation_month_lower_bound, incarceration_events,
                periods_and_events, metric_type
            ))

    return metrics


def characteristic_combinations(person: StatePerson,
                                incarceration_event: IncarcerationEvent,
                                metric_type: IncarcerationMetricType) -> List[Dict[str, Any]]:
    """Calculates all incarceration metric combinations.

    Returns the list of all combinations of the metric characteristics, of all sizes, given the StatePerson and
    IncarcerationEvent. That is, this returns a list of dictionaries where each dictionary is a combination of 0
    to n unique elements of characteristics applicable to the given |person| and |incarceration_event|.

    Args:
        person: the StatePerson we are picking characteristics from
        incarceration_event: the IncarcerationEvent we are picking characteristics from
        metric_type: The IncarcerationMetricType that determines which fields should be added to the characteristics
            dictionary

    Returns:
        A list of dictionaries containing all unique combinations of characteristics.
    """
    # TODO(3058): Limit the output of this function to just the person_level_characteristic_dict dictionary
    #  for all metrics that currently include person-level output
    characteristics: Dict[str, Any] = {}

    # Add characteristics that will be used to generate dictionaries with unique combinations.
    if isinstance(incarceration_event, IncarcerationAdmissionEvent):
        if incarceration_event.admission_reason:
            characteristics['admission_reason'] = incarceration_event.admission_reason
        if incarceration_event.supervision_type_at_admission:
            characteristics['supervision_type_at_admission'] = incarceration_event.supervision_type_at_admission
        if incarceration_event.specialized_purpose_for_incarceration:
            characteristics['specialized_purpose_for_incarceration'] = \
                incarceration_event.specialized_purpose_for_incarceration

    if isinstance(incarceration_event, IncarcerationReleaseEvent):
        if incarceration_event.release_reason:
            characteristics['release_reason'] = incarceration_event.release_reason

    if isinstance(incarceration_event, IncarcerationStayEvent):
        if incarceration_event.admission_reason:
            characteristics['admission_reason'] = incarceration_event.admission_reason
        if incarceration_event.supervision_type_at_admission:
            characteristics['supervision_type_at_admission'] = incarceration_event.supervision_type_at_admission

    # Always include facility as a dimension
    if incarceration_event.facility:
        characteristics['facility'] = incarceration_event.facility

    # Always include county_of_residence as a dimension
    if incarceration_event.county_of_residence:
        characteristics['county_of_residence'] = incarceration_event.county_of_residence

    event_date = incarceration_event.event_date

    if _include_demographic_dimensions_for_metric(metric_type):
        characteristics = add_demographic_characteristics(characteristics, person, event_date)

    if characteristics.get('race') is not None or characteristics.get('ethnicity') is not None:
        all_combinations = for_characteristics_races_ethnicities(characteristics)
    else:
        all_combinations = for_characteristics(characteristics)

    characteristics_with_person_details = add_person_level_characteristics(
        person, incarceration_event, characteristics)

    if metric_type == IncarcerationMetricType.ADMISSION:
        characteristics_with_person_details['admission_date'] = incarceration_event.event_date

    all_combinations.append(characteristics_with_person_details)

    return all_combinations


def add_person_level_characteristics(person, incarceration_event, characteristics):
    """Given |characteristics|, adds new characteristics that do not matter for aggregation and are only important for
    person-level metrics. These characteristics therefore are NOT used to generate the dictionaries with unique
    combinations, but are simply add-ons to those dictionaries.
    """

    characteristics_with_person_details = characteristics_with_person_id_fields(
        characteristics, person, 'incarceration')

    if isinstance(incarceration_event, IncarcerationStayEvent):
        if incarceration_event.most_serious_offense_statute:
            characteristics_with_person_details['most_serious_offense_ncic_code'] = \
                incarceration_event.most_serious_offense_ncic_code
        if incarceration_event.most_serious_offense_statute:
            characteristics_with_person_details['most_serious_offense_statute'] = \
                incarceration_event.most_serious_offense_statute
        if incarceration_event.admission_reason_raw_text:
            characteristics_with_person_details['admission_reason_raw_text'] = \
                incarceration_event.admission_reason_raw_text
    if isinstance(incarceration_event, IncarcerationAdmissionEvent):
        if incarceration_event.admission_reason_raw_text:
            characteristics_with_person_details['admission_reason_raw_text'] = \
                incarceration_event.admission_reason_raw_text
    return characteristics_with_person_details


def map_metric_combinations(
        characteristic_combos: List[Dict[str, Any]],
        incarceration_event: IncarcerationEvent,
        metric_period_end_date: date,
        calculation_month_lower_bound: Optional[date],
        all_incarceration_events: List[IncarcerationEvent],
        periods_and_events: Dict[int, List[IncarcerationEvent]],
        metric_type: IncarcerationMetricType) -> \
        List[Tuple[Dict[str, Any], Any]]:
    """Maps the given time bucket and characteristic combinations to a variety of metrics that track incarceration
    admission and release counts.

     All values will be 1 for these count metrics, because the presence of an IncarcerationEvent for a given month
     implies that the person was counted towards the admission or release for that month.

     Args:
         characteristic_combos: A list of dictionaries containing all unique combinations of characteristics.
         incarceration_event: The incarceration event from which the combination was derived.
         metric_period_end_date: The day the metric periods end
         calculation_month_lower_bound: The date of the first month to be included in the monthly calculations
         all_incarceration_events: All of the person's IncarcerationEvents
         periods_and_events: A dictionary mapping metric period month values to the corresponding relevant
            IncarcerationEvents
         metric_type: The metric type to set on each combination

     Returns:
        A list of key-value tuples representing specific metric combinations and the metric value corresponding to
            that metric.
     """

    metrics = []

    for combo in characteristic_combos:
        combo['metric_type'] = metric_type

        if include_in_monthly_metrics(incarceration_event.event_date.year, incarceration_event.event_date.month,
                                      calculation_month_lower_bound):
            metrics.extend(combination_incarceration_monthly_metrics(
                combo, incarceration_event, all_incarceration_events))

        metrics.extend(combination_incarceration_metric_period_metrics(
            combo, incarceration_event, metric_period_end_date,
            periods_and_events
        ))

    return metrics


def combination_incarceration_monthly_metrics(
        combo: Dict[str, Any],
        incarceration_event: IncarcerationEvent,
        all_incarceration_events: List[IncarcerationEvent]) \
        -> List[Tuple[Dict[str, Any], int]]:
    """Returns all unique incarceration metrics for the given event and combination.

    First, includes an event-based count for the month the event occurred with a metric period of 1 month. Then, if
    this event should be included in the person-based count for the month when the event occurred, adds those person-
    based metrics.

    Args:
        combo: A characteristic combination to convert into metrics
        incarceration_event: The IncarcerationEvent from which the combination was derived
        all_incarceration_events: All of this person's IncarcerationEvents

    Returns:
        A list of key-value tuples representing specific metric combination dictionaries and the number 1 representing
            a positive contribution to that count metric.
    """
    metrics = []

    event_date = incarceration_event.event_date
    event_year = event_date.year
    event_month = event_date.month

    # Add event-based combos for the 1-month period the month of the event
    event_based_same_month_combos = augmented_combo_list(
        combo, incarceration_event.state_code,
        event_year, event_month,
        MetricMethodologyType.EVENT, 1)

    for event_combo in event_based_same_month_combos:
        metrics.append((event_combo, 1))

    # Create the person-based combos for the 1-month period of the month of the event
    person_based_same_month_combos = augmented_combo_list(
        combo, incarceration_event.state_code,
        event_year, event_month,
        MetricMethodologyType.PERSON, 1
    )

    events_in_month: List[IncarcerationEvent] = []

    if isinstance(incarceration_event, IncarcerationAdmissionEvent):
        # All admission events that happened the same month as this one
        events_in_month = [
            event for event in all_incarceration_events
            if isinstance(event, IncarcerationAdmissionEvent) and
            event.event_date.year == event_date.year and
            event.event_date.month == event_date.month
        ]
    elif isinstance(incarceration_event, IncarcerationStayEvent):
        # All stay events that happened the same month as this one
        events_in_month = [
            event for event in all_incarceration_events
            if isinstance(event, IncarcerationStayEvent) and
            event.event_date.year == event_date.year and
            event.event_date.month == event_date.month
        ]
    elif isinstance(incarceration_event, IncarcerationReleaseEvent):
        # All release events that happened the same month as this one
        events_in_month = [
            event for event in all_incarceration_events
            if isinstance(event, IncarcerationReleaseEvent) and
            event.event_date.year == event_date.year and
            event.event_date.month == event_date.month
        ]

    if events_in_month and include_event_in_count(
            incarceration_event,
            last_day_of_month(event_date),
            events_in_month):
        # Include this event in the person-based count
        for person_combo in person_based_same_month_combos:
            metrics.append((person_combo, 1))

    return metrics


def combination_incarceration_metric_period_metrics(
        combo: Dict[str, Any],
        incarceration_event: IncarcerationEvent,
        metric_period_end_date: date,
        periods_and_events: Dict[int, List[IncarcerationEvent]]) \
        -> List[Tuple[Dict[str, Any], int]]:
    """Returns all unique incarceration metrics for the given event, combination, and relevant metric_period_months.

    Returns metrics for each of the metric period length that this event falls into if this event should be included in
    the person-based count for that metric period.

    Args:
        combo: A characteristic combination to convert into metrics
        incarceration_event: The IncarcerationEvent from which the combination was derived
        metric_period_end_date: The day the metric periods end
        periods_and_events: Dictionary mapping metric period month lengths to the IncarcerationEvents that fall in that
            period

    Returns:
        A list of key-value tuples representing specific metric combination dictionaries and the number 1 representing
            a positive contribution to that count metric.
    """
    metrics = []

    period_end_year = metric_period_end_date.year
    period_end_month = metric_period_end_date.month

    for period_length, events_in_period in periods_and_events.items():
        if incarceration_event in events_in_period:
            # This event falls within this metric period
            person_based_period_combos = augmented_combo_list(
                combo, incarceration_event.state_code,
                period_end_year, period_end_month,
                MetricMethodologyType.PERSON, period_length
            )

            related_events_in_period: List[IncarcerationEvent] = []

            if isinstance(incarceration_event, IncarcerationAdmissionEvent):
                related_events_in_period = [
                    event for event in events_in_period
                    if isinstance(event, IncarcerationAdmissionEvent)
                ]
            elif isinstance(incarceration_event, IncarcerationReleaseEvent):
                related_events_in_period = [
                    event for event in events_in_period
                    if isinstance(event, IncarcerationReleaseEvent)
                ]

            if related_events_in_period and include_event_in_count(
                    incarceration_event,
                    metric_period_end_date,
                    related_events_in_period):
                # Include this event in the person-based count for this time period
                for person_combo in person_based_period_combos:
                    metrics.append((person_combo, 1))

    return metrics


def include_event_in_count(incarceration_event: IncarcerationEvent,
                           metric_period_end_date: date,
                           all_events_in_period:
                           Sequence[IncarcerationEvent]) -> bool:
    """Determines whether the given incarceration_event should be included in a person-based count for this given
    calculation_month_upper_bound.

    For the release counts, the last instance of a release before the end of the period is included. For the counts of
    stay events, the last instance of a stay in the period is included.

    For the admission counts, if any of the admissions have an admission_reason indicating a supervision revocation,
    then this incarceration_event is only included if it is the last instance of a revocation admission before the end
    of that period. If none of the admissions in the period are revocation admissions, then this event is included only
    if it is the last instance of an admission before the end of the period.
    """
    events_rest_of_period = incarceration_events_in_period(
        incarceration_event.event_date,
        metric_period_end_date,
        all_events_in_period)

    events_rest_of_period.sort(key=lambda b: b.event_date)

    if isinstance(incarceration_event, IncarcerationReleaseEvent):
        if len(events_rest_of_period) == 1:
            # If this is the last instance of a release before the end of the period, then include it in the
            # person-based count.
            return True
    elif isinstance(incarceration_event, IncarcerationAdmissionEvent):
        revocation_events_in_period = [
            event for event in all_events_in_period
            if isinstance(event, IncarcerationAdmissionEvent) and is_revocation_admission(event.admission_reason)
        ]

        revocation_events_in_period.sort(key=lambda b: b.event_date)

        if (not revocation_events_in_period or
                len(revocation_events_in_period) == len(all_events_in_period)) \
                and len(events_rest_of_period) == 1:
            # If all of the admission events this period are either all revocation or all non-revocation, and this is
            # the last instance of an admission before the end of the period, then include it in the person-based count.
            return True

        if revocation_events_in_period and incarceration_event == \
                revocation_events_in_period[-1]:
            # This person has both revocation and non-revocation admissions during this period. If this is the last
            # revocation admission event, then include it in the person-based count.
            return True
    elif isinstance(incarceration_event, IncarcerationStayEvent):
        # If this is the last recorded event for this month, include it
        if id(incarceration_event) == id(events_rest_of_period[-1]):
            return True

    return False


def incarceration_events_in_period(start_date: date,
                                   end_date: date,
                                   all_incarceration_events:
                                   Sequence[IncarcerationEvent]) -> List[IncarcerationEvent]:
    """Returns all of the events that occurred between the start_date and end_date, inclusive."""
    events_in_period = \
        [event for event in all_incarceration_events if start_date <= event.event_date <= end_date]

    return events_in_period


def _include_demographic_dimensions_for_metric(metric_type: IncarcerationMetricType) -> bool:
    """Returns whether demographic dimensions should be included in metrics of the given metric_type."""
    if metric_type in (
            IncarcerationMetricType.ADMISSION,
            IncarcerationMetricType.POPULATION,
            IncarcerationMetricType.RELEASE
    ):
        return True

    raise ValueError(f"IncarcerationMetricType {metric_type} not handled.")
