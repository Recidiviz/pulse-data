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
    IncarcerationMetricType, IncarcerationMetric, IncarcerationAdmissionMetric, IncarcerationPopulationMetric, \
    IncarcerationReleaseMetric
from recidiviz.calculator.pipeline.utils.calculator_utils import relevant_metric_periods, \
    augmented_combo_for_calculations, get_calculation_month_lower_bound_date, include_in_historical_metrics, \
    get_calculation_month_upper_bound_date, characteristics_dict_builder
from recidiviz.calculator.pipeline.utils.metric_utils import \
    MetricMethodologyType
from recidiviz.calculator.pipeline.utils.person_utils import PersonMetadata
from recidiviz.common.constants.state.state_incarceration_period import is_revocation_admission
from recidiviz.common.date import last_day_of_month
from recidiviz.persistence.entity.state.entities import StatePerson


METRIC_TYPES: Dict[Type[IncarcerationEvent], IncarcerationMetricType] = {
    IncarcerationAdmissionEvent: IncarcerationMetricType.INCARCERATION_ADMISSION,
    IncarcerationStayEvent: IncarcerationMetricType.INCARCERATION_POPULATION,
    IncarcerationReleaseEvent: IncarcerationMetricType.INCARCERATION_RELEASE
}

METRIC_CLASSES: Dict[Type[IncarcerationEvent], Type[IncarcerationMetric]] = {
    IncarcerationAdmissionEvent: IncarcerationAdmissionMetric,
    IncarcerationStayEvent: IncarcerationPopulationMetric,
    IncarcerationReleaseEvent: IncarcerationReleaseMetric
}


def map_incarceration_combinations(person: StatePerson,
                                   incarceration_events:
                                   List[IncarcerationEvent],
                                   metric_inclusions: Dict[IncarcerationMetricType, bool],
                                   calculation_end_month: Optional[str],
                                   calculation_month_count: int,
                                   person_metadata: PersonMetadata) -> List[Tuple[Dict[str, Any], Any]]:
    """Transforms IncarcerationEvents and a StatePerson into metric combinations.

    Takes in a StatePerson and all of their IncarcerationEvent and returns an array of "incarceration combinations".
    These are key-value pairs where the key represents a specific metric and the value represents whether or not
    the person should be counted as a positive instance of that metric.

    This translates a particular incarceration event, e.g. admission or release, into many different incarceration
    metrics.

    Args:
        person: the StatePerson
        incarceration_events: A list of IncarcerationEvents for the given StatePerson.
        metric_inclusions: A dictionary where the keys are each IncarcerationMetricType, and the values are boolean
            flags for whether or not to include that metric type in the calculations
        calculation_end_month: The year and month in YYYY-MM format of the last month for which metrics should be
            calculated. If unset, ends with the current month.
        calculation_month_count: The number of months (including the month of the calculation_month_upper_bound) to
            limit the monthly calculation output to. If set to -1, does not limit the calculations.
        person_metadata: Contains information about the StatePerson that is necessary for the metrics.
    Returns:
        A list of key-value tuples representing specific metric combinations and the value corresponding to that metric.
    """
    metrics: List[Tuple[Dict[str, Any], Any]] = []
    periods_and_events: Dict[int, List[IncarcerationEvent]] = defaultdict()

    calculation_month_upper_bound = get_calculation_month_upper_bound_date(calculation_end_month)

    # If the calculations include the current month, then we will calculate person-based metrics for each metric
    # period in METRIC_PERIOD_MONTHS ending with the current month
    include_metric_period_output = calculation_month_upper_bound == get_calculation_month_upper_bound_date(
        date.today().strftime('%Y-%m'))

    if include_metric_period_output:
        # Organize the events by the relevant metric periods
        for incarceration_event in incarceration_events:
            relevant_periods = relevant_metric_periods(
                incarceration_event.event_date,
                calculation_month_upper_bound.year,
                calculation_month_upper_bound.month)

            if relevant_periods:
                for period in relevant_periods:
                    period_events = periods_and_events.get(period)

                    if period_events:
                        period_events.append(incarceration_event)
                    else:
                        periods_and_events[period] = [incarceration_event]

    calculation_month_lower_bound = get_calculation_month_lower_bound_date(
        calculation_month_upper_bound, calculation_month_count)

    for incarceration_event in incarceration_events:
        metric_type = METRIC_TYPES.get(type(incarceration_event))
        metric_class = METRIC_CLASSES.get((type(incarceration_event)))
        if not metric_type:
            raise ValueError(
                'No metric type mapped to incarceration event of type {}'.format(type(incarceration_event)))

        if not metric_class:
            raise ValueError(
                'No metric class mapped to incarceration event of type {}'.format(type(incarceration_event)))

        if metric_inclusions.get(metric_type):
            characteristic_combo = characteristics_dict(person, incarceration_event, metric_class, person_metadata)

            metrics.extend(map_metric_combinations(
                characteristic_combo, incarceration_event,
                calculation_month_upper_bound, calculation_month_lower_bound, incarceration_events,
                periods_and_events, metric_type, include_metric_period_output
            ))

    return metrics


def characteristics_dict(person: StatePerson,
                         incarceration_event: IncarcerationEvent,
                         metric_class: Type[IncarcerationMetric],
                         person_metadata: PersonMetadata) -> Dict[str, Any]:
    """Builds a dictionary that describes the characteristics of the person and event.

    Args:
        person: the StatePerson we are picking characteristics from
        incarceration_event: the IncarcerationEvent we are picking characteristics from
        metric_class: The IncarcerationMetric provided determines which fields should be added to the characteristics
            dictionary
        person_metadata: Contains information about the StatePerson that is necessary for the metrics.
    Returns:
        A dictionary populated with all relevant characteristics.
    """
    event_date = incarceration_event.event_date

    characteristics = characteristics_dict_builder(pipeline='incarceration',
                                                   event=incarceration_event,
                                                   metric_class=metric_class,
                                                   person=person,
                                                   event_date=event_date,
                                                   include_person_attributes=True,
                                                   person_metadata=person_metadata)
    return characteristics


def map_metric_combinations(
        characteristic_combo: Dict[str, Any],
        incarceration_event: IncarcerationEvent,
        calculation_month_upper_bound: date,
        calculation_month_lower_bound: Optional[date],
        all_incarceration_events: List[IncarcerationEvent],
        periods_and_events: Dict[int, List[IncarcerationEvent]],
        metric_type: IncarcerationMetricType,
        include_metric_period_output: bool) -> \
        List[Tuple[Dict[str, Any], Any]]:
    """Maps the given time bucket and characteristic combinations to a variety of metrics that track incarceration
    admission and release counts.

     All values will be 1 for these count metrics, because the presence of an IncarcerationEvent for a given month
     implies that the person was counted towards the admission or release for that month.

     Args:
         characteristic_combo: A dictionary containing the characteristics of the person and event
         incarceration_event: The incarceration event from which the combination was derived.
         calculation_month_upper_bound: The year and month of the last month for which metrics should be calculated.
         calculation_month_lower_bound: The date of the first month to be included in the monthly calculations
         all_incarceration_events: All of the person's IncarcerationEvents
         periods_and_events: A dictionary mapping metric period month values to the corresponding relevant
            IncarcerationEvents
         metric_type: The metric type to set on each combination
        include_metric_period_output: Whether or not to include metrics for the various metric periods before the
            current month. If False, will still include metric_period_months = 1 for the current month.

     Returns:
        A list of key-value tuples representing specific metric combinations and the metric value corresponding to
            that metric.
     """

    metrics = []

    characteristic_combo['metric_type'] = metric_type

    if include_in_historical_metrics(incarceration_event.event_date.year, incarceration_event.event_date.month,
                                     calculation_month_upper_bound, calculation_month_lower_bound):
        # IncarcerationPopulationMetrics are point-in-time counts for the date of the event, all other
        # IncarcerationMetrics are counts based on the month of the event
        is_daily_metric = metric_type == IncarcerationMetricType.INCARCERATION_POPULATION

        # All other IncarcerationMetrics are counts based on the month of the event
        metrics.extend(combination_incarceration_metrics(
            characteristic_combo, incarceration_event, all_incarceration_events, is_daily_metric))

    if include_metric_period_output and metric_type != IncarcerationMetricType.INCARCERATION_POPULATION:
        metrics.extend(combination_incarceration_metric_period_metrics(
            characteristic_combo, incarceration_event, calculation_month_upper_bound,
            periods_and_events
        ))

    return metrics


def combination_incarceration_metrics(
        combo: Dict[str, Any],
        incarceration_event: IncarcerationEvent,
        all_incarceration_events: List[IncarcerationEvent],
        is_daily_metric: bool) \
        -> List[Tuple[Dict[str, Any], int]]:
    """Returns all unique incarceration metrics for the given event and combination.

    First, includes an event-based count for the event. Then, if this is a daily metric, includes a count of the event
    if it should be included in the person-based count for the day when the event occurred. If this is not a daily
    metric, includes a count of the event if it should be included in the person-based count for the month of the event.

    Args:
        combo: A characteristic combination to convert into metrics
        incarceration_event: The IncarcerationEvent from which the combination was derived
        all_incarceration_events: All of this person's IncarcerationEvents
        is_daily_metric: If True, limits person-based counts to the date of the event. If False, limits person-based
            counts to the month of the event.

    Returns:
        A list of key-value tuples representing specific metric combination dictionaries and the number 1 representing
            a positive contribution to that count metric.
    """
    metrics = []

    event_date = incarceration_event.event_date
    event_year = event_date.year
    event_month = event_date.month

    metric_period_months = 0 if is_daily_metric else 1

    # Add event-based combo for the 1-month period the month of the event
    event_based_same_month_combo = augmented_combo_for_calculations(
        combo, incarceration_event.state_code,
        event_year, event_month,
        MetricMethodologyType.EVENT, metric_period_months=metric_period_months)

    metrics.append((event_based_same_month_combo, 1))

    # Create the person-based combo for the 1-month period of the month of the event
    person_based_same_month_combo = augmented_combo_for_calculations(
        combo, incarceration_event.state_code,
        event_year, event_month,
        MetricMethodologyType.PERSON, metric_period_months=metric_period_months)

    day_match_value = event_date.day if is_daily_metric else None

    # Get the events of the same type that happened in the same month
    events_in_period = matching_events_for_person_based_count(year=event_year,
                                                              month=event_month,
                                                              day=day_match_value,
                                                              event_type=type(incarceration_event),
                                                              all_incarceration_events=all_incarceration_events)

    if events_in_period and include_event_in_count(
            incarceration_event,
            last_day_of_month(event_date),
            events_in_period):
        # Include this event in the person-based count
        metrics.append((person_based_same_month_combo, 1))

    return metrics


def matching_events_for_person_based_count(
        year: int,
        month: int,
        day: Optional[int],
        event_type: Type[IncarcerationEvent],
        all_incarceration_events: List[IncarcerationEvent],
) -> List[IncarcerationEvent]:
    """Returns events that match the given date parameters and match the IncarcerationEvent type. If |day| is set,
    returns only events that happened on the exact date (year-month-day). If |day| is unset, returns events that
    happened in the given |year| and |month|."""
    def _date_matches(event_date: date) -> bool:
        return (event_date.year == year
                and event_date.month == month
                and (day is None or event_date.day == day))

    return [
        event for event in all_incarceration_events
        if isinstance(event, event_type)
        and _date_matches(event.event_date)
    ]


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
            person_based_period_combo = augmented_combo_for_calculations(
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
                metrics.append((person_based_period_combo, 1))

    return metrics


def include_event_in_count(incarceration_event: IncarcerationEvent,
                           metric_period_end_date: date,
                           all_events_in_period:
                           Sequence[IncarcerationEvent]) -> bool:
    """Determines whether the given incarceration_event should be included in a person-based count for this given
    metric_period_end_date.

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
        # If this is the last recorded event for this period, include it
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
