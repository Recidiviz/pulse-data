# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Calculates program metrics from program events.

This contains the core logic for calculating program metrics on a person-by-person basis. It transforms ProgramEvents
into program metrics, key-value pairs where the key represents all of the dimensions represented in the data point, and
the value represents an indicator of whether the person should contribute to that metric.
"""
from collections import defaultdict
from datetime import date
from typing import List, Dict, Tuple, Any, Sequence, Optional, Type

from recidiviz.calculator.pipeline.program.metrics import ProgramMetricType, ProgramMetric,\
    ProgramParticipationMetric, ProgramReferralMetric
from recidiviz.calculator.pipeline.program.program_event import ProgramEvent, \
    ProgramReferralEvent, ProgramParticipationEvent
from recidiviz.calculator.pipeline.utils.calculator_utils import last_day_of_month, relevant_metric_periods, \
    augmented_combo_for_calculations, include_in_historical_metrics, \
    get_calculation_month_lower_bound_date, get_calculation_month_upper_bound_date, characteristics_dict_builder
from recidiviz.calculator.pipeline.utils.metric_utils import \
    MetricMethodologyType
from recidiviz.calculator.pipeline.utils.person_utils import PersonMetadata
from recidiviz.calculator.pipeline.utils.state_utils.state_calculation_config_manager import \
    supervision_types_mutually_exclusive_for_state
from recidiviz.common.date import last_day_of_month
from recidiviz.persistence.entity.state.entities import StatePerson


def map_program_combinations(person: StatePerson,
                             program_events:
                             List[ProgramEvent],
                             metric_inclusions: Dict[ProgramMetricType, bool],
                             calculation_end_month: Optional[str],
                             calculation_month_count: int,
                             person_metadata: PersonMetadata) -> List[Tuple[Dict[str, Any], Any]]:
    """Transforms ProgramEvents and a StatePerson into metric combinations.

    Takes in a StatePerson and all of her ProgramEvents and returns an array of "program combinations". These are
    key-value pairs where the key represents a specific metric and the value represents whether or not
    the person should be counted as a positive instance of that metric.

    This translates a particular interaction with a program into many different program metrics.

    Args:
        person: the StatePerson
        program_events: A list of ProgramEvents for the given StatePerson.
        metric_inclusions: A dictionary where the keys are each ProgramMetricType, and the values are boolean
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
    periods_and_events: Dict[int, List[ProgramEvent]] = defaultdict()

    calculation_month_upper_bound = get_calculation_month_upper_bound_date(calculation_end_month)

    # If the calculations include the current month, then we will calculate person-based metrics for each metric
    # period in METRIC_PERIOD_MONTHS ending with the current month
    include_metric_period_output = calculation_month_upper_bound == get_calculation_month_upper_bound_date(
        date.today().strftime('%Y-%m'))

    if include_metric_period_output:
        # Organize the events by the relevant metric periods
        for program_event in program_events:
            relevant_periods = relevant_metric_periods(
                program_event.event_date,
                calculation_month_upper_bound.year,
                calculation_month_upper_bound.month)

            if relevant_periods:
                for period in relevant_periods:
                    period_events = periods_and_events.get(period)

                    if period_events:
                        period_events.append(program_event)
                    else:
                        periods_and_events[period] = [program_event]

    calculation_month_lower_bound = get_calculation_month_lower_bound_date(
        calculation_month_upper_bound, calculation_month_count)

    for program_event in program_events:
        if (isinstance(program_event, ProgramReferralEvent)
                and metric_inclusions.get(ProgramMetricType.PROGRAM_REFERRAL)):
            characteristic_combo = characteristics_dict(person, program_event, ProgramReferralMetric, person_metadata)

            program_referral_metrics_event_based = map_metric_combinations(
                characteristic_combo, program_event,
                calculation_month_upper_bound, calculation_month_lower_bound,
                program_events, periods_and_events,
                ProgramMetricType.PROGRAM_REFERRAL, include_metric_period_output
            )

            metrics.extend(program_referral_metrics_event_based)
        elif (isinstance(program_event, ProgramParticipationEvent)
              and metric_inclusions.get(ProgramMetricType.PROGRAM_PARTICIPATION)):
            characteristic_combo = characteristics_dict(
                person, program_event, ProgramParticipationMetric, person_metadata)

            program_participation_metrics_event_based = map_metric_combinations(
                characteristic_combo,
                program_event,
                calculation_month_upper_bound,
                calculation_month_lower_bound,
                program_events,
                periods_and_events,
                ProgramMetricType.PROGRAM_PARTICIPATION,
                # The ProgramParticipationMetric is explicitly a daily metric
                include_metric_period_output=False
            )

            metrics.extend(program_participation_metrics_event_based)

    return metrics


def characteristics_dict(person: StatePerson,
                         program_event: ProgramEvent,
                         metric_class: Type[ProgramMetric],
                         person_metadata: PersonMetadata) -> Dict[str, Any]:
    """Builds a dictionary that describes the characteristics of the person and event.

    Args:
        person: the StatePerson we are picking characteristics from
        program_event: the ProgramEvent we are picking characteristics from
        metric_class: The ProgramMetric provided determines which fields should be added to the characteristics
            dictionary
        person_metadata: Contains information about the StatePerson that is necessary for the metrics.

    Returns:
        A dictionary populated with all relevant characteristics.
    """
    event_date = program_event.event_date

    characteristics = characteristics_dict_builder(pipeline='program',
                                                   event=program_event,
                                                   metric_class=metric_class,
                                                   person=person,
                                                   event_date=event_date,
                                                   person_metadata=person_metadata)

    return characteristics


def map_metric_combinations(
        characteristic_combo: Dict[str, Any],
        program_event: ProgramEvent,
        calculation_month_upper_bound: date,
        calculation_month_lower_bound: Optional[date],
        all_program_events: List[ProgramEvent],
        periods_and_events: Dict[int, List[ProgramEvent]],
        metric_type: ProgramMetricType,
        include_metric_period_output: bool) -> \
        List[Tuple[Dict[str, Any], Any]]:
    """Maps the given program event and characteristic combinations to a variety of metrics that track program
    interactions.

    All values will be 1 for these count metrics, because the presence of a ProgramEvent for a given event implies that
    the person interacted with the program in the way being described.

    Args:
        characteristic_combo: A dictionary describing the person and event.
        program_event: The program event from which the combination was derived.
        calculation_month_upper_bound: The year and month of the last month for which metrics should be calculated.
        calculation_month_lower_bound: The date of the first month to be included in the monthly calculations
        all_program_events: All of the person's ProgramEvents
        periods_and_events: A dictionary mapping metric period month values to the corresponding relevant ProgramEvents
        metric_type: The metric type to set on each combination
        include_metric_period_output: Whether or not to include metrics for the various metric periods before the
            current month. If False, will still include metric_period_months = 1 for the current month.

    Returns:
        A list of key-value tuples representing specific metric combinations and the metric value corresponding to that
        metric.
    """
    metrics = []

    characteristic_combo['metric_type'] = metric_type

    if include_in_historical_metrics(program_event.event_date.year, program_event.event_date.month,
                                     calculation_month_upper_bound, calculation_month_lower_bound):
        # ProgramParticipationMetrics are point-in-time metrics for the date of the participation, all
        # other ProgramMetrics are metrics based on the month of the event
        is_daily_metric = metric_type == ProgramMetricType.PROGRAM_PARTICIPATION

        metrics.extend(combination_program_monthly_metrics(
            characteristic_combo, program_event, metric_type, all_program_events, is_daily_metric))

    if include_metric_period_output:
        if not isinstance(program_event, ProgramReferralEvent):
            raise ValueError("Unexpected metric period calculation of an event that's not a ProgramReferralEvent.")

        metrics.extend(combination_program_metric_period_metrics(characteristic_combo,
                                                                 program_event,
                                                                 calculation_month_upper_bound,
                                                                 periods_and_events))

    return metrics


def combination_program_monthly_metrics(
        combo: Dict[str, Any],
        program_event: ProgramEvent,
        metric_type: ProgramMetricType,
        all_program_events: List[ProgramEvent],
        is_daily_metric: bool) -> List[Tuple[Dict[str, Any], int]]:
    """Returns all unique referral metrics for the given event and combination.

    First, includes an event-based count for the month the event occurred with a metric period of 1 month. Then, if
    this event should be included in the person-based count for the month when the event occurred, adds those person-
    based metrics.

    Args:
        combo: A characteristic combination to convert into metrics
        program_event: The program event from which the combination was derived
        metric_type: The type of metric being tracked by this combo
        all_program_events: All of this person's ProgramEvents
        is_daily_metric:  If True, limits person-based counts to the date of the event. If False, limits person-based
            counts to the month of the event.

    Returns:
        A list of key-value tuples representing specific metric combination dictionaries and the number 1 representing
            a positive contribution to that count metric.
    """
    metrics = []

    event_date = program_event.event_date
    event_year = event_date.year
    event_month = event_date.month

    base_metric_period = 0 if is_daily_metric else 1

    # Add event-based combo for the base metric period the month of the event
    event_based_same_month_combo = augmented_combo_for_calculations(
        combo, program_event.state_code,
        event_year, event_month,
        MetricMethodologyType.EVENT, base_metric_period)

    metrics.append((event_based_same_month_combo, 1))

    # Create the person-based combo for the base metric period of the month of the event
    person_based_same_month_combo = augmented_combo_for_calculations(
        combo, program_event.state_code,
        event_year, event_month,
        MetricMethodologyType.PERSON, base_metric_period
    )

    events_in_period: List[ProgramEvent] = []

    if metric_type == ProgramMetricType.PROGRAM_PARTICIPATION:
        # Get all other participation events that happened on the same day as this one
        events_in_period = [
            event for event in all_program_events
            if (isinstance(event, ProgramParticipationEvent))
            and event.event_date == program_event.event_date
        ]
    elif metric_type == ProgramMetricType.PROGRAM_REFERRAL:
        # Get all other referral events that happened in the same month as this one
        events_in_period = [
            event for event in all_program_events
            if isinstance(event, ProgramReferralEvent)
            and event.event_date.year == event_year and
            event.event_date.month == event_month
        ]

    if include_event_in_count(combo,
                              program_event,
                              last_day_of_month(event_date),
                              events_in_period):
        # Include this event in the person-based count
        metrics.append((person_based_same_month_combo, 1))

    return metrics


def combination_program_metric_period_metrics(
        combo: Dict[str, Any],
        program_event: ProgramReferralEvent,
        metric_period_end_date: date,
        periods_and_events: Dict[int, List[ProgramEvent]]) -> List[Tuple[Dict[str, Any], int]]:
    """Returns all unique referral metrics for the given event, combination, and relevant metric_period_months.

    Returns metrics for each of the metric period length that this event falls into if this event should be included in
    the person-based count for that metric period.

    Args:
        combo: A characteristic combination to convert into metrics
        program_event: The program event from which the combination was derived
        metric_period_end_date: The day the metric periods end
        periods_and_events: Dictionary mapping metric period month lengths to the ProgramEvents that fall in that period

    Returns:
        A list of key-value tuples representing specific metric combination dictionaries and the number 1 representing
            a positive contribution to that count metric.
    """
    metrics = []

    period_end_year = metric_period_end_date.year
    period_end_month = metric_period_end_date.month

    for period_length, events_in_period in periods_and_events.items():
        if program_event in events_in_period:
            # This event falls within this metric period
            person_based_period_combo = augmented_combo_for_calculations(
                combo, program_event.state_code,
                period_end_year, period_end_month,
                MetricMethodologyType.PERSON, period_length
            )

            referral_events_in_period = [
                event for event in events_in_period
                if isinstance(event, ProgramReferralEvent)
            ]

            if include_event_in_count(
                    combo,
                    program_event,
                    metric_period_end_date,
                    referral_events_in_period):
                # Include this event in the person-based count for this time period
                metrics.append((person_based_period_combo, 1))

    return metrics


def include_event_in_count(combo: Dict[str, Any],
                           program_event: ProgramEvent,
                           metric_period_end_date: date,
                           all_events_in_period: Sequence[ProgramEvent]) -> bool:
    """Determines whether the given program_event should be included in a person-based count for this given
    calculation_month_upper_bound.

    If the combo has a value for the key 'supervision_type', this means that this will contribute to a metric that is
    specific to a given supervision type. The person-based count for this metric should only be with respect
    to other events that share the same supervision-type. If the combo is not for a supervision-type-specific metric,
    then the person-based count should take into account all events in the period.

    This event is included only if it is the last event to happen before the end of the metric period.
    """
    if not isinstance(program_event, (ProgramReferralEvent, ProgramParticipationEvent)):
        raise ValueError(f"Unexpected program_event of type {program_event.__class__}")

    # If supervision types are mutually exclusive for a given state, then a person who has events with different types
    # of supervision cannot contribute to counts for more than one type
    if supervision_types_mutually_exclusive_for_state(program_event.state_code):
        supervision_type_specific_metric = False
    else:
        # If this combo specifies the supervision type, then limit this inclusion logic to only buckets of the same
        # supervision type
        supervision_type_specific_metric = combo.get('supervision_type') is not None

    # If this is a supervision_type_specific_metric, filter to only the events with the same supervision_type
    relevant_events = [
        event for event in all_events_in_period
        if isinstance(event, (ProgramReferralEvent, ProgramParticipationEvent))
        and not (supervision_type_specific_metric and event.supervision_type != program_event.supervision_type)
    ]

    if isinstance(program_event, ProgramReferralEvent):
        # If the combination specifies the supervision type, then remove any events of other supervision types
        relevant_referral_events = [
            event for event in relevant_events
            if isinstance(event, ProgramReferralEvent)
        ]

        events_rest_of_period = program_events_in_period(
            program_event.event_date,
            metric_period_end_date,
            relevant_referral_events)

        events_rest_of_period.sort(key=lambda b: b.event_date)

        if events_rest_of_period and id(program_event) == id(events_rest_of_period[-1]):
            # If this is the last instance of a referral before the end of the period, then include it in the
            # person-based count.
            return True
    elif isinstance(program_event, ProgramParticipationEvent):
        # If the combination specifies the supervision type, then remove any events of other supervision types
        relevant_participation_events = [
            event for event in relevant_events
            if isinstance(event, ProgramParticipationEvent)
        ]

        # Sort by the program_location_id, with unset program_location_ids at the end. This sort is done to ensure
        # deterministic person-based output.
        relevant_participation_events.sort(key=lambda b: (b.program_location_id, b.program_location_id is None))

        # Include this event if it's the first event in the sorted list.
        return id(program_event) == id(relevant_participation_events[0])

    return False


def program_events_in_period(start_date: date,
                             end_date: date,
                             all_program_events: Sequence[ProgramEvent]) \
        -> List[ProgramEvent]:
    """Returns all of the events that occurred between the start_date and end_date, inclusive."""
    events_in_period = \
        [event for event in all_program_events if start_date <= event.event_date <= end_date]

    return events_in_period
