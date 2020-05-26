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
from typing import List, Dict, Tuple, Any, Sequence, Optional

from recidiviz.calculator.pipeline.program.metrics import ProgramMetricType
from recidiviz.calculator.pipeline.program.program_event import ProgramEvent, \
    ProgramReferralEvent
from recidiviz.calculator.pipeline.utils.calculator_utils import last_day_of_month, relevant_metric_periods, \
    augmented_combo_for_calculations, include_in_monthly_metrics, \
    get_calculation_month_lower_bound_date, \
    characteristics_with_person_id_fields, add_demographic_characteristics, get_calculation_month_upper_bound_date
from recidiviz.calculator.pipeline.utils.assessment_utils import \
    assessment_score_bucket, include_assessment_in_metric
from recidiviz.calculator.pipeline.utils.metric_utils import \
    MetricMethodologyType
from recidiviz.persistence.entity.state.entities import StatePerson


def map_program_combinations(person: StatePerson,
                             program_events:
                             List[ProgramEvent],
                             metric_inclusions: Dict[ProgramMetricType, bool],
                             calculation_end_month: Optional[str],
                             calculation_month_count: int) -> List[Tuple[Dict[str, Any], Any]]:
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
        if isinstance(program_event, ProgramReferralEvent) and metric_inclusions.get(ProgramMetricType.REFERRAL):
            characteristic_combo = characteristics_dict(person, program_event)

            program_referral_metrics_event_based = map_metric_combinations(
                characteristic_combo, program_event,
                calculation_month_upper_bound, calculation_month_lower_bound,
                program_events, periods_and_events,
                ProgramMetricType.REFERRAL, include_metric_period_output
            )

            metrics.extend(program_referral_metrics_event_based)

    return metrics


def characteristics_dict(person: StatePerson,
                         program_event: ProgramEvent) -> Dict[str, Any]:
    """Builds a dictionary that describes the characteristics of the person and event.

    Args:
        person: the StatePerson we are picking characteristics from
        program_event: the ProgramEvent we are picking characteristics from

    Returns:
        A dictionary populated with all relevant characteristics.
    """
    characteristics: Dict[str, Any] = {}

    if isinstance(program_event, ProgramReferralEvent):
        if program_event.supervision_type:
            characteristics['supervision_type'] = program_event.supervision_type
        if program_event.assessment_score and program_event.assessment_type:
            assessment_bucket = assessment_score_bucket(
                assessment_score=program_event.assessment_score,
                assessment_level=None,
                assessment_type=program_event.assessment_type)

            if assessment_bucket and include_assessment_in_metric(
                    'program', program_event.state_code, program_event.assessment_type):
                characteristics['assessment_score_bucket'] = assessment_bucket
                characteristics['assessment_type'] = program_event.assessment_type

        if program_event.participation_status:
            characteristics['participation_status'] = program_event.participation_status
        if program_event.supervising_officer_external_id:
            characteristics['supervising_officer_external_id'] = program_event.supervising_officer_external_id
        if program_event.supervising_district_external_id:
            characteristics['supervising_district_external_id'] = program_event.supervising_district_external_id

    if program_event.program_id:
        characteristics['program_id'] = program_event.program_id

    event_date = program_event.event_date

    characteristics = add_demographic_characteristics(characteristics, person, event_date)

    characteristics_with_person_details = characteristics_with_person_id_fields(characteristics, person, 'program')

    return characteristics_with_person_details


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

    all_referral_events = [
        event for event in all_program_events
        if isinstance(event, ProgramReferralEvent)
    ]

    if metric_type == ProgramMetricType.REFERRAL and isinstance(program_event, ProgramReferralEvent):
        characteristic_combo['metric_type'] = metric_type

        if include_in_monthly_metrics(
                program_event.event_date.year, program_event.event_date.month,
                calculation_month_upper_bound, calculation_month_lower_bound):

            metrics.extend(
                combination_referral_monthly_metrics(characteristic_combo, program_event, all_referral_events))

        if include_metric_period_output:
            metrics.extend(combination_referral_metric_period_metrics(characteristic_combo,
                                                                      program_event,
                                                                      calculation_month_upper_bound,
                                                                      periods_and_events))

    return metrics


def combination_referral_monthly_metrics(
        combo: Dict[str, Any],
        program_event: ProgramReferralEvent,
        all_referral_events:
        List[ProgramReferralEvent]) \
        -> List[Tuple[Dict[str, Any], int]]:
    """Returns all unique referral metrics for the given event and combination.

    First, includes an event-based count for the month the event occurred with a metric period of 1 month. Then, if
    this event should be included in the person-based count for the month when the event occurred, adds those person-
    based metrics.

    Args:
        combo: A characteristic combination to convert into metrics
        program_event: The program event from which the combination was derived
        all_referral_events: All of this person's ProgramReferralEvents

    Returns:
        A list of key-value tuples representing specific metric combination dictionaries and the number 1 representing
            a positive contribution to that count metric.
    """
    metrics = []

    event_date = program_event.event_date
    event_year = event_date.year
    event_month = event_date.month

    # Add event-based combo for the 1-month period the month of the event
    event_based_same_month_combo = augmented_combo_for_calculations(
        combo, program_event.state_code,
        event_year, event_month,
        MetricMethodologyType.EVENT, 1)

    metrics.append((event_based_same_month_combo, 1))

    # Create the person-based combo for the 1-month period of the month of the event
    person_based_same_month_combo = augmented_combo_for_calculations(
        combo, program_event.state_code,
        event_year, event_month,
        MetricMethodologyType.PERSON, 1
    )

    # Get all other referral events that happened the same month as this one
    all_referral_events_in_event_month = [
        event for event in all_referral_events
        if event.event_date.year == event_date.year and
        event.event_date.month == event_date.month
    ]

    if include_referral_in_count(
            combo,
            program_event,
            last_day_of_month(event_date),
            all_referral_events_in_event_month):
        # Include this event in the person-based count
        metrics.append((person_based_same_month_combo, 1))

    return metrics


def combination_referral_metric_period_metrics(
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

            if include_referral_in_count(
                    combo,
                    program_event,
                    metric_period_end_date,
                    referral_events_in_period):
                # Include this event in the person-based count for this time period
                metrics.append((person_based_period_combo, 1))

    return metrics


def include_referral_in_count(combo: Dict[str, Any],
                              program_event: ProgramReferralEvent,
                              metric_period_end_date: date,
                              all_events_in_period:
                              List[ProgramReferralEvent]) -> bool:
    """Determines whether the given program_event should be included in a person-based count for this given
    calculation_month_upper_bound.

    If the combo has a value for the key 'supervision_type', this means that this will contribute to a metric that is
    specific to a given supervision type. The person-based count for this metric should only be with respect
    to other events that share the same supervision-type. If the combo is not for a supervision-type-specific metric,
    then the person-based count should take into account all events in the period.

    This event is included only if it is the last event to happen before the end of the metric period.
    """
    supervision_type_specific_metric = combo.get('supervision_type') is not None

    # If the combination specifies the supervision type, then remove any events of other supervision types
    relevant_events = [
        event for event in all_events_in_period
        if not (supervision_type_specific_metric and event.supervision_type != program_event.supervision_type)
    ]

    events_rest_of_period = program_events_in_period(
        program_event.event_date,
        metric_period_end_date,
        relevant_events)

    events_rest_of_period.sort(key=lambda b: b.event_date)

    if events_rest_of_period and id(program_event) == id(events_rest_of_period[-1]):
        # If this is the last instance of a referral before the end of the period, then include it in the person-based
        # count.
        return True

    return False


def program_events_in_period(start_date: date,
                             end_date: date,
                             all_program_events: Sequence[ProgramEvent]) \
        -> List[ProgramEvent]:
    """Returns all of the events that occurred between the start_date and end_date, inclusive."""
    events_in_period = \
        [event for event in all_program_events if start_date <= event.event_date <= end_date]

    return events_in_period
