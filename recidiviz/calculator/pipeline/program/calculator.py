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
from recidiviz.calculator.pipeline.utils.calculator_utils import age_at_date, \
    age_bucket, for_characteristics_races_ethnicities, for_characteristics, \
    last_day_of_month, relevant_metric_periods, augmented_combo_list, include_in_monthly_metrics, \
    get_calculation_month_lower_bound_date, first_day_of_month, \
    characteristics_with_person_id_fields
from recidiviz.calculator.pipeline.utils.assessment_utils import \
    assessment_score_bucket, include_assessment_in_metric
from recidiviz.calculator.pipeline.utils.metric_utils import \
    MetricMethodologyType
from recidiviz.persistence.entity.state.entities import StatePerson


def map_program_combinations(person: StatePerson,
                             program_events:
                             List[ProgramEvent],
                             inclusions: Dict[str, bool],
                             calculation_month_limit: int) -> List[Tuple[Dict[str, Any], Any]]:
    """Transforms ProgramEvents and a StatePerson into metric combinations.

    Takes in a StatePerson and all of her ProgramEvents and returns an array of "program combinations". These are
    key-value pairs where the key represents a specific metric and the value represents whether or not
    the person should be counted as a positive instance of that metric.

    This translates a particular interaction with a program into many different program metrics. Each metric represents
    one of many possible combinations of characteristics being tracked for that event. For example,
    if a White male is referred to a program, there is a metric that corresponds to White people, one to males, one to
    White males, one to all people, and more depending on other dimensions in the data.

    Args:
        person: the StatePerson
        program_events: A list of ProgramEvents for the given StatePerson.
        inclusions: A dictionary containing the following keys that correspond to characteristic dimensions:
                - age_bucket
                - ethnicity
                - gender
                - race
            Where the values are boolean flags indicating whether to include the dimension in the calculations.
        calculation_month_limit: The number of months (including this one) to limit the monthly calculation output to.
            If set to -1, does not limit the calculations.
    Returns:
        A list of key-value tuples representing specific metric combinations and the value corresponding to that metric.
    """

    metrics: List[Tuple[Dict[str, Any], Any]] = []

    periods_and_events: Dict[int, List[ProgramEvent]] = defaultdict()

    # We will calculate person-based metrics for each metric period in METRIC_PERIOD_MONTHS ending with the current
    # month
    metric_period_end_date = last_day_of_month(date.today())

    calculation_month_lower_bound = get_calculation_month_lower_bound_date(
        metric_period_end_date, calculation_month_limit)

    # Organize the events by the relevant metric periods
    for program_event in program_events:
        relevant_periods = relevant_metric_periods(
            program_event.event_date,
            metric_period_end_date.year,
            metric_period_end_date.month)

        if relevant_periods:
            for period in relevant_periods:
                period_events = periods_and_events.get(period)

                if period_events:
                    period_events.append(program_event)
                else:
                    periods_and_events[period] = [program_event]

    for program_event in program_events:
        if isinstance(program_event, ProgramReferralEvent):
            characteristic_combos = characteristic_combinations(
                person, program_event, inclusions)

            program_referral_metrics_event_based = map_metric_combinations(
                characteristic_combos, program_event,
                metric_period_end_date, calculation_month_lower_bound,
                program_events, periods_and_events, ProgramMetricType.REFERRAL
            )

            metrics.extend(program_referral_metrics_event_based)

    return metrics


def characteristic_combinations(person: StatePerson,
                                program_event: ProgramEvent,
                                inclusions: Dict[str, bool]) -> \
        List[Dict[str, Any]]:
    """Calculates all program metric combinations.

    Returns the list of all combinations of the metric characteristics, of all sizes, given the StatePerson and
    ProgramEvent. That is, this returns a list of dictionaries where each dictionary is a combination of 0
    to n unique elements of characteristics, where n is the number of keys in the given inclusions dictionary that are
    set to True + the dimensions for the given type of event.

    Methodology is not included in the output here. It is added into augmented versions of these combinations later.

    Args:
        person: the StatePerson we are picking characteristics from
        program_event: the ProgramEvent we are picking characteristics from
        inclusions: A dictionary containing the following keys that correspond to characteristic dimensions:
                - age_bucket
                - ethnicity
                - gender
                - race
            Where the values are boolean flags indicating whether to include the dimension in the calculations.

    Returns:
        A list of dictionaries containing all unique combinations of characteristics.
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

        if program_event.supervising_officer_external_id:
            characteristics['supervising_officer_external_id'] = program_event.supervising_officer_external_id
        if program_event.supervising_district_external_id:
            characteristics['supervising_district_external_id'] = program_event.supervising_district_external_id

    if program_event.program_id:
        characteristics['program_id'] = program_event.program_id

    if inclusions.get('age_bucket'):
        start_of_bucket = first_day_of_month(program_event.event_date)
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

    characteristics_with_person_details = characteristics_with_person_id_fields(characteristics, person, 'program')

    all_combinations.append(characteristics_with_person_details)

    return all_combinations


def map_metric_combinations(
        characteristic_combos: List[Dict[str, Any]],
        program_event: ProgramEvent,
        metric_period_end_date: date,
        calculation_month_lower_bound: Optional[date],
        all_program_events: List[ProgramEvent],
        periods_and_events: Dict[int, List[ProgramEvent]],
        metric_type: ProgramMetricType) -> \
        List[Tuple[Dict[str, Any], Any]]:
    """Maps the given program event and characteristic combinations to a variety of metrics that track program
    interactions.

    All values will be 1 for these count metrics, because the presence of a ProgramEvent for a given event implies that
    the person interacted with the program in the way being described.

    Args:
        characteristic_combos: A list of dictionaries containing all unique combinations of characteristics.
        program_event: The program event from which the combination was derived.
        metric_period_end_date: The day the metric periods end
        calculation_month_lower_bound: The date of the first month to be included in the monthly calculations
        all_program_events: All of the person's ProgramEvents
        periods_and_events: A dictionary mapping metric period month values to the corresponding relevant ProgramEvents
        metric_type: The metric type to set on each combination

    Returns:
        A list of key-value tuples representing specific metric combinations and the metric value corresponding to that
        metric.
    """
    metrics = []

    all_referral_events = [
        event for event in all_program_events
        if isinstance(event, ProgramReferralEvent)
    ]

    for combo in characteristic_combos:
        if metric_type == ProgramMetricType.REFERRAL and isinstance(program_event, ProgramReferralEvent):
            combo['metric_type'] = metric_type.value

            if include_in_monthly_metrics(
                    program_event.event_date.year, program_event.event_date.month, calculation_month_lower_bound):

                metrics.extend(combination_referral_monthly_metrics(combo, program_event, all_referral_events))

            metrics.extend(combination_referral_metric_period_metrics(combo, program_event,
                                                                      metric_period_end_date, periods_and_events))

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

    # Add event-based combos for the 1-month period the month of the event
    event_based_same_month_combos = augmented_combo_list(
        combo, program_event.state_code,
        event_year, event_month,
        MetricMethodologyType.EVENT, 1)

    for event_combo in event_based_same_month_combos:
        metrics.append((event_combo, 1))

    # Create the person-based combos for the 1-month period of the month of the event
    person_based_same_month_combos = augmented_combo_list(
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
        for person_combo in person_based_same_month_combos:
            metrics.append((person_combo, 1))

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
            person_based_period_combos = augmented_combo_list(
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
                for person_combo in person_based_period_combos:
                    metrics.append((person_combo, 1))

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
