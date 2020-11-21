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

"""Calculates recidivism metrics from release events.

This contains the core logic for calculating recidivism metrics on a
person-by-person basis. It transforms ReleaseEvents into recidivism metrics,
key-value pairs where the key represents all of the dimensions represented in
the data point, and the value represents a recidivism value, e.g. 0 for no or 1
for yes.

Attributes:
    FOLLOW_UP_PERIODS: a list of integers, the follow-up periods that we measure
        recidivism over, from 1 to 10.
"""
import logging
from typing import Any, Dict, List, Optional, Tuple, Type

import datetime
from datetime import date
from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.recidivism.metrics import \
    ReincarcerationRecidivismMetricType, ReincarcerationRecidivismMetric, ReincarcerationRecidivismCountMetric, \
    ReincarcerationRecidivismRateMetric
from recidiviz.calculator.pipeline.recidivism.release_event import \
    ReleaseEvent, RecidivismReleaseEvent, NonRecidivismReleaseEvent, \
    ReincarcerationReturnType
from recidiviz.calculator.pipeline.utils.metric_utils import \
    MetricMethodologyType
from recidiviz.calculator.pipeline.utils.calculator_utils import augment_combination, \
    relevant_metric_periods, characteristics_dict_builder
from recidiviz.calculator.pipeline.utils.person_utils import PersonMetadata
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodSupervisionType
from recidiviz.common.constants.state.state_supervision_violation import \
    StateSupervisionViolationType
from recidiviz.common.date import last_day_of_month
from recidiviz.persistence.entity.state.entities import StatePerson

# We measure in 1-year follow up periods up to 10 years after date of release.
FOLLOW_UP_PERIODS = range(1, 11)


def map_recidivism_combinations(person: StatePerson,
                                release_events: Dict[int, List[ReleaseEvent]],
                                metric_inclusions: Dict[ReincarcerationRecidivismMetricType, bool],
                                person_metadata: PersonMetadata) \
        -> List[Tuple[Dict[str, Any], Any]]:
    """Transforms ReleaseEvents and a StatePerson into metric combinations.

    Takes in a StatePerson and all of her ReleaseEvents and returns an array
    of "recidivism combinations". These are key-value pairs where the key
    represents a specific metric and the value represents whether or not
    recidivism occurred.

    This translates a particular recidivism event into many different recidivism
    metrics. Both count-based and rate-based metrics are generated. Each metric
    represents one of many possible combinations of characteristics being
    tracked for that event. For example, if an asian male is reincarcerated,
    there is a metric that corresponds to asian people, one to males,
    one to asian males, one to all people, and more depending on other
    dimensions in the data.

    If a release does not count towards recidivism, then the value is 0 for
    the rate-based metrics in either methodology.

    For both count and rate-based metrics, the value is 0 if the dimensions
    of the metric do not fully match the attributes of the person and their type
    of return to incarceration. For example, for a RecidivismReleaseEvent where
    the return_type is 'REVOCATION', there will be metrics produced where the
    return_type is 'NEW INCARCERATION_ADMISSION' and the value is 0.

    Args:
        person: the StatePerson
        release_events: A dictionary mapping release cohorts to a list of
            ReleaseEvents for the given StatePerson.
        metric_inclusions: A dictionary where the keys are each ReincarcerationRecidivismMetricType, and the values
            are boolean flags for whether or not to include that metric type in the calculations
        person_metadata: Contains information about the StatePerson that is necessary for the metrics.

    Returns:
        A list of key-value tuples representing specific metric combinations and
        the recidivism value corresponding to that metric.
    """
    metrics = []
    all_reincarcerations = reincarcerations(release_events)

    metric_period_end_date = last_day_of_month(date.today())

    for _, events in release_events.items():
        for event in events:
            if metric_inclusions.get(ReincarcerationRecidivismMetricType.REINCARCERATION_RATE):
                characteristic_combo_rate = \
                    characteristics_dict(person, event, ReincarcerationRecidivismRateMetric, person_metadata)

                rate_metrics = map_recidivism_rate_combinations(
                    characteristic_combo_rate, event,
                    release_events, all_reincarcerations)

                metrics.extend(rate_metrics)

            if metric_inclusions.get(ReincarcerationRecidivismMetricType.REINCARCERATION_COUNT) and \
                    isinstance(event, RecidivismReleaseEvent):
                characteristic_combo_count = \
                    characteristics_dict(person, event, ReincarcerationRecidivismCountMetric, person_metadata)

                count_metrics = map_recidivism_count_combinations(characteristic_combo_count,
                                                                  event,
                                                                  all_reincarcerations,
                                                                  metric_period_end_date)
                metrics.extend(count_metrics)

    return metrics


def map_recidivism_rate_combinations(
        characteristic_combo: Dict[str, Any],
        event: ReleaseEvent,
        all_release_events: Dict[int, List[ReleaseEvent]],
        all_reincarcerations: Dict[date, RecidivismReleaseEvent]) -> \
        List[Tuple[Dict[str, Any], Any]]:
    """Maps the given event and characteristic combinations to a variety of metrics that track rate-based recidivism.

    Args:
        characteristic_combo: A dictionary describing the person and event
        event: the recidivism event from which the combination was derived
        all_release_events: A dictionary mapping release cohorts to a list of ReleaseEvents for the given StatePerson.
        all_reincarcerations: dictionary where the keys are all dates of reincarceration for the person's ReleaseEvents,
            and the values are the corresponding ReleaseEvents

    Returns:
        A list of key-value tuples representing specific metric combinations and the recidivism value corresponding to
         that metric.
    """
    metrics = []

    reincarcerations_by_follow_up_period = reincarcerations_by_period(event.release_date, all_reincarcerations)

    combo = characteristic_combo.copy()

    combo['metric_type'] = ReincarcerationRecidivismMetricType.REINCARCERATION_RATE

    metrics.extend(combination_rate_metrics(
        combo, event, all_release_events, reincarcerations_by_follow_up_period))

    return metrics


def reincarcerations_by_period(release_date: date, all_reincarcerations: Dict[date, RecidivismReleaseEvent]) -> \
        Dict[int, List[RecidivismReleaseEvent]]:
    """For all relevant follow-up periods following the release_date, determines the reincarcerations that occurred
    between the release and the end of the follow-up period.

    Args:
        release_date: The date the person was released from prison
        all_reincarcerations: dictionary where the keys are all dates of reincarceration for the person's ReleaseEvents,
            and the values are the corresponding ReleaseEvents

    Returns:
        A dictionary where the keys are all relevant follow-up periods for measurement, and the values are lists of
        RecidivismReleaseEvents with reincarceration admissions during that period.
    """
    relevant_periods = relevant_follow_up_periods(release_date, date.today(), FOLLOW_UP_PERIODS)

    reincarcerations_by_follow_up_period: Dict[int, List[RecidivismReleaseEvent]] = {}

    for period in relevant_periods:
        end_of_follow_up_period = release_date + relativedelta(years=period)

        all_reincarcerations_in_window = reincarcerations_in_window(release_date,
                                                                    end_of_follow_up_period,
                                                                    all_reincarcerations)

        reincarcerations_by_follow_up_period[period] = all_reincarcerations_in_window

    return reincarcerations_by_follow_up_period


def map_recidivism_count_combinations(
        characteristic_combo: Dict[str, Any],
        event: ReleaseEvent,
        all_reincarcerations: Dict[date, RecidivismReleaseEvent],
        metric_period_end_date: date) -> \
        List[Tuple[Dict[str, Any], Any]]:
    """Maps the given event and characteristic combinations to a variety of metrics that track count-based recidivism.

    If the event is a RecidivismReleaseEvent, then a count of reincarceration occurred. This produces metrics for both
    the year and the month in which the person was reincarcerated.

    Args:
        characteristic_combo: A dictionary describing the person and event
        event: the recidivism event from which the combination was derived
        all_reincarcerations: dictionary where the keys are all dates of reincarceration for the person's ReleaseEvents,
            and the values are the corresponding ReleaseEvents
        metric_period_end_date: The day the metric periods end

    Returns:
        A list of key-value tuples representing specific metric combinations and the recidivism value corresponding to
            that metric.
    """
    metrics = []

    if isinstance(event, RecidivismReleaseEvent):
        reincarceration_date = event.reincarceration_date

        relevant_periods = relevant_metric_periods(reincarceration_date,
                                                   metric_period_end_date.year,
                                                   metric_period_end_date.month)

        characteristic_combo['metric_type'] = ReincarcerationRecidivismMetricType.REINCARCERATION_COUNT

        combo = characteristic_combo.copy()

        # Bucket for the month of the incarceration
        combo['year'] = reincarceration_date.year
        combo['month'] = reincarceration_date.month
        combo['metric_period_months'] = 1

        end_of_event_month = last_day_of_month(reincarceration_date)

        metrics.extend(combination_count_metrics(combo, event, all_reincarcerations, end_of_event_month))

        # Bucket for each of the relevant metric period month lengths
        for relevant_period in relevant_periods:
            metric_period_combo = characteristic_combo.copy()

            metric_period_combo['year'] = metric_period_end_date.year
            metric_period_combo['month'] = metric_period_end_date.month
            metric_period_combo['metric_period_months'] = relevant_period

            metrics.extend(combination_count_metrics(
                metric_period_combo, event, all_reincarcerations, metric_period_end_date))

    return metrics


def reincarcerations(release_events: Dict[int, List[ReleaseEvent]]) -> Dict[date, RecidivismReleaseEvent]:
    """Finds the reincarcerations within the given ReleaseEvents.

    Returns a dictionary where the keys are all dates of reincarceration for
    the person's ReleaseEvents, and the values are a dictionary containing
    return type and from supervision type information.

    If one of the given events is not an instance of
    recidivism, i.e. it is not a RecidivismReleaseEvent, then it is not
    represented in the output.

    Args:
        release_events: the list of ReleaseEvents.

    Returns:
        A dictionary representing the dates of reincarceration and the return
        descriptors for each reincarceration.
    """
    reincarcerations_dict: Dict[date, RecidivismReleaseEvent] = {}

    for _cohort, events in release_events.items():
        for event in events:
            if isinstance(event, RecidivismReleaseEvent):
                reincarcerations_dict[event.reincarceration_date] = event

    return reincarcerations_dict


def reincarcerations_in_window(start_date: date,
                               end_date: date,
                               all_reincarcerations: Dict[date, RecidivismReleaseEvent]) -> \
        List[RecidivismReleaseEvent]:
    """Finds the number of reincarceration dates during the given window.

    Returns how many of the given reincarceration dates fall within the given
    follow-up period after the given start date, end point exclusive, including
    the start date itself if it is within the given array.

    Args:
        start_date: a Date to start tracking from
        end_date: a Date to stop tracking
        all_reincarcerations: the dictionary of reincarcerations to check

    Returns:
        How many of the given reincarcerations are within the window specified by the given start date (inclusive)
        and end date (exclusive).
    """
    reincarcerations_in_window_dict = \
        [reincarceration for reincarceration_date, reincarceration
         in all_reincarcerations.items()
         if end_date > reincarceration_date >= start_date]

    return reincarcerations_in_window_dict


def returned_within_follow_up_period(event: ReleaseEvent, period: int) -> bool:
    """Returns whether someone was reincarcerated within the given follow-up
    period following their release."""
    start_date = event.release_date
    end_date = start_date + relativedelta(years=period)

    if isinstance(event, RecidivismReleaseEvent):
        return start_date <= event.reincarceration_date < end_date

    return False


def days_at_liberty(event: RecidivismReleaseEvent) -> int:
    """Returns the number of days between a release and a reincarceration."""
    release_date = event.release_date

    return_date = event.reincarceration_date

    delta = return_date - release_date

    if delta.days < 0:
        logging.error("Release date on RecidivismReleaseEvent is before admission date: %s."
                      "The identifier step is not properly identifying returns.", event)

    return delta.days


def relevant_follow_up_periods(release_date: date, current_date: date,
                               follow_up_periods: range) -> List[int]:
    """Finds the given follow-up periods which are relevant to measurement.

    Returns all of the given follow-up periods after the given release date
    which are either complete as the current_date, or still in progress as of
    today.

    Examples where today is 2018-01-26:
        relevant_follow_up_periods("2015-01-05", today, FOLLOW_UP_PERIODS) =
            [1,2,3,4]
        relevant_follow_up_periods("2015-01-26", today, FOLLOW_UP_PERIODS) =
            [1,2,3,4]
        relevant_follow_up_periods("2015-01-27", today, FOLLOW_UP_PERIODS) =
            [1,2,3]
        relevant_follow_up_periods("2016-01-05", today, FOLLOW_UP_PERIODS) =
            [1,2,3]
        relevant_follow_up_periods("2017-04-10", today, FOLLOW_UP_PERIODS) =
            [1]
        relevant_follow_up_periods("2018-01-05", today, FOLLOW_UP_PERIODS) =
            [1]
        relevant_follow_up_periods("2018-02-05", today, FOLLOW_UP_PERIODS) =
            []

    Args:
        release_date: the release Date we are tracking from
        current_date: the current Date we are tracking towards
        follow_up_periods: the list of follow up periods to filter

    Returns:
        The list of follow up periods which are relevant to measure, i.e.
        already completed or still in progress.
    """
    return [period for period in follow_up_periods
            if release_date + relativedelta(years=period - 1) <= current_date]


def characteristics_dict(person: StatePerson,
                         event: ReleaseEvent,
                         metric_class: Type[ReincarcerationRecidivismMetric],
                         person_metadata: PersonMetadata) -> Dict[str, Any]:
    """Builds a dictionary that describes the characteristics of the person and the release event.

    Release cohort, follow-up period, and methodology are not included in the output here. They are added into
    augmented versions of these combinations later.

    Args:
        person: the StatePerson we are picking characteristics from
        event: the ReleaseEvent we are picking characteristics from
        metric_class: The ReincarcerationRecidivismMetric provided determines which fields should be added to the
            characteristics dictionary
        person_metadata: Contains information about the StatePerson that is necessary for the metrics.

    Returns:
        A dictionary populated with all relevant characteristics.
    """
    event_date = event.original_admission_date

    characteristics = characteristics_dict_builder(pipeline='recidivism',
                                                   event=event,
                                                   metric_class=metric_class,
                                                   person=person,
                                                   event_date=event_date,
                                                   include_person_attributes=True,
                                                   person_metadata=person_metadata)

    return characteristics


def sorted_releases_in_year(release_date: date, all_release_events: Dict[int, List[ReleaseEvent]]) -> \
        List[ReleaseEvent]:
    """Returns the releases in a given year, sorted by release date."""
    year_of_release = release_date.year

    releases_in_year = all_release_events.get(year_of_release)

    if not releases_in_year:
        raise ValueError(f"Release year {year_of_release} should be present in release_events: {all_release_events}. "
                         f"Identifier code is not correctly classifying all release events by release cohort year.")

    releases_in_year.sort(key=lambda b: b.release_date)

    return releases_in_year


def combination_rate_metrics(combo: Dict[str, Any],
                             event: ReleaseEvent,
                             all_release_events: Dict[int, List[ReleaseEvent]],
                             reincarcerations_by_follow_up_period: Dict[int, List[RecidivismReleaseEvent]]) -> \
        List[Tuple[Dict[str, Any], int]]:
    """Returns all unique recidivism rate metrics for the given combination.

    For the characteristic combination, i.e. a unique metric, look at all follow-up periods to determine under which
    ones recidivism occurred. For each methodology and period, get a list of combos that are augmented with
    methodology, period, and return details. Then, map each augmented combo to 0 or 1 accordingly.

    Args:
        combo: a characteristic combination to convert into metrics
        event: the release event from which the combination was derived
        all_release_events: A dictionary mapping release cohorts to a list of ReleaseEvents for the given StatePerson.
        reincarcerations_by_follow_up_period: dictionary where the keys are all relevant periods for measurement, and
            the values are lists of dictionaries representing the reincarceration admissions during that period

    Returns:
        A list of key-value tuples representing specific metric combination dictionaries and the recidivism value
            corresponding to that metric.
    """
    metrics = []

    releases_in_year = sorted_releases_in_year(event.release_date, all_release_events)

    # There will always be at least one release in this list that represents the current release event.
    # `sorted_releases_in_year` should fail if that is not the case.
    if not releases_in_year:
        raise ValueError("Function `sorted_releases_in_year` should not be returning empty lists.")

    is_first_release_in_year = (id(event) == id(releases_in_year[0]))

    for period, reincarceration_admissions in reincarcerations_by_follow_up_period.items():
        person_based_augmented_combo = person_level_augmented_combo(combo, event, MetricMethodologyType.PERSON, period)

        event_based_augmented_combo = person_level_augmented_combo(combo, event, MetricMethodologyType.EVENT, period)

        # If they didn't recidivate at all or not yet for this period (or they didn't recidivate until 10 years had
        # passed), assign 0 for both event- and person-based measurement.
        if isinstance(event, NonRecidivismReleaseEvent) or not reincarceration_admissions:

            if is_first_release_in_year:
                # Only count the first release in a year for person-based metrics
                metrics.append((person_based_augmented_combo, 0))

            # Add event-based count
            metrics.append((event_based_augmented_combo, 0))

        # If they recidivated, each unique release of a given person within a follow-up period after the year of release
        # may be counted as an instance of recidivism for event-based measurement. For person-based measurement, only
        # one instance may be counted.
        elif isinstance(event, RecidivismReleaseEvent):
            if is_first_release_in_year:
                # Only count the first release in a year for person-based metrics
                metrics.append((person_based_augmented_combo, recidivism_value_for_metric(
                    person_based_augmented_combo, event.return_type,
                    event.from_supervision_type,
                    event.source_violation_type)))

            for reincarceration in reincarceration_admissions:
                event_combo_copy = event_based_augmented_combo.copy()

                metrics.append((event_combo_copy, recidivism_value_for_metric(
                    event_based_augmented_combo,
                    reincarceration.return_type,
                    reincarceration.from_supervision_type,
                    reincarceration.source_violation_type)))

    return metrics


def combination_count_metrics(combo: Dict[str, Any],
                              event: RecidivismReleaseEvent,
                              all_reincarcerations: Dict[date, RecidivismReleaseEvent],
                              metric_period_end_date: date) -> List[Tuple[Dict[str, Any], int]]:
    """"Returns all unique recidivism count metrics for the given event and combination.

    If the event is an instance of recidivism, then for each methodology, gets a list of combos that are augmented with
    methodology and return details. Then, maps each augmented combo to 0 or 1 accordingly.

    Args:
        combo: a characteristic combination to convert into metrics
        event: the release event from which the combination was derived
        all_reincarcerations: dictionary where the keys are all dates of reincarceration for the person's ReleaseEvents,
            and the values are the corresponding ReleaseEvents
        metric_period_end_date: The day the metric periods end

    Returns:
        A list of key-value tuples representing specific metric combination dictionaries and the recidivism value
            corresponding to that metric.
    """
    metrics = []

    # Each return is included for event-based measurement. However, for person-based measurement, only one instance may
    # be counted in a given window.
    person_based_augmented_combo = person_level_augmented_combo(combo, event, MetricMethodologyType.PERSON, None)

    event_based_augmented_combo = person_level_augmented_combo(combo, event, MetricMethodologyType.EVENT, None)

    # Adds one day because the reincarcerations_in_window function is exclusive of the end date, and we want the count
    # to include reincarcerations that happen on the last day of this count window.
    end_date = metric_period_end_date + datetime.timedelta(days=1)

    all_reincarcerations_in_window = reincarcerations_in_window(
        event.reincarceration_date, end_date, all_reincarcerations)

    if len(all_reincarcerations_in_window) == 1:
        # This function will be called for every single one of the person's release events that resulted in a
        # reincarceration. If this is the last instance of reincarceration before the end of the window, then include
        # this in the person-based count.
        metrics.append((person_based_augmented_combo, recidivism_value_for_metric(
            person_based_augmented_combo, event.return_type,
            event.from_supervision_type,
            event.source_violation_type)))

    metrics.append((event_based_augmented_combo, recidivism_value_for_metric(
        event_based_augmented_combo,
        event.return_type,
        event.from_supervision_type,
        event.source_violation_type)))

    if combo.get('person_id') is not None:
        # Only include person-level count metrics that are applicable to the person
        metrics = [(combination, value) for combination, value in metrics if value == 1]

    return metrics


def person_level_augmented_combo(combo: Dict[str, Any], event: ReleaseEvent,
                                 methodology: MetricMethodologyType,
                                 period: Optional[int]) -> Dict[str, Any]:
    """Returns a dictionary that has been augmented with all of the parameters that apply to the given event.

    Args:
        combo: the base combo to be augmented with methodology and period
        event: the ReleaseEvent from which the combo was derived
        methodology: the MetricMethodologyType to add to each combo
        period: the follow_up_period value to add to each combo

    Returns:
        The augmented combination dictionary.
    """
    parameters: Dict[str, Any] = {'state_code': event.state_code,
                                  'methodology': methodology}

    if period:
        parameters['follow_up_period'] = period

    return augment_combination(combo, parameters)


def recidivism_value_for_metric(
        combo: Dict[str, Any],
        event_return_type:
        Optional[ReincarcerationReturnType],
        event_from_supervision_type:
        Optional[StateSupervisionPeriodSupervisionType],
        event_source_violation_type: Optional[StateSupervisionViolationType]) \
        -> int:
    """Returns the recidivism value corresponding to the given metric combo and
    details of the return.

    Args:
        combo: metric combination
        event_return_type: the ReincarcerationReturnType of the release event
        event_from_supervision_type:
            the StateSupervisionPeriodSupervisionType of the release event
        event_source_violation_type:
            the StateSupervisionViolationType of the violation that eventually
            resulted in this return

    Returns: 1 if the event_return_type, event_from_supervision_type, and
        event_source_violation_type match that of the combo. If the value for
        any of these fields on the combo is None, this is considered a match
        with the event's value. Returns 0 if these fields do not match.
    """
    combo_return_type = combo.get('return_type')
    combo_from_supervision_type = combo.get('from_supervision_type')
    combo_source_violation_type = combo.get('source_violation_type')

    if combo_return_type is None and combo_from_supervision_type is None and \
            combo_source_violation_type is None:
        return 1

    if combo_return_type != event_return_type:
        return 0

    if combo_return_type == \
            ReincarcerationReturnType.REVOCATION:

        if combo_from_supervision_type is None:
            if combo_source_violation_type is None or \
                    combo_source_violation_type == event_source_violation_type:
                return 1

        if combo_from_supervision_type != event_from_supervision_type:
            return 0

        if combo_source_violation_type is None:
            return 1

        if combo_source_violation_type != event_source_violation_type:
            return 0

    return 1
