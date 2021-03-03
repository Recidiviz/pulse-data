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
from typing import Any, Dict, List, Tuple

from datetime import date
from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.recidivism.metrics import (
    ReincarcerationRecidivismMetricType,
    ReincarcerationRecidivismCountMetric,
    ReincarcerationRecidivismRateMetric,
)
from recidiviz.calculator.pipeline.recidivism.release_event import (
    ReleaseEvent,
    RecidivismReleaseEvent,
    NonRecidivismReleaseEvent,
)
from recidiviz.calculator.pipeline.utils.calculator_utils import (
    characteristics_dict_builder,
    augmented_combo_for_calculations,
)
from recidiviz.calculator.pipeline.utils.person_utils import PersonMetadata
from recidiviz.persistence.entity.state.entities import StatePerson

# We measure in 1-year follow up periods up to 10 years after date of release.
FOLLOW_UP_PERIODS = range(1, 11)


def map_recidivism_combinations(
    person: StatePerson,
    release_events: Dict[int, List[ReleaseEvent]],
    metric_inclusions: Dict[ReincarcerationRecidivismMetricType, bool],
    person_metadata: PersonMetadata,
) -> List[Tuple[Dict[str, Any], Any]]:
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

    if metric_inclusions.get(ReincarcerationRecidivismMetricType.REINCARCERATION_RATE):
        for events in release_events.values():
            for event in events:
                event_date = event.release_date

                characteristic_combo = characteristics_dict_builder(
                    pipeline="recidivism",
                    event=event,
                    metric_class=ReincarcerationRecidivismRateMetric,
                    person=person,
                    event_date=event_date,
                    person_metadata=person_metadata,
                )

                reincarcerations_by_follow_up_period = reincarcerations_by_period(
                    event_date, all_reincarcerations
                )

                metrics.extend(
                    combination_rate_metrics(
                        characteristic_combo,
                        event,
                        reincarcerations_by_follow_up_period,
                    )
                )

    if metric_inclusions.get(ReincarcerationRecidivismMetricType.REINCARCERATION_COUNT):
        for reincarceration_event in all_reincarcerations.values():
            event_date = reincarceration_event.reincarceration_date

            characteristic_combo = characteristics_dict_builder(
                pipeline="recidivism",
                event=reincarceration_event,
                metric_class=ReincarcerationRecidivismCountMetric,
                person=person,
                event_date=event_date,
                person_metadata=person_metadata,
            )

            augmented_combo = augmented_combo_for_calculations(
                characteristic_combo,
                state_code=reincarceration_event.state_code,
                year=event_date.year,
                month=event_date.month,
                metric_type=ReincarcerationRecidivismMetricType.REINCARCERATION_COUNT,
            )

            metrics.append((augmented_combo, 1))

    return metrics


def reincarcerations_by_period(
    release_date: date, all_reincarcerations: Dict[date, RecidivismReleaseEvent]
) -> Dict[int, List[RecidivismReleaseEvent]]:
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
    relevant_periods = relevant_follow_up_periods(
        release_date, date.today(), FOLLOW_UP_PERIODS
    )

    reincarcerations_by_follow_up_period: Dict[int, List[RecidivismReleaseEvent]] = {}

    for period in relevant_periods:
        end_of_follow_up_period = release_date + relativedelta(years=period)

        all_reincarcerations_in_window = reincarcerations_in_window(
            release_date, end_of_follow_up_period, all_reincarcerations
        )

        reincarcerations_by_follow_up_period[period] = all_reincarcerations_in_window

    return reincarcerations_by_follow_up_period


def reincarcerations(
    release_events: Dict[int, List[ReleaseEvent]]
) -> Dict[date, RecidivismReleaseEvent]:
    """Finds the reincarcerations within the given ReleaseEvents.

    Returns a dictionary where the keys are all dates of reincarceration for the person's ReleaseEvents, and the values
    are RecidivismReleaseEvents corresponding to that reincarceration.

    If one of the given events is not an instance of recidivism, i.e. it is not a RecidivismReleaseEvent, then it is not
    represented in the output.

    Args:
        release_events: the list of ReleaseEvents.

    Returns:
        A dictionary representing the dates of reincarceration and the RecidivismReleaseEvent for each reincarceration.
    """
    reincarcerations_dict: Dict[date, RecidivismReleaseEvent] = {}

    for _cohort, events in release_events.items():
        for event in events:
            if isinstance(event, RecidivismReleaseEvent):
                if event.reincarceration_date in reincarcerations_dict:
                    # If two valid releases have identified the same admission date as the reincarceration, then
                    # we want to prioritize the one with the fewer days between release and reincarceration
                    release_event_same_reincarceration = reincarcerations_dict[
                        event.reincarceration_date
                    ]
                    reincarcerations_dict[event.reincarceration_date] = (
                        event
                        if event.days_at_liberty
                        < release_event_same_reincarceration.days_at_liberty
                        else release_event_same_reincarceration
                    )
                else:
                    reincarcerations_dict[event.reincarceration_date] = event

    return reincarcerations_dict


def reincarcerations_in_window(
    start_date: date,
    end_date: date,
    all_reincarcerations: Dict[date, RecidivismReleaseEvent],
) -> List[RecidivismReleaseEvent]:
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
    reincarcerations_in_window_dict = [
        reincarceration
        for reincarceration_date, reincarceration in all_reincarcerations.items()
        if end_date > reincarceration_date >= start_date
    ]

    return reincarcerations_in_window_dict


def relevant_follow_up_periods(
    release_date: date, current_date: date, follow_up_periods: range
) -> List[int]:
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
    return [
        period
        for period in follow_up_periods
        if release_date + relativedelta(years=period - 1) <= current_date
    ]


def combination_rate_metrics(
    combo: Dict[str, Any],
    event: ReleaseEvent,
    reincarcerations_by_follow_up_period: Dict[int, List[RecidivismReleaseEvent]],
) -> List[Tuple[Dict[str, Any], int]]:
    """Returns all recidivism rate metrics for the combo given the relevant follow-up periods.

    Args:
        combo: a characteristic combination to convert into metrics
        event: the release event from which the combination was derived
        reincarcerations_by_follow_up_period: dictionary where the keys are all relevant periods for measurement, and
            the values are lists of dictionaries representing the reincarceration admissions during that period

    Returns:
        A list of key-value tuples representing specific metric combination dictionaries and the recidivism value
            corresponding to that metric.
    """
    metrics = []

    for (
        period,
        reincarceration_admissions,
    ) in reincarcerations_by_follow_up_period.items():
        augmented_combo = augmented_combo_for_calculations(
            combo,
            event.state_code,
            metric_type=ReincarcerationRecidivismMetricType.REINCARCERATION_RATE,
        )
        augmented_combo["follow_up_period"] = period

        # If they didn't recidivate at all or not yet for this period (or they didn't recidivate until 10 years had
        # passed), assign a value of 0.
        if (
            isinstance(event, NonRecidivismReleaseEvent)
            or not reincarceration_admissions
        ):
            metrics.append((augmented_combo, 0))

        # If they recidivated, each unique release of a given person within a follow-up period after the year of release
        # may be counted as an instance of recidivism for event-based measurement.
        elif isinstance(event, RecidivismReleaseEvent):
            for reincarceration in reincarceration_admissions:
                event_combo_copy = augmented_combo.copy()
                event_combo_copy["return_type"] = reincarceration.return_type
                event_combo_copy[
                    "from_supervision_type"
                ] = reincarceration.from_supervision_type
                event_combo_copy[
                    "source_violation_type"
                ] = reincarceration.source_violation_type

                metrics.append((event_combo_copy, 1))

    return metrics
