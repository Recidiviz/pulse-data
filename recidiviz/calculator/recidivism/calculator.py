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

from itertools import combinations, repeat

from typing import Any, Dict, List, Optional, Tuple

from datetime import date
from dateutil.relativedelta import relativedelta

from recidiviz.calculator.recidivism.release_event import ReleaseEvent, \
    RecidivismReleaseEvent, NonRecidivismReleaseEvent
from recidiviz.calculator.recidivism.metrics import RecidivismMethodologyType
from recidiviz.persistence.entity.state.entities import StatePerson


# We measure in 1-year follow up periods up to 10 years after date of release.
FOLLOW_UP_PERIODS = range(1, 11)

# TODO(1781): Update all docstrings to accurately portray how races and
#  ethnicities are being stored now


def map_recidivism_combinations(person: StatePerson,
                                release_events:
                                Dict[int, List[ReleaseEvent]]) \
        -> List[Tuple[Dict[str, Any], Any]]:
    """Transforms ReleaseEvents and a StatePerson into metric combinations.

    Takes in a StatePerson and all of her ReleaseEvents and returns an array
    of "recidivism combinations". These are key-value pairs where the key
    represents a specific metric and the value represents whether or not
    recidivism occurred. If a metric does count towards recidivism, then the
    value is 1 if event-based or 1/k if person-based, where k = the number of
    releases for that StatePerson within the follow-up period after the release.
    If it does not count towards recidivism, then the value is 0 in either
    methodology.

    Effectively, this translates a particular recidivism event into many
    recidivism metrics. This is because each metric represents one of many
    possible combinations of characteristics being tracked for that event. For
    example, if an asian male is reincarcerated, there is a metric that
    corresponds to asian people, one to males, one to asian males, one to all
    people, and more depending on other dimensions in the data.

    Example output for a hispanic female age 27 who was released in 2008 and
    went back to prison in 2014:
    [
      ({'methodology': RecidivismMethodologyType.EVENT, 'release_cohort': 2008,
        'follow_up_period': 5, 'gender': Gender.FEMALE, 'age': '25-29'}, 0),
      ({'methodology': RecidivismMethodologyType.PERSON',
            'release_cohort': 2008, 'follow_up_period': 5,
             'gender': Gender.FEMALE, 'age': '25-29'}, 0),
      ({'methodology': RecidivismMethodologyType.EVENT, 'release_cohort': 2008,
        'follow_up_period': 6, 'gender': Gender.FEMALE, 'age': '25-29'}, 1),
      ({'methodology': RecidivismMethodologyType.EVENT, 'release_cohort': 2008,
        'follow_up_period': 6, 'gender': Gender.FEMALE, 'race': 'hispanic'}, 1),
      ...
    ]

    Args:
        person: the StatePerson
        release_events: A dictionary mapping release cohorts to a list of
            ReleaseEvents for the given StatePerson.

    Returns:
        A list of key-value tuples representing specific metric combinations and
        the recidivism value corresponding to that metric.
    """
    metrics = []
    all_reincarceration_dates = reincarceration_dates(release_events)

    for release_cohort, events in release_events.items():
        for event in events:
            characteristic_combos = characteristic_combinations(person, event)

            if isinstance(event, RecidivismReleaseEvent):
                earliest_recidivism_period = \
                    earliest_recidivated_follow_up_period(
                        event.release_date,
                        event.reincarceration_date)
            else:
                earliest_recidivism_period = None

            relevant_periods = relevant_follow_up_periods(
                event.release_date, date.today(), FOLLOW_UP_PERIODS)

            for combo in characteristic_combos:
                combo['release_cohort'] = release_cohort

                metrics.extend(combination_metrics(
                    combo, event, all_reincarceration_dates,
                    earliest_recidivism_period, relevant_periods))

    return metrics


def reincarceration_dates(release_events: Dict[int, List[ReleaseEvent]]) \
        -> List[date]:
    """Finds the dates of reincarceration within the given ReleaseEvents.

    Returns the list of reincarceration dates extracted from the given array of
    ReleaseEvents. If one of the given events is not an instance of
    recidivism, i.e. it is not a RecidivismReleaseEvent, then it is not
    represented in the output.

    Args:
        release_events: the list of ReleaseEvents.

    Returns:
        A list of reincarceration dates, in the order in which they appear in
        the given list of objects.
    """
    dates = []
    for _cohort, events in release_events.items():
        dates.extend([event.reincarceration_date for event in events
                      if isinstance(event, RecidivismReleaseEvent)])

    return dates


def count_reincarcerations_in_window(start_date: date,
                                     follow_up_period: int,
                                     all_reincarceration_dates: List[date]) \
        -> int:
    """Finds the number of reincarceration dates during the given window.

    Returns how many of the given reincarceration dates fall within the given
    follow-up period after the given start date, end point exclusive, including
    the start date itself if it is within the given array.

    Example:
        count_reincarcerations_in_window("2016-05-13", 6,
            ["2012-04-30", "2016-05-13", "2020-11-20",
            "2021-01-12", "2022-05-13"]) = 3

    Args:
        start_date: a Date to start tracking from
        follow_up_period: the follow-up period to count within
        all_reincarceration_dates: the list of reincarceration dates to check

    Returns:
        How many of the given reincarceration dates are within the follow-up
        period from the given start date.
    """
    reincarcerations_in_window = \
        [reincarceration_date for reincarceration_date
         in all_reincarceration_dates
         if start_date + relativedelta(years=follow_up_period)
         > reincarceration_date >= start_date]

    return len(reincarcerations_in_window)


def earliest_recidivated_follow_up_period(
        release_date: date,
        reincarceration_date: Optional[date]) -> Optional[int]:
    """Finds the earliest follow-up period under which recidivism has occurred.

    For example, if someone was released from prison on March 14, 2005 and
    reincarcerated on April 23, 2008, then the earliest follow-up period is 4,
    as they had not yet recidivated within 3 years, but had within 4.

    Args:
        release_date: a Date for when the person was released
        reincarceration_date: a Date for when the person was reincarcerated

    Returns:
        An integer for the earliest follow-up period under which recidivism
        occurred. None if there is no reincarceration date provided.
    """
    if not reincarceration_date:
        return None

    years_apart = reincarceration_date.year - release_date.year

    if years_apart == 0:
        return 1

    after_anniversary = ((reincarceration_date.month, reincarceration_date.day)
                         > (release_date.month, release_date.day))
    return years_apart + 1 if after_anniversary else years_apart


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


def age_at_date(person: StatePerson, check_date: date) -> Optional[int]:
    """Calculates the age of the StatePerson at the given date.

    Args:
        person: the StatePerson
        check_date: the date to check

    Returns:
        The age of the StatePerson at the given date. None if no birthdate is
         known.
    """
    birthdate = person.birthdate
    return None if birthdate is None else \
        check_date.year - birthdate.year - \
        ((check_date.month, check_date.day) < (birthdate.month, birthdate.day))


def age_bucket(age: Optional[int]) -> Optional[str]:
    """Calculates the age bucket that applies to measurement.

    Age buckets for measurement: <25, 25-29, 30-34, 35-39, 40<

    Args:
        age: the person's age

    Returns:
        A string representation of the age bucket for the person. None if the
            age is not known.
    """
    if age is None:
        return None
    if age < 25:
        return '<25'
    if age <= 29:
        return '25-29'
    if age <= 34:
        return '30-34'
    if age <= 39:
        return '35-39'
    return '40<'


def stay_length_from_event(event: ReleaseEvent) -> Optional[int]:
    """Calculates the length of facility stay of a given event in months.

    This is rounded down to the nearest month, so a stay from 2015-01-15 to
    2017-01-14 results in a stay length of 23 months. Note that bucketing in
    stay_length_bucketing is upper bound exclusive, so in this example the
    bucket would be 12-24, and if the stay ended on 2017-01-15, the stay length
    would be 24 months and the bucket would be 24-36.

    Args:
        event: the ReleaseEvent

    Returns:
        The length of the facility stay in months. None if the original
        admission date or release date is not known.
    """
    if event.original_admission_date is None or event.release_date is None:
        return None

    delta = relativedelta(event.release_date, event.original_admission_date)
    return delta.years * 12 + delta.months


def stay_length_bucket(stay_length: Optional[int]) -> Optional[str]:
    """Calculates the stay length bucket that applies to measurement.

    Stay length buckets (upper bound exclusive) for measurement:
        <12, 12-24, 24-36, 36-48, 48-60, 60-72,
        72-84, 84-96, 96-108, 108-120, 120+.

    Args:
        stay_length: the length in months of the person's facility stay.

    Returns:
        A string representation of the age bucket for the person.
    """
    if stay_length is None:
        return None
    if stay_length < 12:
        return '<12'
    if stay_length < 24:
        return '12-24'
    if stay_length < 36:
        return '24-36'
    if stay_length < 48:
        return '36-48'
    if stay_length < 60:
        return '48-60'
    if stay_length < 72:
        return '60-72'
    if stay_length < 84:
        return '72-84'
    if stay_length < 96:
        return '84-96'
    if stay_length < 108:
        return '96-108'
    if stay_length < 120:
        return '108-120'
    return '120<'


def characteristic_combinations(person: StatePerson,
                                event: ReleaseEvent) -> List[Dict[str, Any]]:
    """Calculates all recidivism metric combinations.

    Returns the list of all combinations of the metric characteristics, of all
    sizes, given the StatePerson and ReleaseEvent. That is, this returns a list
    of dictionaries where each dictionary is a combination of 0 to n unique
    elements of characteristics, where n is the size of the given array.

    For each event, we need to calculate metrics across combinations of:
    Release Cohort; Follow-up Period (up to 10 years);
    RecidivismMethodologyType (Event-based, Person-based);
    Demographics (age, race, gender); Location (facility, region);
    Facility Stay Breakdown (stay length); ...
    TODO: Add support for offense and sentencing type (Issues 33 and 32)

    Release cohort, follow-up period, and methodology are not included in the
    output here. They are added into augmented versions of these combinations
    later.

    The output for a black female age 24 and an incarceration that began in
    January 2008 and ended in February 2009 is equal to the output of:
            for_characteristics({'age': '<25', 'race': 'black',
                                 'gender': Gender.FEMALE,
                                 'stay_length': '12-24'})


    Args:
        person: the StatePerson we are picking characteristics from
        event: the ReleaseEvent we are picking characteristics from

    Returns:
        A list of dictionaries containing all unique combinations of
        characteristics.
    """
    entry_age = age_at_date(person, event.original_admission_date)
    entry_age_bucket = age_bucket(entry_age)
    event_stay_length = stay_length_from_event(event)
    event_stay_length_bucket = stay_length_bucket(event_stay_length)

    characteristics: Dict[str, Any] = {'stay_length': event_stay_length_bucket}

    # TODO(1781) Handle multiple races and ethnicities

    if entry_age_bucket is not None:
        characteristics['age'] = entry_age_bucket
    if person.gender is not None:
        characteristics['gender'] = person.gender
    if event.release_facility is not None:
        characteristics['release_facility'] = event.release_facility
    if isinstance(event, RecidivismReleaseEvent) \
            and event.return_type is not None:
        characteristics['return_type'] = event.return_type

    return for_characteristics(characteristics)


def for_characteristics(characteristics) -> List[Dict[str, Any]]:
    """The list of all combinations of the given metric characteristics.

    Example:
        for_characteristics(
        {'race': 'black', 'gender': Gender.FEMALE, 'age': '<25'}) =
            [{},
            {'age': '<25'}, {'race': 'black'}, {'gender': Gender.FEMALE},
            {'age': '<25', 'race': 'black'}, {'age': '<25',
                'gender': Gender.FEMALE},
            {'race': 'black', 'gender': Gender.FEMALE},
            {'age': '<25', 'race': 'black', 'gender': Gender.FEMALE}]


    Args:
        characteristics: a dictionary of metric characteristics to derive
            combinations from

    Returns:
        A list of dictionaries containing all unique combinations of
        characteristics.
    """
    combos: List[Dict[Any, Any]] = [{}]
    for i in range(len(characteristics)):
        i_combinations = map(dict,
                             combinations(characteristics.items(), i + 1))
        for combo in i_combinations:
            combos.append(combo)
    return combos


def combination_metrics(combo: Dict[str, Any], event: ReleaseEvent,
                        all_reincarceration_dates: List[date],
                        earliest_recidivism_period: Optional[int],
                        relevant_periods: List[int]) \
        -> List[Tuple[Dict[str, Any], int]]:
    """Returns all unique recidivism metrics for the given combination.

    For the characteristic combination, i.e. a unique metric, look at all
    follow-up periods to determine under which ones recidivism occurred. Augment
    that combination with methodology and period, and map each augmented combo
    to 0 or 1 accordingly.

    Args:
        combo: a characteristic combination to convert into metrics
        event: the recidivism event from which the combination was derived
        all_reincarceration_dates: all dates of reincarceration for the person's
            ReleaseEvents
        earliest_recidivism_period: the earliest follow-up period under which
            recidivism occurred
        relevant_periods: the list of periods relevant for measurement

    Returns:
        A list of key-value tuples representing specific metric combination
            dictionaries and the recidivism value corresponding to that metric.
    """
    metrics = []

    for period in relevant_periods:
        person_based_combo = augment_combination(
            combo, RecidivismMethodologyType.PERSON, period)
        event_based_combo = augment_combination(
            combo, RecidivismMethodologyType.EVENT, period)

        # If they didn't recidivate at all or not yet for this period
        # (or they didn't recidivate until 10 years had passed),
        # assign 0 for both event- and person-based measurement.
        if isinstance(event, NonRecidivismReleaseEvent) \
                or not earliest_recidivism_period \
                or period < earliest_recidivism_period:
            metrics.append((person_based_combo, 0))
            metrics.append((event_based_combo, 0))

        # If they recidivated, each unique release of a given person
        # within a follow-up period after the year of release is counted
        # as an instance of recidivism for event-based measurement. For
        # person-based measurement, only one instance is counted.
        else:
            metrics.append((person_based_combo, 1))

            reincarcerations_in_window = \
                count_reincarcerations_in_window(
                    event.release_date, period,
                    all_reincarceration_dates)

            for _ in repeat(None, reincarcerations_in_window):
                metrics.append((event_based_combo, 1))

    return metrics


def augment_combination(characteristic_combo: Dict[str, Any],
                        methodology: RecidivismMethodologyType,
                        period: int) -> Dict[str, Any]:
    """Returns a copy of the combo with the additional parameters added.

    Creates a shallow copy of the given characteristic combination and sets the
    given methodology and follow-up period on the copy. This avoids updating the
    existing characteristic combo.

    Args:
        characteristic_combo: the combination to copy and augment
        methodology: the methodology to set, i.e.
            RecidivismMethodologyType.PERSON or RecidivismMethodologyType.EVENT
        period: the follow-up period to set

    Returns:
        The augmented characteristic combination, ready for tracking.
    """
    augmented_combo = characteristic_combo.copy()
    augmented_combo['methodology'] = methodology
    augmented_combo['follow_up_period'] = period
    return augmented_combo
