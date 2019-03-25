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

"""Calculates recidivism metrics from recidivism and non-recidivism events.

This contains the core logic for calculating recidivism metrics on a
person-by-person basis. It transforms RecidivismEvents into recidivism metrics,
key-value pairs where the key represents all of the dimensions represented in
the data point, and the value represents a recidivism value, e.g. 0 for no or 1
for yes.

This class is paired with identifier.py, together providing the ability to
transform a person into an array of recidivism metrics.

Example:
    recidivism_events = identification.find_recidivism(person)
    metric_combinations = calculator.map_recidivism_combinations(
        person, recidivism_events)

Attributes:
    FOLLOW_UP_PERIODS: a list of integers, the follow-up periods that we measure
        recidivism over, from 1 to 10.
"""


from datetime import date
from itertools import combinations
from itertools import repeat
from dateutil.relativedelta import relativedelta


# We measure in 1-year follow up periods up to 10 years after date of release.
FOLLOW_UP_PERIODS = range(1, 11)


def map_recidivism_combinations(person, recidivism_events):
    """Transforms the given recidivism events and person details into unique
    recidivism metric combinations to count.

    Takes in a person and all of her recidivism events and returns an array of
    "recidivism combinations". These are key-value pairs where the key
    represents a specific metric and the value represents whether or not
    recidivism occurred. If a metric does count towards recidivism, then the
    value is 1 if event-based or 1/k if offender-based, where k = the number of
    releases for that person within the follow-up period after the release.
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
      ({"methodology": "EVENT", "release_cohort": 2008, "follow_up_period": 5,
        "sex": "female", "age": "25-29"}, 0),
      ({"methodology": "OFFENDER", "release_cohort": 2008,
        "follow_up_period": 5, "sex": "female", "age": "25-29"}, 0),
      ({"methodology": "EVENT", "release_cohort": 2008, "follow_up_period": 6,
        "sex": "female", "age": "25-29"}, 1),
      ({"methodology": "EVENT", "release_cohort": 2008, "follow_up_period": 6,
        "sex": "female", "race": "hispanic"}, 1),
      ...
    ]

    Args:
        person: the person
        recidivism_events: the list of RecidivismEvents for the person.

    Returns:
        A list of key-value tuples representing specific metric combinations and
        the recidivism value corresponding to that metric.
    """
    metrics = []
    all_reincarceration_dates = reincarceration_dates(recidivism_events)

    for release_cohort, event in recidivism_events.items():
        characteristic_combos = characteristic_combinations(person, event)

        earliest_recidivism_period = earliest_recidivated_follow_up_period(
            event.release_date, event.reincarceration_date)

        relevant_periods = relevant_follow_up_periods(
            event.release_date, date.today(), FOLLOW_UP_PERIODS)

        for combo in characteristic_combos:
            combo['release_cohort'] = release_cohort

            metrics.extend(combination_metrics(
                combo, event, all_reincarceration_dates,
                earliest_recidivism_period, relevant_periods))

    return metrics


def reincarceration_dates(recidivism_events):
    """The dates of reincarceration within the given recidivism events.

    Returns the list of reincarceration dates extracted from the given array of
    recidivism events. If one of the given events is not an instance of
    recidivism, i.e. has no reincarceration date, then it is not represented in
    the output.

    Args:
        recidivism_events: the list of recidivism events.

    Returns:
        A list of reincarceration dates, in the order in which they appear in
        the given list of objects.
    """
    return [event.reincarceration_date
            for _cohort, event in recidivism_events.items()
            if event.reincarceration_date]


def count_reincarcerations_in_window(start_date,
                                     follow_up_period,
                                     all_reincarceration_dates):
    """The number of the given reincarceration dates during the window from the
    start date until the end of the follow-up period.

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


def earliest_recidivated_follow_up_period(release_date, reincarceration_date):
    """The earliest follow-up period under which recidivism has occurred.

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


def relevant_follow_up_periods(release_date, current_date, follow_up_periods):
    """All of the given follow-up periods which are relevant to measurement.

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


def age_at_date(person, check_date):
    """The age of the person at the given date.

    Args:
        person: the person
        check_date: the date to check

    Returns:
        The age of the person at the given date. None if no birthdate is known.
    """
    birthdate = person.birthdate
    return None if birthdate is None else \
        check_date.year - birthdate.year - \
        ((check_date.month, check_date.day) < (birthdate.month, birthdate.day))


def age_bucket(age):
    """The age bucket that applies to measurement.

    Age buckets for measurement: <25, 25-29, 30-34, 35-39, 40<

    Args:
        age: the person's age

    Returns:
        A string representation of the age bucket for the person.
    """
    if age < 25:
        return '<25'
    if age <= 29:
        return '25-29'
    if age <= 34:
        return '30-34'
    if age <= 39:
        return '35-39'
    return '40<'


def stay_length_from_event(event):
    """Length of facility stay of a given event in months.

    This is rounded down to the nearest month, so a stay from 2015-01-15 to
    2017-01-14 results in a stay length of 23 months. Note that bucketing in
    stay_length_bucketing is upper bound exclusive, so in this example the
    bucket would be 12-24, and if the stay ended on 2017-01-15, the stay length
    would be 24 months and the bucket would be 24-36.

    Args:
        event: the event

    Returns:
	The length of the facility stay in months. None if the original entry
        date or release date is not known.
    """
    if event.original_entry_date is None or event.release_date is None:
        return None

    delta = relativedelta(event.release_date, event.original_entry_date)
    return delta.years * 12 + delta.months


def stay_length_bucket(stay_length):
    """The stay length bucket that applies to measurement.

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


def characteristic_combinations(person, event):
    """The list of all combinations of the metric characteristics picked from
    the given person and recidivism event.

    Returns the list of all combinations of the metric characteristics, of all
    sizes. That is, this returns a list of dictionaries where each dictionary
    is a combination of 0 to n unique elements of characteristics, where n is
    the size of the given array.

    For each event, we need to calculate metrics across combinations of:
    Release Cohort; Follow-up Period (up to 10 years);
    Methodology (Event-based, Offender-based);
    Demographics (age, race, sex); Location (facility, region);
    Facility Stay Breakdown (stay length); ...
    TODO: Add support for conditional violations, offense, sentencing
    - Issues 34, 33, 32

    Release cohort, follow-up period, and methodology are not included in the
    output here. They are added into augmented versions of these combinations
    later.

    The output for a black female age 24 and an incarceration that began in
    January 2008 and ended in February 2009 is equal to the output of:
            for_characteristics({'age': '<25', 'race': 'black',
                                 'sex': 'female', 'stay_length': '12-24'})


    Args:
        person: the person we are picking characteristics from
        event: the recidivism event we are picking characteristics from

    Returns:
        A list of dictionaries containing all unique combinations of
        characteristics.
    """
    entry_age = age_at_date(person, event.original_entry_date)
    entry_age_bucket = age_bucket(entry_age)
    event_stay_length = stay_length_from_event(event)
    event_stay_length_bucket = stay_length_bucket(event_stay_length)
    characteristics = {'age': entry_age_bucket,
                       'race': person.race,
                       'sex': person.sex,
                       'stay_length': event_stay_length_bucket,
                       'release_facility': event.release_facility}

    return for_characteristics(characteristics)


def for_characteristics(characteristics):
    """The list of all combinations of the given metric characteristics.

    Example:
        for_characteristics(
        {"race": "black", "sex": "female", "age": "<25"}) =
            [{},
            {'age': '<25'}, {'race': 'black'}, {'sex': 'female'},
            {'age': '<25', 'race': 'black'}, {'age': '<25', 'sex': 'female'},
            {'race': 'black', 'sex': 'female'},
            {'age': '<25', 'race': 'black', 'sex': 'female'}]


    Args:
        characteristics: a dictionary of metric characteristics to derive
            combinations from

    Returns:
        A list of dictionaries containing all unique combinations of
        characteristics.
    """
    combos = [{}]
    for i in range(len(characteristics)):
        i_combinations = map(dict,
                             combinations(characteristics.items(), i + 1))
        for combo in i_combinations:
            combos.append(combo)
    return combos


def combination_metrics(combo, event, all_reincarceration_dates,
                        earliest_recidivism_period, relevant_periods):
    """Returns all unique recidivism metrics for the given combination.

    For the characteristic combination, i.e. a unique metric, look at all
    follow-up periods to determine under which ones recidivism occurred. Augment
    that combination with methodology and period, and map each augmented combo
    to 0 or 1 accordingly.

    Args:
        combo: a characteristic combination to convert into metrics
        event: the recidivism event from which the combination was derived
        all_reincarceration_dates: all dates of reincarceration for the person's
            recidivism events
        earliest_recidivism_period: the earliest follow-up period under which
            recidivism occurred
        relevant_periods: the list of periods relevant for measurement

    Returns:
        A list of key-value tuples representing specific metric combinations and
        the recidivism value corresponding to that metric.
    """
    metrics = []

    for period in relevant_periods:
        offender_based_combo = augment_combination(combo, 'OFFENDER', period)
        event_based_combo = augment_combination(combo, 'EVENT', period)

        # If they didn't recidivate at all or not yet for this period
        # (or they didn't recidivate until 10 years had passed),
        # assign 0 for both event- and offender-based measurement.
        if not event.recidivated \
                or not earliest_recidivism_period \
                or period < earliest_recidivism_period:
            metrics.append((offender_based_combo, 0))
            metrics.append((event_based_combo, 0))

        # If they recidivated, each unique release of a given person
        # within a follow-up period after the year of release is counted
        # as an instance of recidivism for event-based measurement. For
        # offender-based measurement, only one instance is counted.
        else:
            metrics.append((offender_based_combo, 1))

            reincarcerations_in_window = \
                count_reincarcerations_in_window(
                    event.release_date, period,
                    all_reincarceration_dates)

            for _ in repeat(None, reincarcerations_in_window):
                metrics.append((event_based_combo, 1))

    return metrics


def augment_combination(characteristic_combo, methodology, period):
    """A copy of the given combo with the given additional parameters added.

    Creates a shallow copy of the given characteristic combination and sets the
    given methodology and follow-up period on the copy. This avoids updating the
    existing characteristic combo.

    Args:
        characteristic_combo: the combination to copy and augment
        methodology: the methodology to set, i.e. "OFFENDER" or "EVENT"
        period: the follow-up period to set

    Returns:
        The augmented characteristic combination, ready for tracking.
    """

    augmented_combo = characteristic_combo.copy()
    augmented_combo['methodology'] = methodology
    augmented_combo['follow_up_period'] = period
    return augmented_combo
