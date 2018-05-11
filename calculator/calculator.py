# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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

"""Recidivism calculator.

This contains the core logic for identifying and calculating recidivism metrics
on an inmate-by-inmate basis. It operates in two phases:

1. Transform raw records for a given inmate into instances of recidivism or
non-recidivism.

2. Transform those instances into recidivism metrics, key-value pairs where
the key represents all of the dimensions represented in the data point, and
the value represents a recidivism value, e.g. 0 for no or 1 for yes.

Example:
    recidivism_events = find_recidivism(inmate)
    metric_combinations = map_recidivism_combinations(inmate, recidivism_events)

Attributes:
    FOLLOW_UP_PERIODS: a list of integers, the follow-up periods that we measure
        recidivism over, from 1 to 10.
"""


import logging
from datetime import date
from itertools import combinations
from itertools import repeat
from dateutil.relativedelta import relativedelta
from models.snapshot import Snapshot
from models.record import Record

# This is required to permit access to PolyModel attributes on UsNyRecord
from scraper.us_ny.us_ny_record import UsNyRecord #pylint: disable=unused-import


# We measure in 1-year follow up periods up to 10 years after date of release.
FOLLOW_UP_PERIODS = range(1, 11)


# ========================================================================= #
# Phase 1: Transform records into instances of recidivism or non-recidivism #
# ========================================================================= #

def find_recidivism(inmate, include_conditional_violations=False):
    """Classifies all individual sentences for the inmate as either leading to
    recidivism or not.

    Transforms each sentence from which the inmate has been released into a
    mapping from its release cohort to the details of the event. The release
    cohort is an integer for the year, e.g. 2006. The event details are a
    RecidivismEvent object, which represents events of both recidivism and
    non-recidivism. That is, each inmate sentence is transformed into a
    recidivism event unless it is the most recent sentence and they are still
    incarcerated.

    Example output for someone who went to prison in 2006, was released in 2008,
    went back in 2010, was released in 2012, and never returned:
    {
      2008: RecidivismEvent(recidivated=True, original_entry_date="2006-04-05",
                            ...),
      2012: RecidivismEvent(recidivated=False, original_entry_date="2010-09-17",
                            ...)
    }

    Args:
        inmate: an inmate to determine recidivism for.
        include_conditional_violations: a boolean indicating whether or not
                to include violations of conditional release in recidivism
                calculations.

    Returns:
        A dictionary from release cohorts to recidivism events for the given
        inmate in that cohort. No more than one event can be returned per
        release cohort per inmate.
    """
    records = Record.query(ancestor=inmate.key)\
        .order(Record.custody_date)\
        .fetch()
    snapshots = Snapshot.query(ancestor=inmate.key)\
        .order(-Snapshot.created_on)\
        .fetch()

    recidivism_events = {}

    for index, record in enumerate(records):
        record_id = record.key.id()

        original_entry_date = first_entrance(record)

        if original_entry_date is None:
            # If there is no entry date on the record,
            # there is nothing we can process. Skip it. See Issue #49.
            continue

        if record.is_released and not record.latest_release_date:
            # If the record is marked as released but there is no release date,
            # there is nothing we can process. Skip it. See Issue #51.
            continue

        release_date = final_release(record)
        release_cohort = release_date.year if release_date else None
        release_facility = last_facility(record, snapshots)

        # If this is their last (or only) record,
        # then they did not recidivate for this one!
        if len(records) - index == 1:
            # There is only something to calculate
            # if they are out of prison from this last record
            if record.is_released:
                logging.debug("Inmate was released from last or only "
                              "record %s. No recidivism." %
                              record_id)

                recidivism_events[release_cohort] = \
                    RecidivismEvent.non_recidivism_event(original_entry_date,
                                                         release_date,
                                                         release_facility)
            else:
                logging.debug("Inmate is still incarcerated for last or only "
                              "record %s. Nothing to track" %
                              record_id)

        else:
            # If there is a record after this one and they have been released,
            # then they recidivated. Capture the details.
            if record.is_released:
                logging.debug("Inmate was released from record %s and went "
                              "back again. Yes recidivism." %
                              record_id)

                later_records = records[index+1:]

                recidivism_record = later_records[0]
                reincarceration_date = first_entrance(recidivism_record)
                reincarceration_facility = first_facility(recidivism_record,
                                                          snapshots)

                # If the release from the original sentence was final, i.e. was
                # an unconditional release or a conditional release which was
                # never revoked, then this is a relatively simple case of being
                # released from prison and returning for a new crime: recidivism
                sole_release = was_released_only_once()

                # However, if the release was conditional and it was revoked,
                # e.g. a parole violation, prior to the final release then we
                # want to capture that intermediate "recidivism" event for
                # specific metrics.
                if include_conditional_violations and not sole_release:
                    intermediate_release_date = None  # TODO See Issue #34
                    intermediate_release_cohort = intermediate_release_date.year
                    re_entrance = subsequent_entrance(record)

                    recidivism_events[intermediate_release_cohort] = \
                        RecidivismEvent.recidivism_event(
                            original_entry_date, intermediate_release_date,
                            release_facility, re_entrance, release_facility,
                            True)

                recidivism_events[release_cohort] = \
                    RecidivismEvent.recidivism_event(
                        original_entry_date, release_date, release_facility,
                        reincarceration_date, reincarceration_facility, False)

    return recidivism_events


def first_entrance(record):
    """The date of first entrance into prison for the given record.

    The first entrance is when an inmate first when to prison for a new
    sentence. An inmate may be conditionally released and returned to prison on
    violation of conditions for the same sentence, yielding a subsequent
    entrance date.

    Args:
        record: a single record

    Returns:
        A Date for when the inmate first entered into prison for this record.
    """
    return record.custody_date


def subsequent_entrance(record):
    """The date of most recent entrance into prison for the given record.

    A subsequent entrance is when an inmate returns to prison for a given
    sentence, after having already been conditionally for that same sentence.
    It is not returning to prison for a brand new sentence.

    Args:
        record: a single record

    Returns:
        A Date for when the inmate re-entered prison for this record.
    """
    return record.last_custody_date


def final_release(record):
    """The date of final release from prison for the given record.

    An inmate can be released from prison multiple times for a given record if
    they are sent back in the interim for conditional violations. This
    represents the last time they were released for this particular record.

    Args:
        record: a single record

    Returns:
        A Date for when the inmate was released from prison for this record
        for the last time. None if the inmate is still in custody.
    """
    if not record.is_released:
        return None

    return record.latest_release_date


def first_facility(record, snapshots):
    """The facility that the inmate first occupied for the given record.

    Returns the facility that the inmate was first in for the given record.
    That is, the facility that they started that record in, whether or not they
    have since been released.

    This assumes the snapshots are provided in descending order by snapshot
    date, i.e. it picks the facility in the last snapshot in the collection that
    matches the given record. Thus, this also assumes that the given list of
    snapshots contains the full history for the given record. If that is not
    true, this simply returns the earliest facility we are aware of.

    Args:
        record: a single record
        snapshots: a list of all facility snapshots for the inmate.

    Returns:
        The facility that the inmate first occupied for the record. None if we
        have no apparent history available for the record.
    """
    return last_facility(record, reversed(snapshots))


def last_facility(record, snapshots):
    """The facility that the inmate last occupied for the given record.

    Returns the facility that the inmate was last in for the given record.
    That is, the facility that they are currently in if still incarcerated, or
    that they were released from on their final release for that record.

    This assumes the snapshots are provided in descending order by snapshot
    date, i.e. it picks the facility in the first snapshot in the collection
    that matches the given record. Thus, this also assumes that the given list
    of snapshots contains the full history for the given record. If that is not
    true, this simply returns the earliest facility we are aware of.

    Args:
        record: a single record
        snapshots: a list of all facility snapshots for the inmate.

    Returns:
        The facility that the inmate last occupied for the record. None if we
        have no apparent history available for the record.
    """
    return next((snapshot.latest_facility for snapshot in snapshots
                 if snapshot.key.parent().id() == record.key.id()), None)


def was_released_only_once():
    """TODO See Issue #34"""
    return True


# ========================================================================= #
# Phase 2: Transform instances of recidivism or non-recidivism into metrics #
# ========================================================================= #

def map_recidivism_combinations(inmate, recidivism_events):
    """Transforms the given recidivism events and inmate details into unique
    recidivism metric combinations to count.

    Takes in an inmate and all of her recidivism events and returns an array of
    "recidivism combinations". These are key-value pairs where the key
    represents a specific metric and the value represents whether or not
    recidivism occurred. If a metric does count towards recidivism, then the
    value is 1 if event-based or 1/k if offender-based, where k = the number of
    releases for that inmate within the follow-up period after the release.
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
        inmate: the inmate
        recidivism_events: the list of RecidivismEvents for the inmate.

    Returns:
        A list of key-value tuples representing specific metric combinations and
        the recidivism value corresponding to that metric.
    """
    metrics = []
    all_reincarceration_dates = reincarceration_dates(recidivism_events)
    race = inmate.race
    sex = inmate.sex

    # For each event, we need to calculate metrics across combinations of:
    # Release Cohort; Follow-up Period (up to 10 years);
    # Methodology (Event-based, Offender-based);
    # Demographics (age, race, sex); Location (prison, region); ...
    # TODO: Add support for conditional violations, offense metrics,
    # sentence metrics - Issues 34, 33, 32

    for release_cohort, event in recidivism_events.iteritems():
        entry_age = age_at_date(inmate, event.original_entry_date)
        entry_age_bucket = age_bucket(entry_age)

        characteristic_combos = characteristic_combinations(
            {"age": entry_age_bucket,
             "race": race,
             "sex": sex,
             "release_facility": event.release_facility})

        # For each characteristic combination, i.e. each unique metric, look at
        # all follow-up periods to determine under which ones recidivism
        # occurred. Map each metric (including period) to 0 or 1 accordingly.
        for combo in characteristic_combos:
            combo["release_cohort"] = release_cohort

            earliest_follow_up_period = earliest_recidivated_follow_up_period(
                event.release_date, event.reincarceration_date)
            relevant_periods = relevant_follow_up_periods(event.release_date,
                                                          date.today(),
                                                          FOLLOW_UP_PERIODS)

            for period in relevant_periods:
                offender_based_combo = augment_combination(
                    combo, "OFFENDER", period)
                event_based_combo = augment_combination(
                    combo, "EVENT", period)

                # If they didn't recidivate at all or not yet for this period
                # (or they didn't recidivate until 10 years had passed),
                # assign 0 for both event- and offender-based measurement.
                if not event.recidivated \
                        or not earliest_follow_up_period \
                        or period < earliest_follow_up_period:
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
            for _cohort, event in recidivism_events.iteritems()
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
        release_date: a Date for when the inmate was released
        reincarceration_date: a Date for when the inmate was reincarcerated

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


def age_at_date(inmate, check_date):
    """The age of the inmate at the given date.

    Args:
        inmate: the inmate
        check_date: the date to check

    Returns:
        The age of the inmate at the given date. None if no birthday is known.
    """
    birthday = inmate.birthday
    return None if birthday is None else \
        check_date.year - birthday.year - \
        ((check_date.month, check_date.day) < (birthday.month, birthday.day))


def age_bucket(age):
    """The age bucket that applies to measurement.

    Age buckets for measurement: <25, 25-29, 30-34, 35-39, 40<

    Args:
        age: the inmate's age

    Returns:
        A string representation of the age bucket for the inmate.
    """
    if age < 25:
        return "<25"
    elif age <= 29:
        return "25-29"
    elif age <= 34:
        return "30-34"
    elif age <= 39:
        return "35-39"
    return "40<"


def characteristic_combinations(characteristics):
    """The list of all combinations of the given metric characteristics.

    Returns the list of all combinations of the given metric characteristics, of
    all sizes. That is, this returns a list of dictionaries where each
    dictionary is a combination of 0 to n unique elements of characteristics,
    where n is the size of the given array.

    Example:
        characteristic_combinations(
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
                             combinations(characteristics.iteritems(), i + 1))
        for combo in i_combinations:
            combos.append(combo)
    return combos


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
    augmented_combo["methodology"] = methodology
    augmented_combo["follow_up_period"] = period
    return augmented_combo


class RecidivismEvent(object):
    """Models details related to a recidivism or non-recidivism event.

    This includes the information pertaining to a release from prison that we
    will want to track when calculating recidivism metrics, and whether or not
    recidivism later took place.

    Attributes:
        recidivated: A boolean indicating whether or not the inmate actually
            recidivated for this release event.
        original_entry_date: A Date for when the inmate first entered prison
            for this record.
        release_date: A Date for when the inmate was last released from prison
            for this record.
        release_facility: The facility that the inmate was last released from
            for this record.
        reincarceration_date: A Date for when the inmate re-entered prison.
        reincarceration_facility: The facility that the inmate entered into upon
            first return to prison after the release.
        was_conditional: A boolean indicating whether or not the recidivism,
            if it occurred, was due to a conditional violation, as opposed to a
            new incarceration.
    """

    def __init__(self,
                 recidivated,
                 original_entry_date,
                 release_date,
                 release_facility,
                 reincarceration_date=None,
                 reincarceration_facility=None,
                 was_conditional=False):

        self.recidivated = recidivated
        self.original_entry_date = original_entry_date
        self.release_date = release_date
        self.release_facility = release_facility
        self.reincarceration_date = reincarceration_date
        self.reincarceration_facility = reincarceration_facility
        self.was_conditional = was_conditional

    @staticmethod
    def recidivism_event(original_entry_date, release_date, release_facility,
                         reincarceration_date, reincarceration_facility,
                         was_conditional):
        """Creates a RecidivismEvent instance for an event where reincarceration
        occurred.

        Args:
            original_entry_date: A Date for when the inmate first entered prison
                for this record.
            release_date: A Date for when the inmate was last released from
                prison for this record.
            release_facility: The facility that the inmate was last released
                from for this record.
            reincarceration_date: A Date for when the inmate re-entered prison.
            reincarceration_facility: The facility that the inmate entered into
                upon first return to prison after the release.
            was_conditional: A boolean indicating whether or not the recidivism,
                if it occurred, was due to a conditional violation, as opposed
                to a new incarceration.

        Returns:
            A RecidivismEvent for an instance of reincarceration.
        """
        return RecidivismEvent(True, original_entry_date, release_date,
                               release_facility, reincarceration_date,
                               reincarceration_facility, was_conditional)

    @staticmethod
    def non_recidivism_event(original_entry_date, release_date,
                             release_facility):
        """Creates a RecidivismEvent instance for an event where reincarceration
        did not occur.

        Args:
            original_entry_date: A Date for when the inmate first entered
                prison for this record.
            release_date: A Date for when the inmate was last released from
                prison for this record.
            release_facility: The facility that the inmate was last released
                from for this record.

        Returns:
            A RecidivismEvent for an instance where recidivism did not occur.
        """
        return RecidivismEvent(False, original_entry_date, release_date,
                               release_facility)

    def __repr__(self):
        return "<RecidivismEvent recidivated:%s, original_entry_date:%s, " \
               "release_date:%s, release_facility:%s, " \
               "reincarceration_date:%s, reincarceration_facility:%s, " \
               "was_conditional:%s>" \
               % (self.recidivated, self.original_entry_date, self.release_date,
                  self.release_facility, self.reincarceration_date,
                  self.reincarceration_facility, self.was_conditional)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.recidivated == other.recidivated \
                   and self.original_entry_date == other.original_entry_date \
                   and self.release_date == other.release_date \
                   and self.release_facility == other.release_facility \
                   and self.reincarceration_facility == \
                   other.reincarceration_facility \
                   and self.was_conditional == other.was_conditional
        return False
