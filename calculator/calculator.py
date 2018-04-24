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


from datetime import date
from dateutil.relativedelta import relativedelta
import logging
import webapp2

from itertools import combinations
from models.inmate import Inmate
from models.inmate_facility_snapshot import InmateFacilitySnapshot
from models.record import Offense, SentenceDuration, Record
from scraper.us_ny.us_ny_inmate import UsNyInmate
from scraper.us_ny.us_ny_record import UsNyRecord


# We measure in 1-year follow up periods up to 10 years after date of release.
FOLLOW_UP_PERIODS = range(1, 11)


# ============================================================================= #
# Phase 1: Transform raw records into instances of recidivism or non-recidivism #
# ============================================================================= #

def find_recidivism(inmate, include_conditional_violations=False):
    """
    Classifies all individual sentences for the inmate as either leading to recidivism or not.
    Transforms each sentence from which the inmate has been released into a mapping from its
    release cohort to the details of the event. The release cohort is an integer for the year,
    e.g. 2006. The event details are a RecidivismEvent object, which represents events of both
    recidivism and non-recidivism. That is, each inmate sentence is transformed into a
    recidivism event unless it is the most recent sentence and they are still incarcerated.

    Example output for someone who went to prison in 2006, was released in 2008, went back in 2010,
    was released in 2012, and never returned:
    {
      2008: RecidivismEvent(recidivated=True, original_entry_date="2006-04-05", ...),
      2012: RecidivismEvent(recidivated=False, original_entry_date="2010-09-17", ...)
    }

    :param inmate: an inmate to determine recidivism for
    :param include_conditional_violations: whether or not to include violations of conditional
    release as recidivism events
    :return: a dictionary from release cohorts to recidivism events for the given inmate in
    that cohort. No more than one event can be returned per release cohort per inmate.
    """
    records = Record.query(ancestor=inmate.key).order(Record.custody_date).fetch()
    snapshots = InmateFacilitySnapshot.query(ancestor=inmate.key).order(-InmateFacilitySnapshot.snapshot_date).fetch()

    recidivism_events = {}

    for index, record in enumerate(records):
        record_id = record.key.id()

        original_entry_date = first_entrance(record)

        if original_entry_date is None:
            # If there is no entry date on the record, there is nothing we can process. Skip it.
            # See Issue #49.
            continue

        if record.is_released and not record.last_release_date:
            # If the record is marked as released but there is no release date, there is nothing we
            # can process. Skip it. See Issue #51.
            continue

        release_date = final_release(record)
        release_cohort = release_date.year if release_date else None
        release_facility = last_facility(record, snapshots)

        # If this is their last (or only) record, then they did not recidivate for this one!
        if len(records) - index == 1:
            # There is only something to calculate if they are out of prison from this last record
            if record.is_released:
                logging.debug("Inmate was released from last or only record %s. No recidivism." %
                              record_id)
                recidivism_events[release_cohort] = RecidivismEvent.non_recidivism_event(
                    original_entry_date, release_date, release_facility)
            else:
                logging.debug("Inmate is still incarcerated for last or only record %s. Nothing to track" %
                              record_id)

        else:
            # If there is a record after this one and they have been released, then they
            # recidivated. Capture the details.
            if record.is_released:
                logging.debug("Inmate was released from record %s and went back again. Yes recidivism." %
                              record_id)

                later_records = records[index+1:]

                recidivism_record = later_records[0]
                reincarceration_date = first_entrance(recidivism_record)
                reincarceration_facility = first_facility(recidivism_record, snapshots)

                # If the release from the original sentence was final, i.e. was an unconditional
                # release or a conditional release which was never revoked, then this is a
                # relatively simple case of being released from prison and returning for a new
                # crime: recidivism.
                sole_release = was_released_only_once(record)

                # However, if the release was conditional and it was revoked, e.g. a parole violation,
                # prior to the final release then we want to capture that intermediate "recidivism"
                # event for specific metrics.
                if include_conditional_violations and not sole_release:
                    intermediate_release_date = None  # TODO See Issue #34
                    intermediate_release_cohort = intermediate_release_date.year
                    re_entrance = subsequent_entrance(record)

                    recidivism_events[intermediate_release_cohort] = RecidivismEvent.recidivism_event(
                        original_entry_date, intermediate_release_date, release_facility,
                        re_entrance, release_facility, True)

                recidivism_events[release_cohort] = RecidivismEvent.recidivism_event(
                    original_entry_date, release_date, release_facility,
                    reincarceration_date, reincarceration_facility, False)

    return recidivism_events


def first_entrance(record):
    """
    The first entrance is when an inmate first when to prison for a new sentence. An inmate may
    be conditionally released and returned to prison on violation of conditions for the same
    sentence, yielding a subsequent entrance date.

    :param record: a single record
    :return: when the inmate first entered into prison for this record
    """
    return record.custody_date


def subsequent_entrance(record):
    """
    A subsequent entrance is when an inmate returns to prison for a given sentence, after having
    already been conditionally for that same sentence. It is not returning to prison for a brand
    new sentence.

    :param record: a single record
    :return: when the inmate re-entered prison for this record
    """
    return record.last_custody_date


def final_release(record):
    """
    The release cohort is the year in which an inmate was released from prison for a particular
    sentence. This is used to normalize recidivism calculation, e.g. calculating and comparing
    recidivism for all inmates released in a particular calendar year.

    :param record: a single record
    :return: None if the inmate is still in custody, otherwise the final release of this record,
    from which release cohort can be derived
    """
    if not record.is_released:
        return None

    return record.last_release_date


def first_facility(record, inmate_snapshots):
    """
    Returns the facility that the inmate was first in for the given record. That is, the facility that
    they started that record in, whether or not they have since been released.

    This assumes the snapshots are provided in descending order by snapshot date, i.e. it picks the
    facility in the last snapshot in the collection that matches the given record.

    :param record: a single record
    :param inmate_snapshots: all facility snapshots for the inmate
    :return: the facility that the inmate first occupied for the record
    """
    return last_facility(record, reversed(inmate_snapshots))


def last_facility(record, inmate_snapshots):
    """
    Returns the facility that the inmate was last in for the given record. That is, the facility that
    they are currently in if still incarcerated, or that they were released from on their final release
    for that record.

    This assumes the snapshots are provided in descending order by snapshot date, i.e. it picks the
    facility in the first snapshot in the collection that matches the given record.

    :param record: a single record
    :param inmate_snapshots: all facility snapshots for the inmate
    :return: the facility that the inmate last occupied for the record
    """
    return next((snapshot.facility for snapshot in inmate_snapshots
                 if snapshot.key.parent().id() == record.key.id()), None)


def was_released_only_once(record):
    return True  # TODO See Issue #34


# ======================================================================================= #
# Phase 2: Transform instances of recidivism or non-recidivism into metrics for reduction #
# ======================================================================================= #

def map_recidivism_combinations(inmate, recidivism_events):
    """
    Takes in an inmate and all of her recidivism events and returns an array of "recidivism
    combinations". These are key-value pairs where the key represents a specific metric and
    the value represents whether or not recidivism occurred. If a metric does count towards
    recidivism, then the value is 1 if event-based or 1/k if offender-based, where k = the
    number of releases for that inmate within the follow-up period after the release.
    If it does not count towards recidivism, then the value is 0 in either methodology.

    Effectively, this translates a particular recidivism event into many recidivism metrics.
    This is because each metric represents one of many possible combinations of characteristics
    being tracked for that event. For example, if an asian male is reincarcerated, there is a
    metric that corresponds to asian people, one to males, one to asian males, one to all people,
    and more depending on other dimensions in the data.

    Example output for a hispanic female age 27 who was released in 2008 and went back to prison in 2014:
    [
      ({"methodology": "EVENT", "release_cohort": 2008, "follow_up_period": 5, "sex": "female", "age": "25-29"}, 0),
      ({"methodology": "OFFENDER", "release_cohort": 2008, "follow_up_period": 5, "sex": "female", "age": "25-29"}, 0),
      ({"methodology": "EVENT", "release_cohort": 2008, "follow_up_period": 6, "sex": "female", "age": "25-29"}, 1),
      ({"methodology": "EVENT", "release_cohort": 2008, "follow_up_period": 6, "sex": "female", "race": "hispanic"}, 1),
      ...
    ]

    :param inmate: the inmate
    :param recidivism_events: the full set of recidivism events for the inmate
    :return: an array of key-value tuples representing specific metrics and whether they
    add to recidivism for that metric
    """
    metrics = []
    all_release_dates = release_dates(recidivism_events)
    race = inmate.race
    sex = inmate.sex

    # For each recidivism event, we need to calculate metrics across combinations of:
    # Release Cohort; Follow-up Period (up to 10 years); Methodology (Event-based, Offender-based);
    # Demographics (age, race, sex); Location (prison, region); ...
    # TODO: Add support for conditional violations, offense metrics, sentence metrics - Issues 34, 33, 32

    for release_cohort, event in recidivism_events.iteritems():
        entry_age = age_at_date(inmate, event.original_entry_date)
        entry_age_bucket = age_bucket(entry_age)

        characteristic_combos = characteristic_combinations(
            {"age": entry_age_bucket, "race": race, "sex": sex, "release_facility": event.release_facility})

        # For each characteristic combination, i.e. each unique metric, look at all follow-up periods
        # to determine under which ones recidivism occurred. Map each metric (including period) to
        # 0 or 1 accordingly.
        for combo in characteristic_combos:
            combo["release_cohort"] = release_cohort

            earliest_follow_up_period = earliest_recidivated_follow_up_period(
                event.release_date, event.reincarceration_date)
            relevant_periods = relevant_follow_up_periods(event.release_date, date.today(), FOLLOW_UP_PERIODS)

            for period in relevant_periods:
                offender_based_combo = augment_combination(combo, "OFFENDER", period)
                event_based_combo = augment_combination(combo, "EVENT", period)

                # If they didn't recidivate at all or not yet for this period (or they didn't recidivate
                # until 10 years had passed), assign 0.
                if not event.recidivated or not earliest_follow_up_period or period < earliest_follow_up_period:
                    metrics.append((offender_based_combo, 0))
                    metrics.append((event_based_combo, 0))
                else:
                    # TODO: This may be a bug. I think the first instance should be 1/1, the second 1/2,
                    # the third 1/3, and so forth. As written, all instances in the period would be 1/k, which
                    # would reduce the calculated offender-based metric and seemingly cause the issue we see
                    # where recidivism rate goes _down_ in later periods for the offender methodology.

                    # For offender-based recidivism, we weight each instance of recidivism as equal to 1 / k,
                    # where k = the number of releases from prison for that inmate within that window.
                    # See "Following Incarceration, Most Released Offenders Never Return to Prison" by Rhodes et. al.
                    total_releases_in_window = count_releases_in_window(event.release_date, period, all_release_dates)
                    metrics.append((offender_based_combo, 1.0 / total_releases_in_window))
                    metrics.append((event_based_combo, 1))

    return metrics


def release_dates(recidivism_events):
    """
    Returns the array of release dates extracted from the given array of recidivism
    events. The output is the same length as the input.

    :param recidivism_events: the array of recidivism events
    :return: the array of release dates
    """
    return [event.release_date for _cohort, event in recidivism_events.iteritems()]


def count_releases_in_window(start_date, follow_up_period, all_release_dates):
    """
    Returns how many of the given release dates fall within the given follow up period
    after the given start date, end point exclusive, including the start date itself if
    it is within the given array. For example:

    count_releases_in_window("2016-05-13", 6, ["2012-04-30", "2016-05-13", "2020-11-20",
    "2021-01-12", "2022-05-13"]) = 3

    :param start_date: the date to start from
    :param follow_up_period: the follow up period to count within
    :param all_release_dates: the set of release dates to check
    :return: how many of the given release dates are within the follow up period from
    the given start date
    """
    releases_in_window = [release_date for release_date in all_release_dates
                          if start_date + relativedelta(years=follow_up_period)
                          > release_date >= start_date]
    return len(releases_in_window)


def earliest_recidivated_follow_up_period(release_date, reincarceration_date):
    """
    Returns the earliest follow-up period (up to 10 years) under which we can say
    recidivism has occurred. For example, if someone was released from prison on
    March 14, 2005 and reincarcerated on April 23, 2008, then the earliest follow
    up period is 4, as they had not yet recidivated within 3 years, but had within 4.

    :param release_date: the date of release
    :param reincarceration_date: the date of reincarceration
    :return: the earliest follow-up period under which recidivism occurred, or None if
    there is no reincarceration date provided
    """
    if not reincarceration_date:
        return None

    years_apart = reincarceration_date.year - release_date.year

    if years_apart == 0:
        return 1
    else:
        after_anniversary = ((reincarceration_date.month, reincarceration_date.day) >
                             (release_date.month, release_date.day))
        return years_apart + 1 if after_anniversary else years_apart


def relevant_follow_up_periods(release_date, current_date, follow_up_periods):
    """
    Returns all of the given follow up periods after the given release date which are either
    complete as of today, or still in progress as of today. Examples where today is 2018-01-26:

    relevant_follow_up_periods("2015-01-05", FOLLOW_UP_PERIODS) = [1,2,3,4]
    relevant_follow_up_periods("2015-01-26", FOLLOW_UP_PERIODS) = [1,2,3,4]
    relevant_follow_up_periods("2015-01-27", FOLLOW_UP_PERIODS) = [1,2,3]
    relevant_follow_up_periods("2016-01-05", FOLLOW_UP_PERIODS) = [1,2,3]
    relevant_follow_up_periods("2017-04-10", FOLLOW_UP_PERIODS) = [1]
    relevant_follow_up_periods("2018-01-05", FOLLOW_UP_PERIODS) = [1]
    relevant_follow_up_periods("2018-02-05", FOLLOW_UP_PERIODS) = []

    :param release_date: the release date we are tracking from
    :param current_date: the current date we are tracking towards
    :param follow_up_periods: the array of follow up periods to filter
    :return: the array of follow up periods which are relevant to track, i.e. completed or in progress
    """
    return [period for period in follow_up_periods
            if release_date + relativedelta(years=period - 1) <= current_date]


def age_at_date(inmate, check_date):
    """
    Returns the age that the inmate was at the given date.
    :param inmate: the inmate
    :param check_date: the date to check
    :return: the age of the inmate at the given date, or None if no birthday is known
    """
    birthday = inmate.birthday
    return None if birthday is None else \
        check_date.year - birthday.year - \
        ((check_date.month, check_date.day) < (birthday.month, birthday.day))


def age_bucket(age):
    """
    Age buckets for measurement: <25, 25-29, 30-34, 35-39, 40<
    :param age: the inmate's age
    :return: a string representation of the age bucket for the inmate
    """
    if age < 25:
        return "<25"
    elif age <= 29:
        return "25-29"
    elif age <= 34:
        return "30-34"
    elif age <= 39:
        return "35-39"
    else:
        return "40<"


def characteristic_combinations(characteristics):
    """
    Returns the set of all combinations of the given metric characteristics, of all sizes. That is, this returns an
    array of dictionaries where each dictionary is a combination of 0 to n unique elements of characteristics,
    where n is the size of the given array. For example:

    characteristic_combinations({"race": "black", "sex": "female", "age": "<25"}) =
    [{},
    {'age': '<25'}, {'race': 'black'}, {'sex': 'female'},
    {'age': '<25', 'race': 'black'}, {'age': '<25', 'sex': 'female'}, {'race': 'black', 'sex': 'female'},
    {'age': '<25', 'race': 'black', 'sex': 'female'}]

    :param characteristics: a dictionary of metric characteristics to derive combinations from
    :return: an array of dictionaries containing all unique combinations of characteristics
    """
    combos = [{}]
    for i in range(len(characteristics)):
        i_combinations = map(dict, combinations(characteristics.iteritems(), i + 1))
        for combo in i_combinations:
            combos.append(combo)
    return combos


def augment_combination(characteristic_combo, methodology, period):
    """
    Creates a shallow copy of the given characteristic combination and sets the
    given methodology and follow up period on the copy.

    :param characteristic_combo: the combination to copy and augment
    :param methodology: the methodology to set, i.e. "OFFENDER" or "EVENT"
    :param period: the followup period to set
    :return: the augmented characteristic combination
    """
    augmented_combo = characteristic_combo.copy()
    augmented_combo["methodology"] = methodology
    augmented_combo["follow_up_period"] = period
    return augmented_combo


class RecidivismEvent(object):
    """
    Models details related to a recidivism or non-recidivism event, that is, the information
    pertaining to a release from prison, whether or not a reincarceration later took place.
    This includes all of the information that we will want to track when calculating metrics
    pertaining to recidivism.
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
    def recidivism_event(original_entry_date, release_date, release_facility, reincarceration_date,
                         reincarceration_facility, was_conditional):
        """
        Creates a RecidivismEvent instance for an event where reincarceration occurred.

        :param original_entry_date: the date of original entry into prison
        :param release_date: the date of release from prison
        :param release_facility: the facility released from
        :param reincarceration_date: the date of reincarceration (recidivism)
        :param reincarceration_facility: the facility returned to upon reincarceration
        :param was_conditional: whether or not this reincarceration was due to some violation
        of release conditions, e.g. a revocation of parole
        :return: a RecidivismEvent for an instance of reincarceration
        """
        return RecidivismEvent(True, original_entry_date, release_date, release_facility, reincarceration_date,
                               reincarceration_facility, was_conditional)

    @staticmethod
    def non_recidivism_event(original_entry_date, release_date, release_facility):
        """
        Creates a RecidivismEvent instance for an event where reincarceration did not occur.

        :param original_entry_date: the date of original entry into prison
        :param release_date: the date of release from prison
        :param release_facility: the facility released from
        :return: a RecidivismEvent for an instance of non-reincarceration
        """
        return RecidivismEvent(False, original_entry_date, release_date, release_facility)

    def __repr__(self):
        return "<RecidivismEvent recidivated:%s, original_entry_date:%s, release_date:%s, release_facility:%s, " \
               "reincarceration_date:%s, reincarceration_facility:%s, was_conditional:%s>" \
               % (self.recidivated, self.original_entry_date, self.release_date, self.release_facility,
                  self.reincarceration_date, self.reincarceration_facility, self.was_conditional)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.recidivated == other.recidivated \
                   and self.original_entry_date == other.original_entry_date \
                   and self.release_date == other.release_date \
                   and self.release_facility == other.release_facility \
                   and self.reincarceration_facility == other.reincarceration_facility \
                   and self.was_conditional == other.was_conditional
        return False
