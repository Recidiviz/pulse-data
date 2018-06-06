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

"""Identifies instances of recidivism and non-recidivism for calculation.

This contains the core logic for identifying recidivism events on an
inmate-by-inmate basis, transform raw records for a given inmate into instances
of recidivism or non-recidivism as appropriate.

This class is paired with calculator.py, together providing the ability to
transform an inmate into an array of recidivism metrics.

Example:
    recidivism_events = identification.find_recidivism(inmate)
    metric_combinations = calculator.map_recidivism_combinations(
        inmate, recidivism_events)
"""


import logging
from models.snapshot import Snapshot
from models.record import Record
# This is required to permit access to PolyModel attributes on UsNyRecord
from ingest.us_ny.us_ny_record import UsNyRecord #pylint: disable=unused-import
from .recidivism_event import RecidivismEvent


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
        if record.custody_date is None:
            # If there is no entry date on the record,
            # there is nothing we can process. Skip it. See Issue #49.
            continue

        if record.is_released and not record.latest_release_date:
            # If the record is marked as released but there is no release date,
            # there is nothing we can process. Skip it. See Issue #51.
            continue

        original_entry_date = first_entrance(record)
        release_date = final_release(record)
        release_cohort = release_date.year if release_date else None
        release_facility = last_facility(record, snapshots)

        if len(records) - index == 1:
            event = for_last_record(record, original_entry_date,
                                    release_date, release_facility)
            if event:
                recidivism_events[release_cohort] = event
        else:
            # If there is a record after this one and they have been released,
            # then they recidivated. Capture the details.
            if record.is_released:
                event = for_intermediate_record(record, records[index + 1],
                                                snapshots, original_entry_date,
                                                release_date, release_facility)
                recidivism_events[release_cohort] = event

                if include_conditional_violations:
                    (conditional_release_cohort, conditional_event) = \
                        for_conditional_release(record,
                                                original_entry_date,
                                                release_facility)
                    recidivism_events[conditional_release_cohort] = \
                        conditional_event

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


def released_only_once():
    """TODO See Issue #34"""
    return True


def for_last_record(record, original_entry_date,
                    release_date, release_facility):
    """Returns any non-recidivism event relevant to the person's last record.

    If the person has been released from their last record, there is an instance
    of non-recidivism to count. If they are still incarcerated, there is nothing
    to count.

    Args:
        record: the last record for the person
        original_entry_date: when they entered for this record
        release_date: when they were last released for this record
        release_facility: the facility they were last released from for this
            record

    Returns:
        A non-recidivism event if released from this record. None otherwise.
    """
    # There is only something to capture
    # if they are out of prison from this last record
    if record.is_released:
        logging.debug('Inmate was released from last or only '
                      'record %s. No recidivism.', record.key.id())

        return RecidivismEvent.non_recidivism_event(original_entry_date,
                                                    release_date,
                                                    release_facility)

    logging.debug('Inmate is still incarcerated for last or only '
                  'record %s. Nothing to track', record.key.id())
    return None


def for_intermediate_record(record, recidivism_record, snapshots,
                            original_entry_date, release_date,
                            release_facility):
    """Returns the recidivism event relevant to the person's intermediate
    (not last) record.

    There is definitely an instance of recidivism to count if this is not the
    person's last record and they have been released.

    Args:
        record: a record for some person
        recidivism_record: the next record after this, through which recidivism
            has occurred
        snapshots: all snapshots for this person
        original_entry_date: when they entered for this record
        release_date: when they were last released for this record
        release_facility: the facility they were last released from for this
            record

    Returns:
        A recidivism event.
    """
    logging.debug('Inmate was released from record %s and went '
                  'back again. Yes recidivism.', record.key.id())

    reincarceration_date = first_entrance(recidivism_record)
    reincarceration_facility = first_facility(recidivism_record,
                                              snapshots)

    return RecidivismEvent.recidivism_event(
        original_entry_date, release_date, release_facility,
        reincarceration_date, reincarceration_facility, False)


def for_conditional_release(record, original_entry_date, release_facility):
    """Returns a recidivism event if the person was conditionally released
    as part of this record and sent back for a violation of those conditions.

    TODO See Issue #34 - Fully support this.

    Args:
        record: a record for some person
        original_entry_date: when they entered for this record
        release_facility: the facility they were last released from for this
            record
    Returns:
        A recidivism event if there was incarceration due to a violation of
        some conditional release. None otherwise.
    """
    if not released_only_once():
        intermediate_release_date = None
        intermediate_release_cohort = intermediate_release_date.year
        re_entrance = subsequent_entrance(record)

        return intermediate_release_cohort, RecidivismEvent.recidivism_event(
            original_entry_date, intermediate_release_date, release_facility,
            re_entrance, release_facility, True)

    return None
