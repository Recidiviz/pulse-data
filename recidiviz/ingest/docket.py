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

"""Utilities for working with the ingest docket, such as enqueueing and leasing
docket items, and purging the docket queue.

The ingest docket is a pull-based queue of ingest tasks that are ready to be
performed. Each docket item is a high-level task for some region and scraping
type that some scraper must perform before the scraping process can be
considered complete. It typically contains a name, id, and/or other information
to search for in some criminal justice system we are scraping.

The docket is shared by all scrapers, regardless of region and scrape type. Each
item is tagged with identifying information so that it is consumed and processed
by the correct scraper. When a scraper wants more work, it will lease a docket
item: pulling it from the queue and marking it as leased for some duration. When
the work is complete, the scraper deletes the item from the queue. If it has not
been deleted by some specified time period, i.e. it expires, it is returned to
the queue to be attempted again.

Each task can launch zero to many additional tasks within the push-based queue
that the scraper is running atop. A high-level lifecycle looks like this:

1. Scraping is launched for a region and scrape type, e.g. (US_NY, background)
2. One or more docket items are tagged with this information and enqueued in the
docket
3. A scraper worker for (US_NY, background) leases the next appropriate task
from the docket and begins to process it
4. In the lifecycle of that task: a search is made, with a response including
potentially zero to many search results. For each result, a new task is enqueued
on the us-ny-scraper push queue to be picked up by another scraper worker for
(US_NY, background).
5. Based on the implementation of the scraper, a scraper worker may attempt to
fully drain its push queue of work before returning to the docket, or may lease
another docket item on its own time.

Attributes:
    BACKGROUND_BATCH_SIZE: (int) the number of rows to load from a name list
        file into memory at a time, for individual enqueue into the docket,
        for background scrapes specifically
    SNAPSHOT_BATCH_SIZE: (int) the number of snapshots or records to query from
        the database into memory at a time, for individual enqueue into the
        docket, for snapshot scrapes specifically
    SNAPSHOT_DISTANCE_YEARS: (int) the max number of years into the past we will
        search for relevant snapshots whose data to scrape again
    DOCKET_QUEUE_NAME: (string) the immutable name of the docket queue
    FILENAME_PREFIX: (string) the directory in which to find name list files
"""

import csv
import hashlib
import json
import logging
import time

from datetime import datetime
from dateutil.relativedelta import relativedelta
from google.appengine.api import taskqueue
from google.appengine.ext import deferred
from google.appengine.ext import ndb
from recidiviz.ingest.models.scrape_key import ScrapeKey
from recidiviz.utils import regions


BACKGROUND_BATCH_SIZE = 1000
SNAPSHOT_BATCH_SIZE = 100
SNAPSHOT_DISTANCE_YEARS = 10
DOCKET_QUEUE_NAME = "docket"
FILENAME_PREFIX = "./name_lists/"


# ##################### #
# Populating the docket #
# ##################### #


def load_target_list(scrape_key, given_names="", surname=""):
    """Starts docket loading based on scrape type and region.

    Determines correct scrape type and kicks off target list generation,
    loading of docket items based on the target list.

    Args:
        scrape_key: (ScrapeKey) The scraper to load docket for
        given_names: Given names of where to begin
        surname: Surname of where to begin

    Returns:
        N/A
    """
    logging.info("Getting target list for scraper: %s", scrape_key)
    region_code = scrape_key.region_code
    scrape_type = scrape_key.scrape_type

    if scrape_type == "background":
        name_list_file = regions.get_name_list_file(region_code)

        # Construct filename, process user-supplied name query (if provided)
        filename = FILENAME_PREFIX + name_list_file
        query_name = (surname, given_names)

        # If query name not provided, load from the start of the file
        load = (query_name == ("", ""))

        # Kick off docket loading
        load_background_target_list(region_code, filename, query_name, load)

    elif scrape_type == "snapshot":
        # Kick off the snapshot loading
        phase = 0
        load_snapshot_target_list(region_code, phase)


def load_background_target_list(region_code, name_file, query_name, load,
                                start_line=1):
    """Load background scrape docket items, from name file.

    Iterates over a CSV of common names, loading a docket item for the scraper
    to search for each one. We load batches of lines into memory at a time,
    until the entire file has been processed.

    If a name was provided in the initial request, will attempt to only load
    names from the index of that name in the file onward, allowing for
    'resuming' a background scrape from a specific point if there were
    problems. If the provided name is not found in the name file at all, a
    single docket item will be created to search for that name only.

    Args:
        region_code: (string) Region code
        name_file: (string) Name of the name file to be loaded
        query_name: User-provided name, in the form of a tuple (surname,
            given names). Empty strings if not provided.
        load: (bool) Whether to load docket items for names in namelist from
            start_line, or just read them for now
        start_line: (int) Line number to start at

    Returns:
        N/A
    """
    batch_size = BACKGROUND_BATCH_SIZE
    write_to_docket = load
    names = []
    next_line = None
    scrape_type = "background"

    row_num = 0
    max_row = start_line + batch_size

    # Pull next BACKGROUND_BATCH_SIZE lines from CSV file
    with open(name_file, 'rb') as csvfile:
        names_reader = csv.reader(csvfile)

        for row in names_reader:
            row_num += 1

            if row_num < start_line or not row:
                continue
            elif start_line <= row_num < max_row:
                names.append(list(row))
            else:
                next_line = row_num
                break

    # Check whether we're through the name list, convert name list to tuples
    end_of_list = (len(names) < batch_size)
    names = [(name[0], "") if len(name) == 1 else tuple(name) for name in names]

    if not load:
        # We're searching for the user-provided name in the name list,
        # check this part of the list for it.
        (write_to_docket, names) = get_name_list_subset(names, query_name)

        if not write_to_docket and end_of_list:
            # Name not in list, add it as a one-off docket item
            logging.info("Couldn't find user-provided name '%s' in name list, "
                         "adding one-off docket item for the name instead.",
                         str(query_name))
            names = [query_name]
            write_to_docket = True

    if write_to_docket:
        add_to_query_docket(ScrapeKey(region_code, scrape_type), names)

    if not end_of_list:
        # Kick off next batch of CSV lines to read
        deferred.defer(load_background_target_list,
                       region=region_code,
                       name_file=name_file,
                       query_name=query_name,
                       load=write_to_docket,
                       start_line=next_line)
    else:
        logging.info("Finished loading background target list to docket.")


def load_snapshot_target_list(region_code, phase, cursor=None):
    """Load snapshot scrape docket items, based on previously-found people.

    We need to run several queries to find relevant people to follow-up
    on. This method runs them by query type and by batch, using the deferred
    library to iterate through the batches (and the queries) until complete.

    Note that these query types (checking snapshots and records) are likely
    to produce some duplicate people for us to re-snapshot. To prevent
    extra work, we add these items to the docket (pull taskqueue) with task
    names that encode the person and current datetime, accurate to the hour.
    GAE ensures multiple tasks can't be submitted with the same name within
    several days of one another, preventing duplicate tasks / work.

    This set of queries should work reasonably well, but if we need to add
    complexity in the future it may make more sense for snapshot targets
    to be produced by daily mapreduces that evaluate each record and its
    child snapshots in the datastore.

    Args:
        region_code: (string) Region code to snapshot (e.g., us_ny)
        phase: (int) Query phase to continue working through
        cursor: Query cursor for next results

    Returns:
        N/A
    """
    batch_size = SNAPSHOT_BATCH_SIZE
    (start_date, start_datetime) = get_snapshot_start()
    scrape_type = "snapshot"

    # Get relevant sub-kinds for the region
    region = regions.Region(region_code)
    region_record = region.get_record_kind()
    region_snapshot = region.get_snapshot_kind()

    if phase == 0:
        # Query 1: Snapshots
        query_type = "snapshot"

        # Get all snapshots showing people in prison within the date range
        # (Snapshot search aims to catch when they get out / facility changes)
        snapshot_query = region_snapshot.query(ndb.AND(
            region_snapshot.created_on > start_datetime,
            # This must be an equality operator instead of `not ...` to work
            region_snapshot.is_released == False))  # pylint: disable=singleton-comparison

        query_results, next_cursor, more = snapshot_query.fetch_page(
            batch_size, start_cursor=cursor)

    elif phase == 1:
        # Query 2: Records (edge case: record / release predates our scraping)
        query_type = "record"

        # The second query handles the same edge-case described in the
        # get_ignore_records method (records which have release dates within
        # the window of interest, but before we started scraping). These
        # records also won't be on ignore lists from the snapshot query,
        # because we check for and exclude them there. The aim of this query
        # is to catch any for which the core snapshot query didn't apply.
        person_records_query = region_record.query(ndb.AND(
            region_record.latest_release_date > start_date,
            # This must be an equality operator to work
            region_record.is_released == True))  # pylint: disable=singleton-comparison

        query_results, next_cursor, more = person_records_query.fetch_page(
            batch_size, start_cursor=cursor)

    elif phase == 2:
        # Query 3: Records (edge case: no record changes since before window)
        query_type = "record"

        # There's an edge case where the last snapshot during which is_released
        # was updated is earlier than the start date of the window of interest.
        # To compensate, we pull all records for which the subject is still in
        # prison and our record of the event predates the start of the window.
        # Some of these may be duplicates of records found during the snapshot
        # query; those will be de-duplicated by task name when added to the
        # docket. There's no risk of the records we care about from this being
        # in the ignore lists from the snapshot query, however - by definition,
        # people of interest for this edge-case won't have any snapshots
        # updating is_released in that time.
        person_records_query = region_record.query(ndb.AND(
            # This must be an equality operator to work
            region_record.is_released == False,  # pylint: disable=singleton-comparison
            region_record.created_on < start_datetime))

        query_results, next_cursor, more = person_records_query.fetch_page(
            batch_size, start_cursor=cursor)

    else:
        logging.info("Finished loading snapshot target list to docket.")
        return

    (relevant_records, person_keys_ids) = process_query_response(
        query_type, query_results)

    # Invert results to a combination of person + set of records to ignore
    results = get_ignore_records(start_date, region_record, relevant_records,
                                 person_keys_ids)

    # Store the people and ignore records as docket items for snapshot scrape
    add_to_query_docket(ScrapeKey(region_code, scrape_type), results)

    # Move to next phase, if we've completed all results for this phase
    if not more:
        phase += 1
        next_cursor = None

    deferred.defer(load_snapshot_target_list,
                   region_code=region_code,
                   phase=phase,
                   cursor=next_cursor)


def get_snapshot_start():
    """Get the start date for our window of interest for snapshots

    Produces a datetime N years in the past from today, where N is the value of
    SNAPSHOT_DISTANCE_YEARS.

    Args:
        N/A

    Returns:
        Date object of date N years past
    """
    today = datetime.now().date()

    start_date = today - relativedelta(years=SNAPSHOT_DISTANCE_YEARS)
    start_datetime = datetime.combine(start_date, datetime.min.time())

    return start_date, start_datetime


def get_name_list_subset(names, query_name):
    """Search for name in name list, return subset including + after that name

    Takes a list of name tuples (surname, given names), attempts to find the
    name in the file. If found, returns the list including and after that
    name.

    Args:
        names: List of tuples, in the form:
            [('Baker', 'Mandy'),
             ('del Toro', 'Guillero'),
             ('Ma', 'Yo Yo')]
        query_name: User-provided name to search for (tuple in same form)

    Returns:
        Tuple result, in the form: (success (bool), list_subset (list))
    """
    try:
        match_index = names.index(query_name)

        logging.info("Found query name %s in name list, adding subsequent "
                     "names to docket.", str(query_name))
        subset = names[match_index:]

        return True, subset

    except ValueError:
        return False, []


def process_query_response(query_type, query_results):
    """Extracts relevant details for docket items from snapshot query results

    Args:
        query_type: (string) "snapshot" if query was for snapshots, "record"
            if not
        query_results: (list of entities) Result set from the query

    Returns:
        Tuple in the form (relevant_records, person_keys_ids).
            relevant_records: A list of records which should be scraped to
                check for changes
            person_key_ids: Dictionary of parent entities (persons who we
                should re-scrape), keyed (de-duplicated) by person key.
    """
    person_key_ids = {}
    relevant_records = []

    for result in query_results:
        if query_type == "snapshot":
            record_key = result.key.parent()
            record = record_key.get()
        else:
            record = result

        relevant_records.append(record.record_id)
        person_key = record.key.parent()
        person = person_key.get()
        person_key_ids[person.key] = person.person_id

    return relevant_records, person_key_ids


def get_ignore_records(start_date, region_record, relevant_records,
                       person_keys_ids):
    """Invert list of relevant records to produce list scraper should ignore

    Takes a list of persons, and of relevant records, then inverts the
    list to get only a list of records we don't want to waste time
    re-scraping.

    Args:
        start_date: (date) Date when the window of snapshot interest starts
        region_record: (models.Record subclass) The sub-kind for Record for
            the specific region
        person_keys_ids: A dictionary in the form {person_key: person_id, ...}
        relevant_records: (list) Records we should re-scrape

    Returns:
        List of record IDs to ignore. Each list item is a tuple in the form:
            (<person ID>, <list of record IDs to ignore for this person>)
    """
    result_list = []

    # Use the person keys to invert the record list - need to know which
    # records to ignore, not which to scrape, since it's important to
    # scrape new records as well.
    for person_key, person_id in person_keys_ids.iteritems():
        person_ignore_records = []
        records = region_record.query(ancestor=person_key).fetch()

        for record in records:
            record_id = record.record_id
            if record_id not in relevant_records:

                # There's an edge-case where a record's release date is
                # within the date range we care about, but doesn't come
                # up in the snapshot query above because we started
                # scraping after they had already been released. Don't
                # ignore those records - may have been put back e.g.
                # on parole violation.
                if (record.latest_release_date and
                        not record.latest_release_date > start_date):
                    person_ignore_records.append(record_id)

        result_list.append((person_id, person_ignore_records))

    return result_list


def add_to_query_docket(scrape_key, docket_items):
    """Add docket items to the query docket for relevant region / scrape type.

    Adds items in the list to the query docket, tagged with the given region and
    scrape type. The scraper will pull each item from the docket in
    turn for scraping (e.g. each name, if a background scrape, or each person ID
    if a snapshot scrape.)

    Args:
        scrape_key: (ScrapeKey) The scraper to add to the docket for
        docket_items: (list) List of payloads to add.
    """
    logging.info("Populating query docket for scraper: %s", scrape_key)
    region_code = scrape_key.region_code
    scrape_type = scrape_key.scrape_type

    q = taskqueue.Queue(DOCKET_QUEUE_NAME)

    tag_name = "{}-{}".format(region_code, scrape_type)

    tasks_to_add = []
    for item in docket_items:
        payload = json.dumps(item)

        if scrape_type == "snapshot":
            logging.debug("Attempting to add snapshot item to docket: %s",
                          item[0])
            task_name = get_task_name(region_code, item[0])
            new_task = taskqueue.Task(payload=payload,
                                      method='PULL',
                                      tag=tag_name,
                                      name=task_name)

        elif scrape_type == "background":
            new_task = taskqueue.Task(payload=payload,
                                      method='PULL',
                                      tag=tag_name)
        else:
            raise ValueError("Received invalid scrape type [{}]"
                             .format(scrape_type))

        tasks_to_add.append(new_task)

    try:
        q.add(tasks_to_add)
    except (taskqueue.taskqueue.DuplicateTaskNameError,
            taskqueue.taskqueue.TombstonedTaskError,
            taskqueue.taskqueue.TaskAlreadyExistsError):
        logging.debug("Some people have been added to the docket already; "
                      "skipping.")


def get_task_name(region_code, person_id, date_time=None):
    """Generate unique task name for region+person+current datetime

    Generates a unique task name for a particular person ID and region.

    Because taskqueue enforces uniqueness for 9 days even after a task
    has been deleted, and we may want to run (or re-run) snapshot scrapes
    more frequently, we incorporate the datetime accurate to the current hour.

    Because taskqueues become less efficient if task names are closely related
    or sequential, we hash the region/person-specific string chosen for the
    name to try to ensure they're more evenly distributed.

    Args:
        region_code: (string) Region code this task will be tied to
        person_id: (string) Person ID the task is for
        date_time: (datetime) The date and time when the task is generated,
            defaults to right now

    Returns:
        Task name, in the form of an MD5 hash
    """
    # Get the time, rounded down to the last hour. New tasks will have unique
    # names an hour after the last snapshot scrape, but immediate duplicates
    # will be blocked.
    if date_time is None:
        date_time = datetime.now()
    time_component = date_time.replace(microsecond=0, second=0, minute=0)

    string_base = region_code + person_id + str(time_component)

    return hashlib.md5(string_base).hexdigest()


# ########################## #
# Retrieving from the docket #
# ########################## #


def get_new_docket_item(scrape_key, attempt=0, back_off=5):
    """Retrieves arbitrary item from docket for the specified scrape type

    Retrieves an arbitrary item still in the docket (whichever docket
    type is specified). Some retry logic is built-in, since this may
    get called immediately after task creation and it's not clear how
    quickly the taskqueue index updates.

    Args:
        scrape_key: (ScrapeKey) The scraper to lease an item for
        attempt: (int) # of attempts so far. After 2, returns None
        back_off: (int) # of seconds to wait between attempts.

    Returns:
        Task entity from queue
        None if query returns None
    """
    docket_item = None

    # We only lease tasks for 5min, so that they pop back into the queue
    # if we pause or stop the scrape for very long.
    # Note: This may be the right number for us_ny snapshot scraping, but
    #   if reused with another scraper the background scrapes might need
    #   more time depending on e.g. # results for query 'John Doe'.
    q = taskqueue.Queue(DOCKET_QUEUE_NAME)
    tag_name = "{}-{}".format(scrape_key.region_code, scrape_key.scrape_type)
    lease_duration_seconds = 300
    num_tasks = 1
    docket_results = q.lease_tasks_by_tag(lease_duration_seconds,
                                          num_tasks,
                                          tag_name)

    if docket_results:
        docket_item = docket_results[0]
        logging.info("Leased docket item %s from the docket queue.",
                     docket_item.name)
    elif attempt < 2:
        # Datastore index may not have been updated, sleep and then retry
        time.sleep(back_off)
        return get_new_docket_item(scrape_key, attempt=attempt+1,
                                   back_off=back_off)
    else:
        logging.info("No matching docket item found in the docket queue for "
                     "scraper: %s", scrape_key)

    return docket_item


# ######################## #
# Removing from the docket #
# ######################## #


def purge_query_docket(scrape_key):
    """Delete all unleased tasks from the docket for provided region/type

    Retrieves and deletes all current docket items for the relevant scrape
    type and region (e.g., all docket queue tasks with tag 'us_ny-snapshot').

    Note that this doesn't remove tasks which are still currently leased -
    as a result, it's possible for a leased task which fails to re-enter
    the queue alongside the newly-placed docket items.

    Args:
        scrape_key: (ScrapeKey) The scraper whose tasks to purge the docket of

    Returns:
        N/A
    """
    logging.info("Purging existing query docket for scraper: %s", scrape_key)

    lease_seconds = 3600
    max_tasks = 1000
    tag_name = "{}-{}".format(scrape_key.region_code, scrape_key.scrape_type)
    deadline_seconds = 20

    q = taskqueue.Queue(DOCKET_QUEUE_NAME)

    empty = False
    while not empty:
        docket_items = q.lease_tasks_by_tag(lease_seconds,
                                            max_tasks,
                                            tag_name,
                                            deadline_seconds)

        if docket_items:
            q.delete_tasks(docket_items)
        else:
            empty = True


def delete_docket_item(item_name):
    """Delete a specific docket item from the docket pull queue.

    Deletes a specific docket item, e.g. when we've completed it or when we're
    purging a prior docket to build a new one.

    Args:
        item_name: (string) The docket item's name, which we use to select and
            delete it

    Returns:
        True if successful
        False if not
    """
    q = taskqueue.Queue(DOCKET_QUEUE_NAME)

    # Note: taskqueue.delete_tasks_by_name is supposed to accept either
    #   a single task name (string) or a list of task names. However,
    #   a bug currently causes it to treat a string variable as an iterable
    #   as well, and to try to delete tasks named after each char. So we
    #   submit a list of length 1.
    docket_items = q.delete_tasks_by_name([item_name])

    # Verify that docket item was properly deleted
    if not docket_items[0].was_deleted:
        logging.error("Error while deleting docket item with name %s",
                      item_name)
        return False

    return True
