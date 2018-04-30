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


from dateutil.relativedelta import relativedelta
from datetime import datetime
from google.appengine.api import taskqueue
from google.appengine.ext import deferred
from google.appengine.ext import ndb
from scraper.models.scrape_session import ScrapeSession
from utils import environment
from utils import regions
from utils.auth import authenticate_request

import csv
import hashlib
import json
import logging
import webapp2


FILENAME_PREFIX = "./name_lists/"
SNAPSHOT_DISTANCE_YEARS = 10
DOCKET_QUEUE_NAME = "docket"
SCRAPE_TYPES = ["background", "snapshot"]
BACKGROUND_BATCH_SIZE = 1000
SNAPSHOT_BATCH_SIZE = 100


class ScraperStart(webapp2.RequestHandler):

    @authenticate_request
    def get(self):
        """Request handler to start one or several running scrapers

        Kicks off new scrape session for each region and scrape type in request

        Example query:
            /scraper/start?region=us_ny&scrape_type=background

        URL parameters:
            region: (string) Region to take action for, or 'all'
            scrape_type: (string) Type of scrape to take action for, or 'all'
            surname: (string, optional) Name to start scrape at. Required if
                given_names provided
            given_names: (string, optional) Name to start scrape at

        Args:
            N/A

        Returns:
            N/A
        """
        request_params = self.request.GET.items()
        valid_params = get_and_validate_params(request_params)

        if valid_params:
            (scrape_regions, scrape_types, params) = valid_params

            for region in scrape_regions:

                for scrape_type in scrape_types:
                    logging.info("Starting new %s scrape(s) for %s." %
                        (scrape_type, region))

                    scraper = regions.get_scraper(region)

                    # Set up scraper for new session
                    scraper.setup(scrape_type)

                    # Clear prior query docket for this scrape type and add new items
                    purge_query_docket(region, scrape_type)
                    load_target_list(scrape_type, region, params)

                    # Start scraper, but give the target list loader a headstart
                    timer = 30 if not environment.in_prod() else 300
                    logging.info("Starting %s/%s scrape in %d seconds..." %
                        (region, scrape_type, timer))
                    deferred.defer(scraper.start_scrape, scrape_type, _countdown=timer)

        else:
            invalid_params(self.response, "Scrape type or region not recognized.")


class ScraperStop(webapp2.RequestHandler):

    @authenticate_request
    def get(self):
        """Request handler to stop one or several running scrapers.

        Note: Stopping any scrape type for a region involves purging the
        scraping task queue for that region, necessarily killing any other
        in-progress scrape types. Untargeted scrapes killed by this request
        handler will be noted and resumed a moment or two later.

        Unlike the other Scraper action methods, stop_scrape doesn't call
        individually for each scrape type. That could create a race condition,
        as each call noticed the other scrape type was running at the same
        time, kicked off a resume effort with a delay, and then our second
        call came to kill the other type and missed the (delayed / not yet
        in taskqueue) call - effectively not stopping the scrape.

        Instead, we send the full list of scrape_types to stop, and
        Scraper.stop_scrape is responsible for fan-out.

        Example query:
            /scraper/stop?region=us_ny&scrape_type=background

        URL parameters:
            region: (string) Region to take action for, or 'all'
            scrape_type: (string) Type of scrape to take action for, or 'all'

        Args:
            N/A

        Returns:
            N/A
        """
        request_params = self.request.GET.items()
        valid_params = get_and_validate_params(request_params)

        if valid_params:
            (scrape_regions, scrape_types, params) = valid_params

            for region in scrape_regions:

                logging.info("Stopping %s scrapes for %s." %
                    (scrape_types, region))

                scraper = regions.get_scraper(region)
                scraper.stop_scrape(scrape_types)

        else:
            invalid_params(self.response, "Scrape type or region not recognized.")


class ScraperResume(webapp2.RequestHandler):

    @authenticate_request
    def get(self):
        """Request handler to resume one or several stopped scrapers

        Resumes scraping for each region and scrape type in request.

        Example query:
            /scraper/resume?region=us_ny&scrape_type=background

        URL parameters:
            region: (string) Region to take action for, or 'all'
            scrape_type: (string) Type of scrape to take action for, or 'all'

        Args:
            N/A

        Returns:
            N/A
        """
        request_params = self.request.GET.items()
        valid_params = get_and_validate_params(request_params)

        if valid_params:
            (scrape_regions, scrape_types, params) = valid_params

            for region in scrape_regions:

                for scrape_type in scrape_types:
                    logging.info("Resuming %s scrape for %s." %
                        (scrape_type, region))

                    scraper = regions.get_scraper(region)
                    scraper.setup(scrape_type)
                    scraper.resume_scrape(scrape_type)

        else:
            invalid_params(self.response, "Scrape type or region not recognized.")


def get_and_validate_params(request_params):
    """Get common request parameters and validate them

    URL parameters:
        region: (string) Region to take action for, or 'all'
        scrape_type: (string) Type of scrape to take action for, or 'all'

    Args:
        request_params: The request URL parameters

    Returns:
        False if invalid params
        Tuple of params if successful, in the form:
            ([region1, ...], [scrape_type1, ...], other_query_params)
    """
    request_region = get_param("region", request_params, None)
    request_scrape_type = get_param("scrape_type", request_params, "background")

    # Validate scrape_type
    if request_scrape_type == "all":
        request_scrape_type = SCRAPE_TYPES
    elif request_scrape_type in SCRAPE_TYPES:
        request_scrape_type = [request_scrape_type]
    else:
        return False

    # Validate region code
    supported_regions = regions.get_supported_regions()
    if request_region == "all":
        request_region = supported_regions
    elif request_region in supported_regions:
        request_region = [request_region]
    else:
        return False

    result = (request_region, request_scrape_type, request_params)

    return result


def invalid_input(request_response, log_message):
    """Logs problem with request and sets HTTP response code

    Args:
        request_response: Request handler response object
        log_message: (string) Message to write to service logs

    Returns:
        N/A
    """
    logging.error(log_message)

    request_response.write('Missing or invalid parameters, see service logs.')
    request_response.set_status(400)


def get_param(param_name, params, default=None):
    """Retrieves URL parameter from request handler params list

    Takes an iterable list of key/value pairs (URL parameters from the request
    object), finds the key being sought, and returns its value or None if not
    found.

    Args:
        param_name: (string) Name of the URL parameter being sought
        params: List of URL parameter key/value pairs, in tuples (e.g.,
            [("key", "val"), ("key2", "val2"), ...])

    Returns:
        Value for given param_name if found
        Provided default value if not found
        None if no default provided and not found
    """
    for key, val in params:
        if key == param_name:
            val = val.lower().strip()
            return val

    return default


def load_target_list(scrape_type, region_code, params):
    """Starts docket loading based on scrape type and region

    Determines correct scrape type and kicks off target list generation,
    loading of docket items based on the target list.

    Args:
        scrape_type: (string) Type of scrape to load docket for
        region_code: (string) Region code (e.g., us_ny)
        params: List of tuples for other key/value pairs from request

    Returns:
        N/A
    """
    logging.info("Getting target list, %s/%s" % (region_code, scrape_type))

    if scrape_type == "background":
        name_list_file = regions.get_name_list_file(region_code)

        # Construct filename, process user-supplied name query (if provided)
        filename = FILENAME_PREFIX + name_list_file
        given_names = get_param("given_names", params, "")
        surname = get_param("surname", params, "")
        query_name = (surname, given_names)

        # If query name not provided, load from the start of the file
        load = (query_name == ("", ""))

        # Kick off docket loading
        load_background_target_list(region_code, filename, query_name, load)

    elif scrape_type == "snapshot":
        # Kick off the snapshot loading
        phase = 0
        load_snapshot_target_list(region_code, phase)


def load_background_target_list(region_code, name_file, query_name, load, start_line=1):
    """Load background scrape docket items, from name file

    Iterates over a CSV of common names, loading a docket item for the scraper
    to search for each one. We use pandas because it can selectively load lines
    from a large CSV file.

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
    end_of_list = False
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
            logging.info("Couldn't find user-provided name '%s' in name "
                "list, adding one-off docket item for the name instead." %
                str(query_name))
            names = [query_name]
            write_to_docket = True

    if write_to_docket:
        add_to_query_docket(region_code, scrape_type, names)

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
                "names to docket." % str(query_name))
        subset = names[match_index:]

        return (True, subset)

    except ValueError:
        return (False, [])


def load_snapshot_target_list(region_code, phase, cursor=None):
    """Load snapshot scrape docket items, based on previously-found inmates

    We need to run several queries to find relevant inmates to follow-up
    on. This method runs them by query type and by batch, using the deferred
    library to iterate through the batches (and the queries) until complete.

    Note that these query types (checking snapshots and records) are likely
    to produce some duplicate inmates for us to re-snapshot. To prevent
    extra work, we add these items to the docket (pull taskqueue) with task
    names that encode the inmate and current datetime, accurate to the hour.
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
    result = []
    (start_date, start_datetime) = get_snapshot_start()
    scrape_type = "snapshot"

    # Get relevant sub-kinds for the region
    region = regions.Region(region_code)
    region_inmate = region.get_inmate_kind()
    region_record = region.get_record_kind()
    region_snapshot = region.get_snapshot_kind()

    if phase == 0:
        # Query 1: Snapshots
        query_type = "snapshot"

        # Get all snapshots showing inmates in prison within the date range
        # (Snapshot search aims to catch when they get out / facility changes)
        snapshot_query = region_snapshot.query(ndb.AND(
            region_snapshot.created_on > start_datetime,
            region_snapshot.is_released == False))

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
        inmate_records_query = region_record.query(ndb.AND(
            region_record.latest_release_date > start_date,
            region_record.is_released == True))

        query_results, next_cursor, more = inmate_records_query.fetch_page(
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
        # inmates of interest for this edge-case won't have any snapshots
        # updating is_released in that time.
        inmate_records_query = region_record.query(ndb.AND(
            region_record.is_released == False,
            region_record.created_on < start_datetime))

        query_results, next_cursor, more = inmate_records_query.fetch_page(
            batch_size, start_cursor=cursor)

    else:
        logging.info("Finished loading snapshot target list to docket.")
        return

    (relevant_records, inmate_keys_ids) = process_query_response(
        query_type, query_results)

    # Invert results to a combination of inmate + set of records to ignore
    results = get_ignore_records(start_date, region_record, relevant_records, inmate_keys_ids)

    # Store the inmates and ignore records as docket items for snapshot scrape
    add_to_query_docket(region_code, scrape_type, results)

    # Move to next phase, if we've completed all results for this phase
    if not more:
        phase += 1
        next_cursor = None

    deferred.defer(load_snapshot_target_list,
                   region_code=region_code,
                   phase=phase,
                   cursor=next_cursor)


def process_query_response(query_type, query_results):
    """Extracts relevant details for docket items from snapshot query results

    Args:
        query_type: (string) "snapshot" if query was for snapshots, "record" if not
        query_results: (list of entities) Result set from the query

    Returns:
        Tuple in the form (relevant_records, inmate_keys_ids).
            relevant_records: A list of records which should be scraped to
                check for changes
            inmate_keys_ids: Dictionary of parent entities (inmates who we
                should re-scrape), keyed (de-duplicated) by inmate key.
    """
    inmate_keys_ids = {}
    relevant_records = []

    for result in query_results:
        if query_type == "snapshot":
            record_key = result.key.parent()
            record = record_key.get()
        else:
            record = result

        relevant_records.append(record.record_id)
        inmate_key = record.key.parent()
        inmate = inmate_key.get()
        inmate_keys_ids[inmate.key] = inmate.inmate_id

    return (relevant_records, inmate_keys_ids)


def get_ignore_records(start_date, region_record, relevant_records, inmate_keys_ids):
    """Invert list of relevant records to produce list scraper should ignore

    Takes a list of inmates, and of relevant records, then inverts the
    list to get only a list of records we don't want to waste time
    re-scraping.

    Args:
        start_date: (date) Date when the window of snapshot interest starts
        region_record: (models.Record subclass) The sub-kind for Record for
            the specific region
        inmate_keys_ids: A dictionary in the form {inmate_key: inmate_id, ...}
        relevant_records: (list) Records we should re-scrape

    Returns:
        List of record IDs to ignore. Each list item is a tuple in the form:
            (<inmate ID>, <list of record IDs to ignore for this inmate>)
    """
    result_list = []

    # Use the inmate keys to invert the record list - need to know which
    # records to ignore, not which to scrape, since it's important to
    # scrape new records as well.
    for inmate_key, inmate_id in inmate_keys_ids.iteritems():
        inmate_ignore_records = []
        records = region_record.query(ancestor=inmate_key).fetch()

        for record in records:
            record_id = record.record_id
            if record_id not in relevant_records:

                # There's an edge-case where a record's release date is
                # within the date range we care about, but doesn't come
                # up in the snapshot query above because we started
                # scraping after they had already been released. Don't
                # ignore those records - may have been put back e.g.
                # on parole violation.
                if (record.latest_release_date and not
                    record.latest_release_date > start_date):
                    inmate_ignore_records.append(record_id)

        result_list.append((inmate_id, inmate_ignore_records))

    return result_list


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

    return (start_date, start_datetime)


def purge_query_docket(region, scrape_type):
    """Delete all unleased tasks from the docket for provided region/type

    Retrieves and deletes all current docket items for the relevant scrape
    type and region (e.g., all docket queue tasks with tag 'us_ny-snapshot').

    Note that this doesn't remove tasks which are still currently leased -
    as a result, it's possible for a leased task which fails to re-enter
    the queue alongside the newly-placed docket items.

    Args:
        region: (string) Region code, e.g. us_ny
        scrape_type: (string) Type of scape to purge from docket

    Returns:
        N/A
    """
    logging.info("Purging existing query docket for %s/%s" %
        (region, scrape_type))

    lease_seconds = 3600
    max_tasks = 1000
    tag_name = region + "-" + scrape_type
    deadline_seconds = 20

    q = taskqueue.Queue(DOCKET_QUEUE_NAME)

    empty = False
    while not empty:
        docket_items = q.lease_tasks_by_tag(lease_seconds,
                                            max_tasks,
                                            tag_name,
                                            deadline_seconds)

        if docket_items and len(docket_items) > 0:
            q.delete_tasks(docket_items)
        else:
            empty = True

    # Check for tasks which were leased as part of a recent session
    purge_leased_docket_items(region, scrape_type)


def purge_leased_docket_items(region, scrape_type):
    """Check sessions for still-leased tasks, remove and delete them.

    Pulls any sessions from the relevant region / scrape type that are
    still associated with leased tasks, and (if found) deletes the task
    and removes the association with the session.

    Args:
        region: (string) Region code, e.g. us_ny
        scrape_type: (string) Type of docket items to find and remove

    Returns:
        N/A
    """
    q = taskqueue.Queue(DOCKET_QUEUE_NAME)

    # Query for the relevant session
    session_query = ScrapeSession.query()
    session_query = session_query.filter(ScrapeSession.region == region)
    session_query = session_query.filter(ScrapeSession.scrape_type == scrape_type)
    session_query = session_query.filter(ScrapeSession.docket_item != None)
    session_results = session_query.fetch()

    for session in session_results:
        docket_item_name = session.docket_item

        try:
            q.delete_tasks_by_name([docket_item_name])

            session.docket_item = None
            session.put()

        except Exception as e:
            # Possible the docket item was deleted too long ago for the
            # task queue to recognize.
            logging.warning("Failed to remove docket item (%s) from session "
                "(%s):\n%s" % (docket_item_name, session.key, e))

    return


def add_to_query_docket(region_code, scrape_type, docket_items):
    """ Add docket item to the query docket for relevant region / scrape type

    Adds items in the list to the relevant query docket (that matching the region
    and type given). The scraper will pull each item from the docket in turn for
    scraping (e.g. each name, if a background scrape, or each inmate ID if a
    snapshot scrape.)

    Args:
        region_code: (string) Region code, e.g. us_ny
        scrape_type: (string) Scrape type to add item to docket for
        docket_items: (list) List of payloads to add.
    """
    logging.info("Populating query docket for %s/%s." % (region_code, scrape_type))
    q = taskqueue.Queue(DOCKET_QUEUE_NAME)

    tag_name = region_code + "-" + scrape_type

    tasks_to_add = []
    for item in docket_items:
        payload = json.dumps(item)

        if scrape_type == "snapshot":
            logging.debug("Attempting to add snapshot item to docket: %s" % (item[0]))
            task_name = get_task_name(region_code, item[0])
            new_task = taskqueue.Task(payload=payload,
                                      method='PULL',
                                      tag=tag_name,
                                      name=task_name)

        elif scrape_type == "background":
            new_task = taskqueue.Task(payload=payload,
                                      method='PULL',
                                      tag=tag_name)

        tasks_to_add.append(new_task)

    try:
        q.add(tasks_to_add)
    except (taskqueue.taskqueue.DuplicateTaskNameError,
            taskqueue.taskqueue.TombstonedTaskError,
            taskqueue.taskqueue.TaskAlreadyExistsError):
        logging.debug("Some inmates been added to the docket already; "
            "skipping.")


def get_task_name(region_code, inmate_id):
    """Generate unique task name for region+inmate+current datetime

    Generates a unique task name for a particular inmate ID and region.

    Because taskqueue enforces uniqueness for 9 days even after a task
    has been deleted, and we may want to run (or re-run) snapshot scrapes
    more frequently, we incorporate the datetime accurate to the current hour.

    Because taskqueues become less efficient if task names are closely related
    or sequential, we hash the region/inmate-specific string chosen for the
    name to try to ensure they're more evenly distributed.

    Args:
        region_code: (string) Region code this task will be tied to
        inmate_id: (string) Inmate ID the task is for

    Returns:
        Task name, in the form of an MD5 hash
    """
    # Get the time, rounded down to the last hour. New tasks will have unique
    # names an hour after the last snapshot scrape, but immediate duplicates
    # will be blocked.
    time_component = datetime.now().replace(microsecond=0,second=0,minute=0)

    string_base = region_code + inmate_id + str(time_component)

    return hashlib.md5(string_base).hexdigest()


app = webapp2.WSGIApplication([
    ('/scraper/start', ScraperStart),
    ('/scraper/stop', ScraperStop),
    ('/scraper/resume', ScraperResume)
], debug=False)
