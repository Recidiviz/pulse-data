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


"""This file defines a scraper base class that all regional scrapers
inherit from.
"""

import abc
import json
import logging
from datetime import datetime

import requests_toolbelt.adapters.appengine
import requests

from google.appengine.ext import deferred
from google.appengine.ext.db import InternalError
from google.appengine.ext.db import Timeout, TransactionFailedError
from google.appengine.api import taskqueue
from google.appengine.api import urlfetch
from recidiviz.ingest import constants
from recidiviz.ingest import scraper_utils
from recidiviz.ingest import sessions
from recidiviz.ingest.models.scrape_key import ScrapeKey
from recidiviz.ingest import tracker
from recidiviz.utils.regions import Region

# Use the App Engine Requests adapter to make sure that Requests plays
# nice with GAE
requests_toolbelt.adapters.appengine.monkeypatch()
urlfetch.set_default_fetch_deadline(60)

# Squelch urllib3 / sockets platform warning
requests.packages.urllib3.disable_warnings(
    requests.packages.urllib3.contrib.appengine.AppEnginePlatformWarning
)


class Scraper(object):
    """The base for all scraper objects. It handles basic setup, scrape
    process control (start, resume, stop), web requests, task
    queueing, state tracking, and a bunch of static convenience
    methods for data manipulation.

    Note that all child classes must implement the person_id_to_record_id
    method, which is used to iterate docket items.

    """

    __metaclass__ = abc.ABCMeta

    def __init__(self, region_name):
        """Initialize the parent scraper object.

        Args:
            region_name: (string) name of the region of the child scraper.

        """

        self.region = Region(region_name)
        self.fail_counter = (
            self.get_region().region_code + "_next_page_fail_counter")
        self.scraper_work_url = '/scraper/work'

    def person_id_to_record_id(self, person_id):
        """Abstract method for child classes to map a person ID to a DB
        record ID.

        Args: (any type) the person ID to transform

        Returns:
            A record id mapped from the given person id.

        """
        pass

    @abc.abstractmethod
    def get_initial_task(self):
        """Abstract method for child classes to specify the name of the first
        task to run in the scraper.

        Returns:
            The name of the function to run as the first task.

        """
        pass

    def get_region(self):
        """Retrieve the region object associated with this scraper.

        Returns:
            the region object

        """
        return self.region

    def get_now_as_str(self):
        """Returns the current time as a datetime

        Returns:
            datetime object set to the current time
        """
        return datetime.now().strftime(scraper_utils.DATETIME_STR_FORMAT)

    def start_scrape(self, scrape_type):
        """Start new scrape session / query against corrections site

        Retrieves first docket item, enqueues task for initial search
        page scrape to start the new scraping session.

        Args:
            scrape_type: (string) The type of scrape to start

        Returns:
            N/A

        """
        docket_item = self.iterate_docket_item(scrape_type)
        if not docket_item:
            logging.error("Found no %s docket items for %s, shutting down.",
                          scrape_type, self.get_region().region_code)
            sessions.end_session(ScrapeKey(self.get_region().region_code,
                                           scrape_type))
            return

        params = {"scrape_type": scrape_type,
                  "content": docket_item,
                  "scraper_start_time": self.get_now_as_str()}

        self.add_task(self.get_initial_task(), params)

    def stop_scrape(self, scrape_types):
        """Stops all active scraping tasks, resume non-targeted scrape types

        Stops the scraper, even if in the middle of a session. In
        production, this is called by a cron job scheduled to prevent
        interference with the normal operation of the scraped site.

        We share the scraping taskqueue between snapshot and
        background scraping to be certain of our throttling for the
        third-party service. As a result, cleaning up / purging the
        taskqueue necessarily kills all scrape types.  We kick off
        resume_scrape for any ongoing scraping types that aren't
        targets.

        Args:
            scrape_types: (list of strings) Scrape types to terminate

        Returns:
            N/A

        """
        # Check for other running scrapes, and if found kick off a delayed
        # resume for them since the taskqueue purge below will kill them.
        other_scrapes = set([])

        open_sessions = sessions.get_open_sessions(
            self.get_region().region_code)
        for session in open_sessions:
            if session.scrape_type not in scrape_types:
                other_scrapes.add(session.scrape_type)

        for scrape in other_scrapes:
            logging.info("Setting 60s deferred task to resume unaffected "
                         "scrape type: %s.", str(scrape))
            deferred.defer(self.resume_scrape, scrape, _countdown=60)

        q = taskqueue.Queue(self.get_region().queue)
        q.purge()

    def resume_scrape(self, scrape_type):
        """Resume a stopped scrape from where it left off

        Starts the scraper up again at the same place (roughly) as it had been
        stopped previously. This allows for cron jobs to start/stop scrapers at
        different times of day.

        Args:
            scrape_type: (string) Type of scraping to resume

        Returns:
            N/A
        """
        if scrape_type == constants.BACKGROUND_SCRAPE:
            # Background scrape

            # In most scrapers, background scrapes will use
            # short-lived docket items. However, some background
            # scrapes use only one docket item to run a giant scrape,
            # which may run for months. Limitations in GAE Pull Queues
            # make it difficult to keep track of a leased task for
            # that long, so we don't try. Resuming a background scrape
            # simply resumes from session data, and the task stays in
            # the docket un-leased. It will get deleted the next time
            # we start a new background scrape.

            recent_sessions = sessions.get_recent_sessions(ScrapeKey(
                self.get_region().region_code, scrape_type))

            last_scraped = None
            for session in recent_sessions:
                if session.last_scraped:
                    last_scraped = session.last_scraped
                    break

            if last_scraped:
                content = last_scraped.split(', ')
            else:
                logging.error("No earlier session with last_scraped found; "
                              "cannot resume.")
                return

        else:
            # Snapshot scrape

            # Get an item from the docket and continue from there. These queries
            # are very quick, so we don't bother trying to resume the same task
            # we left off on.

            content = self.iterate_docket_item(scrape_type)
            if not content:
                sessions.end_session(
                    ScrapeKey(self.get_region().region_code, scrape_type))
                return

        params = {'scrape_type': scrape_type,
                  'content': content,
                  'scraper_start_time': self.get_now_as_str()}
        self.add_task(self.get_initial_task(), params)

    @staticmethod
    def fetch_page(url, post_data=None, json_data=None):
        """Fetch content from a URL. If data is None (the default), we perform
        a GET for the page. If the data is set, it must be a dict of parameters
        to use as POST data in a POST request to the url.

        Args:
            url: (string) URL to fetch content from
            post_data: dict of parameters to pass into the html POST request

        Returns:
            The content if successful, -1 if fails.

        """
        proxies = scraper_utils.get_proxies()
        headers = scraper_utils.get_headers()

        try:
            if post_data is None and json_data is None:
                page = requests.get(url, proxies=proxies, headers=headers)
            else:
                page = requests.post(url, proxies=proxies, headers=headers,
                                     data=post_data, json=json_data)
        except requests.exceptions.RequestException as ce:
            log_error = "Error: {0}".format(ce)

            request = ce.request
            if request is not None:
                request_error = ("\n\nRequest headers: \n{0}"
                                 "\n\nMethod: {1}"
                                 "\n\nBody: \n{2} ") \
                    .format(request.headers,
                            request.method,
                            request.body)

                log_error += request_error

            response = ce.response
            if response is not None:
                response_error = ("\n\nResponse: \n{0} / {1}"
                                  "\n\nHeaders: \n{2}"
                                  "\n\nText: \n{3}") \
                    .format(response.status_code,
                            response.reason,
                            response.headers,
                            response.text)

                log_error += response_error

            logging.warning("Problem retrieving page, failing task to "
                            "retry. \n\n%s", log_error)
            return -1

        # Proxy errors don't raise an exception. : (
        if page.status_code == 502:
            logging.warning("Problem retrieving page (proxy error), failing "
                            "task to retry.")
            return -1

        return page

    def add_task(self, task_name, params):
        """ Add a task to the task queue.

        Args:
            task_name: (string) name of the function in the scraper class to
                       be invoked
            params: (dict) parameters to be passed to the function

        Returns:
            The content if successful, -1 if fails.

        """

        params_serial = json.dumps(params)

        taskqueue.add(url=self.scraper_work_url,
                      queue_name=self.get_region().queue,
                      params={'region': self.get_region().region_code,
                              'task': task_name,
                              'params': params_serial})

    def iterate_docket_item(self, scrape_type):
        """Leases new docket item, updates current session, returns item
        contents

        Returns an entity to scrape as provided by the docket item.

        Args:
            scrape_type: (string) Type of docket item to retrieve

        Returns:
            False if there was any failure to retrieve a new docket item.
            If successful:
                Background scrape: ("surname", "given names")
                Snapshot scrape:   ("record_id", ["records to ignore", ...])

        """

        item_content = tracker.iterate_docket_item(
            ScrapeKey(self.get_region().region_code, scrape_type))

        if item_content is None:
            return False

        if scrape_type == constants.SNAPSHOT_SCRAPE:
            # Content will be in the form (person ID, [list of records
            # to ignore]); allow the child class to convert person to
            # record
            record_id = self.person_id_to_record_id(item_content[0])

            if not record_id:
                logging.error("Couldn't convert docket item [%s] to record",
                              str(item_content))
                return False

            return record_id, item_content[1]

        return item_content

    def compare_and_set_snapshot(self, old_record, snapshot):
        """Check for updates since last scrape, if found persist new snapshot
        Checks the last snapshot taken for this record, and if fields
        have changed since the last time the record was updated (or
        the record has no old snapshots) stores new snapshot.

        The new snapshot will only include those fields which have changed.
        Args:
            old_record: (UsVtRecord) The record entity this
                snapshot pertains to
            snapshot: (UsVtSnapshot) Snapshot object with details from
                current scrape.
        Returns:
            True if successful
            False if datastore errors

        """
        new_snapshot = False

        # pylint:disable=protected-access
        snapshot_class = self.region.get_snapshot_kind()

        last_snapshot = snapshot_class.query(
            ancestor=old_record.key).order(
                -snapshot_class.created_on).get()

        if last_snapshot:
            snapshot_attrs = snapshot_class._properties
            for attribute in snapshot_attrs:
                if attribute not in ["class", "created_on", "offense"]:
                    current_value = getattr(snapshot, attribute)
                    last_value = getattr(old_record, attribute)
                    if current_value != last_value:
                        new_snapshot = True
                        logging.info("Found change in person snapshot: field "
                                     "%s was '%s', is now '%s'.",
                                     attribute, last_value, current_value)
                    else:
                        setattr(snapshot, attribute, None)

                elif attribute == "offense":
                    # Offenses have to be treated differently -
                    # setting them to None means an empty list or
                    # tuple instead of None, and an unordered list of
                    # them can't be compared to another unordered list
                    # of them.
                    offense_changed = False

                    # Check if any new offenses have been added
                    for charge in snapshot.offense:
                        if charge in old_record.offense:
                            pass
                        else:
                            offense_changed = True

                    # Check if any old offenses have been removed
                    for charge in old_record.offense:
                        if charge in snapshot.offense:
                            pass
                        else:
                            offense_changed = True

                    if offense_changed:
                        new_snapshot = True
                        logging.info("Found change in person snapshot: field "
                                     "%s was '%s', is now '%s'.", attribute,
                                     old_record.offense, snapshot.offense)
                    else:
                        setattr(snapshot, attribute, [])

        else:
            # This is the first snapshot, store everything
            new_snapshot = True

        if new_snapshot:
            try:
                snapshot.put()
            except (Timeout, TransactionFailedError, InternalError):
                logging.warning("Couldn't store new snapshot for record %s",
                                old_record.record_id)
                return False

        return True
