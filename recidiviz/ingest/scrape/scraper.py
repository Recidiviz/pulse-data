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


"""This file defines a scraper base class that all regional scrapers
inherit from.
"""

import abc
import logging
from datetime import datetime

import requests
import urllib3

from recidiviz.ingest.ingestor import Ingestor
from recidiviz.ingest.models.scrape_key import ScrapeKey
from recidiviz.ingest.scrape import (constants, scraper_utils, sessions,
                                     tracker)
from recidiviz.ingest.scrape.constants import BATCH_PUBSUB_TYPE
from recidiviz.ingest.scrape.scraper_cloud_task_manager import \
    ScraperCloudTaskManager
from recidiviz.ingest.scrape.task_params import QueueRequest, Task
from recidiviz.utils import regions, pubsub_helper


class FetchPageError(Exception):

    def __init__(self, request, response):
        details = ''
        if request is not None:
            details += ("\n\nRequest headers: \n{0}"
                        "\n\nMethod: {1}"
                        "\n\nBody: \n{2} ") \
                .format(request.headers, request.method, request.body)
        if response is not None:
            details += ("\n\nResponse: \n{0} / {1}"
                        "\n\nHeaders: \n{2}"
                        "\n\nText: \n{3}") \
                .format(response.status_code, response.reason,
                        response.headers,
                        response.text)

        msg = "Problem retrieving page: {}".format(details)
        super().__init__(msg)


class Scraper(Ingestor, metaclass=abc.ABCMeta):
    """The base for all scraper objects. It handles basic setup, scrape
    process control (start, resume, stop), web requests, task
    queueing, state tracking, and a bunch of static convenience
    methods for data manipulation.

    Note that all child classes must implement the person_id_to_record_id
    method, which is used to iterate docket items.

    """

    def __init__(self, region_name):
        """Initialize the parent scraper object.

        Args:
            region_name: (string) name of the region of the child scraper.

        """

        # Passing verify=False in the requests produces a warning,
        # disable it here.
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        self.region = regions.get_region(region_name)
        self.scraper_work_url = '/scraper/work/{}'.format(region_name)
        self.cloud_task_manager = ScraperCloudTaskManager()

    @abc.abstractmethod
    def get_initial_task_method(self):
        """Abstract method for child classes to specify the name of the first
        task to run in the scraper.

        Returns:
            The name of the function to run as the first task.

        """

    @abc.abstractmethod
    def get_initial_task(self) -> Task:
        """Returns the initial task to use for the first call."""

    def get_region(self):
        """Retrieve the region object associated with this scraper.

        Returns:
            the region object

        """
        return self.region

    def start_scrape(self, scrape_type):
        """Start new scrape session / query against corrections site

        Retrieves first docket item, enqueues task for initial search
        page scrape to start the new scraping session.

        Args:
            scrape_type: (ScrapeType) The type of scrape to start

        Returns:
            N/A

        """
        docket_item = self.iterate_docket_item(scrape_type)
        scrape_key = ScrapeKey(self.get_region().region_code, scrape_type)
        # Ensure that the topic and subscription are created on start.
        pubsub_helper.create_topic_and_subscription(
            scrape_key, BATCH_PUBSUB_TYPE)
        if not docket_item:
            logging.error("Found no %s docket items for %s, shutting down.",
                          scrape_type, self.get_region().region_code)
            sessions.close_session(scrape_key)
            return

        self.add_task(self.get_initial_task_method(),
                      QueueRequest(scrape_type=scrape_type,
                                   scraper_start_time=datetime.now(),
                                   next_task=self.get_initial_task()))

    def stop_scrape(self, scrape_type, respect_is_stoppable=False) -> bool:
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
            scrape_type: Scrape type to terminate
            respect_is_stoppable: Defaults to false, in which case the scraper
                will be stopped regardless of whether `is_stoppable` is set to
                true. Otherwise, stops the region's scraper only if its
                `is_stoppable` is set to true.
        Returns:
            A bool indicating whether or not the scrape was stopped.
        """
        region = self.get_region()

        if respect_is_stoppable and not region.is_stoppable:
            logging.info(
                "Stop scrape was called and ignored for the region: %s "
                "because the region's manifest is flagged as not stoppable",
                region.region_code)
            return False

        logging.info("Stopping scrape for the region: %s", region.region_code)

        try:
            self.cloud_task_manager.purge_scrape_tasks(
                region_code=region.region_code,
                queue_name=region.get_queue_name())
        except Exception as e:
            logging.error("Caught an exception while trying to purge scrape "
                          "tasks. The message was:\n%s", str(e))
            return False

        # Check for other running scrapes, and if found kick off a delayed
        # resume for them since the taskqueue purge will kill them.
        other_scrapes = set([])
        open_sessions = sessions.get_sessions(region.region_code,
                                              include_closed=False)
        for session in open_sessions:
            if session.scrape_type != scrape_type:
                other_scrapes.add(session.scrape_type)

        for scrape in other_scrapes:
            logging.info("Resuming unaffected scrape type: %s.", str(scrape))
            self.resume_scrape(scrape)

        return True

    def resume_scrape(self, scrape_type):
        """Resume a stopped scrape from where it left off

        Starts the scraper up again at the same place (roughly) as it had been
        stopped previously. This allows for cron jobs to start/stop scrapers at
        different times of day.

        Args:
            scrape_type: (ScrapeType) Type of scraping to resume

        Returns:
            N/A
        """
        # Note: None of the current scrapers support resumes, so this function
        # doesn't fully work. For instance, content is thrown away.
        if scrape_type is constants.ScrapeType.BACKGROUND:
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
                sessions.close_session(
                    ScrapeKey(self.get_region().region_code, scrape_type))
                return

        self.add_task(self.get_initial_task_method(),
                      QueueRequest(scrape_type=scrape_type,
                                   scraper_start_time=datetime.now(),
                                   next_task=self.get_initial_task()))

    @staticmethod
    def fetch_page(url, headers=None, cookies=None, params=None,
                   post_data=None, json_data=None, should_proxy=True):
        """Fetch content from a URL. If data is None (the default), we perform
        a GET for the page. If the data is set, it must be a dict of parameters
        to use as POST data in a POST request to the url.

        Args:
            url: (string) URL to fetch content from
            headers: (dict) any headers to send in addition to the default
            cookies: (dict) any cookies to send in the request.
            params: dict of parameters to pass in the url of a GET request
            post_data: dict of parameters to pass into the html POST request
            json_data: dict of parameters in JSON format to pass into the html
                       POST request
            extra_headers: dict of parameters to add to the headers of this
                           request
            should_proxy: (bool) whether or not to use a proxy.

        Returns:
            The content.

        """
        if should_proxy:
            proxies = scraper_utils.get_proxies()
        else:
            proxies = None
        headers = headers.copy() if headers else {}
        if 'User-Agent' not in headers:
            headers.update(scraper_utils.get_headers())

        try:
            if post_data is None and json_data is None:
                page = requests.get(
                    url, proxies=proxies, headers=headers, cookies=cookies,
                    params=params, verify=False)
            elif params is None:
                page = requests.post(
                    url, proxies=proxies, headers=headers, cookies=cookies,
                    data=post_data, json=json_data, verify=False)
            else:
                raise ValueError(
                    "Both params ({}) for a GET request and either post_data "
                    "({}) or json_data ({}) for a POST request were set." \
                        .format(params, post_data, json_data))
            page.raise_for_status()
        except requests.exceptions.RequestException as ce:
            raise FetchPageError(ce.request, ce.response) from ce

        return page

    def add_task(self, task_name, request: QueueRequest):
        """ Add a task to the task queue.

        Args:
            task_name: (string) name of the function in the scraper class to
                       be invoked
            request: (dict) parameters to be passed to the function
        """
        self.cloud_task_manager.create_scrape_task(
            region_code=self.get_region().region_code,
            queue_name=self.get_region().get_queue_name(),
            url=self.scraper_work_url,
            body={
                'region': self.get_region().region_code,
                'task': task_name,
                'params': request.to_serializable(),
            }
        )

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
        """

        item_content = tracker.iterate_docket_item(
            ScrapeKey(self.get_region().region_code, scrape_type))

        if item_content is None:
            return False

        return item_content
