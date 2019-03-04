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


"""Generic implementation of the scraper.  This class abstracts away the need
for scraper implementations to know or understand tasks.  Child scrapers need
only to know how to extract data from a given page and whether or not more pages
need to be scraped.

This is a class that aims to handle as many DB related operations as possible
as well as operations around how to get the content of a page.  It provides
generic implementations of getter functions which aim to scrape out important
fields we care about.

In order to subclass this the following functions must be implemented:

    1.  get_more_tasks: This function takes the page content as well as the
        scrape task params and returns a list of `Tasks` defining what to scrape
        next.  The `Task` must include 'endpoint' and 'task_type' which tell the
        generic scraper what endpoint we are getting and what we are doing
        with the endpoint when we do get it.
    2.  populate_data:  This function is called whenever a task loads a page
        that has important data in it.
"""

import abc
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from lxml import html
from lxml.etree import XMLSyntaxError  # pylint:disable=no-name-in-module

from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models.scrape_key import ScrapeKey
from recidiviz.ingest.scrape import constants, ingest_utils
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.scrape.errors import ScraperFetchError, \
    ScraperGetMoreTasksError, ScraperPopulateDataError
from recidiviz.ingest.scrape.scraper import Scraper
from recidiviz.ingest.scrape.task_params import QueueRequest, ScrapedData,\
    Task
from recidiviz.persistence import batch_persistence, persistence


class BaseScraper(Scraper):
    """Generic class for scrapers."""

    # TODO 1055: Remove this when batch reader is complete.
    BATCH_WRITES = False

    def __init__(self, region_name):
        super(BaseScraper, self).__init__(region_name)

    def get_initial_task_method(self):
        """
        Get the name of the first task to be run.  For generic scraper we always
        call into the same function.

        Returns:
            The name of the task to run first.

        """
        return '_generic_scrape'

    # Each scraper can override this, by default it is treated as a url endpoint
    # but any scraper can override this and treat it as a different type of
    # endpoint like an API endpoint for example.
    def _fetch_content(self, endpoint, response_type, headers=None,
                       cookies=None, params=None, post_data=None,
                       json_data=None) -> Tuple[Any, Optional[Dict[str, str]]]:
        """Returns the page content.

        Args:
            endpoint: the endpoint to make a request to.
            headers: dict of headers to send in the request.
            cookies: dict of cookies to send in the request.
            params: dict of url params to send in the request.
            post_data: dict of parameters to pass into the html request.

        Returns:
            Returns a tuple of the content of the page (or -1) and a dict
            of cookies (or None).
        """
        logging.info('Fetching content with endpoint: %s', endpoint)

        # Create cookie jar to pass to fetch
        response = self.fetch_page(
            endpoint, headers=headers, cookies=cookies, params=params,
            post_data=post_data, json_data=json_data)
        if response == -1:
            return -1, None

        # Extract any cookies from the response and convert back to dict.
        cookies.update(response.cookies.get_dict())

        # If the character set was not explicitly set in the response, use the
        # detected encoding instead of defaulting to 'ISO-8859-1'. See
        # http://docs.python-requests.org/en/master/user/advanced/#encodings
        if 'charset' not in response.headers['content-type'] and \
            not response.apparent_encoding == 'ascii':
            response.encoding = response.apparent_encoding

        if response_type is constants.ResponseType.HTML:
            try:
                return self._parse_html_content(response.text), cookies
            except XMLSyntaxError as e:
                logging.error("Error parsing page. Error: %s\nPage:\n\n%s",
                              e, response.text)
                return -1, None
        if response_type is constants.ResponseType.JSON:
            try:
                return json.loads(response.text), cookies
            except json.JSONDecodeError as e:
                logging.error("Error parsing page. Error: %s\nPage:\n\n%s",
                              e, response.text)
                return -1, None
        if response_type is constants.ResponseType.TEXT:
            return response.text, cookies
        if response_type is constants.ResponseType.RAW:
            return response.content, cookies
        logging.error("Unexpected response type '%s' for endpoint '%s'",
                      response_type, endpoint)
        return -1, None

    def _parse_html_content(self, content_string: str) -> html.HtmlElement:
        """Parses a string into a structured HtmlElement.

        Args:
            content_string: string representing HTML content
        Returns:
            an lxml.html.HtmlElement
        """
        return html.fromstring(content_string)

    def _generic_scrape(self, request: QueueRequest):
        """
        General handler for all scrape tasks.  This function is a generic entry
        point into all types of scrapes.  It decides what to call based on
        params.

        Args:
            params: dict of parameters passed from the last scrape session.

        Returns:
            Nothing if successful, -1 if it fails
        """
        try:
            task = request.next_task

            # Here we handle a special case where we weren't really sure
            # we were going to get data when we submitted a task, but then
            # we ended up with data, so no more requests are required,
            # just the content we already have.
            # TODO(#680): remove this
            if task.content is not None:
                content = self._parse_html_content(task.content)
            else:
                post_data = task.post_data

                # Let the child transform the post_data if it wants before
                # sending the requests.  This hook is in here in case the
                # child did something like compress the post_data before
                # it put it on the queue.
                self.transform_post_data(post_data)

                # We always fetch some content before doing anything.
                # Note that we use get here for the post_data to return a
                # default value of None if this scraper doesn't set it.
                try:
                    content, cookies = self._fetch_content(
                        task.endpoint, task.response_type, headers=task.headers,
                        cookies=task.cookies, params=task.params,
                        post_data=post_data, json_data=task.json)
                    if content == -1:
                        return -1
                except Exception as e:
                    raise ScraperFetchError() from e

            scraped_data = None
            if self.should_scrape_data(task.task_type):
                # If we want to scrape data, we should either create an
                # ingest_info object or get the one that already exists.
                logging.info('Scraping data for %s and endpoint: %s',
                             self.region.region_code, task.endpoint)
                try:
                    scraped_data = self.populate_data(
                        content, task, request.ingest_info or IngestInfo())
                except Exception as e:
                    raise ScraperPopulateDataError() from e

            if self.should_get_more_tasks(task.task_type):
                logging.info('Getting more tasks for %s and endpoint: %s',
                             self.region.region_code, task.endpoint)

                # Only send along ingest info if it will not be persisted now.
                ingest_info_to_send = None
                if scraped_data is not None and not scraped_data.persist:
                    ingest_info_to_send = scraped_data.ingest_info

                try:
                    # pylint: disable=assignment-from-no-return
                    next_tasks = self.get_more_tasks(content, task)
                except Exception as e:
                    raise ScraperGetMoreTasksError() from e
                for next_task in next_tasks:
                    # Include cookies received from response, if any
                    if cookies:
                        cookies.update(next_task.cookies)
                        next_task = Task.evolve(next_task, cookies=cookies)
                    self.add_task('_generic_scrape', QueueRequest(
                        scrape_type=request.scrape_type,
                        scraper_start_time=request.scraper_start_time,
                        next_task=next_task,
                        ingest_info=ingest_info_to_send,
                    ))

            if scraped_data is not None and scraped_data.persist:
                # Something is wrong if we get here but no fields are set in the
                # ingest info.
                if not scraped_data.ingest_info:
                    raise ValueError('IngestInfo must be populated')

                logging.info(
                    'Writing ingest_info (%d people) to the database for %s',
                    len(scraped_data.ingest_info.people),
                    self.region.region_code)
                logging.info('Logging at most 4 people:')
                loop_count = min(len(scraped_data.ingest_info.people),
                                 constants.MAX_PEOPLE_TO_LOG)
                for i in range(loop_count):
                    logging.info(scraped_data.ingest_info.people[i])
                logging.info('Last seen time of person being set as: %s',
                             request.scraper_start_time)
                metadata = IngestMetadata(self.region.region_code,
                                          request.scraper_start_time,
                                          self.get_enum_overrides())
                if self.BATCH_WRITES:
                    scrape_key = ScrapeKey(
                        self.region.region_code, request.scrape_type)
                    batch_persistence.write(
                        ingest_info=scraped_data.ingest_info,
                        scraper_start_time=request.scraper_start_time,
                        task=task,
                        scrape_key=scrape_key,
                    )
                else:
                    persistence.write(
                        ingest_utils.convert_ingest_info_to_proto(
                            scraped_data.ingest_info), metadata)
            return None
        except Exception as e:
            if self.BATCH_WRITES:
                scrape_key = ScrapeKey(
                    self.region.region_code, request.scrape_type)
                batch_persistence.write_error(
                    error=type(e).__name__,
                    task=task,
                    scrape_key=scrape_key,
                )
            raise e

    def is_initial_task(self, task_type):
        """Tells us if the task_type is initial task_type.

        Args:
            A hexcode representing the task_type

        Returns:
            boolean representing whether or not the task_type is initial task.
        """
        return task_type & constants.TaskType.INITIAL

    def should_get_more_tasks(self, task_type):
        """Tells us if we should get more tasks.

        Args:
            A hexcode representing the task_type

        Returns:
            boolean whether or not we should get more tasks
        """
        return task_type & constants.TaskType.GET_MORE_TASKS

    def should_scrape_data(self, task_type):
        """Tells us if we should scrape data from a page.

        Args:
            A hexcode representing the task_type

        Returns:
            boolean whether or not we should scrape a person from the page.
        """
        return task_type & constants.TaskType.SCRAPE_DATA

    def get_more_tasks(self, content, task: Task) -> List[Task]:
        """
        Gets more tasks based on the content and task passed in.  This
        function should determine which tasks, if any, should be
        added to the queue.

        Every scraper must implement this.  It should return a list of tasks.

        Args:
            content: An lxml html tree.
            task: Task from the last scrape.

        Returns:
            A list of Tasks containing endpoint and task_type at minimum
        """
        raise NoMoreTasksError()

    @abc.abstractmethod
    def populate_data(self, content, task: Task,
                      ingest_info: IngestInfo) -> Optional[ScrapedData]:
        """
        Populates the ingest info object from the content and task given

        Args:
            content: An lxml html tree.
            task: Task with parameters passed from the last scrape.
            ingest_info: The IngestInfo object to populate
        """

    def get_enum_overrides(self) -> EnumOverrides:
        """
        Returns a dict that contains all string to enum mappings that are
        region specific. These overrides have a higher precedence than the
        global mappings in ingest/constants.

        Note: Before overriding this method, consider directly adding each
        mapping directly into the respective global mappings instead.
        """
        return EnumOverrides.empty()

    def transform_post_data(self, data):
        """If the child needs to transform the data in any way before it sends
        the request, it can override this function.

        Args:
            data: dict of parameters to send as data to the post request.
        """

    def get_initial_task(self) -> Task:
        """Returns the initial parameters to use for the first call."""
        return Task(
            task_type=constants.TaskType.INITIAL_AND_MORE,
            endpoint=self.get_region().base_url,
        )


class NoMoreTasksError(NotImplementedError):
    """Raised if the scraper should get more tasks and get_more_tasks is not
    implemented."""
