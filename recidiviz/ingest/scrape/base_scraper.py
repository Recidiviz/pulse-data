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

import attr
from lxml import html
from lxml.etree import XMLSyntaxError  # pylint:disable=no-name-in-module

from recidiviz.common.common_utils import get_trace_id_from_flask
from recidiviz.common.ingest_metadata import IngestMetadata, SystemLevel
from recidiviz.ingest.models import serialization
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.models.scrape_key import ScrapeKey
from recidiviz.ingest.scrape import constants, sessions
from recidiviz.ingest.scrape.errors import (
    ScraperFetchError,
    ScraperGetMoreTasksError,
    ScraperPopulateDataError,
)
from recidiviz.ingest.scrape.scraper import Scraper
from recidiviz.ingest.scrape.task_params import QueueRequest, ScrapedData, Task
from recidiviz.persistence import batch_persistence, persistence, single_count
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey


class ParsingError(Exception):
    """Exception containing the text that failed to parse"""

    def __init__(self, response_type: constants.ResponseType, text: str):
        msg = "Error parsing response as {}:\n{}".format(response_type, text)
        super().__init__(msg)


class BaseScraper(Scraper):
    """Generic class for scrapers."""

    # TODO(#1055): Remove this when batch reader is complete.
    BATCH_WRITES = True

    def __init__(self, region_name):
        super().__init__(region_name)

    def get_initial_task_method(self):
        """
        Get the name of the first task to be run.  For generic scraper we always
        call into the same function.

        Returns:
            The name of the task to run first.

        """
        return "_generic_scrape"

    # Each scraper can override this, by default it is treated as a url endpoint
    # but any scraper can override this and treat it as a different type of
    # endpoint like an API endpoint for example.
    def _fetch_content(
        self,
        endpoint,
        response_type,
        headers=None,
        cookies=None,
        params=None,
        post_data=None,
        json_data=None,
    ) -> Tuple[Any, Optional[Dict[str, str]]]:
        """Returns the page content.

        Args:
            endpoint: the endpoint to make a request to.
            headers: dict of headers to send in the request.
            cookies: dict of cookies to send in the request.
            params: dict of url params to send in the request.
            post_data: dict of parameters to pass into the html request.

        Returns:
            Returns a tuple of the content of the page and a dict of cookies.
        """
        logging.info("Fetching content with endpoint: [%s]", endpoint)

        # Create cookie jar to pass to fetch
        should_proxy = self.get_region().should_proxy
        response = self.fetch_page(
            endpoint,
            headers=headers,
            cookies=cookies,
            params=params,
            post_data=post_data,
            json_data=json_data,
            should_proxy=should_proxy,
        )

        # Extract any cookies from the response and convert back to dict.
        cookies.update(response.cookies.get_dict())

        # If the character set was not explicitly set in the response, use the
        # detected encoding instead of defaulting to 'ISO-8859-1'. See
        # http://docs.python-requests.org/en/master/user/advanced/#encodings
        if (
            "content-type" not in response.headers
            or "charset" not in response.headers["content-type"]
        ) and not response.apparent_encoding == "ascii":
            response.encoding = response.apparent_encoding

        if response_type is constants.ResponseType.HTML:
            try:
                return self._parse_html_content(response.text), cookies
            except XMLSyntaxError as e:
                raise ParsingError(response_type, response.text) from e
        if response_type is constants.ResponseType.JSON:
            try:
                return json.loads(response.text), cookies
            except json.JSONDecodeError as e:
                if not response.text:
                    return [], cookies
                raise ParsingError(response_type, response.text) from e
        if response_type is constants.ResponseType.JSONP:
            json_text = self._jsonp_to_json(response.text)
            try:
                return json.loads(json_text), cookies
            except json.JSONDecodeError as e:
                raise ParsingError(response_type, response.text) from e
        if response_type is constants.ResponseType.TEXT:
            return response.text, cookies
        if response_type is constants.ResponseType.RAW:
            return response.content, cookies

        raise ValueError(
            "Unexpected response type '{}' for endpoint '{}'".format(
                response_type, endpoint
            )
        )

    @staticmethod
    def _jsonp_to_json(jsonp: str) -> str:
        """Takes 'JSONP' and turns it to a JSON string.
        JSONP looks like foo(<regular serialized JSON>);
        """
        return jsonp[jsonp.index("(") + 1 : jsonp.rindex(")")]

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
                cookies = None
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
                        task.endpoint,
                        task.response_type,
                        headers=task.headers,
                        cookies=task.cookies,
                        params=task.params,
                        post_data=post_data,
                        json_data=task.json,
                    )
                except Exception as e:
                    raise ScraperFetchError(str(e)) from e

            scraped_data = None
            if self.should_scrape_data(task.task_type):
                # If we want to scrape data, we should either create an
                # ingest_info object or get the one that already exists.
                logging.info(
                    "Scraping data for [%s] and endpoint: [%s]",
                    self.region.region_code,
                    task.endpoint,
                )
                try:
                    scraped_data = self.populate_data(
                        content, task, request.ingest_info or IngestInfo()
                    )
                except Exception as e:
                    raise ScraperPopulateDataError(str(e)) from e

            if self.should_get_more_tasks(task.task_type):
                logging.info(
                    "Getting more tasks for [%s] and endpoint: [%s]",
                    self.region.region_code,
                    task.endpoint,
                )

                # Only send along ingest info if it will not be persisted now.
                ingest_info_to_send = None
                if scraped_data is not None and not scraped_data.persist:
                    ingest_info_to_send = scraped_data.ingest_info

                try:
                    # pylint: disable=assignment-from-no-return
                    next_tasks = self.get_more_tasks(content, task)
                except Exception as e:
                    raise ScraperGetMoreTasksError(str(e)) from e
                for next_task in next_tasks:
                    # Include cookies received from response, if any
                    if cookies:
                        cookies.update(next_task.cookies)
                        next_task = Task.evolve(next_task, cookies=cookies)
                    self.add_task(
                        "_generic_scrape",
                        QueueRequest(
                            scrape_type=request.scrape_type,
                            scraper_start_time=request.scraper_start_time,
                            next_task=next_task,
                            ingest_info=ingest_info_to_send,
                        ),
                    )

            if scraped_data is not None and scraped_data.persist:
                if scraped_data.ingest_info:
                    logging.info(
                        "Logging at most 4 people (were %d):",
                        len(scraped_data.ingest_info.people),
                    )
                    loop_count = min(
                        len(scraped_data.ingest_info.people),
                        constants.MAX_PEOPLE_TO_LOG,
                    )
                    for i in range(loop_count):
                        logging.info("[%s]", str(scraped_data.ingest_info.people[i]))
                    logging.info(
                        "Last seen time of person being set as: [%s]",
                        request.scraper_start_time,
                    )
                    metadata = IngestMetadata(
                        region=self.region.region_code,
                        jurisdiction_id=self.region.jurisdiction_id,
                        ingest_time=request.scraper_start_time,
                        enum_overrides=self.get_enum_overrides(),
                        system_level=SystemLevel.COUNTY,
                        database_key=SQLAlchemyDatabaseKey.for_schema(SchemaType.JAILS),
                    )
                    if self.BATCH_WRITES:
                        logging.info(
                            "Queuing ingest_info ([%d] people) to "
                            "batch_persistence for [%s]",
                            len(scraped_data.ingest_info.people),
                            self.region.region_code,
                        )
                        scrape_key = ScrapeKey(
                            self.region.region_code, request.scrape_type
                        )
                        batch_persistence.write(
                            ingest_info=scraped_data.ingest_info,
                            scrape_key=scrape_key,
                            task=task,
                        )
                    else:
                        logging.info(
                            "Writing ingest_info ([%d] people) to the database"
                            " for [%s]",
                            len(scraped_data.ingest_info.people),
                            self.region.region_code,
                        )
                        persistence.write_ingest_info(
                            serialization.convert_ingest_info_to_proto(
                                scraped_data.ingest_info
                            ),
                            metadata,
                        )
                for sc in scraped_data.single_counts:
                    if not sc.date:
                        scrape_key = ScrapeKey(
                            self.region.region_code, constants.ScrapeType.BACKGROUND
                        )
                        session = sessions.get_current_session(scrape_key)
                        if session:
                            sc = attr.evolve(sc, date=session.start.date())
                    single_count.store_single_count(sc, self.region.jurisdiction_id)
        except Exception as e:
            if self.BATCH_WRITES:
                scrape_key = ScrapeKey(self.region.region_code, request.scrape_type)
                batch_persistence.write_error(
                    error=str(e),
                    trace_id=get_trace_id_from_flask(),
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
    def populate_data(
        self, content, task: Task, ingest_info: IngestInfo
    ) -> ScrapedData:
        """
        Populates the ingest info object from the content and task given

        Args:
            content: An lxml html tree.
            task: Task with parameters passed from the last scrape.
            ingest_info: The IngestInfo object to populate
        """

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
