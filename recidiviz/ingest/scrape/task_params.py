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

"""Holds parameters for a specific scrape task"""

import datetime
from typing import Any, Dict, List, Optional

import attr
import cattr

from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.models.single_count import SingleCount
from recidiviz.ingest.scrape import constants
from recidiviz.ingest.scrape.errors import ScraperError


def validate_scraped_data(cls: 'ScrapedData', _attribute: attr.Attribute, _value: Any) -> None:
    if cls.persist and not (cls.ingest_info or cls.single_counts):
        raise ScraperError(
            "If persisting, at least one of ingest_info or single_counts "
            "must be set.")


@attr.s(frozen=True)
class ScrapedData:
    """The data returned from a scrape"""

    # The ingest info scraped from the content
    ingest_info: Optional[IngestInfo] = attr.ib(
        default=None, validator=validate_scraped_data)
    # Any single counts we might collect during scraping
    single_counts: List[SingleCount] = attr.ib(
        factory=list, validator=validate_scraped_data)
    # Whether to persist the `ingest_info` now. Set this to `False` if the data
    # for a person or booking is spread across multiple pages and you need to
    # pass this on to the next page instead of persisting it.
    persist: bool = attr.ib(default=True)


@attr.s(frozen=True)
class Task:
    """Describes a scrape task to be performed"""

    # The type of task, this controls which methods are called on the scraper
    task_type: constants.TaskType = attr.ib()
    # The endpoint used when sending the request
    endpoint: str = attr.ib()

    # The expected response type to be used when parsing the response
    response_type: constants.ResponseType = \
        attr.ib(default=constants.ResponseType.HTML)
    # Any http headers to be sent in the request
    headers: Optional[Dict[str, str]] = attr.ib(default=None)
    # Any cookies to be sent with the request. Cookies from any prior requests
    # will be sent automatically along with any supplied manually.
    cookies: Dict[str, str] = attr.ib(factory=dict)
    # Any url parameters to send in the request (forces a GET request)
    params: Optional[Dict[str, Any]] = attr.ib(default=None)
    # Any post data to send in the request (forces a POST request)
    post_data: Optional[Dict[str, Any]] = attr.ib(default=None)
    # Any json data to be sent in the request (forces a POST request)
    json: Optional[Dict[str, Any]] = attr.ib(default=None)

    # Can be used by the child scraper to store extra information they need
    custom: Dict[Any, Any] = attr.ib(factory=dict)

    # Rare: used in cases when content needs to be passed on to the next task
    # TODO(#680): Remove when we support `us_ny` use case
    content: Optional[str] = attr.ib(default=None)

    @staticmethod
    def evolve(next_task: 'Task', **kwds: Any) -> 'Task':
        """Convenience so that other modules don't need to import attr."""
        return attr.evolve(next_task, **kwds)

    def to_serializable(self) -> Any:
        return cattr.unstructure(self)

@attr.s(frozen=True)
class QueueRequest:
    """A wrapper around `Task` with some extra information to run the request

    This is the object that is serialized and put on the queue, it should never
    be instantiated directly by the child scraper.
    """
    # The type of scrape being performed (currently only background scrapes are
    # performed)
    scrape_type: constants.ScrapeType = attr.ib()
    # The time at which this scraper was started, used to associate the entities
    # ingested during throughout the scrape
    scraper_start_time: datetime.datetime = attr.ib()

    # The information need ed to perform the scrape task
    next_task: Task = attr.ib()

    # In the case that a person's information is spread across multiple pages
    # this is used to pass it along to the next page.
    ingest_info: Optional[IngestInfo] = attr.ib(default=None)

    def to_serializable(self) -> Any:
        return cattr.unstructure(self)

    @classmethod
    def from_serializable(cls, serializable: Any) -> 'QueueRequest':
        return cattr.structure(serializable, cls)
