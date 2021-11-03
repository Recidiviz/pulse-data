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

"""Utilities for managing sessions among ingest processes."""

import logging
import time
from datetime import datetime
from typing import Iterator, List, Optional

from google.cloud import datastore

from recidiviz.common.common_utils import retry_grpc
from recidiviz.ingest.models.scrape_key import ScrapeKey
from recidiviz.ingest.models.scraper_success import ScraperSuccess
from recidiviz.ingest.scrape import constants, scrape_phase
from recidiviz.persistence.scraper_success import store_scraper_success
from recidiviz.utils import environment, regions

_ds = None


def ds() -> datastore.Client:
    global _ds
    if not _ds:
        _ds = environment.get_datastore_client()
    return _ds


@environment.test_only
def clear_ds() -> None:
    global _ds
    _ds = None


NUM_GRPC_RETRIES = 2


class ScrapeSession:
    """Model to describe a scraping session's current state

    Datastore model for information about a scraping session. Used by the
    scraper to store state during scraping, and handle resumption of work from a
    prior session if continuing work after a pause.

    Attributes:
        start: (datetime) Date/time this session started
        end: (datetime) Date/time when this session finished
        docket_ack_id: (string) Ack id of the docket message currently leased by
            this session. Used to delete docket items after completion.
        last_scraped: (string) String in the form "SURNAME, FIRST" of the last
            scraped name in this session
        region: (string) Region code, e.g. us_ny
        scrape_type: (string) 'background' or 'snapshot'
    """

    @classmethod
    def from_entity(cls, entity):
        return cls(entity)

    @classmethod
    def new(
        cls,
        key,
        start=None,
        end=None,
        docket_ack_id=None,
        last_scraped=None,
        region=None,
        scrape_type=None,
        phase=None,
    ):
        session = cls(datastore.Entity(key))
        session.start = start if start else datetime.now()  # type: datetime
        session.end = end  # type: datetime
        session.docket_ack_id = docket_ack_id  # type: string
        session.last_scraped = last_scraped  # type: string
        session.region = region  # type: string
        session.scrape_type = scrape_type  # type: constants.ScrapeType
        session.phase = phase  # type: scrape_phase.ScrapePhase
        return session

    def __init__(self, entity):
        self.__dict__["_entity"] = entity

    def __setattr__(self, attr, value):
        if attr == "scrape_type":
            if value not in constants.ScrapeType:
                raise ValueError(f"Invalid scrape type: {value}.")
            value = value.value
        elif attr == "phase":
            if value not in scrape_phase.ScrapePhase:
                raise ValueError(f"Invalid phase: {value}.")
            value = value.value
        self.__dict__["_entity"][attr] = value

    def __getattr__(self, attr):
        if attr in self.__dict__["_entity"]:
            if attr == "scrape_type":
                return constants.ScrapeType(self._entity[attr])
            if attr == "phase":
                return scrape_phase.ScrapePhase(self._entity[attr])
            return self.__dict__["_entity"][attr]
        raise AttributeError(
            f"{self.__class__.__name__!r} object has no attribute {attr!r}"
        )

    def to_entity(self):
        return self._entity


SCRAPE_SESSION_KIND = ScrapeSession.__name__


def create_session(scrape_key: ScrapeKey) -> ScrapeSession:
    """Creates a new session to allow starting the given scraper.

    Should be prior to any specific scraping tasks for the region. Ends any open
    sessions for the given scraper and creates a brand new one.

    Args:
        scrape_key: (ScrapeKey) The scraper to setup a new session for
    """
    logging.info("Creating new scrape session for: [%s]", scrape_key)

    # TODO(#1598): We already skip starting a session if a session already
    # exists so we should be able to remove this. We could move the skip to here
    close_session(scrape_key)
    new_session = ScrapeSession.new(
        ds().key(SCRAPE_SESSION_KIND),
        scrape_type=scrape_key.scrape_type,
        region=scrape_key.region_code,
        phase=scrape_phase.ScrapePhase.START,
    )

    retry_grpc(NUM_GRPC_RETRIES, ds().put, new_session.to_entity())
    return new_session


def close_session(scrape_key: ScrapeKey) -> List[ScrapeSession]:
    """Closes any open session for the given scraper.

    Resets relevant session info after a scraping session is concluded and
    before another one starts, including updating the ScrapeSession entity for
    the old session.

    Args:
        scrape_key: (ScrapeKey) The scraper to clean up session info for

    Returns: list of the scrape sessions which were closed
    """
    # TODO(#1598): Much of our code assumes there is only one open session (i.e.
    # all sessions go to the same batch persistence queue). Other code
    # specifically supports there being multiple open sessions, we should
    # reconcile this.
    open_sessions = get_sessions(
        scrape_key.region_code, include_closed=False, scrape_type=scrape_key.scrape_type
    )

    closed_sessions = []
    for session in open_sessions:
        session.end = datetime.now()
        retry_grpc(NUM_GRPC_RETRIES, ds().put, session.to_entity())
        closed_sessions.append(session)

    return closed_sessions


def update_session(last_scraped: str, scrape_key: ScrapeKey) -> bool:
    """Updates ScrapeSession entity with most recently successfully scraped
    person.

    Updates the most recent open session entity with the person. This allows us
    to pause and resume long-lived scrapes without losing our place.

    Args:
        last_scraped: (String) Name of the last successfully scraped person
        scrape_key: (ScrapeKey) The scraper to update session info for

    Returns:
        True if successful
        False if not
    """
    current_session = get_current_session(scrape_key)

    if not current_session:
        logging.error("No open sessions found to update.")
        return False

    current_session.last_scraped = last_scraped
    retry_grpc(NUM_GRPC_RETRIES, ds().put, current_session.to_entity())
    return True


def update_phase(session: ScrapeSession, phase: scrape_phase.ScrapePhase):
    """Updates the phase of the session to the given phase."""
    #  TODO(#1665): remove once dangling PERSIST session investigation
    #   is complete.
    logging.info("Updating phase from %s to %s", session.phase, phase)

    previous_phase = session.phase

    session.phase = phase
    retry_grpc(NUM_GRPC_RETRIES, ds().put, session.to_entity())

    if (
        previous_phase == scrape_phase.ScrapePhase.RELEASE
        and phase == scrape_phase.ScrapePhase.DONE
    ):
        jid = regions.get_region(session.region).jurisdiction_id
        success_date = session.start.date()
        store_scraper_success(ScraperSuccess(date=success_date), jid)


def add_docket_item_to_current_session(
    docket_ack_id: str, scrape_key: ScrapeKey, attempt: int = 0
) -> bool:
    """Adds newly leased docket item to scrape session for tracking

    Adds a provided item's id to the current session info, so that the session
    can know which item to remove from the docket when work is done on it.

    Args:
        docket_ack_id: (string) Id used to ack the docket message
        scrape_key: (ScrapeKey) The scraper whose session to add to
        attempt: (int) # of attempts so far. After 2, returns False

    Returns:
        True is successful
        False if not
    """
    session = get_current_session(scrape_key)

    if session:
        return add_docket_item_to_session(docket_ack_id, session)

    if attempt > 2:
        # Usually means we (manually or via cron) commanded scraper to stop.
        logging.info("No open session to update with docket item.")
        return False

    # Give a bit of space for eventual consistency;
    # our newly-minted session isn't yet coming up in query results.
    time.sleep(2)
    return add_docket_item_to_current_session(docket_ack_id, scrape_key, attempt + 1)


def add_docket_item_to_session(docket_ack_id: str, session: ScrapeSession) -> bool:
    """Adds docket item to the given session

    Args:
        docket_ack_id: (string) Id used to ack the docket message
        session: (ScrapeSession) The session to add to

    Returns:
        True if successful otherwise False
    """
    session.docket_ack_id = docket_ack_id
    retry_grpc(NUM_GRPC_RETRIES, ds().put, session.to_entity())
    return True


def remove_docket_item_from_session(session: ScrapeSession) -> str:
    """Removes the docket item from the session.

    Args:
        session: (ScrapeSession) The session to remove from

    Returns:
        Id used to ack the docket message
    """
    docket_ack_id = session.docket_ack_id
    session.docket_ack_id = None
    retry_grpc(NUM_GRPC_RETRIES, ds().put, session.to_entity())
    return docket_ack_id


def get_current_session(scrape_key: ScrapeKey) -> Optional[ScrapeSession]:
    """Retrieves the current, open session for the given scraper.

    Args:
        scrape_key: (ScrapeKey) The scraper whose session to retrieve
    Returns:
        The current, open session for the given scraper if one exists.
        None, otherwise.
    """
    return next(
        get_sessions(
            scrape_key.region_code,
            include_closed=False,
            most_recent_only=True,
            scrape_type=scrape_key.scrape_type,
        ),
        None,
    )


def get_recent_sessions(scrape_key: ScrapeKey) -> Iterator[ScrapeSession]:
    """Retrieves recent sessions for the given scraper.

    Returns a list of sessions for the given scraper, regardless of whether they
    are open or not, in descending order by start datetime.

    Args:
        scrape_key: (ScrapeKey) The scraper whose session to retrieve
    Returns:
        A generator of recent sessions for the given scraper. Empty if there has
        never been a session for the scraper.
    """
    return get_sessions(
        scrape_key.region_code,
        most_recent_only=False,
        scrape_type=scrape_key.scrape_type,
    )


def get_most_recent_completed_session(
    region_code: str, scrape_type: constants.ScrapeType = None
) -> Optional[ScrapeSession]:
    return next(
        get_sessions(
            region_code,
            include_open=False,
            most_recent_only=True,
            scrape_type=scrape_type,
        ),
        None,
    )


def get_sessions(
    region_code: str,
    include_open: bool = True,
    include_closed: bool = True,
    most_recent_only: bool = False,
    scrape_type: constants.ScrapeType = None,
) -> Iterator[ScrapeSession]:
    """Retrieves scrape sessions.

    Retrieves some combination of scrape session entities based on the arguments
    provided.

    If both `include_open` and `include_closed` are False the returned generator
    will be empty.

    Args:
        region_code: (string) Region code to fetch sessions for
        include_open: (bool) Return open sessions
        include_closed: (bool) Return closed sessions
        most_recent_only: (bool) Only return the most recent session entity
        scrape_type: (string) Only return sessions of this scrape type

    Returns:
        A generator of ScrapeSessions
    """
    session_query = ds().query(kind=SCRAPE_SESSION_KIND)
    session_query.add_filter("region", "=", region_code)
    session_query.order = ["-start"]

    if not include_closed:
        if include_open:
            session_query.add_filter("end", "=", None)
        else:
            return (_ for _ in ())

    if scrape_type:
        session_query.add_filter("scrape_type", "=", scrape_type.value)

    limit = None
    # If `include_open` is not set we have to filter after fetching the results
    # so we cannot limit to the first result.
    if most_recent_only and include_open:
        limit = 1

    results = retry_grpc(NUM_GRPC_RETRIES, session_query.fetch, limit=limit)

    # Datastore doesn't allow an inequality filter on `end` because we are
    # sorting on `start`, so we have to do the filter after we get the results.
    if not include_open:
        results = (result for result in results if result.get("end"))
        if most_recent_only:
            first_result = next(results, None)
            results = [first_result] if first_result else []

    return _sessions_from_entities(results)


def get_sessions_with_leased_docket_items(
    scrape_key: ScrapeKey,
) -> Iterator[ScrapeSession]:
    """Retrieves scrape sessions that still have leased docket items attached.

    Retrieves scrape session entities that have the given region and scrape type
    and whose attached docket item is not None.

    Args:
        scrape_key: (ScrapeKey) The scraper to fetch sessions for

    Returns:
        Result set of ScrapeSession entities,
        or None if no matching sessions found
    """
    session_query = ds().query(kind=SCRAPE_SESSION_KIND)
    session_query.add_filter("region", "=", scrape_key.region_code)
    session_query.add_filter("scrape_type", "=", scrape_key.scrape_type.value)
    session_query.add_filter("docket_ack_id", ">", None)

    return _sessions_from_entities(retry_grpc(NUM_GRPC_RETRIES, session_query.fetch))


def _sessions_from_entities(
    entity_generator: Iterator[datastore.Entity],
) -> Iterator[ScrapeSession]:
    return (ScrapeSession.from_entity(entity) for entity in entity_generator)


class ScrapedRecord:
    """Model to describe a record that has been scraped, to prevent re-scraping

    Datastore model for a scraped record entry. We use this to track which
    records we've already scraped in the session to avoid accidental duplicate
    scraping during the same session.

    Fields:
        created_on: (datetime) Date/time when this entry was created
        record_id: (string) Dept. ID Number, the ID for a record we've scraped
        region: (string) Region code, e.g. us_ny
    """

    @classmethod
    def from_entity(cls, entity):
        return cls(entity)

    @classmethod
    def new(cls, key, record_id=None, region=None, created_on=None):
        entity = datastore.Entity(key)
        entity["record_id"] = record_id  # type: string
        entity["region"] = region  # type: string
        entity["created_on"]: datetime = created_on if created_on else datetime.now()

        return cls(entity)

    def __init__(self, entity):
        self._entity = entity

    def update(self, keys_values):
        self._entity.update(keys_values)

    def __getattr__(self, attr):
        if attr in self._entity:
            return self._entity[attr]
        raise AttributeError(
            f"{self.__class__.__name__!r} object has no attribute {attr!r}"
        )

    def to_entity(self):
        return self._entity


SCRAPED_RECORD_KIND = ScrapedRecord.__name__


def write_scraped_record(*args, **kwds):
    """Passes any arguments to create a scraped record and writes it to
    datastore

    Should be prior to any specific scraping tasks for the region. Ends any open
    sessions for the given scraper and creates a brand new one.

    Args:
        scrape_key: (ScrapeKey) The scraper to setup a new session for
    """
    new_record = ScrapedRecord.new(ds().key(SCRAPED_RECORD_KIND), *args, **kwds)

    try:
        retry_grpc(NUM_GRPC_RETRIES, ds().put, new_record.to_entity())
    except Exception as e:
        logging.warning(
            "Couldn't persist ScrapedRecord entry, " "record_id: %s\n%s",
            new_record["record_id"],
            e,
        )


def already_scraped_record(region_code, record_id, start):
    """Checks datastore to see if a matching record already exists.

    Args:
        region_code: Code for the region
        record_id: Id for the record
        start: Time to check if the record is newer than

    Returns:
        True if a record exists otherwise False
    """
    record_query = ds().query(kind=SCRAPED_RECORD_KIND)
    record_query.add_filter("region", "=", region_code)
    record_query.add_filter("record_id", "==", record_id)
    record_query.add_filter("created_on", ">", start)
    return bool(next(retry_grpc(NUM_GRPC_RETRIES, record_query.fetch), None))
