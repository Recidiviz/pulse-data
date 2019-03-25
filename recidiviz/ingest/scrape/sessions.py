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
from typing import List

from google.cloud import datastore

from recidiviz.ingest.scrape import constants
from recidiviz.utils import environment

_ds = None


def ds():
    global _ds
    if not _ds:
        _ds = environment.get_datastore_client()
    return _ds


@environment.test_only
def clear_ds():
    global _ds
    _ds = None


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
    def new(cls, key, start=None, end=None, docket_ack_id=None,
            last_scraped=None, region=None, scrape_type=None):
        entity = datastore.Entity(key)
        entity['start'] = start if start else datetime.now()  # type: datetime
        entity['end'] = end  # type: datetime
        entity['docket_ack_id'] = docket_ack_id  # type: string
        entity['last_scraped'] = last_scraped  # type: string
        entity['region'] = region  # type: string
        if scrape_type:
            if scrape_type not in constants.ScrapeType:
                raise ValueError("Invalid scrape type: {}.".format(scrape_type))
            entity['scrape_type'] = scrape_type.value
        return cls(entity)

    def __init__(self, entity):
        self._entity = entity

    def update(self, keys_values):
        self._entity.update(keys_values)

    def __getattr__(self, attr):
        if attr in self._entity:
            if attr == 'scrape_type':
                return constants.ScrapeType(self._entity[attr])
            return self._entity[attr]
        raise AttributeError("%r object has no attribute %r" %
                             (self.__class__.__name__, attr))

    def to_entity(self):
        return self._entity


SCRAPE_SESSION_KIND = ScrapeSession.__name__


def create_session(scrape_key):
    """Creates a new session to allow starting the given scraper.

    Should be prior to any specific scraping tasks for the region. Ends any open
    sessions for the given scraper and creates a brand new one.

    Args:
        scrape_key: (ScrapeKey) The scraper to setup a new session for
    """
    logging.info("Creating new scrape session for: [%s]", scrape_key)

    end_session(scrape_key)
    new_session = ScrapeSession.new(ds().key(SCRAPE_SESSION_KIND),
                                    scrape_type=scrape_key.scrape_type,
                                    region=scrape_key.region_code)

    try:
        ds().put(new_session.to_entity())
    except Exception as e:
        logging.warning("Couldn't create new session entity:\n%s", e)


def end_session(scrape_key) -> List[ScrapeSession]:
    """Ends any open session for the given scraper.

    Resets relevant session info after a scraping session is concluded and
    before another one starts, including updating the ScrapeSession entity for
    the old session.

    Args:
        scrape_key: (ScrapeKey) The scraper to clean up session info for

    Returns: list of the scrape sessions which were closed
    """
    open_sessions = get_sessions(scrape_key.region_code,
                                 include_closed=False,
                                 scrape_type=scrape_key.scrape_type)

    closed_sessions = []
    for session in open_sessions:
        session.update({'end':  datetime.now()})
        try:
            ds().put(session.to_entity())
        except Exception as e:
            logging.warning("Couldn't set end time on prior sessions:\n%s", e)
        closed_sessions.append(session)

    return closed_sessions


def update_session(last_scraped, scrape_key):
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

    if current_session:
        current_session.update({'last_scraped': last_scraped})
        try:
            ds().put(current_session.to_entity())
        except Exception as e:
            logging.warning("Couldn't persist last scraped name: [%s]\n%s",
                            last_scraped, e)
            return False
    else:
        logging.error("No open sessions found to update.")
        return False

    return True


def add_docket_item_to_current_session(docket_ack_id,
                                       scrape_key,
                                       attempt=0):
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
    return add_docket_item_to_current_session(
        docket_ack_id, scrape_key, attempt+1)


def add_docket_item_to_session(docket_ack_id, session):
    """Adds docket item to the given session

    Args:
        docket_ack_id: (string) Id used to ack the docket message
        session: (ScrapeSession) The session to add to

    Returns:
        True if successful otherwise False
    """
    session.update({'docket_ack_id': docket_ack_id})

    try:
        ds().put(session.to_entity())
    except Exception as e:
        logging.warning("%s", e)
        return False
    return True


def remove_docket_item_from_session(session):
    """Removes the docket item from the session.

    Args:
        session: (ScrapeSession) The session to remove from

    Returns:
        Id used to ack the docket message
    """
    docket_ack_id = session.docket_ack_id
    session.update({'docket_ack_id': None})
    try:
        ds().put(session.to_entity())
    except Exception as e:
        logging.error("Failed to persist session [%s] after deleting "
                      "docket item [%s]:\n%s", session.key, docket_ack_id, e)
    return docket_ack_id


def get_current_session(scrape_key):
    """Retrieves the current, open session for the given scraper.

    Args:
        scrape_key: (ScrapeKey) The scraper whose session to retrieve
    Returns:
        The current, open session for the given scraper if one exists.
        None, otherwise.
    """
    return next(get_sessions(scrape_key.region_code,
                             include_closed=False,
                             most_recent_only=True,
                             scrape_type=scrape_key.scrape_type), None)


def get_recent_sessions(scrape_key):
    """Retrieves recent sessions for the given scraper.

    Returns a list of sessions for the given scraper, regardless of whether they
    are open or not, in descending order by start datetime.

    Args:
        scrape_key: (ScrapeKey) The scraper whose session to retrieve
    Returns:
        A generator of recent sessions for the given scraper. Empty if there has
        never been a session for the scraper.
    """
    return get_sessions(scrape_key.region_code,
                        most_recent_only=False,
                        scrape_type=scrape_key.scrape_type)


def get_most_recent_completed_session(region_code, scrape_type=None):
    return next(get_sessions(region_code,
                             include_open=False,
                             most_recent_only=True,
                             scrape_type=scrape_type), None)


def get_sessions(region_code, include_open=True, include_closed=True,
                 most_recent_only=False, scrape_type=None):
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
    session_query.add_filter('region', '=', region_code)
    session_query.order = ['-start']

    if not include_closed:
        if include_open:
            session_query.add_filter('end', '=', None)
        else:
            return (_ for _ in ())

    if scrape_type:
        session_query.add_filter('scrape_type', '=', scrape_type.value)

    limit = None
    # If `include_open` is not set we have to filter after fetching the results
    # so we cannot limit to the first result.
    if most_recent_only and include_open:
        limit = 1

    results = session_query.fetch(limit=limit)

    # Datastore doesn't allow an inequality filter on `end` because we are
    # sorting on `start`, so we have to do the filter after we get the results.
    if not include_open:
        results = (result for result in results if result.get('end'))
        if most_recent_only:
            first_result = next(results, None)
            results = [first_result] if first_result else []

    return _sessions_from_entities(results)


def get_sessions_with_leased_docket_items(scrape_key):
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
    session_query.add_filter('region', '=', scrape_key.region_code)
    session_query.add_filter('scrape_type', '=', scrape_key.scrape_type.value)
    session_query.add_filter('docket_ack_id', '>', None)

    return _sessions_from_entities(session_query.fetch())


def _sessions_from_entities(entity_generator):
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
        entity['record_id'] = record_id  # type: string
        entity['region'] = region  # type: string
        entity['created_on']: datetime = \
            created_on if created_on else datetime.now()

        return cls(entity)

    def __init__(self, entity):
        self._entity = entity

    def update(self, keys_values):
        self._entity.update(keys_values)

    def __getattr__(self, attr):
        if attr in self._entity:
            return self._entity[attr]
        raise AttributeError("%r object has no attribute %r" %
                             (self.__class__.__name__, attr))

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
    new_record = ScrapedRecord.new(ds().key(SCRAPED_RECORD_KIND),
                                   *args, **kwds)

    try:
        ds().put(new_record.to_entity())
    except Exception as e:
        logging.warning("Couldn't persist ScrapedRecord entry, "
                        "record_id: %s\n%s", new_record['record_id'], e)


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
    record_query.add_filter('region', '=', region_code)
    record_query.add_filter('record_id', '==', record_id)
    record_query.add_filter('created_on', '>', start)
    return bool(next(record_query.fetch(), None))
