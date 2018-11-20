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

"""Utilities for managing sessions among ingest processes."""


import logging
import time
from datetime import datetime

from google.appengine.api import memcache
from google.appengine.ext import ndb
from google.appengine.ext.db \
    import Timeout, TransactionFailedError, InternalError

from recidiviz.ingest import constants

class ScrapeSession(ndb.Model):
    """Model to describe a scraping session's current state

    Datastore model for information about a scraping session. Used by the
    scraper to store state during scraping, and handle resumption of work from a
    prior session if continuing work after a pause.

    Attributes:
        start: (datetime) Date/time this session started
        end: (datetime) Date/time when this session finished
        docket_item: (string) The task name of the docket item currently leased
            by this session. Used to delete docket items after completion.
        last_scraped: String in the form "SURNAME, FIRST" of the last
            scraped name in this session
        region: Region code, e.g. us_ny
        scrape_type: (string) 'background' or 'snapshot'
    """
    start = ndb.DateTimeProperty(auto_now_add=True)
    end = ndb.DateTimeProperty()
    docket_item = ndb.StringProperty()
    last_scraped = ndb.StringProperty()
    region = ndb.StringProperty()
    scrape_type = ndb.StringProperty(choices=(constants.BACKGROUND_SCRAPE,
                                              constants.SNAPSHOT_SCRAPE))


class ScrapedRecord(ndb.Model):
    """Model to describe a record that has been scraped, to prevent re-scraping

    Datastore model for a scraped record entry. We use this to track which
    records we've already scraped in the session to avoid accidental duplicate
    scraping during the same session.

    Fields:
        created_on: (datetime) Date/time when this entry was created
        record_id: (string) Dept. ID Number, the ID for a record we've scraped
        region: (string) Region code, e.g. us_ny
    """
    record_id = ndb.StringProperty()
    region = ndb.StringProperty()
    created_on = ndb.DateTimeProperty(auto_now_add=True)


def create_session(scrape_key):
    """Creates a new session to allow starting the given scraper.

    Should be prior to any specific scraping tasks for the region. Ends any open
    sessions for the given scraper and creates a brand new one.

    Args:
        scrape_key: (ScrapeKey) The scraper to setup a new session for
    """
    logging.info("Creating new scrape session for: [%s]", scrape_key)

    end_session(scrape_key)
    new_session = ScrapeSession(scrape_type=scrape_key.scrape_type,
                                region=scrape_key.region_code)

    try:
        new_session.put()
    except (Timeout, TransactionFailedError, InternalError):
        logging.warning("Couldn't create new session entity.")


def end_session(scrape_key):
    """Ends any open session for the given scraper.

    Resets relevant session info after a scraping session is concluded and
    before another one starts. This includes both updating the ScrapeSession
    entity for the old session, and clearing out memcache values.

    Args:
        scrape_key: (ScrapeKey) The scraper to clean up session info for
    """
    fail_counter = scrape_key.region_code + "_next_page_fail_counter"
    memcache.set(key=fail_counter, value=None)

    open_sessions = get_open_sessions(scrape_key.region_code,
                                      scrape_type=scrape_key.scrape_type)

    for session in open_sessions:
        session.end = datetime.now()
        try:
            session.put()
        except (Timeout, TransactionFailedError, InternalError):
            logging.warning("Couldn't set end time on prior sessions.")
            return

    return


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
        current_session.last_scraped = last_scraped
        try:
            current_session.put()
        except (Timeout, TransactionFailedError, InternalError):
            logging.warning("Couldn't persist last scraped name: [%s]",
                            last_scraped)
            return False
    else:
        logging.error("No open sessions found to update.")
        return False

    return True


def add_docket_item_to_current_session(docket_item_name,
                                       scrape_key,
                                       attempt=0):
    """Adds newly leased docket item to scrape session for tracking

    Adds a provided item's key to the current session info, so that the session
    can know which item to remove from the docket when work is done on it.

    Args:
        docket_item_name: (string) A docket queue task name
        scrape_key: (ScrapeKey) The scraper whose session to add to
        attempt: (int) # of attempts so far. After 2, returns False

    Returns:
        True is successful
        False if not
    """
    session = get_current_session(scrape_key)

    if session:
        session.docket_item = docket_item_name

        try:
            session.put()
        except (Timeout, TransactionFailedError, InternalError):
            return False
    else:
        if attempt > 2:
            # Usually means we (manually or via cron) commanded scraper to stop.
            logging.info("No open session to update with docket item.")
            return False

        # Give a bit of space for eventual consistency;
        # our newly-minted session isn't yet coming up in query results.
        time.sleep(2)
        return add_docket_item_to_current_session(docket_item_name,
                                                  scrape_key,
                                                  attempt+1)
    return True


def get_current_session(scrape_key):
    """Retrieves the current, open session for the given scraper.

    Args:
        scrape_key: (ScrapeKey) The scraper whose session to retrieve
    Returns:
        The current, open session for the given scraper if one exists.
        None, otherwise.
    """
    return get_open_sessions(scrape_key.region_code,
                             open_only=True,
                             most_recent_only=True,
                             scrape_type=scrape_key.scrape_type)


def get_recent_sessions(scrape_key):
    """Retrieves recent sessions for the given scraper.

    Returns a list of sessions for the given scraper, regardless of whether they
    are open or not, in descending order by start datetime.

    Args:
        scrape_key: (ScrapeKey) The scraper whose session to retrieve
    Returns:
        A list of recent sessions for the given scraper. Empty if there has
        never been a session for the scraper.
    """
    return get_open_sessions(scrape_key.region_code,
                             open_only=False,
                             most_recent_only=False,
                             scrape_type=scrape_key.scrape_type)


def get_open_sessions(region_code,
                      open_only=True,
                      most_recent_only=False,
                      scrape_type=None):
    """Retrieves open scrape sessions.

    Retrieves some combination of scrape session entities based on the arguments
    provided.

    Args:
        region_code: (string) Region code to fetch sessions for
        open_only: (bool) Only return currently open sessions
        most_recent_only: (bool) Only return the most recent session entity
        scrape_type: (string) Only return sessions of this scrape type

    Returns:
        Result set of ScrapeSession entities,
        or None if no matching sessions found
    """
    session_query = ScrapeSession.query()
    session_query = session_query.filter(
        ScrapeSession.region == region_code).order(-ScrapeSession.start)

    if open_only:
        # This must be an equality operator instead of `is None` to work
        session_query = session_query.filter(ScrapeSession.end == None)  # pylint: disable=singleton-comparison

    if scrape_type:
        session_query = session_query.filter(
            ScrapeSession.scrape_type == scrape_type)

    if most_recent_only:
        session_results = session_query.get()
    else:
        session_results = session_query.fetch()

    return session_results


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
    session_query = ScrapeSession.query()

    session_query = session_query.filter(
        ScrapeSession.region == scrape_key.region_code)

    session_query = session_query.filter(
        ScrapeSession.scrape_type == scrape_key.scrape_type)

    session_query = session_query.filter(
        ScrapeSession.docket_item != None)

    return session_query.fetch()
