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

from google.appengine.ext.db \
    import Timeout, TransactionFailedError, InternalError
from ingest.models.scrape_session import ScrapeSession


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
    session = get_open_sessions(scrape_key.region_code,
                                open_only=True,
                                most_recent_only=True,
                                scrape_type=scrape_key.scrape_type)

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
