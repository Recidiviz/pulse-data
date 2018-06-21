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

"""Utilities for managing the tracking of work amongst dockets and sessions."""


import json
import logging

from google.appengine.ext.db \
    import Timeout, TransactionFailedError, InternalError
from ingest import docket
from ingest import sessions


def iterate_docket_item(scrape_key, back_off=5):
    """Leases new docket item, updates current session, returns item contents

    Pulls an arbitrary new item from the docket type provided, adds it to the
    current session info, and returns the payload of the docket item.

    This payload should be an entity fit to scrape, or information suitable for
    retrieving an entity fit to scrape, depending on scrape type.

    Args:
        scrape_key: (ScrapeKey) The scraper to retrieve a docket item for
        back_off: (int) # of seconds to wait between attempts.

    Returns:
        The payload of the next docket item, if successfully retrieved and added
        to the current session for this region and scrape type. If not retrieved
        or not successfully added to the session, returns None.
    """

    docket_item = docket.get_new_docket_item(scrape_key, back_off=back_off)

    if not docket_item:
        logging.info("No items in docket for %s. Ending scrape.", scrape_key)
        return None

    item_content = json.loads(docket_item.payload)

    item_added = sessions.add_docket_item_to_current_session(docket_item.name,
                                                             scrape_key)
    if not item_added:
        logging.error("Failed to update session for scraper %s "
                      "with docket item %s.", scrape_key, str(item_content))
        return None

    return item_content


def remove_item_from_session_and_docket(scrape_key):
    """Deletes currently leased docket item, removes from scrape session

    Fetches the current session, determines which item from the docket is
    currently being worked on, then deletes that item and resets the session
    item to blank.

    Args:
        scrape_key: (ScrapeKey) The scraper to remote currently leased item for

    Returns:
        N/A
    """
    # Get the current session, remove and delete its docket item
    session = sessions.get_open_sessions(scrape_key.region_code,
                                         open_only=True,
                                         most_recent_only=True,
                                         scrape_type=scrape_key.scrape_type)

    if not session:
        logging.warning("No open sessions found to remove docket item.")
        return

    docket_item_name = session.docket_item
    if docket_item_name:
        item_deleted = docket.delete_docket_item(docket_item_name)

        if item_deleted:
            session.docket_item = None
            try:
                session.put()
            except (Timeout, TransactionFailedError, InternalError) as e:
                logging.error("Failed to persist session [%s] after deleting "
                              "docket item [%s]:\n%s",
                              session.key, docket_item_name, e)

    return


def purge_docket_and_session(scrape_key):
    """Purges the docket and sessions for the given region and scrape type.

    First, deletes all unleased tasks from the docket for provided region/type.

    Second, checks sessions for still-leased tasks, then removes them. That is,
    this pulls any sessions from the relevant region / scrape type that are
    still associated with leased tasks, and (if found) deletes the task
    and removes the association with the session.

    Args:
        scrape_key: (ScrapeKey) The scraper whose sessions to purge and whose
            items to purge from the docket

    Returns:
        N/A
    """
    docket.purge_query_docket(scrape_key)

    session_results = sessions.get_sessions_with_leased_docket_items(scrape_key)

    for session in session_results:
        docket_item_name = session.docket_item

        try:
            docket.delete_docket_item(docket_item_name)
            session.docket_item = None
            session.put()

        except Exception as e:
            # Possible the docket item was deleted too long ago for the
            # task queue to recognize.
            logging.warning("Failed to remove docket item (%s) from session "
                            "(%s):\n%s", docket_item_name, session.key, e)

    return
