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

"""Utilities for managing the tracking of work amongst dockets and sessions."""


import json
import logging

from recidiviz.ingest.scrape import sessions, docket


def iterate_docket_item(scrape_key, return_immediately=False):
    """Leases new docket item, updates current session, returns item contents

    Pulls an arbitrary new item from the docket type provided, adds it to the
    current session info, and returns the payload of the docket item.

    This payload should be an entity fit to scrape, or information suitable for
    retrieving an entity fit to scrape, depending on scrape type.

    Args:
        scrape_key: (ScrapeKey) The scraper to retrieve a docket item for
        return_immediately: (bool) Whether to return immediately or to wait for
            a bounded period of time for a message to enter the docket.

    Returns:
        The payload of the next docket item, if successfully retrieved and added
        to the current session for this region and scrape type. If not retrieved
        or not successfully added to the session, returns None.
    """

    docket_item = docket.get_new_docket_item(
        scrape_key, return_immediately=return_immediately
    )

    if not docket_item:
        logging.info("No items in docket for [%s]. Ending scrape.", scrape_key)
        return None

    item_content = json.loads(docket_item.message.data.decode())
    item_added = sessions.add_docket_item_to_current_session(
        docket_item.ack_id, scrape_key
    )
    if not item_added:
        logging.error(
            "Failed to update session for scraper [%s] " "with docket item [%s].",
            scrape_key,
            str(item_content),
        )
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
    session = sessions.get_current_session(scrape_key)

    if not session:
        logging.warning("No open sessions found to remove docket item.")
        return

    docket_ack_id = sessions.remove_docket_item_from_session(session)

    if docket_ack_id:
        docket.ack_docket_item(scrape_key, docket_ack_id)


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
        sessions.remove_docket_item_from_session(session)
