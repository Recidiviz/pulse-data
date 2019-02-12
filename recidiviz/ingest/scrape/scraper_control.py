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

"""Requests handlers for queue control requests, i.e. starting, stopping, and
resuming ingest processes, such as scrapers.

Attributes:
    SCRAPE_TYPES: (list(string)) the list of acceptable scrape types
"""

from http import HTTPStatus
import logging
import threading
import time

from flask import Blueprint, request

from recidiviz.ingest.scrape import sessions, ingest_utils, tracker, docket
from recidiviz.ingest.models.scrape_key import ScrapeKey
from recidiviz.utils import regions
from recidiviz.utils.auth import authenticate_request
from recidiviz.utils.params import get_value, get_values

scraper_control = Blueprint('scraper_control', __name__)


@scraper_control.route('/start')
@authenticate_request
def scraper_start():
    """Request handler to start one or several running scrapers

    Kicks off new scrape session for each region and scrape type in request

    Example query:
        /scraper_control/start?region=us_ny&scrape_type=background

    URL parameters:
        region: (string) Region to take action for, or 'all'
        scrape_type: (string) Type of scrape to take action for, or 'all'
        timezone: (string) The timezone to scrape.
        surname: (string, optional) Name to start scrape at. Required if
            given_names provided
        given_names: (string, optional) Name to start scrape at

    Args:
        N/A

    Returns:
        N/A
    """
    def _start_scraper(region, scrape_type):
        scrape_key = ScrapeKey(region, scrape_type)
        logging.info("Starting new scraper for: %s", scrape_key)

        scraper = regions.get_region(region).get_scraper()

        sessions.create_session(scrape_key)

        # Help avoid race condition with new session info
        # vs updating that w/first task.
        time.sleep(1)

        # Clear prior query docket for this scrape type and start adding new
        # items in a background thread. In the case that there is a large
        # names list, loading it can take some time. Loading it in the
        # background allows us to start the scraper before it is fully
        # loaded.
        tracker.purge_docket_and_session(scrape_key)
        load_docket_thread = threading.Thread(
            target=docket.load_target_list,
            args=(scrape_key, given_names, surname))
        load_docket_thread.start()

        # Start scraper, if the docket is empty this will wait for a bounded
        # period of time for an item to be published (~90 seconds).
        logging.info("Starting %s/%s scrape...", region, scrape_type)
        scraper.start_scrape(scrape_type)

        # Wait for the docket to be loaded
        load_docket_thread.join()

    timezone = request.args.get("timezone")
    scrape_regions = ingest_utils.validate_regions(
        get_values("region", request.args), timezone=timezone)
    scrape_types = ingest_utils.validate_scrape_types(get_values("scrape_type",
                                                                 request.args))

    if not scrape_regions or not scrape_types:
        return ('Missing or invalid parameters, see service logs.',
                HTTPStatus.BAD_REQUEST)

    given_names = get_value("given_names", request.args, "")
    surname = get_value("surname", request.args, "")

    start_scraper_threads = []
    for region_name in scrape_regions:
        for scraper_type in scrape_types:
            # If timezone wasn't provided, return all but if it was, only
            # return regions that match the timezone.
            start_scraper_thread = threading.Thread(
                target=_start_scraper,
                args=(region_name, scraper_type)
            )
            start_scraper_thread.start()
            start_scraper_threads.append(start_scraper_thread)

    # Once everything is started, lets wait for everything to end before
    # returning.
    for start_scraper_thread in start_scraper_threads:
        start_scraper_thread.join()

    return ('', HTTPStatus.OK)


@scraper_control.route('/stop')
@authenticate_request
def scraper_stop():
    """Request handler to stop one or several running scrapers.

    Note: Stopping any scrape type for a region involves purging the
    scraping task queue for that region, necessarily killing any other
    in-progress scrape types. Untargeted scrapes killed by this request
    handler will be noted and resumed a moment or two later.

    Unlike the other Scraper action methods, stop_scrape doesn't call
    individually for each scrape type. That could create a race condition,
    as each call noticed the other scrape type was running at the same
    time, kicked off a resume effort with a delay, and then our second
    call came to kill the other type and missed the (delayed / not yet
    in taskqueue) call - effectively not stopping the scrape.

    Instead, we send the full list of scrape_types to stop, and
    Scraper.stop_scrape is responsible for fan-out.

    Example query:
        /scraper_control/stop?region=us_ny&scrape_type=background

    URL parameters:
        region: (string) Region to take action for, or 'all'
        scrape_type: (string) Type of scrape to take action for, or 'all'

    Args:
        N/A

    Returns:
        N/A
    """
    timezone = request.args.get("timezone")
    scrape_regions = ingest_utils.validate_regions(
        get_values("region", request.args), timezone=timezone)
    scrape_types = ingest_utils.validate_scrape_types(get_values("scrape_type",
                                                                 request.args))

    def _stop_scraper(region):
        for scrape_type in scrape_types:
            sessions.end_session(ScrapeKey(region, scrape_type))

        region_scraper = regions.get_region(region).get_scraper()
        region_scraper.stop_scrape(scrape_types)

    if not scrape_regions or not scrape_types:
        return ('Missing or invalid parameters, see service logs.',
                HTTPStatus.BAD_REQUEST)

    stop_scraper_threads = []
    for region_name in scrape_regions:
        logging.info("Stopping %s scrapes for %s.", scrape_types, region_name)

        stop_scraper_thread = threading.Thread(
            target=_stop_scraper,
            args=[region_name]
        )
        stop_scraper_thread.start()
        stop_scraper_threads.append(stop_scraper_thread)

    for stop_scraper_thread in stop_scraper_threads:
        stop_scraper_thread.join()


    return ('', HTTPStatus.OK)


@scraper_control.route('/resume')
@authenticate_request
def scraper_resume():
    """Request handler to resume one or several stopped scrapers

    Resumes scraping for each region and scrape type in request.

    Example query:
        /scraper_control/resume?region=us_ny&scrape_type=background

    URL parameters:
        region: (string) Region to take action for, or 'all'
        scrape_type: (string) Type of scrape to take action for, or 'all'

    Args:
        N/A

    Returns:
        N/A
    """
    scrape_regions = ingest_utils.validate_regions(get_values("region",
                                                              request.args))
    scrape_types = ingest_utils.validate_scrape_types(get_values("scrape_type",
                                                                 request.args))

    if not scrape_regions or not scrape_types:
        return ('Missing or invalid parameters, see service logs.',
                HTTPStatus.BAD_REQUEST)

    for region in scrape_regions:

        for scrape_type in scrape_types:
            logging.info("Resuming %s scrape for %s.", scrape_type, region)

            sessions.create_session(ScrapeKey(region, scrape_type))

            # Help avoid race condition with new session info
            # vs updating that w/first task.
            time.sleep(5)

            scraper = regions.get_region(region).get_scraper()
            scraper.resume_scrape(scrape_type)

    return ('', HTTPStatus.OK)
