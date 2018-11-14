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


import httplib
import logging
import time

from flask import Flask, request
from google.appengine.ext import deferred

from recidiviz.ingest import docket, sessions, tracker
from recidiviz.ingest.models.scrape_key import ScrapeKey
from recidiviz.utils import environment, regions
from recidiviz.utils.auth import authenticate_request
from recidiviz.utils.params import get_value, get_values

SCRAPE_TYPES = ["background", "snapshot"]

app = Flask(__name__)

@app.route('/scraper/start')
@authenticate_request
def scraper_start():
    """Request handler to start one or several running scrapers

    Kicks off new scrape session for each region and scrape type in request

    Example query:
        /scraper/start?region=us_ny&scrape_type=background

    URL parameters:
        region: (string) Region to take action for, or 'all'
        scrape_type: (string) Type of scrape to take action for, or 'all'
        surname: (string, optional) Name to start scrape at. Required if
            given_names provided
        given_names: (string, optional) Name to start scrape at

    Args:
        N/A

    Returns:
        N/A
    """
    scrape_regions = validate_regions(get_values("region", request.args))
    scrape_types = validate_scrape_types(get_values("scrape_type",
                                                    request.args))

    if not scrape_regions or not scrape_types:
        return ('Missing or invalid parameters, see service logs.',
                httplib.BAD_REQUEST)

    given_names = get_value("given_names", request.args, "")
    surname = get_value("surname", request.args, "")

    for region in scrape_regions:

        for scrape_type in scrape_types:
            scrape_key = ScrapeKey(region, scrape_type)
            logging.info("Starting new scraper for: %s", scrape_key)

            scraper = regions.get_scraper_from_cache(region)

            sessions.create_session(scrape_key)

            # Help avoid race condition with new session info
            # vs updating that w/first task.
            time.sleep(5)

            # Clear prior query docket for this scrape type and add new
            # items
            tracker.purge_docket_and_session(scrape_key)
            docket.load_target_list(scrape_key, given_names, surname)

            # Start scraper, but give the target list loader a headstart
            timer = 30 if not environment.in_prod() else 300
            logging.info("Starting %s/%s scrape in %d seconds..."
                         % (region, scrape_type, timer))
            deferred.defer(scraper.start_scrape, scrape_type, _countdown=timer)

    return ('', httplib.OK)


@app.route('/scraper/stop')
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
        /scraper/stop?region=us_ny&scrape_type=background

    URL parameters:
        region: (string) Region to take action for, or 'all'
        scrape_type: (string) Type of scrape to take action for, or 'all'

    Args:
        N/A

    Returns:
        N/A
    """
    scrape_regions = validate_regions(get_values("region", request.args))
    scrape_types = validate_scrape_types(get_values("scrape_type",
                                                    request.args))

    if not scrape_regions or not scrape_types:
        return ('Missing or invalid parameters, see service logs.',
                httplib.BAD_REQUEST)

    for region in scrape_regions:
        logging.info("Stopping %s scrapes for %s." % (scrape_types, region))

        for scrape_type in scrape_types:
            sessions.end_session(ScrapeKey(region, scrape_type))

        scraper = regions.get_scraper_from_cache(region)
        scraper.stop_scrape(scrape_types)

    return ('', httplib.OK)


@app.route('/scraper/resume')
@authenticate_request
def scraper_resume():
    """Request handler to resume one or several stopped scrapers

    Resumes scraping for each region and scrape type in request.

    Example query:
        /scraper/resume?region=us_ny&scrape_type=background

    URL parameters:
        region: (string) Region to take action for, or 'all'
        scrape_type: (string) Type of scrape to take action for, or 'all'

    Args:
        N/A

    Returns:
        N/A
    """
    scrape_regions = validate_regions(get_values("region", request.args))
    scrape_types = validate_scrape_types(get_values("scrape_type",
                                                    request.args))

    if not scrape_regions or not scrape_types:
        return ('Missing or invalid parameters, see service logs.',
                httplib.BAD_REQUEST)

    for region in scrape_regions:

        for scrape_type in scrape_types:
            logging.info("Resuming %s scrape for %s." % (scrape_type, region))

            sessions.create_session(ScrapeKey(region, scrape_type))

            # Help avoid race condition with new session info
            # vs updating that w/first task.
            time.sleep(5)

            scraper = regions.get_scraper_from_cache(region)
            scraper.resume_scrape(scrape_type)

    return ('', httplib.OK)


def validate_regions(region_list):
    """Validates the region arguments.

    If any region in |region_list| is "all", then all supported regions will be
    returned.

    Args:
        region_list: List of regions from URL parameters

    Returns:
        False if invalid regions
        List of regions to scrape if successful
    """
    regions_list_output = region_list

    supported_regions = regions.get_supported_regions()
    for region in region_list:
        if region == "all":
            regions_list_output = supported_regions
        elif region not in supported_regions:
            logging.error("Region '%s' not recognized." % region)
            return False

    return regions_list_output


def validate_scrape_types(scrape_type_list):
    """Validates the scrape type arguments.

    If any scrape type in |scrape_type_list| is "all", then all supported scrape
    types will be returned.

    Args:
        scrape_type_list: List of scrape types from URL parameters

    Returns:
        False if invalid scrape types
        List of scrape types if successful
    """
    if not scrape_type_list:
        return ["background"]

    scrape_types_list_output = scrape_type_list

    for scrape_type in scrape_type_list:
        if scrape_type == "all":
            scrape_types_list_output = SCRAPE_TYPES
        elif scrape_type not in SCRAPE_TYPES:
            logging.error("Scrape type '%s' not recognized." % scrape_type)
            return False

    return scrape_types_list_output
