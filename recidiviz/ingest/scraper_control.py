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


import logging
import time
import webapp2

from google.appengine.ext import deferred
from recidiviz.ingest import sessions
from recidiviz.ingest.docket import load_target_list
from recidiviz.ingest.models.scrape_key import ScrapeKey
from recidiviz.ingest.tracker import purge_docket_and_session
from recidiviz.utils import environment
from recidiviz.utils import regions
from recidiviz.utils.auth import authenticate_request
from recidiviz.utils.params import get_param


SCRAPE_TYPES = ["background", "snapshot"]


class ScraperStart(webapp2.RequestHandler):
    """Request handler for triggering scrapers."""

    @authenticate_request
    def get(self):
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
        request_params = self.request.GET.items()
        valid_params = get_and_validate_params(request_params)

        if valid_params:
            (scrape_regions, scrape_types, params) = valid_params

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
                    purge_docket_and_session(scrape_key)
                    load_target_list(scrape_key, params)

                    # Start scraper, but give the target list loader a headstart
                    timer = 30 if not environment.in_prod() else 300
                    logging.info("Starting %s/%s scrape in %d seconds..."
                                 % (region, scrape_type, timer))
                    deferred.defer(scraper.start_scrape,
                                   scrape_type,
                                   _countdown=timer)

        else:
            invalid_input(self.response,
                          "Scrape type or region not recognized.")


class ScraperStop(webapp2.RequestHandler):
    """Request handler for pausing//stopping scrapers."""

    @authenticate_request
    def get(self):
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
        request_params = self.request.GET.items()
        valid_params = get_and_validate_params(request_params)

        if valid_params:
            (scrape_regions, scrape_types, _params) = valid_params

            for region in scrape_regions:
                logging.info("Stopping %s scrapes for %s."
                             % (scrape_types, region))

                for scrape_type in scrape_types:
                    sessions.end_session(ScrapeKey(region, scrape_type))

                scraper = regions.get_scraper_from_cache(region)
                scraper.stop_scrape(scrape_types)

        else:
            invalid_input(self.response,
                          "Scrape type or region not recognized.")


class ScraperResume(webapp2.RequestHandler):
    """Request handler for resuming paused//stoped scrapers."""

    @authenticate_request
    def get(self):
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
        request_params = self.request.GET.items()
        valid_params = get_and_validate_params(request_params)

        if valid_params:
            (scrape_regions, scrape_types, _params) = valid_params

            for region in scrape_regions:

                for scrape_type in scrape_types:
                    logging.info("Resuming %s scrape for %s."
                                 % (scrape_type, region))

                    sessions.create_session(ScrapeKey(region, scrape_type))

                    # Help avoid race condition with new session info
                    # vs updating that w/first task.
                    time.sleep(5)

                    scraper = regions.get_scraper_from_cache(region)
                    scraper.resume_scrape(scrape_type)

        else:
            invalid_input(self.response,
                          "Scrape type or region not recognized.")


def get_and_validate_params(request_params):
    """Get common request parameters and validate them

    URL parameters:
        region: (string) Region to take action for, or 'all'
        scrape_type: (string) Type of scrape to take action for, or 'all'

    Args:
        request_params: The request URL parameters

    Returns:
        False if invalid params
        Tuple of params if successful, in the form:
            ([region1, ...], [scrape_type1, ...], other_query_params)
    """
    request_region = get_param("region", request_params, None)
    request_scrape_type = get_param("scrape_type", request_params, "background")

    # Validate scrape_type
    if request_scrape_type == "all":
        request_scrape_type = SCRAPE_TYPES
    elif request_scrape_type in SCRAPE_TYPES:
        request_scrape_type = [request_scrape_type]
    else:
        return False

    # Validate region code
    supported_regions = regions.get_supported_regions()
    if request_region == "all":
        request_region = supported_regions
    elif request_region in supported_regions:
        request_region = [request_region]
    else:
        return False

    result = (request_region, request_scrape_type, request_params)

    return result


def invalid_input(request_response, log_message):
    """Logs problem with request and sets HTTP response code

    Args:
        request_response: Request handler response object
        log_message: (string) Message to write to service logs

    Returns:
        N/A
    """
    logging.error(log_message)

    request_response.write('Missing or invalid parameters, see service logs.')
    request_response.set_status(400)


app = webapp2.WSGIApplication([
    ('/scraper/start', ScraperStart),
    ('/scraper/stop', ScraperStop),
    ('/scraper/resume', ScraperResume)
], debug=False)
