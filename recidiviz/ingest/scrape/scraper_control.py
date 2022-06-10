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

"""Requests handlers for queue control requests, i.e. starting, stopping, and
resuming ingest processes, such as scrapers.

Attributes:
    SCRAPE_TYPES: (list(string)) the list of acceptable scrape types
"""

import logging
import threading
import time
from concurrent import futures
from http import HTTPStatus

from flask import Blueprint, request, url_for

from recidiviz.ingest.models.scrape_key import ScrapeKey
from recidiviz.ingest.scrape import (
    docket,
    ingest_utils,
    scrape_phase,
    sessions,
    tracker,
)
from recidiviz.ingest.scrape.constants import BATCH_PUBSUB_TYPE
from recidiviz.ingest.scrape.scraper_cloud_task_manager import ScraperCloudTaskManager
from recidiviz.utils import monitoring, pubsub_helper, regions, structured_logging
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.params import get_str_param_value, get_str_param_values

scraper_control = Blueprint("scraper_control", __name__)


@scraper_control.route("/start")
@requires_gae_auth
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

    @monitoring.with_region_tag
    def _start_scraper(region, scrape_type):
        scrape_key = ScrapeKey(region, scrape_type)

        most_recent_session = next(
            sessions.get_sessions(
                region_code=scrape_key.region_code,
                include_closed=True,
                most_recent_only=True,
                scrape_type=scrape_key.scrape_type,
            ),
            None,
        )
        if most_recent_session and not most_recent_session.phase.has_persisted():
            raise Exception(
                f"Session already running for region [{region}]. Could "
                "not start a new session"
            )

        logging.info(
            "Purging pubsub queue for scrape_key: [%s] and pubsub_type: [%s]",
            scrape_key,
            BATCH_PUBSUB_TYPE,
        )
        pubsub_helper.purge(scrape_key, BATCH_PUBSUB_TYPE)

        logging.info("Starting new scraper for: [%s]", scrape_key)
        scraper = regions.get_region(region).get_scraper()

        current_session = sessions.create_session(scrape_key)

        # Help avoid race condition with new session info
        # vs updating that w/first task.
        time.sleep(1)

        # Clear prior query docket for this scrape type and start adding new
        # items in a background thread. In the case that there is a large
        # names list, loading it can take some time. Loading it in the
        # background allows us to start the scraper before it is fully
        # loaded.
        tracker.purge_docket_and_session(scrape_key)
        # Note, the request context isn't copied when launching this thread, so
        # any logs from within `load_target_list` will not be associated with
        # the start scraper request.
        load_docket_thread = threading.Thread(
            target=structured_logging.with_context(docket.load_target_list),
            args=(scrape_key, given_names, surname),
        )
        load_docket_thread.start()

        # Start scraper, if the docket is empty this will wait for a bounded
        # period of time for an item to be published (~90 seconds).
        logging.info("Starting [%s]/[%s] scrape...", region, scrape_type)
        scraper.start_scrape(scrape_type)

        sessions.update_phase(current_session, scrape_phase.ScrapePhase.SCRAPE)

        # Wait for the docket to be loaded
        load_docket_thread.join()

    timezone = ingest_utils.lookup_timezone(request.args.get("timezone"))
    stripe_value = get_str_param_values("stripe", request.args)
    region_value = get_str_param_values("region", request.args)
    # If a timezone wasn't provided start all regions. If it was only start
    # regions that match the timezone.
    scrape_regions = ingest_utils.validate_regions(
        region_value, timezone=timezone, stripes=stripe_value
    )
    scrape_types = ingest_utils.validate_scrape_types(
        get_str_param_values("scrape_type", request.args)
    )

    if not scrape_regions or not scrape_types:
        return (
            "Missing or invalid parameters, or no regions found, see logs.",
            HTTPStatus.BAD_REQUEST,
        )

    given_names = get_str_param_value("given_names", request.args, "")
    surname = get_str_param_value("surname", request.args, "")

    failed_starts = []
    with futures.ThreadPoolExecutor() as executor:
        # Start all of the calls.
        future_to_args = {
            executor.submit(
                structured_logging.with_context(_start_scraper),
                region_code,
                scrape_type,
            ): (region_code, scrape_type)
            for scrape_type in scrape_types
            for region_code in scrape_regions
        }

        # Wait for all the calls to finish.
        for future in futures.as_completed(future_to_args):
            region_code, scrape_type = future_to_args[future]
            with monitoring.push_tags({monitoring.TagKey.REGION: region_code}):
                try:
                    future.result()
                except Exception:
                    logging.exception(
                        "An exception occured when starting region [%s] for [%s]",
                        region_code,
                        scrape_type,
                    )
                    failed_starts.append((region_code, scrape_type))
                else:
                    logging.info(
                        "Finished starting region [%s] for [%s].",
                        region_code,
                        scrape_type,
                    )

    if failed_starts:
        # This causes the whole request to be retried. Any regions whose session
        # was opened during this call will be immediately skipped in the next
        # call when we check for open sessions. Any regions we failed to start
        # likely still had sessions opened and thus will be skipped, but it is
        # worth retrying anyway.
        return (
            f"Failed to start regions: {failed_starts}",
            HTTPStatus.INTERNAL_SERVER_ERROR,
        )
    return ("", HTTPStatus.OK)


@scraper_control.route("/stop")
@requires_gae_auth
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
    timezone = ingest_utils.lookup_timezone(request.args.get("timezone"))
    stripe = get_str_param_values("stripe", request.args)
    respect_is_stoppable = get_str_param_value("respect_is_stoppable", request.args)

    # If a timezone wasn't provided stop all regions. If it was only stop
    # regions that match the timezone. If stripe provided, stop only regions
    # with matching stripe
    scrape_regions = ingest_utils.validate_regions(
        get_str_param_values("region", request.args), timezone=timezone, stripes=stripe
    )
    scrape_types = ingest_utils.validate_scrape_types(
        get_str_param_values("scrape_type", request.args)
    )

    next_phase = scrape_phase.next_phase(request.endpoint)
    next_phase_url = url_for(next_phase) if next_phase else None

    @monitoring.with_region_tag
    def _stop_scraper(region: str):
        logging.info("Trying to stop scraper for region [%s].", region)
        for scrape_type in scrape_types:
            key = ScrapeKey(region_code=region, scrape_type=scrape_type)
            session = sessions.get_current_session(key)
            if not session:
                logging.info(
                    "No [%s] scrape to stop for region: [%s]", scrape_type, region
                )
                continue

            region_scraper = regions.get_region(region).get_scraper()
            was_stopped = region_scraper.stop_scrape(scrape_type, respect_is_stoppable)
            if was_stopped:
                closed_sessions = sessions.close_session(key)
                for closed_session in closed_sessions:
                    sessions.update_phase(
                        closed_session, scrape_phase.ScrapePhase.PERSIST
                    )
                if next_phase:
                    logging.info("Enqueueing %s for region [%s].", next_phase, region)
                    ScraperCloudTaskManager().create_scraper_phase_task(
                        region_code=region, url=next_phase_url
                    )

    if not scrape_regions or not scrape_types:
        return (
            "Missing or invalid parameters, see service logs.",
            HTTPStatus.BAD_REQUEST,
        )

    failed_stops = []
    with futures.ThreadPoolExecutor() as executor:
        # Start all of the calls.
        future_to_regions = {
            executor.submit(
                structured_logging.with_context(_stop_scraper), region_code
            ): region_code
            for region_code in scrape_regions
        }

        # Wait for all the calls to finish.
        for future in futures.as_completed(future_to_regions):
            region_code = future_to_regions[future]
            with monitoring.push_tags({monitoring.TagKey.REGION: region_code}):
                try:
                    future.result()
                except Exception:
                    logging.exception(
                        "An exception occured when stopping region [%s] for [%s]",
                        region_code,
                        scrape_types,
                    )
                    failed_stops.append(region_code)
                else:
                    logging.info(
                        "Finished stopping region [%s] for [%s].",
                        region_code,
                        scrape_types,
                    )

    if failed_stops:
        # This causes the whole request to be retried. Any regions whose session
        # was closed during this call will be immediately skipped in the next
        # call as we won't find any sessions to close. Any regions we failed to
        # start likely still had their sessions closed and thus will be skipped,
        # but it is worth retrying anyway.
        return (
            f"Failed to stop regions: {failed_stops}",
            HTTPStatus.INTERNAL_SERVER_ERROR,
        )
    return ("", HTTPStatus.OK)


@scraper_control.route("/resume")
@requires_gae_auth
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
    scrape_regions = ingest_utils.validate_regions(
        get_str_param_values("region", request.args)
    )
    scrape_types = ingest_utils.validate_scrape_types(
        get_str_param_values("scrape_type", request.args)
    )

    if not scrape_regions or not scrape_types:
        return (
            "Missing or invalid parameters, see service logs.",
            HTTPStatus.BAD_REQUEST,
        )

    for region in scrape_regions:

        for scrape_type in scrape_types:
            logging.info("Resuming [%s] scrape for [%s].", scrape_type, region)

            sessions.create_session(ScrapeKey(region, scrape_type))

            # Help avoid race condition with new session info
            # vs updating that w/first task.
            time.sleep(5)

            scraper = regions.get_region(region).get_scraper()
            scraper.resume_scrape(scrape_type)

    return ("", HTTPStatus.OK)
