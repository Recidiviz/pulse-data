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
# ============================================================================
"""Checks the status of scrapers to detect when they have completed."""

import logging
from concurrent import futures
from http import HTTPStatus

from flask import Blueprint, request, url_for

from recidiviz.common import queues
from recidiviz.ingest.models.scrape_key import ScrapeKey
from recidiviz.ingest.scrape import (constants, ingest_utils,
                                     scrape_phase, sessions)
from recidiviz.utils import monitoring, regions, structured_logging
from recidiviz.utils.auth import authenticate_request
from recidiviz.utils.params import get_values

scraper_status = Blueprint('scraper_status', __name__)

@scraper_status.route('/check_finished')
@authenticate_request
def check_for_finished_scrapers():
    """Checks for any finished scrapers and kicks off next processes."""

    next_phase = scrape_phase.next_phase(request.endpoint)
    next_phase_url = url_for(next_phase) if next_phase else None

    @structured_logging.copy_trace_id_to_thread
    @monitoring.with_region_tag
    def _check_finished(region_code: str):
        # If there are no sessions currently scraping, nothing to check.
        session = sessions.get_current_session(
            ScrapeKey(region_code, constants.ScrapeType.BACKGROUND))
        if not session or not session.phase.is_actively_scraping():
            return

        if is_scraper_finished(region_code):
            logging.info("Region [%s] has finished scraping.", region_code)

            if next_phase:
                logging.info("Enqueueing [%s] for region [%s].",
                             next_phase, region_code)
                queues.enqueue_scraper_phase(
                    region_code=region_code, url=next_phase_url)

    region_codes = ingest_utils.validate_regions(
        get_values('region', request.args))

    failed_regions = []
    with futures.ThreadPoolExecutor() as executor:
        future_to_region = \
            {executor.submit(_check_finished, region_code): region_code
             for region_code in region_codes}
        for future in futures.as_completed(future_to_region):
            region_code = future_to_region[future]
            with monitoring.push_tags({monitoring.TagKey.REGION: region_code}):
                try:
                    future.result()
                except Exception:
                    logging.exception(
                        'An exception occured when checking region [%s]',
                        region_code)
                    failed_regions.append(region_code)

    if failed_regions:
        return ('Failed to check regions: {}'.format(failed_regions),
                HTTPStatus.INTERNAL_SERVER_ERROR)
    return ('', HTTPStatus.OK)

def is_scraper_finished(region_code: str):
    region = regions.get_region(region_code)
    # Note: if listing the tasks repeatedly is too heavy weight, we could mark
    # the most recently enqueued task time on the session and check that first.
    return not queues.list_scrape_tasks(region_code=region_code,
                                        queue_name=region.get_queue_name())
