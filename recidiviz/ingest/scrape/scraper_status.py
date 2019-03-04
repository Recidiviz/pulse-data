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
# ============================================================================
"""Checks the status of scrapers to detect when they have completed."""

from http import HTTPStatus
import logging
import threading

from flask import Blueprint, request

from recidiviz.ingest.scrape import ingest_utils, queues
from recidiviz.utils import regions
from recidiviz.utils.auth import authenticate_request
from recidiviz.utils.params import get_values

scraper_status = Blueprint('scraper_status', __name__)

@scraper_status.route('/check_finished')
@authenticate_request
def check_for_finished_scrapers():
    """Checks for any finished scrapers and kicks off next processes."""

    def _check_finished(region_code: str):
        if is_scraper_finished(region_code):
            # TODO: create task to stop scraper, kick off batch write
            logging.info('Region \'%s\' has completed.', region_code)

    region_codes = ingest_utils.validate_regions(
        get_values('region', request.args))

    threads = []
    for region_code in region_codes:
        # Kick off a check for each region
        thread = threading.Thread(
            target=_check_finished,
            args=(region_code,)
        )
        thread.start()
        threads.append(thread)

    # Wait for all the checks to complete.
    for thread in threads:
        thread.join()
    return ('', HTTPStatus.OK)

def is_scraper_finished(region_code: str):
    region = regions.get_region(region_code)
    # Note: if listing the tasks repeatedly is too heavy weight, we could mark
    # the most recently enqueued task time on the session and check that first.
    return not queues.list_tasks(region_code=region_code,
                                 queue_name=region.get_queue_name())
