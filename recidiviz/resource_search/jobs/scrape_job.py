# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""
This script initializes logging and Sentry error tracking, then runs the asynchronous resource scraping job for the Recidiviz platform.

It uses the scrape_resources coroutine to collect resource data and save it to the database. This script is intended to be run as a standalone entry point for scraping resources as part of the resource search pipeline.
"""

import asyncio
import logging

import sentry_sdk

from recidiviz.resource_search.scraper.scrape import scrape_resources
from recidiviz.resource_search.src.constants import RESOURCE_SEARCH_SENTRY_DSN

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sentry_sdk.init(
        dsn=RESOURCE_SEARCH_SENTRY_DSN,
    )

    # Scrape Resources and save them to DB
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(scrape_resources())
    finally:
        loop.close()
