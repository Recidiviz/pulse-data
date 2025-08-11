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
"""Main scraper module for processing resource search inputs and running scrapers.

This module provides the main entry point for the scraper system. It parses input
directories containing JSON configuration files, runs scrapers based on those
configurations, and processes the resulting resource candidates.
"""
import asyncio
import logging
import os
from typing import List

from recidiviz.resource_search.scraper.engine.input import ScraperInput
from recidiviz.resource_search.scraper.engine.scraper import run_scraper
from recidiviz.resource_search.src.handlers.helpers import process_and_store_candidates
from recidiviz.resource_search.src.settings import Settings
from recidiviz.resource_search.src.typez.handlers.text_search import (
    TextSearchBodyParams,
)


def parse_input_directory(db_name: str) -> List[ScraperInput]:
    """Parse the input directory where each json file represents a separate spider input."""
    input_directory = "recidiviz/resource_search/scraper/scraper_input/" + db_name
    files = os.listdir(input_directory)

    inputs = []
    for file_name in files:
        if not file_name.endswith(".json"):
            continue
        with open(f"{input_directory}/{file_name}", "r", encoding="utf-8") as file:
            inputs.append(ScraperInput.model_validate_json(file.read()))

    return inputs


async def scrape_resources(db_name: str) -> None:
    inputs = parse_input_directory(db_name=db_name)
    settings = Settings(db_name=db_name)
    async for resource_candidates in run_scraper(inputs):
        print(f"{'-' * 10}Got new batch; length {len(resource_candidates)}{'-' * 10}")
        try:
            if not resource_candidates:
                continue

            await process_and_store_candidates(
                body=TextSearchBodyParams(
                    category=resource_candidates[0].category, textSearch=""
                ),
                resource_candidates=resource_candidates,
                settings=settings,
            )
        except ValueError as err:
            logging.error("Error while storing resources: %s", err)
            continue
        except asyncio.CancelledError as err:
            logging.error("CancelledError: Error while storing resources: %s", err)
            continue
