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
"""Core scraper engine module for managing spider execution and resource collection.

This module contains the main scraper orchestration logic, including the SpiderManager
class for coordinating multiple spiders and the run_scraper function for executing
scraping operations with proper async event loop management.
"""
import asyncio
import logging
from typing import Any, AsyncIterator, Dict, List, Optional, Set

from scrapy import Spider, signals
from scrapy.crawler import CrawlerProcess
from twisted.internet import asyncioreactor
from twisted.internet.error import ReactorAlreadyInstalledError
from twisted.python.failure import Failure

from recidiviz.resource_search.scraper.engine.input import ScraperInput
from recidiviz.resource_search.scraper.engine.pipelines import OutputItemPipeline
from recidiviz.resource_search.scraper.engine.spider import RootSpider
from recidiviz.resource_search.src.models.resource_enums import ResourceCategory
from recidiviz.resource_search.src.typez.crud.resources import ResourceCandidate


class SpiderSettings:
    """Configuration settings for Scrapy spiders.

    Contains default settings for request headers, download handlers,
    depth limits, and other spider configuration parameters.
    """

    DEFAULT_REQUEST_HEADERS: dict[str, Any] = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en",
    }
    TWISTED_REACTOR: str = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
    DOWNLOAD_HANDLERS: dict[str, str] = {
        "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    }
    DEPTH_LIMIT: int = 4
    ITEM_PIPELINES: dict[Any, int] = {
        OutputItemPipeline: 300,
    }
    LOG_LEVEL: str = "INFO"
    custom_settings = {
        "PLAYWRIGHT_PAGE_GOTO_WAIT_UNTIL": "domcontentloaded",
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 60000,
    }


class SpiderManager:
    """Manages multiple spiders and coordinates resource collection.

    Handles spider lifecycle events, batches resource candidates by category,
    and provides async coordination for spider execution and result collection.
    """

    def __init__(self) -> None:
        # Store results for each spider by category
        self.results_by_category: Dict[ResourceCategory, List[ResourceCandidate]] = {}

        self.active_spiders: Set[str] = set()

        self.result_available: asyncio.Event = asyncio.Event()

        self.error_occurred: bool = False
        self.last_error: Optional[Exception] = None

        # Batch size for processing items
        # If the amount of resources for a category for a single spider will be equal to this value
        # We will iterate over all results for all current spiders and their results
        # Gather all items within the same category and yield them;
        # at the same time clearing results_by_category from these items
        self.batch_size = 10

        self.items_to_yield: Dict[ResourceCategory, List[ResourceCandidate]] = {}

    async def handle_item(self, item: ResourceCandidate, spider: Spider) -> None:
        """Memoize item after it's returned from the pipeline"""
        if not item or not item.category:
            return

        if item.category not in self.results_by_category:
            self.results_by_category[item.category] = []

        self.results_by_category[item.category].append(item)

        spider.logger.info(
            f"Received item for category {item.category.value} for {spider.name}"
        )

        if len(self.results_by_category[item.category]) < self.batch_size:
            return

        category_to_yield = item.category

        for category, resources in self.results_by_category.items():
            if category is not category_to_yield:
                continue
            if category_to_yield not in self.items_to_yield:
                self.items_to_yield[category_to_yield] = resources
            else:
                self.items_to_yield[category_to_yield].extend(resources)
            self.results_by_category[category] = []

        spider.logger.info(
            f"Gathered items for category {category_to_yield.value}: {len(self.items_to_yield)}"
        )

        spider.logger.info(f"Yielding items for category {category_to_yield.value}")
        self.result_available.set()

    def handle_spider_error(self, failure: Failure, spider: Spider) -> None:
        """Handle spider errors and ensure cleanup - sync version"""
        self.error_occurred = True

        spider.logger.info(
            f"Error occurred in spider {spider.name}: {failure.getErrorMessage()}"
        )
        self.last_error = failure.value

        # Set the event to indicate we're done
        self.result_available.set()

        # Clean up the spider
        if spider.name in self.active_spiders:
            self.active_spiders.remove(spider.name)

    async def handle_spider_closed(self, spider: Spider) -> None:
        """Handle spider closed signal and mark it as completed"""
        if spider.name in self.active_spiders:
            self.active_spiders.remove(spider.name)
            self.result_available.set()

    def register_spider(self, spider_name: str) -> None:
        """Register a new spider"""
        self.active_spiders.add(spider_name)


async def run_scraper(
    scraper_inputs: List[ScraperInput],
) -> AsyncIterator[List[ResourceCandidate]]:
    """Run scrapers for the given inputs and yield resource candidates in batches.

    Args:
        scraper_inputs: List of scraper input configurations to process.

    Yields:
        Lists of ResourceCandidate objects, batched by category.

    Raises:
        ValueError: If no inputs are provided or if input validation fails.
        RuntimeError: If spider execution fails with unknown error.
    """
    if len(scraper_inputs) == 0:
        raise ValueError("There are no inputs to process")

    # Twisted reactor which is used in scrapy also has an event loop,
    # and we can't run 2 event loops within one application.
    # But we can set an existing event loop for twisted reactor.
    loop = asyncio.get_event_loop()
    try:
        asyncioreactor.install(loop)
    except ReactorAlreadyInstalledError:
        pass

    runner = CrawlerProcess(
        settings={
            "DEFAULT_REQUEST_HEADERS": {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en",
            },
            "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
            "DOWNLOAD_HANDLERS": {
                "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
                "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            },
            "DEPTH_LIMIT": 4,
            "ITEM_PIPELINES": {
                "recidiviz.resource_search.scraper.engine.pipelines.OutputItemPipeline": 300,
            },
            "LOG_LEVEL": "INFO",
            "PLAYWRIGHT_PAGE_GOTO_WAIT_UNTIL": "domcontentloaded",
            "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 600000,
        }
    )

    spider_manager = SpiderManager()

    try:
        # For each input - we create and run a separate spider
        for scraper_input in scraper_inputs:
            if not scraper_input.input_tree.children:
                raise ValueError("Input root doesn't have children")

            spider_manager.register_spider(scraper_input.name)
            crawler = runner.create_crawler(RootSpider)

            # Connect all signals
            crawler.signals.connect(spider_manager.handle_item, signals.item_scraped)
            crawler.signals.connect(
                spider_manager.handle_spider_closed, signals.spider_closed
            )
            crawler.signals.connect(
                spider_manager.handle_spider_error, signals.spider_error
            )
            runner.crawl(crawler_or_spidercls=crawler, scraper_input=scraper_input)

        while spider_manager.active_spiders:

            await spider_manager.result_available.wait()
            # If an error occurred, break the loop and cleanup
            if spider_manager.error_occurred:
                if spider_manager.last_error:
                    raise spider_manager.last_error
                raise RuntimeError("Spider failed with unknown error")

            # There are two cases why we get here:
            # 1. when we fill a batch for some category
            # 2. when spider is closed

            # First we are processing the 2nd case, and we check
            # if it's the last active spider after which the loop will break
            # If yes - we need to yield all results both
            # from the results_by_category & items_to_yield
            if not spider_manager.active_spiders:
                for category, result in spider_manager.results_by_category.items():
                    items: List[ResourceCandidate] = result.copy()

                    if category in spider_manager.items_to_yield:
                        items.extend(spider_manager.items_to_yield[category].copy())

                    yield items
                    spider_manager.results_by_category[category] = []
                    spider_manager.items_to_yield[category] = []

            else:
                # Else we just process items_to_yield
                for category in list(spider_manager.items_to_yield.keys()):
                    items = spider_manager.items_to_yield[category]
                    if items:
                        yield items  # Yield the actual list
                        spider_manager.items_to_yield[
                            category
                        ] = []  # Clear the list for this category

            spider_manager.result_available.clear()

    finally:
        try:
            # Check if runner is still running before trying to stop it
            # pylint: disable=protected-access
            # There is no public API to check if the runner is running, so we must access _engine
            if hasattr(runner, "_engine") and getattr(runner._engine, "running", False):
                await runner.stop()
        except Exception as e:
            # Log the error but don't raise it to avoid masking other issues
            logging.warning("Error stopping scraper runner: %s", e)
