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
"""Spider implementation for the scraper engine."""
from typing import AsyncGenerator, Iterable, List, Optional, Set, Union

from playwright.async_api import Page
from scrapy import Selector
from scrapy.http import Request, Response
from scrapy.http.response.text import TextResponse
from scrapy.spiders import Spider

from recidiviz.resource_search.scraper.engine.input import (
    InputSelector,
    PaginationSelector,
    ScraperInput,
    ScraperNode,
    ScraperNodeType,
    SelectorMethod,
)
from recidiviz.resource_search.scraper.engine.items import FieldTypes, OutputItem


class RootSpider(Spider):
    """Main spider class for scraping resource information from websites.

    Implements a configurable web scraper that can extract structured data
    from web pages based on selector configurations. Supports both item
    extraction and group processing with pagination handling.
    """

    def __init__(self, scraper_input: ScraperInput) -> None:
        super().__init__(name=scraper_input.name)  # Call base Spider __init__
        self.processed_links: set[str] = set()

        self.allowed_domains = scraper_input.allowed_domains
        self.start_url = scraper_input.url

        self.scraper_tree = scraper_input.input_tree

        self.category_mapper = {
            key.lower().strip(): value
            for key, value in scraper_input.category_mapper.items()
        }

    def start_requests(self) -> Iterable[Request]:
        yield Request(
            url=self.start_url,
            callback=self.parse,
            meta={
                "playwright": True,
                "playwright_include_page": True,
                "playwright_page_methods": [
                    {
                        "method": "goto",
                        "kwargs": {
                            "url": "<your-url>",
                            "wait_until": "domcontentloaded",
                            "timeout": 60000,
                        },
                    }
                ],
            },
            priority=0,
            dont_filter=True,
        )

    async def parse(
        self, response: Response
    ) -> AsyncGenerator[Union[OutputItem, Request], None]:
        """
        For each page we process root only once.

        While processing root we will:
            * Process items if there are any in the children list
            & yield them to the Output Pipeline;
            * Process groups if there are any in the children list;

        At the end we will extract links from the page,
        check with the processed links list and yield new requests.
        """
        self.logger.info(f"Processing URL: {response.url}")
        if response.url in self.processed_links:
            self.logger.info(f"URL {response.url} was already processed.")
            return

        section = (
            Selector(
                TextResponse(url=response.url, body=response.text, encoding="utf-8")
            )
            .xpath(self.scraper_tree.pattern)
            .get()
        )

        if section is None:
            self.logger.info(f"Section not found for {response.url}")
            return

        new_response = TextResponse(url=response.url, body=section, encoding="utf-8")

        if self.scraper_tree.pagination:
            page: Page = response.meta["playwright_page"]

            new_response = await self._process_pagination(
                page,
                TextResponse(
                    url=new_response.url, body=new_response.text, encoding="utf-8"
                ),
                self.scraper_tree.pattern,
                self.scraper_tree.pagination,
            )

        for node in self.scraper_tree.children:

            # If there are items in the root children list -> just yield items
            if node.node_type == ScraperNodeType.ITEM:
                output_item = OutputItem(unique_id=response.url)
                processed_items = await self._process_item(
                    new_response, node, output_item
                )
                for item in processed_items:
                    yield item

            # If there are groups in the root children list -> follow the links & scrape the data
            elif node.node_type == ScraperNodeType.GROUP:
                async for result in self.process_group(new_response, node):
                    yield result

        self.processed_links.add(new_response.url)
        #
        # # Extract links from the page and yield new requests
        # for request in await self._scrap_links(response):
        #     yield request

    async def process_group(
        self,
        response: Response,
        node: ScraperNode,
        parent_item: Optional[OutputItem] = None,
    ) -> AsyncGenerator[Union[Request, OutputItem], None]:
        """
        For groups we need to follow all links & process items
        """

        elements = self._scrap_group_section(response, node.pattern, node.method)

        for element in elements:
            current_item = parent_item or OutputItem(unique_id=response.url)

            # Process group selectors
            await self._process_selectors(node.selectors, element, current_item)

            # Process children of the group
            links = await self._extract_links(
                TextResponse(url=response.url, body=element.get(), encoding="utf-8")
            )

            if len(links) == 0:
                yield current_item

            group_processed_urls: dict[str, List[str]] = {}

            # Generate requests for children based on found links
            for link in links:
                if link in self.processed_links:
                    continue
                abs_url = response.urljoin(link)

                if link in group_processed_urls.get(abs_url, []):
                    continue

                group_processed_urls[abs_url] = group_processed_urls.get(
                    abs_url, []
                ) + [link]

                for child in node.children:

                    if child.node_type == ScraperNodeType.ITEM:
                        # For items - just extract data from the following link
                        yield Request(
                            url=abs_url,
                            callback=self.process_item_request,
                            meta={"playwright": True},
                            cb_kwargs={
                                "node": child,
                                "parent_item": current_item.model_copy(deep=True),
                            },
                            priority=999,
                        )
                    else:
                        # For groups - continue group processing for the following link
                        yield Request(
                            url=abs_url,
                            callback=self.process_group_request,
                            meta={"playwright": True},
                            cb_kwargs={
                                "node": child,
                                "parent_item": current_item.model_copy(deep=True),
                            },
                            priority=999,
                        )

    async def process_item_request(
        self, response: Response, node: ScraperNode, parent_item: OutputItem
    ) -> AsyncGenerator[OutputItem, None]:
        """Handle requests for item processing"""

        self.logger.info(f"Processing item for URL: {response.url}")

        processed_items = await self._process_item(
            response, node, parent_item.model_copy(deep=True)
        )
        for item in processed_items:
            yield item

    async def process_group_request(
        self, response: Response, node: ScraperNode, parent_item: OutputItem
    ) -> AsyncGenerator[Union[Request, OutputItem], None]:
        """Handle requests for group processing"""

        async for result in self.process_group(
            response, node=node, parent_item=parent_item
        ):
            yield result

    async def _process_item(
        self, response: Response, node: ScraperNode, item: OutputItem
    ) -> List[OutputItem]:
        """For items we don't need to follow any links, just scrape the data & yield it"""
        elements = self._scrap_group_section(response, node.pattern, node.method)

        output: List[OutputItem] = []

        for element in elements:
            copied_item = item.model_copy(deep=True)

            if await self._process_selectors(node.selectors, element, copied_item):
                output.append(copied_item)
        return output

    async def _process_selectors(
        self, selectors: List[InputSelector], element: Selector, item: OutputItem
    ) -> bool:
        has_data = False

        for selector in selectors:
            value = await self._extract_value(element, selector)

            if not value:
                continue

            item.set_by_type(selector.field_type, value)

            if selector.field_type == FieldTypes.ADDITIONAL_DATA:
                item.set_additional_data({selector.pattern: value})

            if selector.field_type == FieldTypes.CATEGORY:
                item.map_category(value, self.category_mapper)

            has_data = True
        return has_data

    async def _extract_value(
        self, element: Selector, selector: InputSelector
    ) -> Optional[str]:
        """Extract value using selector configuration"""
        value: Optional[str] = None
        match selector.method:
            case SelectorMethod.XPATH:
                value = element.xpath(selector.pattern).get()
            case SelectorMethod.CSS:
                value = element.css(selector.pattern).get()
            case SelectorMethod.REGEX:
                matches = element.re(selector.pattern)
                value = matches[0] if matches else None

        if not value:
            return None

        if selector.keywords and not any(kw in value for kw in selector.keywords):
            return None

        return value

    def _scrap_group_section(
        self, response: Response, pattern: str, method: SelectorMethod
    ) -> List[Selector]:
        """
        Scrape group section using the provided pattern and Selector

        Returns a list of Selector instances that represent the group elements.
        """
        text_response = TextResponse(
            url=response.url, body=response.text, encoding="utf-8"
        )
        group_elements = self._extract_strings_by_pattern(
            method, text_response, pattern
        )

        if group_elements is None:
            return []
        return [Selector(text=element) for element in group_elements]

    def _extract_strings_by_pattern(
        self, method: SelectorMethod, response: TextResponse, pattern: str
    ) -> List[str]:
        res: List[str] = []

        if method == SelectorMethod.XPATH:
            res = Selector(response).xpath(pattern).getall()
        elif method == SelectorMethod.CSS:
            res = Selector(response).css(pattern).getall()

        return res

    async def _scrap_links(self, response: Response) -> List[Request]:
        """Helper function to extract links from the input Response instance"""
        links = await self._extract_links(
            TextResponse(url=response.url, body=response.text, encoding="utf-8")
        )

        return [
            Request(
                url=link,
                callback=self.parse,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_page_methods": [
                        {
                            "method": "goto",
                            "kwargs": {
                                "url": "<your-url>",
                                "wait_until": "domcontentloaded",
                                "timeout": 60000,  # Optional: increase timeout to 60s
                            },
                        }
                    ],
                },
                priority=-1,
            )
            for link in links
        ]

    async def _extract_links(self, response: TextResponse) -> List[str]:
        """Transform response to TextResponse & Extract links from the input Response instance"""
        links = response.xpath("//a/@href").getall()

        unprocessed_links: List[str] = []

        for link in links:
            current_link = response.urljoin(link)
            if current_link in self.processed_links:
                continue

            if not self._is_allowed_domain(current_link):
                continue

            unprocessed_links.append(current_link)

        return unprocessed_links

    def _is_allowed_domain(self, url: str) -> bool:
        """Check if URL belongs to allowed domains"""
        return (
            any(allowed_domain in url for allowed_domain in self.allowed_domains)
            if self.allowed_domains
            else True
        )

    async def _process_pagination(
        self,
        page: Page,
        response: TextResponse,
        content_pattern: str,
        pagination: PaginationSelector,
    ) -> TextResponse:
        """Process pagination by clicking through pagination links and collecting content.

        Args:
            page: Playwright page instance for browser interaction.
            response: Original response containing the page content.
            content_pattern: XPath pattern to extract content from each page.
            pagination: Pagination configuration with selector pattern and method.

        Returns:
            TextResponse containing concatenated content from all paginated pages.
        """
        try:

            pagination_links: List[str] = self._extract_strings_by_pattern(
                pagination.method,
                response,
                pagination.pattern,
            )

            unique_pages: Set[str] = set(pagination_links.copy())
            processed_pages: Set[str] = set()

            pagination_content: List[str] = []

            while unique_pages:
                current_page = unique_pages.pop()
                if current_page in processed_pages:
                    continue

                processed_pages.add(current_page)

                try:
                    await page.click(f'a[href="{current_page}"]')

                    await page.wait_for_timeout(1000)

                    page_content = await page.content()
                    current_content = (
                        Selector(
                            TextResponse(
                                url=response.url, body=page_content, encoding="utf-8"
                            )
                        )
                        .xpath(content_pattern)
                        .get()
                    )

                    if current_content:
                        pagination_content.append(current_content)
                    else:
                        continue

                    for new_page in self._extract_strings_by_pattern(
                        pagination.method,
                        TextResponse(
                            url=response.url, body=current_content, encoding="utf-8"
                        ),
                        pagination.pattern,
                    ):
                        if new_page not in processed_pages:
                            unique_pages.add(new_page)

                except Exception as err:
                    self.logger.error(f"Error processing pagination page: {err}")
                    continue

            return TextResponse(
                url=response.url, body="\n".join(pagination_content), encoding="utf-8"
            )

        finally:
            await page.close()
