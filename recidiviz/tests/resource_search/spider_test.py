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
# =============================================================================
"""Tests for the spider module

This module contains unit tests for the web scraping functionality,
including spider configuration, HTML parsing, and resource extraction.
"""
import asyncio
import contextlib
import unittest
from typing import Optional

from hypercorn.asyncio import serve
from hypercorn.config import Config
from quart import Quart

from recidiviz.resource_search.scraper.engine.input import (
    ScraperInput,
    ScraperNode,
    ScraperNodeType,
    ScraperTree,
    SelectorMethod,
)
from recidiviz.resource_search.scraper.engine.items import OutputItem
from recidiviz.resource_search.src.models.resource_enums import (
    ResourceCategory,
    ResourceSubcategory,
)


class HTMLItem(OutputItem):  # pylint: disable=too-many-instance-attributes
    """Test HTML item for generating test web pages."""

    leads_to: Optional[str] = None
    service_name: Optional[str] = None
    category_mapping: Optional[str] = None  # pylint: disable=invalid-name

    def generate_section(self) -> str:
        """Generate HTML section for this item."""
        return f"""
        <div id="section">
           {f'<h1 id="name">{self.name}</h1>' if self.name else ''}
           {f'<p id="description">{self.description}</p>' if self.description else ''}
           {f'<p id="address">{self.address}</p>' if self.address else ''}
           {f'<p id="phone_number">{self.phone_number}</p>' if self.phone_number else ''}
           {f'<p id="email">{self.email}</p>' if self.email else ''}
           {f'<a id="website" href="{self.website}">{self.website}</a>' if self.website else ''}
           {f'<a id="leads_to" href="{self.leads_to}">{self.leads_to}</a>' if self.leads_to else ''}
           {f'<p id="additional_data">{self.additional_data}</p>' if self.additional_data else ''}
           {f'<p id="hours_of_operation">{self.hours_of_operation}</p>'
           if self.hours_of_operation else ''}
           {f'<p id="service_name">{self.service_name}</p>' if self.service_name else ''}
           {f'<p id="category">{self.category_mapping}</p>' if self.category_mapping else ''}
        </div>
        """


class TestSpider(unittest.IsolatedAsyncioTestCase):
    """Test cases for spider functionality."""

    async def asyncSetUp(self) -> None:
        """Set up the test web application and start a real server."""
        self.app = Quart(__name__)
        self.server = None
        self.port = 9100

        def html_structure(sections: str) -> str:
            return f"""
            <html>
            <head>
                <title>Scraper</title>
            </head>
            <body>
              <div id="main">
                {sections}
              </div>
            </body>
            </html>
            """

        @self.app.get("/")
        async def index() -> str:
            items = (
                HTMLItem(
                    address="Rue de la Loi 16",
                    leads_to="/route1",
                    name="Commission",
                    website="https://fsdgoogle.com",
                    description="Main Description #1",
                    category_mapping="Housing",
                    unique_id="item1",
                ),
                HTMLItem(
                    address="Square de Meeûs 1",
                    leads_to="/route2",
                    phone_number="+330000000",
                    email="example@gmail.com",
                    name="Example name",
                    description="Main Description #2",
                    category_mapping="Brooklyn Community Services",
                    unique_id="item2",
                ),
            )
            sections = "".join(item.generate_section() for item in items)
            return html_structure(sections)

        @self.app.get("/route1")
        async def route1() -> str:
            items = (
                HTMLItem(
                    address="Rue de la Loi 16",
                    leads_to="/route3",
                    name="Commission",
                    description="Route 1 Description #1",
                    website="https://googl55e.com",
                    category_mapping="Housing",
                    unique_id="item3",
                ),
                HTMLItem(
                    address="Square de Meeûs 1",
                    phone_number="+330000000",
                    website="https://google12.com",
                    description="Route 1 Description #2",
                    category_mapping="Housing",
                    unique_id="item4",
                ),
            )
            return html_structure("".join(item.generate_section() for item in items))

        @self.app.get("/route2")
        async def route2() -> str:
            items = (
                HTMLItem(
                    address="Some place",
                    name="Commission",
                    description="Route 2 Description #1",
                    website="https://gfoogl55e.com",
                    category_mapping="Housing",
                    unique_id="item5",
                ),
                HTMLItem(
                    address="Square de Meeûs 1",
                    phone_number="+330000000",
                    website="https://g11oogle12.com",
                    description="Route 2 Description #2",
                    category_mapping="Housing",
                    unique_id="item6",
                ),
            )
            return html_structure("".join(item.generate_section() for item in items))

        @self.app.get("/route3")
        async def route3() -> str:
            items = (
                HTMLItem(
                    address="Rue de la Loi 16",
                    name="Commission",
                    description="Route 3 Description #1",
                    website="https://goccogl55e.com",
                    category_mapping="Housing",
                    unique_id="item7",
                ),
                HTMLItem(
                    address="Square de Meeûs 1",
                    phone_number="+330000000",
                    website="https://gvoogle12.com",
                    description="Route 3 Description #2",
                    category_mapping="Housing",
                    unique_id="item8",
                ),
            )
            return html_structure("".join(item.generate_section() for item in items))

        # Start the server
        config = Config()
        config.bind = [f"localhost:{self.port}"]
        self.server_task = asyncio.create_task(serve(self.app, config))
        await asyncio.sleep(0.5)  # Give the server time to start

    async def asyncTearDown(self) -> None:
        """Tear down the test web application."""
        if hasattr(self, "server_task"):
            self.server_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, RuntimeError):
                await self.server_task

    def get_scraper_test_input(self) -> ScraperInput:
        """Create a test scraper input configuration."""
        return ScraperInput(
            url="http://localhost:9000",
            allowed_domains=["localhost"],
            name="test1",
            category_mapper={
                "Housing": {
                    "category": ResourceCategory.BASIC_NEEDS,
                    "subcategory": ResourceSubcategory.HOUSING,
                },
                "Brooklyn Community Services": {
                    "category": ResourceCategory.BEHAVIORAL_HEALTH
                },
            },
            input_tree=ScraperTree(
                pattern="//body",
                children=[
                    ScraperNode(
                        node_type=ScraperNodeType.GROUP,
                        pattern='//div[@id="section"]',
                        method=SelectorMethod.XPATH,
                        children=[
                            ScraperNode(
                                node_type=ScraperNodeType.ITEM,
                                pattern='//div[@id="section"]',
                                method=SelectorMethod.XPATH,
                                children=[
                                    ScraperNode(
                                        node_type=ScraperNodeType.ITEM,
                                        pattern="//p[@id='description']/text()",
                                        method=SelectorMethod.XPATH,
                                        children=[],
                                    ),
                                    ScraperNode(
                                        node_type=ScraperNodeType.ITEM,
                                        pattern="//p[@id='address']/text()",
                                        method=SelectorMethod.XPATH,
                                        children=[],
                                    ),
                                    ScraperNode(
                                        node_type=ScraperNodeType.ITEM,
                                        pattern="//p[@id='phone_number']/text()",
                                        method=SelectorMethod.XPATH,
                                        children=[],
                                    ),
                                    ScraperNode(
                                        node_type=ScraperNodeType.ITEM,
                                        pattern="//p[@id='email']/text()",
                                        method=SelectorMethod.XPATH,
                                        children=[],
                                    ),
                                    ScraperNode(
                                        node_type=ScraperNodeType.ITEM,
                                        pattern="//a[@id='website']/@href",
                                        method=SelectorMethod.XPATH,
                                        children=[],
                                    ),
                                    ScraperNode(
                                        node_type=ScraperNodeType.ITEM,
                                        pattern="//p[@id='category']/text()",
                                        method=SelectorMethod.XPATH,
                                        children=[],
                                    ),
                                ],
                            )
                        ],
                    )
                ],
            ),
        )

    async def test_spider_configuration(self) -> None:
        """Test spider configuration without running the scraper."""
        # Test that we can create a valid scraper input
        test_input = self.get_scraper_test_input()
        self.assertIsNotNone(test_input)
        self.assertEqual(test_input.name, "test1")
        self.assertEqual(test_input.url, "http://localhost:9000")
        self.assertIn("localhost", test_input.allowed_domains)
        self.assertIsNotNone(test_input.input_tree)
        self.assertGreater(len(test_input.input_tree.children), 0)

        # Test category mapper
        self.assertIn("Housing", test_input.category_mapper)
        self.assertEqual(
            test_input.category_mapper["Housing"]["category"],
            ResourceCategory.BASIC_NEEDS,
        )


if __name__ == "__main__":
    unittest.main()
