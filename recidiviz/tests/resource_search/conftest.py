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
"""Pytest configuration"""

import asyncio
from typing import AsyncGenerator, Optional

import pytest
import pytest_asyncio
from hypercorn.asyncio import serve
from hypercorn.config import Config
from quart import Quart

from recidiviz.resource_search.scraper.engine.input import (
    InputSelector,
    ScraperInput,
    ScraperNode,
    ScraperNodeType,
    ScraperTree,
    SelectorMethod,
)
from recidiviz.resource_search.scraper.engine.items import FieldTypes, OutputItem
from recidiviz.resource_search.src.models.resource_enums import (
    ResourceCategory,
    ResourceSubcategory,
)


class HTMLItem(OutputItem):  # pylint: disable=too-many-instance-attributes
    leads_to: Optional[str] = None
    service_name: Optional[str] = None
    category_mapping: Optional[str] = None  # pylint: disable=invalid-name

    def generate_section(self) -> str:
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


@pytest.fixture
def scraper_test_input() -> ScraperInput:
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
                    selectors=[
                        InputSelector(
                            keywords=["description"],
                            field_type=FieldTypes.DESCRIPTION,
                            pattern="//p[@id='description']/text()",
                            method=SelectorMethod.XPATH,
                        ),
                        InputSelector(
                            keywords=[],
                            field_type=FieldTypes.ADDRESS,
                            pattern="//p[@id='address']/text()",
                            method=SelectorMethod.XPATH,
                        ),
                        InputSelector(
                            keywords=[],
                            field_type=FieldTypes.PHONE_NUMBER,
                            pattern="//p[@id='phone_number']/text()",
                            method=SelectorMethod.XPATH,
                        ),
                        InputSelector(
                            keywords=[],
                            field_type=FieldTypes.EMAIL,
                            pattern="//p[@id='email']/text()",
                            method=SelectorMethod.XPATH,
                        ),
                        InputSelector(
                            keywords=[],
                            field_type=FieldTypes.WEBSITE,
                            pattern="//a[@id='website']/@href",
                            method=SelectorMethod.XPATH,
                        ),
                        InputSelector(
                            keywords=[],
                            field_type=FieldTypes.CATEGORY,
                            pattern="//p[@id='category']/text()",
                            method=SelectorMethod.XPATH,
                        ),
                    ],
                    children=[
                        ScraperNode(
                            node_type=ScraperNodeType.ITEM,
                            pattern='//div[@id="section"]',
                            method=SelectorMethod.XPATH,
                            selectors=[
                                InputSelector(
                                    keywords=[],
                                    field_type=FieldTypes.DESCRIPTION,
                                    pattern="//p[@id='description']/text()",
                                    method=SelectorMethod.XPATH,
                                ),
                                InputSelector(
                                    keywords=[],
                                    field_type=FieldTypes.ADDRESS,
                                    pattern="//p[@id='address']/text()",
                                    method=SelectorMethod.XPATH,
                                ),
                                InputSelector(
                                    keywords=[],
                                    field_type=FieldTypes.PHONE_NUMBER,
                                    pattern="//p[@id='phone_number']/text()",
                                    method=SelectorMethod.XPATH,
                                ),
                                InputSelector(
                                    keywords=[],
                                    field_type=FieldTypes.EMAIL,
                                    pattern="//p[@id='email']/text()",
                                    method=SelectorMethod.XPATH,
                                ),
                                InputSelector(
                                    keywords=[],
                                    field_type=FieldTypes.WEBSITE,
                                    pattern="//a[@id='website']/@href",
                                    method=SelectorMethod.XPATH,
                                ),
                                InputSelector(
                                    keywords=[],
                                    field_type=FieldTypes.CATEGORY,
                                    pattern="//p[@id='category']/text()",
                                    method=SelectorMethod.XPATH,
                                ),
                            ],
                        )
                    ],
                )
            ],
        ),
    )


@pytest_asyncio.fixture(scope="function")
async def web_app() -> AsyncGenerator[Quart, None]:
    """Web App Fixture for Engine Test"""
    app = Quart(__name__)

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

    @app.get("/")
    async def index() -> str:
        items = (
            HTMLItem(
                unique_id="1",
                address="Rue de la Loi 16",
                leads_to="/route1",
                name="Commission",
                website="https://fsdgoogle.com",
                description="Main Description #1",
                category_mapping="Housing",
            ),
            HTMLItem(
                unique_id="2",
                address="Square de Meeûs 1",
                leads_to="/route2",
                phone_number="+330000000",
                email="example@gmail.com",
                name="Example name",
                description="Main Description #2",
                category_mapping="Brooklyn Community Services",
            ),
        )

        sections = "".join(item.generate_section() for item in items)

        return html_structure(sections)

    @app.get("/route1")
    async def route1() -> str:
        items = (
            HTMLItem(
                unique_id="11",
                address="Rue de la Loi 16",
                leads_to="/route3",
                name="Commission",
                description="Route 1 Description #1",
                website="https://googl55e.com",
                category_mapping="Housing",
            ),
            HTMLItem(
                unique_id="12",
                address="Square de Meeûs 1",
                phone_number="+330000000",
                website="https://google12.com",
                description="Route 1 Description #2",
                category_mapping="Housing",
            ),
        )
        return html_structure("".join(item.generate_section() for item in items))

    @app.get("/route2")
    async def route2() -> str:
        items = (
            HTMLItem(
                unique_id="21",
                address="Some place",
                name="Commission",
                description="Route 2 Description #1",
                website="https://gfoogl55e.com",
                category_mapping="Housing",
            ),
            HTMLItem(
                unique_id="22",
                address="Square de Meeûs 1",
                phone_number="+330000000",
                website="https://g11oogle12.com",
                description="Route 2 Description #2",
                category_mapping="Housing",
            ),
        )
        return html_structure("".join(item.generate_section() for item in items))

    @app.get("/route3")
    async def route3() -> str:
        items = (
            HTMLItem(
                unique_id="31",
                address="Rue de la Loi 16",
                name="Commission",
                description="Route 3 Description #1",
                website="https://goccogl55e.com",
                category_mapping="Housing",
            ),
            HTMLItem(
                unique_id="32",
                address="Square de Meeûs 1",
                phone_number="+330000000",
                website="https://gvoogle12.com",
                description="Route 3 Description #2",
                category_mapping="Housing",
            ),
            HTMLItem(
                unique_id="33",
                address="Square de Meeûs 1",
                phone_number="+330000000",
                website="https://gvvvvoogle12.com",
                leads_to="/",
                description="Route 3 Description #3",
                category_mapping="Housing",
            ),
        )
        return html_structure("".join(item.generate_section() for item in items))

    config = Config()
    config.bind = ["localhost:9000"]

    server = asyncio.create_task(serve(app, config))

    await asyncio.sleep(0.1)

    try:
        yield app
    finally:
        server.cancel()
        try:
            await asyncio.shield(server)
        except asyncio.CancelledError:
            pass
