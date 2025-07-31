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
"""Tests for spider implementation for the scraper engine."""

import pytest
from quart import Quart

from recidiviz.resource_search.scraper.engine.input import ScraperInput
from recidiviz.resource_search.scraper.engine.scraper import run_scraper

pytestmark = pytest.mark.asyncio()


# pylint: disable=unused-argument
async def test_multiple_inputs(
    web_app: Quart, scraper_test_input: ScraperInput
) -> None:
    # Should work fine with 3 inputs
    counter = 0

    test_input1 = scraper_test_input.model_copy()
    test_input1.name = "test2"
    test_input1.url = "http://localhost:9000/route3"

    print("TEST_INPUT_1", test_input1)

    test_input2 = scraper_test_input.model_copy()
    test_input2.name = "test3"
    test_input2.url = "http://localhost:9000/route1"

    async for batch in run_scraper([scraper_test_input, test_input1, test_input2]):

        assert len(batch) > 0
        for item in batch:
            assert item.website is not None
        counter += 1

    assert counter == 3


async def test_invalid_input(scraper_test_input: ScraperInput) -> None:
    # empty input
    with pytest.raises(ValueError) as exc_info:
        async for _ in run_scraper([]):
            raise AssertionError("Should not reach here")
    assert str(exc_info.value) == "There are no inputs to process"

    # root that doesn't have children should raise an error
    test_input = scraper_test_input.model_copy()
    test_input.input_tree.children = []

    with pytest.raises(ValueError) as exc_info:
        async for _ in run_scraper([test_input]):
            raise AssertionError("Should not reach here")
    assert str(exc_info.value) == "Input root doesn't have children"
