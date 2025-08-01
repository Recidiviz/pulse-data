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
"""Pipeline components for processing scraped items."""
from typing import List, Optional

from scrapy import Spider

from recidiviz.resource_search.scraper.engine.items import OutputItem
from recidiviz.resource_search.src.typez.crud.resources import ResourceCandidate


class OutputItemPipeline:
    def __init__(self) -> None:
        self.processed_items: List[str] = []

    async def process_item(
        self, item: OutputItem, spider: Spider
    ) -> Optional[ResourceCandidate]:
        try:
            item.create_id()

            if not item.unique_id:
                return None

            if item.unique_id in self.processed_items:
                return None

            self.processed_items.append(item.unique_id)

        except ValueError as err:
            spider.logger.error(f"Error processing item: {err}")

        return item.produce_resource_candidate()
