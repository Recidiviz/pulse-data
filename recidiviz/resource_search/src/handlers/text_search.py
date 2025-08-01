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

"""Handler for text search"""

import logging

from recidiviz.persistence.database.schema.resource_search import schema
from recidiviz.resource_search.src.external_apis.base import ResourceQuery
from recidiviz.resource_search.src.handlers.helpers import (
    get_search_results,
    process_and_store_candidates,
    validate_subcategory,
)
from recidiviz.resource_search.src.typez.handlers.text_search import (
    TextSearchBodyParams,
)


async def text_search_handler(
    body: TextSearchBodyParams, db_name: str
) -> list[schema.Resource]:
    """Store a search result as a resource"""
    validate_subcategory(body.category, body.subcategory)

    query = ResourceQuery(
        category=body.category,
        subcategory=body.subcategory,
        textSearch=body.textSearch,
        pageSize=body.limit,
        pageOffset=body.offset,
    )

    logging.debug("Searching for resources with query: %s", query)

    # get external resources
    resource_candidates = await get_search_results(query)
    logging.debug("Found %s resources", len(resource_candidates))
    if not resource_candidates:
        return []
    return await process_and_store_candidates(
        body, resource_candidates, db_name=db_name
    )
