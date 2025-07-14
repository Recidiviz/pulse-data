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
"""Handler for parameter search"""

import logging
import os

from recidiviz.persistence.database.schema.resource_search import schema
from recidiviz.resource_search.src.external_apis.base import DistanceBias, ResourceQuery
from recidiviz.resource_search.src.external_apis.distance import (
    add_transport_info_to_resources,
    approximate_travel_distance,
    miles_to_meters,
)
from recidiviz.resource_search.src.external_apis.geocoding import point_from_address
from recidiviz.resource_search.src.handlers.helpers import (
    get_search_results,
    process_and_store_candidates,
    validate_subcategory,
)
from recidiviz.resource_search.src.typez.handlers.parameter_search import (
    ParameterSearchBodyParams,
)


async def parameter_search_handler(
    body: ParameterSearchBodyParams,
) -> list[schema.Resource]:
    """Search for resources from external APIs"""
    validate_subcategory(body.category, body.subcategory)

    if body.time is not None and body.mode is not None:
        body.distance = approximate_travel_distance(body.mode, body.time)

    verbose = os.getenv("VERBOSE_LOGGING", "false").lower() == "true"
    if verbose:
        logging.info("Verbose mode enabled: Logging all request statuses.")

    query = ResourceQuery(
        category=body.category,
        subcategory=body.subcategory,
        pageSize=body.limit,
        pageOffset=body.offset,
        resourceDescription=body.textSearch,
        address=body.address,
    )

    if body.distance:
        point = await point_from_address(body.address)

        # Ensure distance is within the valid range
        distance = min(miles_to_meters(body.distance), 50000)

        query.distance_bias = DistanceBias(
            lat=point.x,
            lon=point.y,
            radius=distance,
        )

    verbose = os.getenv("VERBOSE_LOGGING", "false").lower() == "true"
    if verbose:
        logging.info("Verbose mode enabled: Logging all request statuses.")

    logging.debug(
        "Searching for resources with query: %s", query.model_dump_json(indent=2)
    )

    # get external resources
    resource_candidates = await get_search_results(query)
    logging.debug("Found %s resources candidates", len(resource_candidates))
    if not resource_candidates:
        return []
    resources = await process_and_store_candidates(body, resource_candidates)
    logging.debug("Saved %s resources candidates", len(resource_candidates))

    if body.mode:
        try:
            resources = await add_transport_info_to_resources(
                mode=body.mode,
                origin_address=body.address,
                resources=resources,
            )
        except Exception as error:
            logging.error("Failed to get distances: %s", error)

    return resources
