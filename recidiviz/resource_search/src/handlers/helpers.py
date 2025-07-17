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
"""Helpers for handlers"""

import asyncio
import logging
from typing import List, Optional, Union

import httpx
from quart import abort

from recidiviz.persistence.database.schema.resource_search import schema
from recidiviz.resource_search.src.crud.helpers import (
    ResourceCreate,
    backfill_resource_uri,
    make_resource_create,
)
from recidiviz.resource_search.src.crud.resources import upsert_resources
from recidiviz.resource_search.src.db import transaction_session
from recidiviz.resource_search.src.embedding.helpers import make_embedding_text_batch
from recidiviz.resource_search.src.external_apis.base import ResourceQuery
from recidiviz.resource_search.src.external_apis.geocoding import (
    get_coordinates_from_addresses_with_ids,
)
from recidiviz.resource_search.src.external_apis.plugins.google_places import (
    GooglePlacesApiPlugin,
)
from recidiviz.resource_search.src.external_apis.utils import ResourceApiPluginException
from recidiviz.resource_search.src.models.resource_enums import (
    CATEGORY_SUBCATEGORY_MAP,
    ResourceCategory,
    ResourceSubcategory,
)
from recidiviz.resource_search.src.modules.llm.validate import run_validation
from recidiviz.resource_search.src.typez.crud.resources import (
    ResourceCandidate,
    ResourceCandidateWithURICoord,
)
from recidiviz.resource_search.src.typez.handlers.parameter_search import (
    ParameterSearchBodyParams,
)
from recidiviz.resource_search.src.typez.handlers.text_search import (
    TextSearchBodyParams,
)


async def get_search_results(
    query: ResourceQuery,
) -> List[ResourceCandidate]:
    """Fetch and transform search results from external API"""
    try:
        google_places_plugin = GooglePlacesApiPlugin()
        responses = await asyncio.gather(google_places_plugin.text_search(query))

        # Flatten and transform the search results
        return [
            ResourceCandidate(**result.model_dump())
            for response in responses
            for result in response
        ]
    except httpx.HTTPError as error:
        # Handles network and HTTP-related errors
        abort(502, f"Failed to connect to external service: {error}")
    except ResourceApiPluginException as error:
        # Handles API-specific errors (like missing API key)
        abort(502, f"External API error: {error}")
    except (ValueError, TypeError) as error:
        # Handles data transformation errors
        abort(400, f"Invalid response format: {error}")


async def process_and_store_candidates(
    body: Union[TextSearchBodyParams, ParameterSearchBodyParams],
    resource_candidates: list[ResourceCandidate],
) -> list[schema.Resource]:
    """Process and store API search results"""
    resources_with_uris = backfill_resource_uri(resource_candidates)

    logging.debug("Validating %s candidates", len(resources_with_uris))
    validated_candidates = await run_validation(
        resources_with_uris,
        body.category,
        body.subcategory,
    )

    embeddings = await make_embedding_text_batch(
        getattr(body, "textSearch", "") or "", validated_candidates
    )

    coordinates_by_id = await get_coordinates_from_addresses_with_ids(
        [
            (
                candidate.address
                or f"{candidate.street} {candidate.city} {candidate.state} {candidate.zip}",
                candidate.uri,
            )
            for candidate in validated_candidates
            if not candidate.lat or not candidate.lon
        ]
    )

    resources_to_create: list[ResourceCreate] = []
    for idx, candidate in enumerate(validated_candidates):
        if coordinates := coordinates_by_id.get(candidate.uri):
            resources_to_create.append(
                make_resource_create(
                    ResourceCandidateWithURICoord(
                        **candidate.model_dump(exclude={"lat", "lon"}),
                        lat=coordinates[0],
                        lon=coordinates[1],
                    ),
                    embeddings[idx],
                )
            )
        elif candidate.lat and candidate.lon:
            resources_to_create.append(
                make_resource_create(
                    ResourceCandidateWithURICoord(**candidate.model_dump()),
                    embeddings[idx],
                )
            )
        else:
            logging.warning("No coordinates found for %s", candidate.uri)

    async with transaction_session() as session:
        logging.debug("Storing %s resources", len(validated_candidates))
        resources = await upsert_resources(session, resources_to_create)

        logging.debug("Stored %s resources", len(resources))

    return resources


def validate_subcategory(
    category: Optional[ResourceCategory] = None,
    subcategory: Optional[ResourceSubcategory] = None,
) -> None:
    """Check if a subcategory is valid for a category"""
    if subcategory is None:
        return
    valid_subcategories = (
        CATEGORY_SUBCATEGORY_MAP.get(category, []) if category is not None else []
    )
    if subcategory not in valid_subcategories:
        raise ValueError(
            f"Subcategory {subcategory} is not valid for category {category}."
        )
