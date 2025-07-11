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
"""Plugin for retrieving resources from the Google Places API"""

import logging
import os
from typing import Any, Dict, Optional

import httpx
from pydantic import ValidationError

from recidiviz.resource_search.src.external_apis.base import (
    ExternalApiPluginBase,
    SearchQuery,
    TextSearchQuery,
)
from recidiviz.resource_search.src.external_apis.utils import ResourceApiPluginException
from recidiviz.resource_search.src.models.resource_enums import (
    DistanceMode,
    ResourceOrigin,
)
from recidiviz.resource_search.src.typez.crud.resources import ResourceCandidate

GOOGLE_PLACES_API_BASE = "https://places.googleapis.com/v1/places"
DEFAULT_FIELDS = (
    "places.name,"
    "places.id,"
    "places.displayName,"
    "places.types,"
    "places.primaryType,"
    "places.primaryTypeDisplayName,"
    "places.nationalPhoneNumber,"
    "places.internationalPhoneNumber,"
    "places.formattedAddress,"
    "places.shortFormattedAddress,"
    "places.addressComponents,"
    "places.plusCode,"
    "places.location,"
    "places.rating,"
    "places.googleMapsUri,"
    "places.websiteUri,"
    "places.reviews,"
    "places.regularOpeningHours,"
    "places.businessStatus,"
    "places.priceLevel,"
    "places.attributions,"
    "places.currentOpeningHours,"
    "places.currentSecondaryOpeningHours,"
    "places.regularSecondaryOpeningHours,"
    "places.editorialSummary,"
    "places.paymentOptions,"
    "places.parkingOptions,"
    "places.generativeSummary,"
    "places.userRatingCount,"
    "places.accessibilityOptions,"
)

# Radius if no mode is specified
DEFAULT_LOCATION_BIAS_RADIUS = 5000
# Radius in meters for each mode
LOCATION_BIAS_RADIUS_BY_MODE = {
    DistanceMode.DRIVING: 10000,
    DistanceMode.WALKING: 1000,
    DistanceMode.BICYCLING: 3000,
    DistanceMode.TRANSIT: 5000,
}


class GooglePlacesApiPlugin(ExternalApiPluginBase):
    """Google Places API plugin offering structured and text search"""

    origin = ResourceOrigin.GOOGLE_PLACES

    async def text_search(self, query: TextSearchQuery) -> list[ResourceCandidate]:
        """
        Perform a text search using the Google Places API

        Args:
            query: TextSearchQuery containing search parameters

        Returns:
            ResourceQueryResponse with search results

        Raises:
            ResourceApiPluginException: If the API request fails
        """
        api_key = os.getenv("GOOGLE_PLACES_API_KEY")
        if not api_key:
            raise ResourceApiPluginException("Google Places API key not found")

        url = f"{GOOGLE_PLACES_API_BASE}:searchText"
        headers = {
            "X-Goog-Api-Key": api_key,
            "X-Goog-FieldMask": DEFAULT_FIELDS,
        }

        request_body: Dict[str, Any] = {"textQuery": query.textSearch}

        if bias := query.distance_bias:
            radius = bias.radius
            if radius is None and bias.mode is not None:
                radius = LOCATION_BIAS_RADIUS_BY_MODE[bias.mode]
            # Ensure radius is within the valid range
            radius = min(radius or DEFAULT_LOCATION_BIAS_RADIUS, 50000)

            request_body["locationBias"] = {
                "circle": {
                    "center": {"latitude": bias.lat, "longitude": bias.lon},
                    "radius": radius or DEFAULT_LOCATION_BIAS_RADIUS,
                }
            }

        if query.pageSize is not None:
            request_body["pageSize"] = query.pageSize

        async with httpx.AsyncClient() as client:
            try:
                logging.debug("Sending request to Google Places API: %s", request_body)
                response = await client.post(url, headers=headers, json=request_body)

                logging.debug(
                    "Received response from Google Places API: %s", response.text
                )
                response.raise_for_status()

                data = response.json()

                search_results = []
                for place in data.get("places", []):
                    result = self.map_to_resource_candidate(place)
                    if result:
                        result.category = query.category
                        result.subcategory = query.subcategory
                        search_results.append(result)

                return search_results

            except httpx.HTTPStatusError as error:
                logging.error("Google Places API request failed: %s", error)
                logging.error("Request URL: %s", url)
                logging.error("Request Body: %s", request_body)
                raise ResourceApiPluginException(
                    f"Google Places API request failed with status {error.response.status_code}: {error.response.text}"
                ) from error

    async def search(self, query: SearchQuery) -> list[ResourceCandidate]:
        """
        Perform a structured search using the Google Places API

        Args:
            query: SearchQuery containing search parameters

        Returns:
            ResourceQueryResponse with search results
        """

        # Extract location from address field
        location_string = query.address or ""
        category_string = (
            query.subcategory if query.subcategory else query.category or ""
        )

        # Use resource description directly without including category
        resource_description = query.resourceDescription or ""
        text_search = " ".join([category_string, resource_description, location_string])

        text_query = TextSearchQuery(
            category=query.category,
            subcategory=query.subcategory,
            distance_bias=query.distance_bias,
            textSearch=text_search,
            pageSize=query.pageSize or 20,
            pageOffset=query.pageOffset,
        )

        search_results = await self.text_search(text_query)
        return search_results

    def map_to_resource_candidate(self, data: dict) -> Optional[ResourceCandidate]:
        """
        Normalize the google place data

        Args:
            place: a Google Places API Place object

        Returns:
            The place data as an  ResourceCandidate
        """
        address_components = data.get("addressComponents", [])
        street = ""
        city = ""
        state = ""
        zip_code = ""

        for component in address_components:
            types = component.get("types", [])
            if "street_number" in types or "route" in types:
                street = (street + " " + component.get("longText", "")).strip()
            elif "locality" in types:
                city = component.get("longText", "")
            elif "administrative_area_level_1" in types:
                state = component.get("longText", "")
            elif "postal_code" in types:
                zip_code = component.get("longText", "")

        location = data.get("location", {})

        price_level = (
            str(data.get("priceLevel", "")).replace("PRICE_LEVEL_", "").lower()
            if data.get("priceLevel")
            else None
        )
        try:
            return ResourceCandidate(
                website=data.get("websiteUri"),
                maps_url=data.get("googleMapsUri"),
                origin=self.origin,
                lat=location.get("latitude", 0.0),
                lon=location.get("longitude", 0.0),
                street=street,
                city=city,
                state=state,
                zip=zip_code,
                name=data.get("displayName", {}).get("text"),
                phone=data.get("nationalPhoneNumber"),
                tags=data.get("types"),
                rating=data.get("rating"),
                ratingCount=data.get("userRatingCount"),
                reviews=[
                    review.get("text", {}).get("text", "")
                    for review in data.get("reviews", [])
                ],
                operationalStatus=data.get("businessStatus"),
                rawData=data,
                price_level=price_level,
            )
        except ValidationError as error:
            logging.warning("Error parsing Google Places API result: %s", error)
            logging.warning("Google Places API result: %s", data)
            logging.warning("Google Places URL: %s", data.get("googleMapsUri"))
            return None
