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

"""Tests for the Google Places API plugin"""

import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import httpx

from recidiviz.resource_search.src.external_apis.base import (
    DistanceBias,
    SearchQuery,
    TextSearchQuery,
)
from recidiviz.resource_search.src.external_apis.plugins.google_places import (
    DEFAULT_FIELDS,
    LOCATION_BIAS_RADIUS_BY_MODE,
    GooglePlacesApiPlugin,
)
from recidiviz.resource_search.src.external_apis.utils import ResourceApiPluginException
from recidiviz.resource_search.src.models.resource_enums import (
    DistanceMode,
    ResourceCategory,
    ResourceOrigin,
)
from recidiviz.resource_search.src.typez.crud.resources import ResourceCandidate


class TestResourceSearchExternalAPI(unittest.IsolatedAsyncioTestCase):
    """
    Tests the Google Places API plugin integration.
    """

    async def test_text_search_success(self) -> None:
        mock_response_data = {
            "places": [
                {
                    "displayName": {"text": "Place 1"},
                    "formattedAddress": "123 Main St",
                    "types": ["point_of_interest"],
                    "googleMapsUri": "https://maps.google.com/1",
                    "location": {"latitude": 37.0, "longitude": -122.0},
                    "addressComponents": [
                        {"types": ["street_number"], "longText": "123"},
                        {"types": ["route"], "longText": "Main St"},
                        {"types": ["locality"], "longText": "City"},
                        {"types": ["administrative_area_level_1"], "longText": "State"},
                        {"types": ["postal_code"], "longText": "12345"},
                    ],
                    "rating": 4.5,
                    "userRatingCount": 100,
                    "businessStatus": "OPERATIONAL",
                    "reviews": [
                        {"text": {"languageCode": "en", "text": "What a great place!"}}
                    ],
                    "priceLevel": "PRICE_LEVEL_MODERATE",
                }
            ]
        }

        expected_search_results = [
            ResourceCandidate(
                name="Place 1",
                maps_url="https://maps.google.com/1",
                origin=ResourceOrigin.GOOGLE_PLACES,
                lat=37.0,
                lon=-122.0,
                street="123 Main St",
                city="City",
                state="State",
                zip="12345",
                phone=None,
                tags=["point_of_interest"],
                category=ResourceCategory.BASIC_NEEDS,
                rating=4.5,
                ratingCount=100,
                reviews=["What a great place!"],
                operationalStatus="OPERATIONAL",
                rawData=mock_response_data["places"][0],
                price_level="moderate",
            )
        ]

        with patch.dict(os.environ, {"GOOGLE_PLACES_API_KEY": "test-api-key"}):
            with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
                mock_response = MagicMock()
                mock_response.json.return_value = mock_response_data
                mock_response.status_code = 200
                mock_post.return_value = mock_response

                plugin = GooglePlacesApiPlugin()
                distance_mode = DistanceMode.WALKING
                query = TextSearchQuery(
                    category=ResourceCategory.BASIC_NEEDS,
                    distance_bias=DistanceBias(
                        lat=37.0, lon=-122.0, mode=distance_mode
                    ),
                    textSearch="test query",
                    pageSize=20,
                    pageOffset=None,
                )

                result = await plugin.text_search(query)

                mock_post.assert_called_once()
                call_kwargs = mock_post.call_args[1]
                self.assertEqual(
                    call_kwargs["headers"],
                    {
                        "X-Goog-Api-Key": "test-api-key",
                        "X-Goog-FieldMask": DEFAULT_FIELDS,
                    },
                )
                self.assertEqual(
                    call_kwargs["json"],
                    {
                        "textQuery": "test query",
                        "pageSize": 20,
                        "locationBias": {
                            "circle": {
                                "center": {"latitude": 37.0, "longitude": -122.0},
                                "radius": LOCATION_BIAS_RADIUS_BY_MODE[distance_mode],
                            }
                        },
                    },
                )

                self.assertEqual(result, expected_search_results)

    async def test_text_search_failure(self) -> None:
        with patch.dict(os.environ, {"GOOGLE_PLACES_API_KEY": "test-api-key"}):
            mock_request = httpx.Request(
                "POST", "https://places.googleapis.com/v1/places:searchText"
            )
            mock_response = httpx.Response(500, request=mock_request)
            with patch(
                "httpx.AsyncClient.post",
                new_callable=AsyncMock,
                side_effect=httpx.HTTPStatusError(
                    "Request failed", request=mock_request, response=mock_response
                ),
            ):
                plugin = GooglePlacesApiPlugin()
                query = TextSearchQuery(
                    category=ResourceCategory.BASIC_NEEDS,
                    textSearch="test query",
                    pageSize=None,
                    pageOffset=None,
                )

                with self.assertRaises(ResourceApiPluginException) as cm:
                    await plugin.text_search(query)
                self.assertIn("Google Places API request failed", str(cm.exception))

    async def test_missing_api_key(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            plugin = GooglePlacesApiPlugin()
            query = TextSearchQuery(
                category=ResourceCategory.BASIC_NEEDS,
                textSearch="test query",
                pageSize=None,
                pageOffset=None,
            )

            with self.assertRaises(ResourceApiPluginException) as cm:
                await plugin.text_search(query)
            self.assertIn("Google Places API key not found", str(cm.exception))

    async def test_search_success(self) -> None:
        mock_search_results = [
            ResourceCandidate(
                name="Place 1",
                maps_url="https://maps.google.com/1",
                origin=ResourceOrigin.GOOGLE_PLACES,
                lat=37.0,
                lon=-122.0,
                street="123 Main St",
                city="City",
                state="State",
                zip="12345",
                phone=None,
                tags=["point_of_interest"],
                category=ResourceCategory.BASIC_NEEDS,
                price_level="moderate",
                rawData={},
            )
        ]

        with patch.object(
            GooglePlacesApiPlugin, "text_search", new_callable=AsyncMock
        ) as mock_text_search:
            mock_text_search.return_value = mock_search_results

            plugin = GooglePlacesApiPlugin()
            query = SearchQuery(
                category=ResourceCategory.BASIC_NEEDS,
                address="Test City, TC, 12345",
                resourceDescription=None,
                pageSize=None,
                pageOffset=None,
            )

            result = await plugin.search(query)

            mock_text_search.assert_called_once()
            text_query = mock_text_search.call_args[0][0]
            self.assertEqual(text_query.textSearch, "Basic Needs  Test City, TC, 12345")
            self.assertEqual(text_query.pageSize, 20)
            self.assertEqual(text_query.category, ResourceCategory.BASIC_NEEDS)

            self.assertEqual(result, mock_search_results)
