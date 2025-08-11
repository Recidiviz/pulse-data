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

"""
Asynchronous geocoding utilities for address-to-coordinate conversion.

This module provides helper functions to convert address strings into geographic coordinates (latitude and longitude) using external geocoding APIs. It supports both single and batch address lookups, handles error cases, and returns results as shapely Points or coordinate tuples. Designed for use in resource search and location-based features.
"""

import asyncio
import logging
from collections import ChainMap
from typing import Dict, Tuple

from shapely import Point  # type: ignore

from recidiviz.resource_search.src.external_apis.utils import async_googlemaps_client
from recidiviz.resource_search.src.settings import Settings


async def point_from_address(settings: Settings, address: str) -> Point:
    """
    Get a point from a full address string
    """
    coordinates = await get_coordinates_from_address_str(
        settings=settings, address=address
    )
    return Point(*coordinates)


# pylint: disable=invalid-name
async def get_coordinates_from_address_str(
    settings: Settings, address: str
) -> Tuple[float, float]:
    """
    Get coordinates of an address

    Args:
        address: Address string to get coordinates for

    Returns:
        Tuple of (latitude, longitude)

    Raises:
        ValueError: If no coordinates found for the address
    """
    async with async_googlemaps_client(settings=settings) as client:
        geocoding = await client.geocode(address=address)

        if not geocoding or len(geocoding) == 0:
            raise ValueError(f"No coordinates found for address: {address}")
        if (
            not geocoding[0]
            or "geometry" not in geocoding[0]
            or "location" not in geocoding[0]["geometry"]
        ):
            raise ValueError(f"No coordinates found for address: {address}")

        location = geocoding[0]["geometry"]["location"]
        return location["lat"], location["lng"]


async def get_coordinates_from_addresses_with_ids(
    settings: Settings,
    addresses: list[Tuple[str, str]],
) -> Dict[str, Tuple[float, float]]:
    """
    Get coordinates for multiple addresses concurrently. Beware of 3000rq/min rate limit.

    Args:
        address: List of id-Address pair to get coordinates for

    Returns:
        Dictionary of tuples containing (id: latitude, longitude)

    Raises:
        ValueError: If no coordinates found for the address
    """

    async def process_address(
        addr_tuple: Tuple[str, str],
    ) -> Dict[str, Tuple[float, float]]:
        """Helper function to process a single address and format return value"""
        try:
            address, id_str = addr_tuple

            coords = await get_coordinates_from_address_str(
                settings=settings, address=address
            )

            return {id_str: (coords[0], coords[1])}
        # pylint: disable=broad-except
        except Exception as err:
            logging.warning("Error while getting coordinates for %s: %s", address, err)
            return {id_str: (0.0, 0.0)}

    results = await asyncio.gather(*(process_address(addr) for addr in addresses))
    logging.debug("Found %i coordinates for %i addresses", len(results), len(addresses))
    return dict(ChainMap(*results))
