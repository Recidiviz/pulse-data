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
"""Common util for external API plugins"""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

import aiohttp
from async_googlemaps import AsyncClient  # type: ignore

from recidiviz.resource_search.src.settings import settings


class ResourceApiPluginException(Exception):
    """Custom exception for resource API plugin errors"""


@asynccontextmanager
async def async_google_places_client() -> AsyncGenerator[AsyncClient, None]:
    """
    Context manager for creating a google places client instance

    Yields:
        An instance of async_googlemaps.AsyncClient
    """
    async with aiohttp.ClientSession() as http_session:
        client = AsyncClient(http_session, key=settings.google_places_api_key)
        yield client


@asynccontextmanager
async def async_googlemaps_client() -> AsyncGenerator[AsyncClient, None]:
    """
    Context manager for creating a googlemaps client instance

    Yields:
        An instance of async_googlemaps.AsyncClient
    """
    async with aiohttp.ClientSession() as http_session:
        client = AsyncClient(http_session, key=settings.google_geocoding_api_key)
        yield client
