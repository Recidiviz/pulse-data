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
Base types and models for resource search handlers.

This module defines foundational data models used in resource search operations, including the `ApiSearchResult` class, which represents a resource search result with associated metadata such as location, contact information, category, and scoring details.
"""

import uuid as uuid_lib
from typing import Optional

from pydantic import BaseModel

from recidiviz.resource_search.src.models.resource_enums import (
    DistanceMode,
    ResourceCategory,
    ResourceSubcategory,
)


class ApiSearchResult(BaseModel):
    id: uuid_lib.UUID
    uri: str
    category: ResourceCategory
    name: str
    lat: float
    lon: float
    origin: str
    website: Optional[str] = None
    maps_url: Optional[str] = None
    street: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[list[str]] = None
    banned: Optional[bool] = None
    banned_reason: Optional[str] = None
    score: Optional[float] = None
    llm_rank: Optional[int] = None
    llm_valid: Optional[bool] = None
    rating: Optional[float] = None
    ratingCount: Optional[int] = None
    operationalStatus: Optional[str] = None
    price_level: Optional[str] = None
    subcategory: Optional[ResourceSubcategory] = None
    transport_mode: Optional[DistanceMode] = None
    transport_minutes: Optional[int] = None
