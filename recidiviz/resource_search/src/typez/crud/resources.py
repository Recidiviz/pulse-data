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
Types and models for resource CRUD operations.

This module defines the data models used for create, read, update, and delete (CRUD) operations on resources. It includes query parameter models for resource searches and candidate models representing resources with associated metadata, such as location, contact information, category, scoring, and more.
"""

import uuid
from typing import Optional

from pydantic import BaseModel, ConfigDict
from shapely import Point  # type: ignore

from recidiviz.resource_search.src.models.resource_enums import (
    ResourceCategory,
    ResourceOrigin,
    ResourceSubcategory,
)
from recidiviz.resource_search.src.typez.base import PageQueryParams


class ResourceQueryParams(PageQueryParams):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    category: ResourceCategory
    subcategory: ResourceSubcategory
    position: Point
    distance: int = 1000
    embedding: list[float]


class ResourceCandidate(BaseModel):
    """
    Represents a resource entity with associated metadata for CRUD operations.

    This model includes fields for unique identification, origin, category, subcategory, location (latitude, longitude, address), contact information, descriptive attributes, scoring, LLM validation, and other metadata relevant to resource search and management.
    """

    id: Optional[uuid.UUID] = None
    origin: Optional[ResourceOrigin] = None
    category: Optional[ResourceCategory] = None
    subcategory: Optional[ResourceSubcategory] = None
    website: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    street: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    address: Optional[str] = None
    zip: Optional[str] = None
    name: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[list[str]] = None
    rating: Optional[float] = None
    ratingCount: Optional[int] = None
    reviews: Optional[list[str]] = None
    operationalStatus: Optional[str] = None
    llm_rank: Optional[int] = None
    llm_valid: Optional[bool] = None
    banned: Optional[bool] = False
    banned_reason: Optional[str] = None
    score: Optional[int] = 5
    rawData: Optional[dict] = None
    embedding_text: Optional[str] = None
    price_level: Optional[str] = None
    maps_url: Optional[str] = None


class ResourceCandidateWithURI(ResourceCandidate):
    uri: str


class ResourceCandidateWithURICoord(ResourceCandidateWithURI):
    lat: float
    lon: float
