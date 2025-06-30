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
Defines the Resource data models for the resource search system.

This module contains Pydantic models representing resources, including their
categories, origins, location, contact information, and metadata. These models
are used for resource creation, validation, and management within the Recidiviz
resource search functionality.
"""

from typing import Any, List, Optional

from pydantic import BaseModel, field_validator

from recidiviz.resource_search.src.models.resource_enums import (
    CATEGORY_SUBCATEGORY_MAP,
    ResourceCategory,
    ResourceOrigin,
    ResourceSubcategory,
)


class ResourceBase(BaseModel):
    """
    Base Pydantic model representing a resource in the resource search system.

    This class defines the core fields and validation logic for a resource, including
    its category, origin, location, contact information, metadata, and other attributes.
    It serves as the foundation for resource creation, validation, and management
    within the Recidiviz resource search functionality.
    """

    uri: str
    category: Optional[ResourceCategory] = ResourceCategory.UNKNOWN
    origin: Optional[ResourceOrigin] = None

    location: Optional[Any] = None  # You can validate this further if needed
    embedding: Optional[list[float]] = None
    embedding_text: Optional[str] = None

    name: Optional[str] = None
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
    subcategory: Optional[ResourceSubcategory] = None

    banned: bool = False
    banned_reason: Optional[str] = None
    score: float = 5

    llm_rank: Optional[int] = None
    llm_valid: Optional[bool] = None

    extra_data: Optional[dict[str, Any]] = None
    google_places_rating: Optional[float] = None
    google_places_rating_count: Optional[int] = None

    @field_validator("subcategory")
    @classmethod
    def validate_subcategory(
        cls, subcategory: Optional[ResourceSubcategory], info: Any
    ) -> Optional[ResourceSubcategory]:
        if subcategory is None:
            return None
        category = info.data.get("category")
        if category is None:
            raise ValueError("Category must be provided when setting subcategory")
        valid_subcategories = CATEGORY_SUBCATEGORY_MAP.get(category, [])
        if subcategory not in valid_subcategories:
            raise ValueError(
                f"Subcategory {subcategory} is not valid for category {category}."
            )
        return subcategory


class ResourceCreate(ResourceBase):
    location: Optional[Any] = None
    embedding: Optional[Any] = None
    embedding_text: Optional[str] = None
    street: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    website: Optional[str] = None
    description: Optional[str] = None
    subcategory: Optional[ResourceSubcategory] = None
    tags: Optional[List[str]] = None
    banned: bool = False
    banned_reason: Optional[str] = None
    llm_rank: Optional[int] = None
    llm_valid: Optional[bool] = None
    maps_url: Optional[str] = None
    extra_data: Optional[dict[Any, Any]] = {}
