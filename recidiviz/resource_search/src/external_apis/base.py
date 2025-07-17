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
"""Common types for external API plugins"""

from abc import ABC, abstractmethod
from typing import Optional

from pydantic import BaseModel

from recidiviz.resource_search.src.models.resource_enums import (
    DistanceMode,
    ResourceCategory,
    ResourceSubcategory,
)
from recidiviz.resource_search.src.typez.crud.resources import ResourceCandidate


class DistanceBias(BaseModel):
    lat: float
    lon: float
    mode: Optional[DistanceMode] = None
    radius: Optional[float] = None


class ResourceQuery(BaseModel):
    category: Optional[ResourceCategory] = None
    subcategory: Optional[ResourceSubcategory] = None
    distance_bias: Optional[DistanceBias] = None
    pageSize: Optional[int] = None
    pageOffset: Optional[int] = None
    resourceDescription: Optional[str] = None
    address: Optional[str] = None
    textSearch: Optional[str] = None


class ExternalApiPluginBase(ABC):
    """Base class for external API plugins"""

    @abstractmethod
    async def search(self, query: ResourceQuery) -> list[ResourceCandidate]:
        pass

    @abstractmethod
    def map_to_resource_candidate(self, data: dict) -> Optional[ResourceCandidate]:
        pass

    async def text_search(self, query: ResourceQuery) -> list[ResourceCandidate]:
        raise NotImplementedError("Text search is not supported.")
