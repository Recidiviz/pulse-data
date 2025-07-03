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
Foundational types and base models for resource search operations.

This module defines the core data models used throughout the resource search system, including pagination parameters, time and distance query parameters, and a generic API response model. These base types are extended by more specific parameter and response models in other modules.
"""

from typing import Generic, Optional, TypeVar

from pydantic import BaseModel

from recidiviz.resource_search.src.models.resource_enums import DistanceMode

T = TypeVar("T")


class PageQueryParams(BaseModel):
    limit: Optional[int] = 20
    offset: Optional[int] = 0


class TimeDistanceParams(BaseModel):
    distance: Optional[int] = None
    time: Optional[int] = None
    mode: Optional[DistanceMode] = None

    def __init__(self, **data: dict) -> None:
        super().__init__(**data)
        if not (self.distance is not None) != (self.time is not None):
            raise ValueError("Exactly one of 'distance' or 'time' must be provided")
        if self.time is not None and self.mode is None:
            raise ValueError("'mode' is required when 'time' is provided")


class APIResponse(BaseModel, Generic[T]):
    data: T
