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
Parameter models for resource store search handlers.

This module defines the data models used to represent the parameters for resource store search operations. It includes the `ResourceSearchBodyParams` class, which extends pagination and time/distance query parameter models, and adds fields for text search, resource category, subcategory, and address.
"""

from recidiviz.resource_search.src.models.resource_enums import (
    ResourceCategory,
    ResourceSubcategory,
)
from recidiviz.resource_search.src.typez.base import PageQueryParams, TimeDistanceParams


class ResourceSearchBodyParams(PageQueryParams, TimeDistanceParams):
    textSearch: str
    category: ResourceCategory
    subcategory: ResourceSubcategory
    address: str
