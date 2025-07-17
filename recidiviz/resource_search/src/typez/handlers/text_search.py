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
Parameter models for text-based resource search handlers.

This module defines the data models used to represent the parameters for text-based resource search operations. It includes the `TextSearchBodyParams` class, which extends pagination query parameter models, and adds fields for resource category, text search, and an optional subcategory.
"""

from typing import Optional

from recidiviz.resource_search.src.models.resource_enums import (
    ResourceCategory,
    ResourceSubcategory,
)
from recidiviz.resource_search.src.typez.base import PageQueryParams


class TextSearchBodyParams(PageQueryParams):
    category: Optional[ResourceCategory] = None
    textSearch: str
    subcategory: Optional[ResourceSubcategory] = None
