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
Types and models for embedding operations.

This module defines the data models used for embedding-related operations in the resource search system. It includes the `EmbeddingParts` class, which represents the components of an embedding text, such as query, name, description, tags, and price level.
"""

from typing import Optional

from pydantic import BaseModel


class EmbeddingParts(BaseModel):
    """Parts of an embedding text"""

    query: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[list[str]] = None
    price_level: Optional[str] = None
