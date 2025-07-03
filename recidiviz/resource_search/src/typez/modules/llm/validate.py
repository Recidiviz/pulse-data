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
Types and models for LLM-based validation and ranking modules.

This module defines the data models used for validation and ranking operations performed by large language models (LLMs). It includes input models for LLMs, as well as output models for validation, ranking, and merged results.
"""

from typing import Optional

from pydantic import BaseModel


class LlmInput(BaseModel):
    """Map the data for the LLM"""

    tags: Optional[list[str]] = None
    rating: Optional[float] = None
    id: str
    ratingCount: Optional[int] = None
    name: Optional[str] = None
    reviews: Optional[list[str]] = None
    operationalStatus: Optional[str] = None


class ValidationOutput(BaseModel):
    id: str
    valid: bool


class RankingOutput(BaseModel):
    id: str
    rank: int


class MergedOutput(BaseModel):
    rank: Optional[int] = None
    valid: Optional[bool] = None
