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
# ============================================================================
"""Input models and configuration for the scraper engine."""
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field

from recidiviz.resource_search.scraper.engine.items import (
    CategoryMapperType,
    FieldTypes,
)


class SelectorMethod(Enum):
    REGEX = "regex"
    CSS = "css"
    XPATH = "xpath"


class ScraperNodeType(Enum):
    """
    Enum that represents the type of the node in the scraper tree

    * ROOT - Root node, here all links will be followed;
    * ITEM - Leaf node, here only data will be scrapped and passed to the parent group;
    * GROUP - Group node, here data will be scrapped and links will be followed.
    """

    ROOT = "root"
    ITEM = "item"
    GROUP = "group"


class PaginationType(Enum):
    FRAGMENT_PAGINATION = "fragment_pagination"


class InputSelector(BaseModel):
    field_type: FieldTypes = Field(
        ..., description="Which type is corresponding to this value"
    )

    keywords: List[str] = Field(
        [], title="Keywords", description="Optional keywords to search for"
    )

    pattern: str = Field(..., title="Pattern", description="Pattern to search for")
    method: SelectorMethod = Field(
        SelectorMethod.XPATH, description="Which method to use to extract this pattern"
    )


class PaginationSelector(BaseModel):
    pagination_type: PaginationType = Field(
        PaginationType.FRAGMENT_PAGINATION, description="Pagination type"
    )

    pattern: str = Field(
        ...,
        description="""
Used to define pagination selector.
in most cases it's optional, as if the pagination is implemented using navigation and not fragments scraper will gather these links automatically.
        """,
    )
    method: SelectorMethod = Field(
        SelectorMethod.XPATH, description="Which method to use to extract this pattern"
    )


class ScraperNode(BaseModel):
    node_type: ScraperNodeType = Field(..., description="Tree node type")
    selectors: List[InputSelector] = []

    pattern: str = Field(
        ...,
        description="""Pattern that extracts a portion of html
        code and uses it to search for the nested selectors.""",
    )
    method: SelectorMethod = Field(
        SelectorMethod.XPATH, description="Which method to use to extract this pattern"
    )

    children: List["ScraperNode"] = []


class ScraperTree(BaseModel):

    pattern: str
    children: list[ScraperNode]

    node_type: ScraperNodeType = ScraperNodeType.ROOT
    pagination: Optional[PaginationSelector] = None


class ScraperInput(BaseModel):
    url: str = Field(..., title="URL", description="URL that will be processed")

    allowed_domains: List[str] = Field(
        ..., title="Allowed Domains", description="Allowed domains"
    )
    input_tree: ScraperTree
    name: str

    category_mapper: CategoryMapperType = Field(
        {},
        title="Category Mapper",
        description="""
A mapping dictionary that aids in categorizing and standardizing scraped or imported resource data by matching found text to predefined subcategories. This structure enables flexible text-to-category mapping for data integration and normalization.

Structure:
- Key: Categories enum value representing a main resource category (e.g., BASIC_NEEDS, EDUCATION)
- Value: List of tuples, where each tuple contains:
  1. Subcategories enum value: The standardized subcategory to map to
  2. String pattern: Raw text pattern that, if found in source data, should be mapped to this subcategory

Features:
- Supports multiple text patterns mapping to the same subcategory
- Enables fuzzy matching of various text representations to standardized categories
- Handles variations in how resources might be described across different data sources
- Maintains data consistency through standardized enum mappings

Example Usage:
{
    Categories.BASIC_NEEDS: [
        (Subcategories.HOUSING, "Emergency shelter"),
        (Subcategories.HOUSING, "Temporary housing"),
        (Subcategories.CLOTHING, "Free clothing bank")
    ],
    Categories.EMPLOYMENT_AND_CAREER_SUPPORT: [
        (Subcategories.CLOTHING, "Interview clothes")
    ]
}
        """,
    )
