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
"""Enum representing a type of product or product component that may have its own set of
tool features and/or functionality."""
from enum import Enum


class ProductType(Enum):
    """
    Represents a type of product or product component that may have its own set of
    features and/or functionality. Each product type is associated with a specific
    context page keyword and can be associated with a subset of Segment events.

    If changing the values of this enum, note that the name of the table that unions
    together all usage events for a given product type is derived from the value of
    the enum, e.g., `all_workflows_segment_events`. The name of the Segment dataset for
    a given product type is also derived from the value of the enum, e.g.,
    `segment_events__workflows`, and so downstream references should be
    updated accordingly. Generally we would not change the values of this enum,
    but rather add new values for new product types, unless it is helpful to break
    up impact tracking (Looker, leadership reports, etc.) for a new sub-component of an
    existing product.
    """

    CASE_NOTE_SEARCH = "CASE_NOTE_SEARCH"
    MILESTONES = "MILESTONES"
    PSI_CASE_INSIGHTS = "PSI_CASE_INSIGHTS"
    SUPERVISOR_HOMEPAGE_OUTCOMES_MODULE = "SUPERVISOR_HOMEPAGE_OUTCOMES_MODULE"
    SUPERVISOR_HOMEPAGE_OPPORTUNITIES_MODULE = (
        "SUPERVISOR_HOMEPAGE_OPPORTUNITIES_MODULE"
    )
    SUPERVISOR_HOMEPAGE_OPERATIONS_MODULE = "SUPERVISOR_HOMEPAGE_OPERATIONS_MODULE"
    TASKS = "TASKS"
    WORKFLOWS = "WORKFLOWS"

    @property
    def pretty_name(self) -> str:
        return self.value.lower()

    @property
    def context_page_keyword(self) -> str:
        """Returns the string contained in the url path returned by Segment events
        associated with a given product type. We use the context page keyword to filter
        Segment events by product type."""

        if self == ProductType.CASE_NOTE_SEARCH:
            return "workflows"
        if self == ProductType.MILESTONES:
            return "workflows/milestones"
        if self == ProductType.PSI_CASE_INSIGHTS:
            return "psi"
        if self == ProductType.SUPERVISOR_HOMEPAGE_OUTCOMES_MODULE:
            return "insights"
        if self == ProductType.SUPERVISOR_HOMEPAGE_OPPORTUNITIES_MODULE:
            return "insights"
        if self == ProductType.SUPERVISOR_HOMEPAGE_OPERATIONS_MODULE:
            return "operations"
        if self == ProductType.TASKS:
            return "workflows/tasks"
        if self == ProductType.WORKFLOWS:
            return "workflows"
        raise ValueError(f"Unknown context page for product type: {self}")

    @property
    def segment_dataset_name(self) -> str:
        """Returns the dataset for a segment view with a given product type."""
        return f"segment_events__{self.value.lower()}"
