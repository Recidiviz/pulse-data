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
    CLIENT_PAGE = "CLIENT_PAGE"
    MILESTONES = "MILESTONES"
    PSI_CASE_INSIGHTS = "PSI_CASE_INSIGHTS"
    SUPERVISOR_HOMEPAGE_OUTCOMES_MODULE = "SUPERVISOR_HOMEPAGE_OUTCOMES_MODULE"
    SUPERVISOR_HOMEPAGE_OPPORTUNITIES_MODULE = (
        "SUPERVISOR_HOMEPAGE_OPPORTUNITIES_MODULE"
    )
    SUPERVISOR_HOMEPAGE_OPERATIONS_MODULE = "SUPERVISOR_HOMEPAGE_OPERATIONS_MODULE"
    TASKS = "TASKS"
    WORKFLOWS = "WORKFLOWS"
    VITALS = "VITALS"

    @property
    def pretty_name(self) -> str:
        return self.value.lower()

    def context_page_filter_query_fragment(
        self, context_page_url_col_name: str = "context_page_path"
    ) -> str:
        """Returns the query fragment that identifies the Segment event url paths
        associated with a given product type. Since some segment events are shared across
        multiple product surfaces, we use the context page keyword to further filter
        Segment events by product type based on where they were triggered in the UI."""

        if self == ProductType.CASE_NOTE_SEARCH:
            return f"REGEXP_CONTAINS({context_page_url_col_name}, r'/workflows')"
        if self == ProductType.CLIENT_PAGE:
            return f"REGEXP_CONTAINS({context_page_url_col_name}, r'/workflows/clients|/workflows/residents')"
        if self == ProductType.MILESTONES:
            return f"REGEXP_CONTAINS({context_page_url_col_name}, r'/workflows/milestones')"
        if self == ProductType.PSI_CASE_INSIGHTS:
            return f"REGEXP_CONTAINS({context_page_url_col_name}, r'/psi')"
        if self == ProductType.SUPERVISOR_HOMEPAGE_OUTCOMES_MODULE:
            return f"REGEXP_CONTAINS({context_page_url_col_name}, r'/insights')"
        if self == ProductType.SUPERVISOR_HOMEPAGE_OPPORTUNITIES_MODULE:
            return f"REGEXP_CONTAINS({context_page_url_col_name}, r'/insights')"
        if self == ProductType.SUPERVISOR_HOMEPAGE_OPERATIONS_MODULE:
            return f"REGEXP_CONTAINS({context_page_url_col_name}, r'/insights')"
        if self == ProductType.TASKS:
            return f"REGEXP_CONTAINS({context_page_url_col_name}, r'/workflows/tasks')"
        if self == ProductType.WORKFLOWS:
            return (
                f"REGEXP_CONTAINS({context_page_url_col_name}, r'/workflows') "
                f"AND NOT REGEXP_CONTAINS({context_page_url_col_name}, r'/workflows/(clients|residents|milestones|tasks)')"
            )
        if self == ProductType.VITALS:
            return f"REGEXP_CONTAINS({context_page_url_col_name}, r'/operations')"
        raise ValueError(f"Unknown context page filter for product type: {self}")

    @property
    def segment_dataset_name(self) -> str:
        """Returns the dataset for a segment view with a given product type."""
        return f"segment_events__{self.value.lower()}"

    @property
    def columns_to_include_in_unioned_segment_view(self) -> list[str]:
        """Returns any additional attribute columns that should be included in the
        unioned Segment view for this product type.
        """
        if self in [
            ProductType.WORKFLOWS,
            ProductType.SUPERVISOR_HOMEPAGE_OPPORTUNITIES_MODULE,
            ProductType.CLIENT_PAGE,
            ProductType.TASKS,
        ]:
            return ["opportunity_type"]
        return []
