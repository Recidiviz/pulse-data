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
from typing import List, Optional

from recidiviz.common.constants.auth import RosterPredefinedRoles
from recidiviz.common.constants.states import StateCode
from recidiviz.common.str_field_utils import snake_to_camel, snake_to_title


class ProductType(Enum):
    """
    Represents a type of product or product component that may have its own set of
    features and/or functionality. Each product type is associated with a specific
    context page keyword and can be associated with a subset of Segment events.

    If changing the values of this enum, note that the name of the table that unions
    together all usage events for a given product type is derived from the value of
    the enum, e.g., `all_workflows_segment_events`. Generally we would not change the values of this enum,
    but rather add new values for new product types, unless it is helpful to break
    up impact tracking (Looker, leadership reports, etc.) for a new sub-component of an
    existing product.
    """

    CASE_NOTE_SEARCH = "CASE_NOTE_SEARCH"
    CASE_PLANNING_ASSISTANT = "CASE_PLANNING_ASSISTANT"
    CLIENT_PAGE = "CLIENT_PAGE"
    LANTERN = "LANTERN"
    MILESTONES = "MILESTONES"
    PATHWAYS = "PATHWAYS"
    PSI_CASE_INSIGHTS = "PSI_CASE_INSIGHTS"
    ROUTE_PLANNER = "ROUTE_PLANNER"
    SUPERVISOR_HOMEPAGE_OUTCOMES_MODULE = "SUPERVISOR_HOMEPAGE_OUTCOMES_MODULE"
    SUPERVISOR_HOMEPAGE_OPPORTUNITIES_MODULE = (
        "SUPERVISOR_HOMEPAGE_OPPORTUNITIES_MODULE"
    )
    SUPERVISOR_HOMEPAGE_OPERATIONS_MODULE = "SUPERVISOR_HOMEPAGE_OPERATIONS_MODULE"
    TASKS = "TASKS"
    VITALS = "VITALS"
    WORKFLOWS = "WORKFLOWS"

    @property
    def url_base(self) -> str:
        """Returns the base URL for this product type."""
        if self == ProductType.CASE_PLANNING_ASSISTANT:
            return "https://plan.recidiviz.org"
        return "https://dashboard.recidiviz.org"

    @property
    def pretty_name(self) -> str:
        return self.value.lower()

    @property
    def display_name(self) -> str:
        return snake_to_title(self.value)

    def is_primary_pages_product_type(self) -> bool:
        """Returns True if `pages` page view events based on context page filtering
        should be attributable to this product type. For example, if a page view occurs
        on the `/workflows` context page, we want to attribute this to the WORKFLOWS
        tool rather than the CASE_NOTE_SEARCH tool.
        If more than one of the products in the list that returns True have overlapping
        regex, then pages will be attributed to the first product in the conditional
        returned by `context_page_filter_query_fragment`."""
        if self in [
            ProductType.CASE_PLANNING_ASSISTANT,
            ProductType.CLIENT_PAGE,
            ProductType.LANTERN,
            ProductType.MILESTONES,
            ProductType.PATHWAYS,
            ProductType.PSI_CASE_INSIGHTS,
            ProductType.SUPERVISOR_HOMEPAGE_OPPORTUNITIES_MODULE,
            ProductType.TASKS,
            ProductType.ROUTE_PLANNER,
            ProductType.WORKFLOWS,
            ProductType.VITALS,
        ]:
            return True
        if self in [
            ProductType.CASE_NOTE_SEARCH,
            ProductType.SUPERVISOR_HOMEPAGE_OPERATIONS_MODULE,
            ProductType.SUPERVISOR_HOMEPAGE_OUTCOMES_MODULE,
        ]:
            return False
        raise ValueError(
            f"No configuration for `is_primary_pages_product_type` for product type: {self}"
        )

    def context_page_filter_query_fragment(
        self, context_page_url_col_name: str = "context_page_url"
    ) -> str:
        """Returns the query fragment that identifies the Segment event url paths
        associated with a given product type. Since some segment events are shared across
        multiple product surfaces, we use the url and context page keyword to further filter
        Segment events by product type based on where they were triggered in the UI."""

        # Ensure URL starts with the product's url_base
        url_base_check = f"STARTS_WITH({context_page_url_col_name}, '{self.url_base}')"

        # Path-based filtering for each product type
        if self == ProductType.CASE_NOTE_SEARCH:
            path_filter = f"REGEXP_CONTAINS({context_page_url_col_name}, r'/workflows')"
        elif self == ProductType.CASE_PLANNING_ASSISTANT:
            # No additional path filtering needed - just the url_base check
            return url_base_check
        elif self == ProductType.CLIENT_PAGE:
            path_filter = f"REGEXP_CONTAINS({context_page_url_col_name}, r'/workflows/clients|/workflows/residents')"
        elif self == ProductType.MILESTONES:
            path_filter = f"REGEXP_CONTAINS({context_page_url_col_name}, r'/workflows/milestones')"
        elif self == ProductType.PATHWAYS:
            path_filter = f"REGEXP_CONTAINS({context_page_url_col_name}, r'/system') AND NOT REGEXP_CONTAINS({context_page_url_col_name}, r'methodology') "
        elif self == ProductType.PSI_CASE_INSIGHTS:
            path_filter = f"REGEXP_CONTAINS({context_page_url_col_name}, r'/psi')"
        elif self == ProductType.SUPERVISOR_HOMEPAGE_OUTCOMES_MODULE:
            path_filter = f"REGEXP_CONTAINS({context_page_url_col_name}, r'/insights')"
        elif self == ProductType.SUPERVISOR_HOMEPAGE_OPPORTUNITIES_MODULE:
            path_filter = f"REGEXP_CONTAINS({context_page_url_col_name}, r'/insights')"
        elif self == ProductType.SUPERVISOR_HOMEPAGE_OPERATIONS_MODULE:
            path_filter = f"REGEXP_CONTAINS({context_page_url_col_name}, r'/insights')"
        elif self == ProductType.ROUTE_PLANNER:
            path_filter = f"REGEXP_CONTAINS({context_page_url_col_name}, r'/workflows/tasks/route-planner') "
        elif self == ProductType.TASKS:
            path_filter = (
                f"REGEXP_CONTAINS({context_page_url_col_name}, r'/workflows/tasks')"
                f"AND NOT REGEXP_CONTAINS({context_page_url_col_name}, r'/workflows/tasks/route-planner')"
            )
        elif self == ProductType.WORKFLOWS:
            path_filter = (
                f"REGEXP_CONTAINS({context_page_url_col_name}, r'/workflows') "
                f"AND NOT REGEXP_CONTAINS({context_page_url_col_name}, r'/workflows/(clients|residents|milestones|tasks)')"
            )
        elif self == ProductType.VITALS:
            path_filter = (
                f"REGEXP_CONTAINS({context_page_url_col_name}, r'/operations')"
            )
        elif self == ProductType.LANTERN:
            path_filter = (
                f"REGEXP_CONTAINS({context_page_url_col_name}, r'/revocations')"
            )
        else:
            raise ValueError(f"Unknown context page filter for product type: {self}")

        # Combine url_base check with path filter
        return f"({url_base_check}) AND ({path_filter})"

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

    @property
    def supported_states(self) -> list[StateCode] | None:
        """Returns the list of states that support this product type. If None, this
        product is ungated for all states."""
        if self == ProductType.MILESTONES:
            return [StateCode.US_CA]
        return None

    @property
    def auth_routes(self) -> list[str]:
        """Returns the routes that should be used to identify users who are provisioned
        to access this product type via auth tables. All routes should be in snake_case.
        """

        if self == ProductType.CASE_PLANNING_ASSISTANT:
            return ["cpa"]
        if self == ProductType.CLIENT_PAGE:
            return [
                "workflows",
                "workflows_supervision",
                "workflows_facilities",
                "tasks",
            ]
        if self == ProductType.LANTERN:
            return ["lantern"]
        if self == ProductType.MILESTONES:
            return ["workflows", "workflows_supervision"]
        if self == ProductType.PATHWAYS:
            return [
                "system_liberty_to_prison",
                "system_prison",
                "system_prison_to_supervision",
                "system_supervision",
                "system_supervision_to_prison",
                "system_supervision_to_liberty",
            ]
        if self == ProductType.PSI_CASE_INSIGHTS:
            return ["psi"]
        if self == ProductType.SUPERVISOR_HOMEPAGE_OUTCOMES_MODULE:
            return ["insights"]
        if self == ProductType.SUPERVISOR_HOMEPAGE_OPPORTUNITIES_MODULE:
            return ["insights"]
        if self == ProductType.SUPERVISOR_HOMEPAGE_OPERATIONS_MODULE:
            return ["insights"]
        if self == ProductType.TASKS:
            return ["tasks"]
        if self == ProductType.ROUTE_PLANNER:
            return ["tasks"]
        if self == ProductType.WORKFLOWS:
            return ["workflows", "workflows_supervision", "workflows_facilities"]
        if self == ProductType.VITALS:
            return ["operations"]
        return []

    @property
    def product_roster_routes(self) -> list[str]:
        """Returns the routes that should be used to identify users who are provisioned
        to access this product type via product roster. Converts the auth0 routes to camelCase
        to match the product roster schema.

        Note that auth0 actually derives its routes from the product roster routes,
        but we convert routes in this direction because of relative ease of
        snake -> camel conversion."""

        return [snake_to_camel(route) for route in self.auth_routes]

    @property
    def auth_feature_variants(self) -> list[str]:
        """Returns the feature variants that should be used to identify users who are provisioned
        to access this product type via auth0 tables. All feature variants should be in snake_case.
        """

        if self == ProductType.CASE_NOTE_SEARCH:
            return ["case_note_search"]
        if self == ProductType.ROUTE_PLANNER:
            return ["tasks_route_planner"]
        if self == ProductType.SUPERVISOR_HOMEPAGE_OPPORTUNITIES_MODULE:
            return ["supervisor_homepage_workflows"]
        if self == ProductType.SUPERVISOR_HOMEPAGE_OPERATIONS_MODULE:
            return ["supervisor_homepage"]
        # Note: Supervisor homepage outcomes module has an associated feature variant
        # (`outcomesModule`) but this variant is not granted via admin panel, so it is
        # not observable in auth0 tables. We just assume that users who are provisioned
        # based on routes will receive this feature variant in the frontend.
        return []

    @property
    def primary_role_types(self) -> list[str]:
        """Returns the primary role types for this product type in the product roster."""
        if self == ProductType.CASE_PLANNING_ASSISTANT:
            return ["cpa_staff"]
        if self == ProductType.SUPERVISOR_HOMEPAGE_OUTCOMES_MODULE:
            return [RosterPredefinedRoles.SUPERVISION_OFFICER_SUPERVISOR.value.lower()]
        if self == ProductType.SUPERVISOR_HOMEPAGE_OPPORTUNITIES_MODULE:
            return [RosterPredefinedRoles.SUPERVISION_OFFICER_SUPERVISOR.value.lower()]
        if self == ProductType.SUPERVISOR_HOMEPAGE_OPERATIONS_MODULE:
            return [RosterPredefinedRoles.SUPERVISION_OFFICER_SUPERVISOR.value.lower()]
        if self == ProductType.WORKFLOWS:
            return [
                RosterPredefinedRoles.SUPERVISION_LINE_STAFF.value.lower(),
                "supervision_officer",
                "supervision_staff",
                "facilities_line_staff",
                "facilities_staff",
            ]
        if self == ProductType.ROUTE_PLANNER:
            return [
                RosterPredefinedRoles.SUPERVISION_LINE_STAFF.value.lower(),
            ]
        if self == ProductType.TASKS:
            return [
                RosterPredefinedRoles.SUPERVISION_LINE_STAFF.value.lower(),
            ]
        return []

    @property
    def product_roster_feature_variants(self) -> list[str]:
        """Returns the feature variants that should be used to identify users who are provisioned
        to access this product type via product roster. Converts the auth0 feature variants to camelCase
        to match the product roster schema."""

        return [snake_to_camel(route) for route in self.auth_feature_variants]

    def get_product_roster_filter_query_fragment(
        self, table_prefix: Optional[str] = None
    ) -> str:
        """Returns the query fragment that identifies the whether a user in the
        product roster matches the permissions for a given product type, based on
        the product roster routes and feature variants defined for that product type."""

        table_prefix_str = f"{table_prefix}." if table_prefix else ""
        conditions = []
        if self.supported_states:
            supported_states_str = ", ".join(
                [f"'{state.value}'" for state in self.supported_states]
            )
            conditions.append(
                f"{table_prefix_str}state_code IN ({supported_states_str})"
            )
        # Append conditions for product roster routes and feature variants
        # Prioritize override routes over default routes
        if self.product_roster_routes:
            conditions.append(
                " OR ".join(
                    [
                        f"COALESCE(JSON_EXTRACT_SCALAR({table_prefix_str}override_routes, '$.{route}') = 'true', JSON_EXTRACT_SCALAR({table_prefix_str}default_routes, '$.{route}') = 'true')"
                        for route in self.product_roster_routes
                    ]
                )
            )
        if self.product_roster_feature_variants:
            conditions.append(
                " OR ".join(
                    [
                        f"JSON_EXTRACT_SCALAR({table_prefix_str}default_feature_variants, '$.{fv}') = 'true'"
                        for fv in self.product_roster_feature_variants
                    ]
                )
            )
        if not conditions:
            raise ValueError(
                f"No product roster routes or feature variants defined for product type: {self}"
            )
        return f"({' AND '.join(conditions)})"

    def get_auth_filter_query_fragment(
        self,
        table_prefix: Optional[str] = None,
        dummy_routes: Optional[List[str]] = None,
    ) -> str:
        """Returns the query fragment that identifies the whether a user in the
        auth0 tables matches the permissions for a given product type, based on
        the auth0 routes and feature variants defined for that product type.
        Indicate routes that are not present in an auth0 table by passing them into
        dummy_routes."""

        table_prefix_str = f"{table_prefix}." if table_prefix else ""
        conditions = []
        if self.supported_states:
            supported_states_str = ", ".join(
                [f"'{state.value}'" for state in self.supported_states]
            )
            conditions.append(
                f"{table_prefix_str}state_code IN ({supported_states_str})"
            )

        # Append conditions for product roster routes and feature variants
        if self.auth_routes:
            routes_query_fragment = " OR ".join(
                [
                    f"{table_prefix_str}routes_{route}"
                    for route in self.auth_routes
                    if route not in (dummy_routes or [])
                ]
            )
            conditions.append(
                routes_query_fragment if routes_query_fragment else "FALSE"
            )
        if self.auth_feature_variants:
            conditions.append(
                " OR ".join(
                    [
                        f"{table_prefix_str}feature_variants_{fv} IS TRUE"
                        for fv in self.auth_feature_variants
                    ]
                )
            )

        if not conditions:
            raise ValueError(
                f"No auth0 routes or feature variants defined for product type: {self}"
            )
        return f"({' AND '.join(conditions)})"
