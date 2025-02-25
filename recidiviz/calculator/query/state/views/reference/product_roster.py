# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""View of all users that may have access to Polaris products"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import merge_permissions_query_str
from recidiviz.calculator.query.state import dataset_config
from recidiviz.case_triage.views.dataset_config import CASE_TRIAGE_FEDERATED_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PRODUCT_ROSTER_VIEW_NAME = "product_roster"

PRODUCT_ROSTER_DESCRIPTION = """View of all users that may have access to Polaris products.
Pulls data from roster Cloud SQL tables. Should only be used for Polaris product-related views."""

PRODUCT_ROSTER_QUERY_TEMPLATE = f"""
    -- Unnest user roles into separate rows
    WITH user_roles AS (
        SELECT
            COALESCE(user_override.email_address, roster.email_address) AS email_address,
            COALESCE(user_override.state_code, roster.state_code) AS state_code,
            role
        FROM
            `{{project_id}}.{{case_triage_federated_dataset_id}}.roster` roster
        FULL OUTER JOIN
            `{{project_id}}.{{case_triage_federated_dataset_id}}.user_override` user_override
        USING (email_address)
        CROSS JOIN
            UNNEST(COALESCE(user_override.roles, roster.roles)) AS role
    ),
    -- Combine the permissions from each role into arrays
    aggregated_permissions AS (
        SELECT
            user_roles.email_address AS email_address,
            ARRAY_AGG(state_role.routes) AS routes,
            ARRAY_AGG(state_role.feature_variants) AS feature_variants
        FROM
            user_roles
        FULL OUTER JOIN
            `{{project_id}}.{{case_triage_federated_dataset_id}}.state_role_permissions` state_role
        ON
            user_roles.state_code = state_role.state_code
            AND user_roles.role = state_role.role 
        GROUP BY
            email_address,
            user_roles.state_code    
    ),
    {merge_permissions_query_str("routes", "aggregated_permissions")},
    {merge_permissions_query_str("feature_variants", "aggregated_permissions")},
    final_permissions AS (
        SELECT
            aggregated_permissions.email_address,
            merged_routes.routes,
            merged_feature_variants.feature_variants
        FROM
            aggregated_permissions
        FULL OUTER JOIN
            merged_routes
        USING(email_address)
        FULL OUTER JOIN
            merged_feature_variants
        USING(email_address)
    ),
    product_roster_permissions AS (
        SELECT
            {{columns_query}}
        FROM
            `{{project_id}}.{{case_triage_federated_dataset_id}}.roster` roster
        FULL OUTER JOIN
            `{{project_id}}.{{case_triage_federated_dataset_id}}.user_override` user_override
        USING (email_address)
        FULL OUTER JOIN
            final_permissions
        USING(email_address)
        FULL OUTER JOIN
            `{{project_id}}.{{case_triage_federated_dataset_id}}.permissions_override` permissions_override
        USING(email_address)
    )
    SELECT
        {{joined_columns}},
        {{expanded_routes}}
    FROM product_roster_permissions
"""

ROSTER_COLUMNS = [
    "state_code",
    "external_id",
    "email_address",
    "roles",
    "district",
    "user_hash",
    "pseudonymized_id",
    "first_name",
    "last_name",
]

PERMISSIONS_COLUMNS = [
    "routes",
    "feature_variants",
]

ROUTES = [
    "system_libertyToPrison",
    "system_prison",
    "system_prisonToSupervision",
    "system_supervision",
    "system_supervisionToPrison",
    "system_supervisionToLiberty",
    "operations",
    "workflows",
    "workflowsSupervision",
    "workflowsFacilities",
    "insights",
    "insights_supervision_supervisors-list",
    "psi",
    "lantern",
]

PRODUCT_ROSTER_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    case_triage_federated_dataset_id=CASE_TRIAGE_FEDERATED_DATASET,
    view_id=PRODUCT_ROSTER_VIEW_NAME,
    view_query_template=PRODUCT_ROSTER_QUERY_TEMPLATE,
    description=PRODUCT_ROSTER_DESCRIPTION,
    should_materialize=True,
    columns_query="\n            ".join(
        [
            f"COALESCE(user_override.{col}, roster.{col}) AS {col},"
            for col in ROSTER_COLUMNS
        ]
        + [
            f"""final_permissions.{col} AS default_{col},
            permissions_override.{col} AS override_{col},"""
            for col in PERMISSIONS_COLUMNS
        ]
    ),
    joined_columns=",\n        ".join(
        ROSTER_COLUMNS
        + [
            new_col
            for col in PERMISSIONS_COLUMNS
            for new_col in (f"default_{col}", f"override_{col}")
        ]
    ),
    expanded_routes="\n        ".join(
        [
            f"COALESCE(CAST(JSON_VALUE(override_routes, '$.{route}') AS BOOL), CAST(JSON_VALUE(default_routes, '$.{route}') AS BOOL), FALSE) AS routes_{route.replace('-', '_')},"
            for route in ROUTES
        ]
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PRODUCT_ROSTER_VIEW_BUILDER.build_and_print()
