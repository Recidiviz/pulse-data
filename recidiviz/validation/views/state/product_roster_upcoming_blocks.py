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
"""A view that surfaces users in the Polaris product roster who have upcoming block dates.
A user will have a block date set for one week in the future if they are present in Roster
but do not appear in the latest roster sync from their state. If the user was mistakenly
left out of the roster sync, the block will be removed if they appear in the next sync. 
Alternatively, the block can be removed from the admin panel.

Users who have only the "unknown" role and no custom permissions are excluded from this
validation because they don't have access to any products and therefore blocking them 
doesn't change anything."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

PRODUCT_ROSTER_UPCOMING_BLOCKS_VIEW_NAME = "product_roster_upcoming_blocks"

PRODUCT_ROSTER_UPCOMING_BLOCKS_DESCRIPTION = """This view surfaces when a Polaris product
user has an upcoming block date"""

PRODUCT_ROSTER_UPCOMING_BLOCKS_QUERY_TEMPLATE = """
  SELECT
    user_override.state_code,
    user_override.state_code AS region_code,
    user_override.email_address,
    user_override.blocked_on,
    user_override.external_id,
    user_override.district,
    user_override.roles,
    user_override.first_name,
    user_override.last_name,
    user_override.user_hash,
    user_override.pseudonymized_id, 
    product_roster_materialized.default_routes,
    product_roster_materialized.override_routes,
    product_roster_materialized.default_feature_variants,
    product_roster_materialized.override_feature_variants
  FROM 
    `{project_id}.case_triage_federated.user_override` user_override
  LEFT JOIN
    `{project_id}.case_triage_federated.permissions_override` permissions_override
  USING(email_address)
  LEFT JOIN
    `{project_id}.reference_views.product_roster_materialized` product_roster_materialized
  USING(email_address)
  WHERE 
    blocked_on > CURRENT_TIMESTAMP() AND
    NOT (ARRAY_LENGTH(user_override.roles) = 1 AND "unknown" IN UNNEST(user_override.roles) AND
      permissions_override.email_address IS NULL)
"""

PRODUCT_ROSTER_UPCOMING_BLOCKS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=PRODUCT_ROSTER_UPCOMING_BLOCKS_VIEW_NAME,
    view_query_template=PRODUCT_ROSTER_UPCOMING_BLOCKS_QUERY_TEMPLATE,
    description=PRODUCT_ROSTER_UPCOMING_BLOCKS_DESCRIPTION,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PRODUCT_ROSTER_UPCOMING_BLOCKS_VIEW_BUILDER.build_and_print()
