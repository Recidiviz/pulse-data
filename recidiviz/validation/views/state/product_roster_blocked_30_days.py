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
"""A view that surfaces users in the Polaris product roster who have been blocked for at
least 30 days."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

PRODUCT_ROSTER_BLOCKED_30_DAYS_VIEW_NAME = "product_roster_blocked_30_days"

PRODUCT_ROSTER_BLOCKED_30_DAYS_DESCRIPTION = """This view surfaces Polaris product
users who have been blocked for 30+ days and do not exist in the Roster table, and 
therefore can be safely deleted. The script to delete these users is located at https://github.com/Recidiviz/pulse-data/tree/main/recidiviz/tools/auth/delete_blocked_users.py"""

PRODUCT_ROSTER_BLOCKED_30_DAYS_QUERY_TEMPLATE = """
  SELECT
    user_override.state_code AS state_code,
    user_override.state_code AS region_code,
    user_override.email_address AS email_address,
    user_override.blocked_on AS blocked_on,
    user_override.external_id AS external_id,
    user_override.district AS district,
    user_override.roles AS roles,
    user_override.first_name AS first_name,
    user_override.last_name AS last_name,
    user_override.user_hash AS user_hash,
    user_override.pseudonymized_id As pseudonymized_id
  FROM 
    `{project_id}.case_triage_federated.user_override` user_override
  LEFT JOIN
    `{project_id}.case_triage_federated.roster` roster
  USING(email_address)
  WHERE 
    DATE_DIFF(CURRENT_TIMESTAMP(), blocked_on, DAY) >= 30 AND
    roster.email_address IS NULL
  ORDER BY blocked_on
"""

PRODUCT_ROSTER_BLOCKED_30_DAYS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=PRODUCT_ROSTER_BLOCKED_30_DAYS_VIEW_NAME,
    view_query_template=PRODUCT_ROSTER_BLOCKED_30_DAYS_QUERY_TEMPLATE,
    description=PRODUCT_ROSTER_BLOCKED_30_DAYS_DESCRIPTION,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PRODUCT_ROSTER_BLOCKED_30_DAYS_VIEW_BUILDER.build_and_print()
