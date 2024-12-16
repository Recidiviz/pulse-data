# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Flag users currently labeled as Insights "primary users" (supervisors)
in the product roster that cannot be joined to state_staff via email address,
indicating a potential issue between ingest and product roster, possibly in
the email and/or role type columns.

These users are silently dropped when pulling some usage metrics at the supervisor level,
causing discrepancies between state & supervisor level metrics.

These errors can be fixed with one of the following changes:
- Ingesting the user into `state_staff` with their user email address
- Updating the user's role type so they are no longer considered a primary user
- Removing the user from the product roster if they should not have access to the tool
"""


from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

INSIGHTS_PRIMARY_USERS_NOT_IN_STATE_STAFF_VIEW_NAME = (
    "insights_primary_users_not_in_state_staff"
)

INSIGHTS_PRIMARY_USERS_NOT_IN_STATE_STAFF_QUERY_TEMPLATE = """
SELECT
    state_code AS region_code,
    *
FROM
    `{project_id}.analyst_data.insights_provisioned_user_registration_sessions_materialized` roster
WHERE
    -- Only check open registration sessions for primary users
    end_date_exclusive IS NULL
    AND is_primary_user
    AND staff_id IS NULL
"""

INSIGHTS_PRIMARY_USERS_NOT_IN_STATE_STAFF_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=INSIGHTS_PRIMARY_USERS_NOT_IN_STATE_STAFF_VIEW_NAME,
    view_query_template=INSIGHTS_PRIMARY_USERS_NOT_IN_STATE_STAFF_QUERY_TEMPLATE,
    description=__doc__,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INSIGHTS_PRIMARY_USERS_NOT_IN_STATE_STAFF_VIEW_BUILDER.build_and_print()
