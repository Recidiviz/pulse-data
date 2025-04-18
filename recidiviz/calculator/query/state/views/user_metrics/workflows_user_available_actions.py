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
"""Surface all the Workflows users with an actionable caseload currently visible in the tool:
- At least 1 client eligible & not marked ineligible/submitted for a fully launched opportunity
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION
from recidiviz.utils.metadata import local_project_id_override

WORKFLOWS_USER_AVAILABLE_ACTIONS_VIEW_NAME = "workflows_user_available_actions"

WORKFLOWS_USER_AVAILABLE_ACTIONS_QUERY_TEMPLATE = """
SELECT
    metrics.state_code,
    metrics.officer_id,
    metrics.officer_name,
    users.workflows_user_email_address,
    IFNULL(users.location_name, users.location_id) AS location_name,
    (
        IFNULL(metrics.workflows_distinct_people_eligible_and_actionable, 0)
        + IFNULL(metrics.workflows_distinct_people_almost_eligible_and_actionable, 0)
    ) AS total_opportunities,
FROM
    `{project_id}.user_metrics.workflows__supervision_officer_aggregated_metrics_materialized` metrics
INNER JOIN
    `{project_id}.analyst_data.workflows_provisioned_user_registration_sessions_materialized` users
ON
    users.state_code = metrics.state_code
    AND users.staff_external_id = metrics.officer_id
    AND users.end_date_exclusive IS NULL
WHERE
    # Pull the current day metrics representing the latest data
    metrics.period = "CURRENT_DAY"

    # Only include users with available Workflows opportunities
    AND (
        IFNULL(metrics.workflows_distinct_people_eligible_and_actionable, 0)
        + IFNULL(metrics.workflows_distinct_people_almost_eligible_and_actionable, 0)
    ) > 0

    # Do not include users that have access to Insights and Workflows
    # since the Insights users are included in a different view
    AND IFNULL(metrics.distinct_provisioned_insights_users, 0) != 1
"""

WORKFLOWS_USER_AVAILABLE_ACTIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.USER_METRICS_DATASET_ID,
    view_id=WORKFLOWS_USER_AVAILABLE_ACTIONS_VIEW_NAME,
    view_query_template=WORKFLOWS_USER_AVAILABLE_ACTIONS_QUERY_TEMPLATE,
    description=__doc__,
    clustering_fields=["state_code"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_PRODUCTION):
        WORKFLOWS_USER_AVAILABLE_ACTIONS_VIEW_BUILDER.build_and_print()
