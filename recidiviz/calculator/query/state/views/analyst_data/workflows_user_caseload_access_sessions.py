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
"""
View representing the assignments of all workflows users to their states over time.
Assumes that current product roster represents all historical assignments
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import MAGIC_START_DATE
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

WORKFLOWS_USER_CASELOAD_ACCESS_SESSIONS_VIEW_NAME = (
    "workflows_user_caseload_access_sessions"
)

WORKFLOWS_USER_CASELOAD_ACCESS_SESSIONS_VIEW_DESCRIPTION = """
View representing the assignments of all workflows users to their states over time.
Assumes that current product roster represents all historical assignments.

TODO(#26455): Add more granular district/caseload assignment information
"""

WORKFLOWS_USER_CASELOAD_ACCESS_SESSIONS_QUERY_TEMPLATE = f"""
SELECT
    state_code,
    email_address,
    CAST("{MAGIC_START_DATE}" AS DATE) AS start_date,
    CAST(NULL AS DATE) AS end_date_exclusive,
    routes_workflowsSupervision AS has_supervision_workflows,
    routes_workflowsFacilities AS has_facilities_workflows,
FROM
    `{{project_id}}.reference_views.product_roster_materialized`
WHERE
    routes_workflowsSupervision
    OR routes_workflowsFacilities
"""

WORKFLOWS_USER_CASELOAD_ACCESS_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=WORKFLOWS_USER_CASELOAD_ACCESS_SESSIONS_VIEW_NAME,
    description=WORKFLOWS_USER_CASELOAD_ACCESS_SESSIONS_VIEW_DESCRIPTION,
    view_query_template=WORKFLOWS_USER_CASELOAD_ACCESS_SESSIONS_QUERY_TEMPLATE,
    clustering_fields=["state_code"],
    should_materialize=True,
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        WORKFLOWS_USER_CASELOAD_ACCESS_SESSIONS_VIEW_BUILDER.build_and_print()
