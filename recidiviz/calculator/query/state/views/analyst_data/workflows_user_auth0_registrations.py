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
View that represents the date on which every workflows user (identified via email
address) first logged in to a Recidiviz tool while having access to Workflows.
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "workflows_user_auth0_registrations"

_VIEW_DESCRIPTION = """
View that represents the date on which every workflows user (identified via email
address) first logged in to a Recidiviz tool while having access to Workflows."""

_QUERY_TEMPLATE = """
WITH all_auth_events AS (
    SELECT
        state_code,
        email_address,
        signup_date AS event_date,
    FROM `{project_id}.analyst_data.all_auth0_signup_events_materialized`
    WHERE has_workflows_access

    UNION ALL

    SELECT 
        state_code,
        email_address,
        login_date AS event_date,
    FROM `{project_id}.analyst_data.all_auth0_login_events_materialized`
    WHERE has_workflows_access
)
SELECT
    state_code,
    email_address AS workflows_user_email_address,
    MIN(event_date) AS workflows_registration_date,
FROM
    all_auth_events
GROUP BY
    1, 2
"""

WORKFLOWS_USER_AUTH0_REGISTRATIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=_VIEW_NAME,
    description=_VIEW_DESCRIPTION,
    view_query_template=_QUERY_TEMPLATE,
    clustering_fields=["state_code"],
    should_materialize=True,
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        WORKFLOWS_USER_AUTH0_REGISTRATIONS_VIEW_BUILDER.build_and_print()
