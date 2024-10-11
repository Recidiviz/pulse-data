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
View that represents the date on which every Insights user (identified via email
address) first logged in to a Recidiviz tool while having access to Insights.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "insights_user_signups"

_VIEW_DESCRIPTION = """
View that represents the date on which every Insights user (identified via email
address) first logged in to a Recidiviz tool while having access to Insights."""

_QUERY_TEMPLATE = """
#TODO(#34100): Refactor after pulling insights logins and signups into a shared view
WITH all_auth_events AS (
    SELECT
        UPPER(state_code) AS state_code,
        LOWER(email) AS insights_user_email_address,
        timestamp,
    FROM
        `{project_id}.auth0_prod_action_logs.success_signup`
    WHERE
        routes_insights
        
    UNION ALL
    
    SELECT
        UPPER(state_code) AS state_code,
        LOWER(email) AS insights_user_email_address,
        timestamp,
    FROM
        `{project_id}.auth0_prod_action_logs.success_login`
    WHERE
        routes_insights
)
SELECT
    CASE state_code WHEN "US_ID" THEN "US_IX" ELSE state_code END AS state_code,
    insights_user_email_address,
    CAST(MIN(timestamp) AS DATETIME) AS insights_signup_date,
FROM
    all_auth_events
WHERE
    state_code IS NOT NULL
    AND timestamp IS NOT NULL
GROUP BY
    1, 2
"""

INSIGHTS_USER_SIGNUPS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=_VIEW_NAME,
    description=_VIEW_DESCRIPTION,
    view_query_template=_QUERY_TEMPLATE,
    clustering_fields=["state_code"],
    should_materialize=True,
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INSIGHTS_USER_SIGNUPS_VIEW_BUILDER.build_and_print()
