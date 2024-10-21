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
View that contains all auth0 login events, formatted with flags indicating
which tools a user had access to at time of login.
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "all_auth0_login_events"

_VIEW_DESCRIPTION = """
View that contains all auth0 login events, formatted with flags indicating
which tools a user had access to at time of login."""

_QUERY_TEMPLATE = """
WITH all_logins AS (
    SELECT
        UPPER(state_code) AS state_code,
        LOWER(email) AS email_address,
        timestamp,
        (routes_workflows OR routes_workflows_supervision OR routes_workflows_facilities) AS has_workflows_access,
        routes_insights AS has_insights_access,
    FROM
        `{project_id}.auth0_events.success_login`
    
    UNION ALL
    
    SELECT
        UPPER(state_code) AS state_code,
        LOWER(email) AS email_address,
        timestamp,
        (routes_workflows OR routes_workflows_supervision OR routes_workflows_facilities) AS has_workflows_access,
        routes_insights AS has_insights_access,
    FROM
        `{project_id}.auth0_prod_action_logs.success_login`
)
SELECT
    CASE state_code WHEN "US_ID" THEN "US_IX" ELSE state_code END AS state_code,
    email_address,
    CAST(timestamp AS DATETIME) AS login_date,
    has_workflows_access,
    has_insights_access,
FROM
    all_logins
WHERE
    state_code IS NOT NULL
    AND state_code != "RECIDIVIZ"
    AND timestamp IS NOT NULL
"""

ALL_AUTH0_LOGIN_EVENTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=_VIEW_NAME,
    description=_VIEW_DESCRIPTION,
    view_query_template=_QUERY_TEMPLATE,
    clustering_fields=["state_code"],
    should_materialize=True,
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        ALL_AUTH0_LOGIN_EVENTS_VIEW_BUILDER.build_and_print()
