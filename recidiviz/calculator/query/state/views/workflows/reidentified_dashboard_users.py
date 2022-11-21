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
"""View of re-identified dashboard users, linking segment users to the officer ids"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    REFERENCE_VIEWS_DATASET,
    WORKFLOWS_VIEWS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REIDENTIFIED_DASHBOARD_USERS_VIEW_NAME = "reidentified_dashboard_users"

REIDENTIFIED_DASHBOARD_USERS_VIEW_DESCRIPTION = (
    """View of re-identified dashboard users"""
)

REIDENTIFIED_DASHBOARD_USERS_QUERY_TEMPLATE = """
SELECT
    user_hash AS user_id,
    rosters.id AS user_external_id,
    users.state_code,
FROM `{project_id}.{reference_views_dataset}.dashboard_user_restrictions_materialized` users
INNER JOIN `{project_id}.{workflows_views_dataset}.staff_record_materialized` rosters
    ON users.state_code = rosters.state_code
    AND LOWER(users.restricted_user_email) = LOWER(rosters.email)
"""

REIDENTIFIED_DASHBOARD_USERS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=WORKFLOWS_VIEWS_DATASET,
    view_id=REIDENTIFIED_DASHBOARD_USERS_VIEW_NAME,
    description=REIDENTIFIED_DASHBOARD_USERS_VIEW_DESCRIPTION,
    view_query_template=REIDENTIFIED_DASHBOARD_USERS_QUERY_TEMPLATE,
    reference_views_dataset=REFERENCE_VIEWS_DATASET,
    workflows_views_dataset=WORKFLOWS_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        REIDENTIFIED_DASHBOARD_USERS_VIEW_BUILDER.build_and_print()
