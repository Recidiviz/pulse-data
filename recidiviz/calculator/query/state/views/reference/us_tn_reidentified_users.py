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
"""View of re-identified dashboard users"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    REFERENCE_VIEWS_DATASET,
    STATIC_REFERENCE_TABLES_DATASET,
    US_TN_RAW_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_REIDENTIFIED_USERS_VIEW_NAME = "us_tn_reidentified_users"

US_TN_REIDENTIFIED_USERS_VIEW_DESCRIPTION = """View of re-identified dashboard users"""

US_TN_REIDENTIFIED_USERS_QUERY_TEMPLATE = """
    /* {description} */
    SELECT
        user_hash AS user_id,
        StaffId as user_external_id,
        state_code,
    FROM `{project_id}.{reference_views_dataset}.dashboard_user_restrictions` users
    LEFT JOIN `{project_id}.{static_reference_dataset}.us_tn_roster` tn_roster
        ON LOWER(users.restricted_user_email) = LOWER(tn_roster.email_address)
    LEFT JOIN `{project_id}.{us_tn_raw_dataset}.Staff_latest` staff
        ON tn_roster.external_id = staff.UserID
    WHERE state_code = "US_TN"  
"""

US_TN_REIDENTIFIED_USERS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=REFERENCE_VIEWS_DATASET,
    view_id=US_TN_REIDENTIFIED_USERS_VIEW_NAME,
    description=US_TN_REIDENTIFIED_USERS_VIEW_DESCRIPTION,
    view_query_template=US_TN_REIDENTIFIED_USERS_QUERY_TEMPLATE,
    reference_views_dataset=REFERENCE_VIEWS_DATASET,
    static_reference_dataset=STATIC_REFERENCE_TABLES_DATASET,
    us_tn_raw_dataset=US_TN_RAW_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_REIDENTIFIED_USERS_VIEW_BUILDER.build_and_print()
