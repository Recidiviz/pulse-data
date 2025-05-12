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
-- Dedup product roster archive
WITH roster_archive_hashes AS (
    SELECT
        IF(state_code = "US_ID", "US_IX", state_code) as state_code,
        user_hash,
        external_id,
        district,
        email_address,
        first_name,
        last_name,
    FROM
        `{project_id}.reference_views.product_roster_archive_materialized`
    -- Filter to at most one row per user, getting the most recent district
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY state_code, email_address
        ORDER BY export_date DESC
    ) = 1
)
SELECT
    users.state_code,
    users.user_hash AS user_id,
    -- Not all users have entries in *_staff_record so fill in information from the roster for them
    COALESCE(supervision_staff.id, incarceration_staff.id, users.external_id) AS user_external_id,
    COALESCE(supervision_staff.district, incarceration_staff.district, users.district) AS district,
    COALESCE(supervision_staff.email, incarceration_staff.email, users.email_address) AS email,
    COALESCE(staff.full_name_clean, CONCAT(INITCAP(SPLIT(users.first_name, " ")[OFFSET(0)]), " ", INITCAP(users.last_name))) AS user_full_name_clean,
FROM roster_archive_hashes users
-- TODO(#27254) Get this data from somewhere that's not *_staff_record
LEFT JOIN `{project_id}.{workflows_views_dataset}.supervision_staff_record_materialized` supervision_staff
    -- The roster only has US_ID and the staff record only has US_IX
    ON (users.state_code = supervision_staff.state_code OR (users.state_code = "US_ID" AND supervision_staff.state_code = "US_IX"))
    AND LOWER(users.email_address) = LOWER(supervision_staff.email)
LEFT JOIN `{project_id}.{workflows_views_dataset}.incarceration_staff_record_materialized` incarceration_staff
    -- The roster only has US_ID and the staff record only has US_IX
    ON (users.state_code = incarceration_staff.state_code OR (users.state_code = "US_ID" AND incarceration_staff.state_code = "US_IX"))
    AND LOWER(users.email_address) = LOWER(incarceration_staff.email)
LEFT JOIN `{project_id}.{reference_views_dataset}.state_staff_with_names` staff
    -- The roster only has US_ID and should use US_IX for the staff ingested data 
    ON (users.state_code = staff.state_code OR (users.state_code = "US_ID" AND staff.state_code = "US_IX"))
    AND LOWER(users.email_address) = LOWER(staff.email)
QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code, user_id, user_external_id, email, district ORDER BY user_full_name_clean) = 1
"""

REIDENTIFIED_DASHBOARD_USERS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=WORKFLOWS_VIEWS_DATASET,
    view_id=REIDENTIFIED_DASHBOARD_USERS_VIEW_NAME,
    description=REIDENTIFIED_DASHBOARD_USERS_VIEW_DESCRIPTION,
    view_query_template=REIDENTIFIED_DASHBOARD_USERS_QUERY_TEMPLATE,
    reference_views_dataset=REFERENCE_VIEWS_DATASET,
    workflows_views_dataset=WORKFLOWS_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        REIDENTIFIED_DASHBOARD_USERS_VIEW_BUILDER.build_and_print()
