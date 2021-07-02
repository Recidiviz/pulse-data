# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Reference table for UP Dashboard user restrictions.
"""

# pylint: disable=trailing-whitespace
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

DASHBOARD_USER_RESTRICTIONS_VIEW_NAME = "dashboard_user_restrictions"

DASHBOARD_USER_RESTRICTIONS_DESCRIPTION = (
    """Reference table for UP Dashboard user restrictions."""
)

DASHBOARD_USER_RESTRICTIONS_QUERY_TEMPLATE = """
    /*{description}*/
    WITH
    mo_restricted_access AS (
        SELECT
            'US_MO' AS state_code,
            EMAIL AS restricted_user_email,
            # TODO(#7413) Remove allowed_level_1_supervision_location_ids once FE is no longer using it
            CASE
                WHEN STRING_AGG(DISTINCT DISTRICT, ',') IS NOT NULL
                THEN STRING_AGG(DISTINCT DISTRICT, ',')
                ELSE ''
            END AS allowed_level_1_supervision_location_ids,
            CASE
                WHEN STRING_AGG(DISTINCT DISTRICT, ',') IS NOT NULL
                THEN STRING_AGG(DISTINCT DISTRICT, ',')
                ELSE ''
            END AS allowed_supervision_location_ids,
            IF(STRING_AGG(DISTINCT DISTRICT, ',') IS NOT NULL, 'level_1_supervision_location', NULL) AS allowed_supervision_location_level,
            IF(STRING_AGG(DISTINCT DISTRICT, ',') IS NOT NULL, 'level_1_access_role', 'leadership_role') as internal_role,
            -- All users can access leadership dashboard
            TRUE AS can_access_leadership_dashboard,
            -- US_MO is not currently using Case Triage
            FALSE AS can_access_case_triage
        FROM `{project_id}.us_mo_raw_data_up_to_date_views.LANTERN_DA_RA_LIST_latest`
        WHERE EMAIL IS NOT NULL
        GROUP BY EMAIL
    ),
    id_restricted_access AS (
        SELECT
            'US_ID' AS state_code,
            LOWER(email_address) AS restricted_user_email,
            '' AS allowed_level_1_supervision_location_ids,
            '' AS allowed_supervision_location_ids,
            CAST(NULL AS STRING) as allowed_supervision_location_level,
            internal_role,
            CASE
                WHEN internal_role LIKE '%leadership_role%' THEN TRUE
                ELSE FALSE
            END AS can_access_leadership_dashboard,
            FALSE AS can_access_case_triage
        FROM `{project_id}.{static_reference_dataset_id}.us_id_leadership_users`
    )
    SELECT {columns} FROM mo_restricted_access
    UNION ALL
    SELECT {columns} FROM id_restricted_access;
    """

DASHBOARD_USER_RESTRICTIONS_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    static_reference_dataset_id=dataset_config.STATIC_REFERENCE_TABLES_DATASET,
    view_id=DASHBOARD_USER_RESTRICTIONS_VIEW_NAME,
    view_query_template=DASHBOARD_USER_RESTRICTIONS_QUERY_TEMPLATE,
    description=DASHBOARD_USER_RESTRICTIONS_DESCRIPTION,
    columns=[
        "state_code",
        "restricted_user_email",
        # TODO(#7413) Remove allowed_level_1_supervision_location_ids once FE is no longer using it
        "allowed_level_1_supervision_location_ids",
        "allowed_supervision_location_ids",
        "allowed_supervision_location_level",
        "internal_role",
        "can_access_leadership_dashboard",
        "can_access_case_triage",
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        DASHBOARD_USER_RESTRICTIONS_VIEW_BUILDER.build_and_print()
