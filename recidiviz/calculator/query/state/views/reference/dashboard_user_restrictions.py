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
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

DASHBOARD_USER_RESTRICTIONS_VIEW_NAME = "dashboard_user_restrictions"

DASHBOARD_USER_RESTRICTIONS_DESCRIPTION = (
    """Reference table for UP Dashboard user restrictions."""
)

# TODO(#8758): Add an `exported_at` column.
DASHBOARD_USER_RESTRICTIONS_QUERY_TEMPLATE = """
    /*{description}*/
    WITH
    co_restricted_access AS (
        SELECT
            'US_CO' AS state_code,
            LOWER(leadership.email_address) AS restricted_user_email,
            '' AS allowed_supervision_location_ids,
            CAST(NULL AS STRING) as allowed_supervision_location_level,
            internal_role,
            TRUE AS can_access_leadership_dashboard,
            FALSE AS can_access_case_triage,
            FALSE AS should_see_beta_charts,
            TO_JSON_STRING(STRUCT(
                system_libertyToPrison,
                system_prison,
                system_prisonToSupervision,
                system_supervision,
                system_supervisionToLiberty,
                system_supervisionToPrison,
                operations,
                FALSE AS workflows
            )) AS routes
        FROM
            `{project_id}.{static_reference_dataset_id}.us_co_leadership_users` leadership
    ),
    id_restricted_access AS (
        SELECT
            'US_ID' AS state_code,
            LOWER(IF(leadership.email_address IS NULL,
                    id_roster.email_address,
                    leadership.email_address)) AS restricted_user_email,
            '' AS allowed_supervision_location_ids,
            CAST(NULL AS STRING) as allowed_supervision_location_level,
            IF(internal_role IS NULL,
                'line_staff_user',
                internal_role) AS internal_role,
            CASE
                WHEN internal_role LIKE '%leadership_role%' THEN TRUE
                ELSE FALSE
            END AS can_access_leadership_dashboard,
            (id_roster.email_address IS NOT NULL) AS can_access_case_triage,
            CASE
                WHEN internal_role LIKE '%leadership_role%' THEN should_see_beta_charts
                ELSE FALSE
            END AS should_see_beta_charts,
            CASE
                WHEN internal_role LIKE '%leadership_role%'
                    THEN TO_JSON_STRING(STRUCT(
                        community_projections,
                        facilities_projections,
                        community_practices,
                        operations,
                        system_libertyToPrison,
                        system_prison,
                        system_prisonToSupervision,
                        system_supervision,
                        system_supervisionToLiberty,
                        system_supervisionToPrison
                    ))
                ELSE TO_JSON_STRING(NULL)
            END AS routes,
        FROM
            `{project_id}.{static_reference_dataset_id}.us_id_leadership_users` leadership
        FULL OUTER JOIN
            `{project_id}.{static_reference_dataset_id}.us_id_roster` id_roster
        USING (email_address)
    ),
    me_restricted_access AS (
        SELECT
            'US_ME' AS state_code,
            LOWER(leadership.email_address) AS restricted_user_email,
            '' AS allowed_supervision_location_ids,
            CAST(NULL AS STRING) as allowed_supervision_location_level,
            internal_role,
            TRUE AS can_access_leadership_dashboard,
            FALSE AS can_access_case_triage,
            FALSE AS should_see_beta_charts,
            TO_JSON_STRING(STRUCT(
                system_libertyToPrison,
                system_prison,
                system_prisonToSupervision,
                system_supervision,
                system_supervisionToLiberty,
                system_supervisionToPrison,
                operations
            )) AS routes
        FROM
            `{project_id}.{static_reference_dataset_id}.us_me_leadership_users` leadership
    ),
    mi_restricted_access AS (
        SELECT
            'US_MI' AS state_code,
            LOWER(leadership.email_address) AS restricted_user_email,
            '' AS allowed_supervision_location_ids,
            CAST(NULL AS STRING) as allowed_supervision_location_level,
            internal_role,
            TRUE AS can_access_leadership_dashboard,
            FALSE AS can_access_case_triage,
            FALSE AS should_see_beta_charts,
            TO_JSON_STRING(STRUCT(
                system_libertyToPrison,
                system_prison,
                system_prisonToSupervision,
                system_supervision,
                system_supervisionToLiberty,
                system_supervisionToPrison,
                operations,
                FALSE AS workflows
            )) AS routes
        FROM
            `{project_id}.{static_reference_dataset_id}.us_mi_leadership_users` leadership
    ),
    mo_restricted_access AS (
        SELECT
            'US_MO' AS state_code,
            LOWER(EMAIL) AS restricted_user_email,
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
            FALSE AS can_access_case_triage,
            FALSE AS should_see_beta_charts,
            -- US_MO has not yet launched any user restricted pages
            TO_JSON_STRING(NULL) as routes
        FROM `{project_id}.{us_mo_raw_data_up_to_date_dataset}.LANTERN_DA_RA_LIST_latest`
        WHERE EMAIL IS NOT NULL
        GROUP BY LOWER(EMAIL)
    ),
    nd_restricted_access AS (
        SELECT
            'US_ND' AS state_code,
            LOWER(leadership.email_address) AS restricted_user_email,
            '' AS allowed_supervision_location_ids,
            CAST(NULL AS STRING) as allowed_supervision_location_level,
            internal_role,
            TRUE AS can_access_leadership_dashboard,
            FALSE AS can_access_case_triage,
            FALSE AS should_see_beta_charts,
            TO_JSON_STRING(STRUCT(
                community_projections,
                facilities_projections,
                community_practices,
                system_libertyToPrison,
                system_prison,
                system_prisonToSupervision,
                system_supervision,
                system_supervisionToLiberty,
                system_supervisionToPrison,
                operations,
                workflows
            )) AS routes
        FROM
            `{project_id}.{static_reference_dataset_id}.us_nd_leadership_users` leadership
    ),
    tn_restricted_access AS (
        SELECT
            'US_TN' AS state_code,
            LOWER(IF(leadership.email_address IS NULL,
                    tn_roster.email_address,
                    leadership.email_address)) AS restricted_user_email,
            '' AS allowed_supervision_location_ids,
            CAST(NULL AS STRING) as allowed_supervision_location_level,
            IF(internal_role IS NULL,
                'line_staff_user',
                internal_role) AS internal_role,
            TRUE AS can_access_leadership_dashboard,
            FALSE AS can_access_case_triage,
            FALSE AS should_see_beta_charts,
            CASE
                WHEN internal_role LIKE '%leadership_role%'
                    THEN TO_JSON_STRING(STRUCT(
                        system_libertyToPrison,
                        system_prison,
                        system_prisonToSupervision,
                        system_supervision,
                        system_supervisionToLiberty,
                        system_supervisionToPrison,
                        operations,
                        TRUE AS workflows
                    ))
                ELSE TO_JSON_STRING(STRUCT(TRUE AS workflows))
            END AS routes
        FROM
            `{project_id}.{static_reference_dataset_id}.us_tn_leadership_users` leadership
        FULL OUTER JOIN
            `{project_id}.{static_reference_dataset_id}.us_tn_roster` tn_roster
        ON LOWER(leadership.email_address)=LOWER(tn_roster.email_address)
    ),
    recidiviz_test_users AS (
        SELECT
            state_code,
            email_address AS restricted_user_email,
            allowed_supervision_location_ids,
            allowed_supervision_location_level,
            internal_role,
            can_access_leadership_dashboard,
            can_access_case_triage,
            should_see_beta_charts,
            TO_JSON_STRING(NULL) as routes
        FROM `{project_id}.{static_reference_dataset_id}.recidiviz_unified_product_test_users`
    )
    , all_users AS (
        SELECT * FROM co_restricted_access
        UNION ALL
        SELECT * FROM id_restricted_access
        UNION ALL
        SELECT * FROM me_restricted_access
        UNION ALL
        SELECT * FROM mi_restricted_access
        UNION ALL
        SELECT * FROM mo_restricted_access
        UNION ALL
        SELECT * FROM nd_restricted_access
        UNION ALL
        SELECT * FROM tn_restricted_access
        UNION ALL
        SELECT * FROM recidiviz_test_users
    )
    , all_users_hashed_emails AS (
        SELECT
            *,
            TO_BASE64(SHA256(LOWER(restricted_user_email))) AS user_hash,
        FROM all_users
    )

    SELECT {columns} FROM all_users_hashed_emails

    """

DASHBOARD_USER_RESTRICTIONS_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    static_reference_dataset_id=dataset_config.STATIC_REFERENCE_TABLES_DATASET,
    view_id=DASHBOARD_USER_RESTRICTIONS_VIEW_NAME,
    view_query_template=DASHBOARD_USER_RESTRICTIONS_QUERY_TEMPLATE,
    description=DASHBOARD_USER_RESTRICTIONS_DESCRIPTION,
    us_mo_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        StateCode.US_MO.value
    ),
    columns=[
        "state_code",
        "restricted_user_email",
        "allowed_supervision_location_ids",
        "allowed_supervision_location_level",
        "internal_role",
        "can_access_leadership_dashboard",
        "can_access_case_triage",
        "routes",
        "should_see_beta_charts",
        "user_hash",
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        DASHBOARD_USER_RESTRICTIONS_VIEW_BUILDER.build_and_print()
