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
"""Identify rows from Segment analytics tables that are Recidiviz users that we need to delete. """
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    PULSE_DASHBOARD_SEGMENT_DATASET,
    WORKFLOWS_VIEWS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION
from recidiviz.utils.metadata import local_project_id_override

RECIDIVIZ_USERS_TO_DELETE_FROM_ANALYTICS_VIEW_NAME = (
    "recidiviz_users_to_delete_from_analytics"
)

RECIDIVIZ_USERS_TO_DELETE_FROM_ANALYTICS_DESCRIPTION = """View containing invalid events from misidentified Recidiviz user sessions in the Segment tables"""

PULSE_DASHBOARD_SEGMENT_METRICS_TABLES = [
    "frontend_caseload_search",
    "frontend_milestones_congratulated_another_way",
    "frontend_milestones_congratulations_sent",
    "frontend_milestones_message_declined",
    "frontend_milestones_side_panel_opened",
    "frontend_milestones_tab_clicked",
    "frontend_opportunity_marked_eligible",
    "frontend_opportunity_previewed",
    "frontend_opportunity_snoozed",
    "frontend_opportunity_status_updated",
    "frontend_opportunity_tab_clicked",
    "frontend_outliers_staff_page_viewed",
    "frontend_outliers_supervisor_page_viewed",
    "frontend_profile_opportunity_link_clicked",
    "frontend_profile_viewed",
    "frontend_referral_form_copied_to_clipboard",
    "frontend_referral_form_downloaded",
    "frontend_referral_form_edited",
    "frontend_referral_form_first_edited",
    "frontend_referral_form_printed",
    "frontend_referral_form_submitted",
    "frontend_referral_form_viewed",
    "frontend_surfaced_in_list",
    "frontend_task_filter_selected",
    "frontend_tasks_previewed",
]


def build_segment_subqueries() -> str:
    """For each Segment table, find the sessions that do not exist in the identifies table. The identifies table
    gets populated once per session when a user logs in and is identified to Segment, and Recidiviz users who
    are impersonating another user do not have the impersonated session captured in this table."""
    return ", ".join(
        f"""
            {table_name}_invalid_sessions AS (
                SELECT 
                    id, -- This is a unique key for each table
                    session_id,
                    original_timestamp,
                    context_ip AS ip_address,
                    user_id AS user_hash,
                    '{table_name}' AS segment_table, 
                FROM `{{project_id}}.{{segment_dataset}}.{table_name}`
                WHERE session_id NOT IN (
                    SELECT DISTINCT session_id FROM `{{project_id}}.{{segment_dataset}}.identifies`
                )
                
                -- Capture only production 
                AND CONTAINS_SUBSTR(context_page_url, 'dashboard.recidiviz.org')
            )
        """
        for table_name in PULSE_DASHBOARD_SEGMENT_METRICS_TABLES
    )


def union_all_segment_tables() -> str:
    """Create one view of the invalid sessions across all segment tables."""
    return " UNION ALL ".join(
        f"""SELECT * FROM {table_name}_invalid_sessions"""
        for table_name in PULSE_DASHBOARD_SEGMENT_METRICS_TABLES
    )


RECIDIVIZ_USERS_TO_DELETE_FROM_ANALYTICS_QUERY_TEMPLATE = f"""
    WITH 
        {build_segment_subqueries()}
        ,
        all_invalid_sessions AS (
            {union_all_segment_tables()}
        ),
            
        auth0_logins_for_invalid_sessions as (
          SELECT 
            login.email,
            login.last_ip AS login_ip_address,
            i.ip_address,
            login.original_timestamp AS auth0_timestamp,
            i.original_timestamp AS segment_timestamp,
            login.user_hash as login_user_hash,
            i.user_hash,
            i.id AS segment_id,
          FROM `{{project_id}}.auth0_prod_action_logs.success_login` login
          INNER JOIN all_invalid_sessions i

          -- Find logins from the same IP address that happened the same time as the event
          -- This will not capture all sessions that occured within 10 minutes after login.
          ON login.last_ip = i.ip_address
          AND TIMESTAMP_TRUNC(login.original_timestamp, MINUTE) BETWEEN DATE_SUB(TIMESTAMP_TRUNC(i.original_timestamp, MINUTE), INTERVAL 9 MINUTE) AND  TIMESTAMP_TRUNC(i.original_timestamp, MINUTE)
        
          -- Auth0 logins with null user_hash are recidiviz users
          WHERE login.user_hash IS NULL AND i.user_hash IS NOT NULL
        )
                
        SELECT 
            *
        FROM auth0_logins_for_invalid_sessions
        WHERE CONTAINS_SUBSTR(email, 'recidiviz')
    
"""

RECIDIVIZ_USERS_TO_DELETE_FROM_ANALYTICS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=WORKFLOWS_VIEWS_DATASET,
    segment_dataset=PULSE_DASHBOARD_SEGMENT_DATASET,
    view_id=RECIDIVIZ_USERS_TO_DELETE_FROM_ANALYTICS_VIEW_NAME,
    description=RECIDIVIZ_USERS_TO_DELETE_FROM_ANALYTICS_DESCRIPTION,
    view_query_template=RECIDIVIZ_USERS_TO_DELETE_FROM_ANALYTICS_QUERY_TEMPLATE,
    projects_to_deploy={GCP_PROJECT_PRODUCTION},
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_PRODUCTION):
        RECIDIVIZ_USERS_TO_DELETE_FROM_ANALYTICS_VIEW_BUILDER.build_and_print()
