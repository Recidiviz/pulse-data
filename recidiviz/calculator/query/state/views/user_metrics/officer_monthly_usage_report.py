# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Supervision officer monthly Recidiviz tool usage data for the Usage by User report"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION
from recidiviz.utils.metadata import local_project_id_override

OFFICER_MONTHLY_USAGE_REPORT_VIEW_NAME = "officer_monthly_usage_report"

OFFICER_MONTHLY_USAGE_REPORT_QUERY_TEMPLATE = f"""
WITH
# Combine the aggregated metrics data across time interval units,
# restrict the metrics to only registered users,
# and add supervision officer metadata (name, email, region, supervisor)
officer_monthly_usage_metrics AS (
    SELECT
        monthly_metrics.state_code,
        monthly_metrics.start_date,
        monthly_metrics.end_date,
        monthly_metrics.officer_id,
        INITCAP(JSON_VALUE(staff.full_name, "$.surname")) AS last_name,
        INITCAP(JSON_VALUE(staff.full_name, "$.given_names")) AS first_name,
        users.workflows_user_email_address AS officer_email,
        IFNULL(users.location_name, attrs.supervision_district_name_inferred) AS supervision_district,
        attrs.supervisor_staff_id_array,
        monthly_metrics.workflows_primary_user_active_usage_events AS actions_taken_this_month,
        monthly_metrics.workflows_primary_user_logins AS logins_this_month,
        IFNULL(end_of_month_metrics.workflows_distinct_people_eligible_and_actionable, 0) AS clients_eligible_end_of_month,
    FROM
        `{{project_id}}.{{user_metrics_dataset}}.supervision_officer_aggregated_metrics_materialized` AS monthly_metrics
    LEFT JOIN
        `{{project_id}}.{{user_metrics_dataset}}.supervision_officer_aggregated_metrics_materialized` AS end_of_month_metrics
    USING
        (state_code, officer_id, end_date)
    LEFT JOIN
        `{{project_id}}.analyst_data.workflows_provisioned_user_registration_sessions_materialized` users
    ON
        users.state_code = monthly_metrics.state_code
        AND users.staff_external_id = monthly_metrics.officer_id
        # Pick registration sessions that overlap the last day of the month
        # (aggregated metrics end_date - 1 day)
        AND DATE_SUB(monthly_metrics.end_date, INTERVAL 1 DAY) BETWEEN users.start_date
            AND {nonnull_end_date_exclusive_clause("users.end_date_exclusive")}
        AND users.system_type = "SUPERVISION"
    LEFT JOIN
        `{{project_id}}.sessions.supervision_staff_attribute_sessions_materialized` attrs
    ON
        attrs.state_code = users.state_code
        AND attrs.staff_id = users.staff_id
        # Pull the supervisors assigned on the metrics end date
        AND monthly_metrics.end_date
            BETWEEN attrs.start_date AND {nonnull_end_date_exclusive_clause("attrs.end_date_exclusive")}
    LEFT JOIN
        `{{project_id}}.normalized_state.state_staff` staff
    ON
        staff.state_code = users.state_code
        AND staff.staff_id = users.staff_id
    WHERE
        # Only include registered supervision users within this report
        end_of_month_metrics.distinct_registered_users_supervision = 1
        AND monthly_metrics.period = "MONTH"
        AND end_of_month_metrics.period = "DAY"
)
,
# Sum up the rolling monthly login count in the calendar year
officer_yearly_logins AS (
    SELECT
        state_code,
        officer_id,
        start_date,
        end_date,
        SUM(logins_this_month) OVER (
          PARTITION BY state_code, officer_id, EXTRACT(YEAR FROM start_date)
          ORDER BY end_date
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS logins_this_year
    FROM
        officer_monthly_usage_metrics
)
,
# Aggregate all the distinct supervisor per officer over each span
supervisors AS (
    SELECT
        monthly_metrics.state_code,
        officer_id,
        start_date,
        end_date,
        STRING_AGG(LOWER(supervisors.email), ", " ORDER BY supervisors.email) AS supervisor_email,
    FROM
        officer_monthly_usage_metrics monthly_metrics,
    UNNEST
        (supervisor_staff_id_array) AS supervisor_staff_id
    LEFT JOIN
        `{{project_id}}.normalized_state.state_staff` supervisors
    ON
        supervisors.staff_id = supervisor_staff_id
        AND supervisors.state_code = monthly_metrics.state_code
    GROUP BY
        1, 2, 3, 4
)
SELECT
    start_date,
    end_date,
    state_code,
    officer_id,
    last_name,
    first_name,
    officer_email,
    supervisors.supervisor_email,
    supervision_district,
    IFNULL(monthly_usage.clients_eligible_end_of_month, 0) AS clients_eligible_end_of_month,
    IFNULL(monthly_usage.logins_this_month, 0) AS total_logins_this_month,
    IFNULL(monthly_usage.actions_taken_this_month, 0) AS actions_taken_this_month,
    IFNULL(yearly_usage.logins_this_year, 0) AS logins_this_year,
FROM
    officer_monthly_usage_metrics monthly_usage
INNER JOIN
    officer_yearly_logins yearly_usage
USING
    (state_code, officer_id, start_date, end_date)
LEFT JOIN
    supervisors
USING
    (state_code, officer_id, start_date, end_date)
"""

OFFICER_MONTHLY_USAGE_REPORT_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.USER_METRICS_DATASET_ID,
    view_id=OFFICER_MONTHLY_USAGE_REPORT_VIEW_NAME,
    view_query_template=OFFICER_MONTHLY_USAGE_REPORT_QUERY_TEMPLATE,
    description=__doc__,
    clustering_fields=["state_code"],
    should_materialize=True,
    user_metrics_dataset=dataset_config.USER_METRICS_DATASET_ID,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_PRODUCTION):
        OFFICER_MONTHLY_USAGE_REPORT_VIEW_BUILDER.build_and_print()
