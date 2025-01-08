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
"""View representing monthly cohorts with a boolean indicating
whether the officer had outlier status across one of their metrics at any point in a
given month, along with information about their supervisor's login behavior in that
month"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "insights_officer_outlier_usage_cohort"

_VIEW_DESCRIPTION = """View representing monthly cohorts with a boolean indicating
whether the officer had outlier status across one of their metrics at any point in a
given month, along with information about their supervisor's login behavior in that
month"""

_QUERY_TEMPLATE = f"""
WITH outlier_status_cohorts AS (
    SELECT
        state_code,
        officer_id,
        cohort_month_start_date,
        metric_id,
        LOGICAL_OR(is_surfaceable_outlier) AS is_outlier_officer,
    FROM
        `{{project_id}}.analyst_data.insights_supervision_officer_outlier_status_archive_sessions_materialized`,
        UNNEST(GENERATE_DATE_ARRAY(DATE_TRUNC(DATE_SUB(CURRENT_DATE("US/Eastern"), INTERVAL 3 YEAR), MONTH), CURRENT_DATE("US/Eastern"), INTERVAL 1 MONTH)) AS cohort_month_start_date,
        UNNEST([metric_id, "ANY"]) AS metric_id
    -- Count a row toward the status on the cohort month
    WHERE
        cohort_month_start_date
            BETWEEN DATE_TRUNC(start_date, MONTH) 
            AND {nonnull_end_date_exclusive_clause("DATE_ADD(DATE_TRUNC(end_date_exclusive, MONTH), INTERVAL 1 MONTH)")}
    GROUP BY 1, 2, 3, 4
)
,
login_cohorts AS (
    SELECT DISTINCT
        staff.state_code,
        staff.officer_id,
        DATE_TRUNC(logins.login_date, MONTH) AS cohort_month_start_date,
    FROM
        `{{project_id}}.analyst_data.all_auth0_login_events_materialized` logins
    INNER JOIN
        `{{project_id}}.normalized_state.state_staff` email
    ON
        logins.state_code = email.state_code
        AND LOWER(logins.email_address) = LOWER(email.email)
    -- Associated supervisor logins with the officers whom they were supervising at the time of login
    INNER JOIN
        `{{project_id}}.sessions.supervision_staff_attribute_sessions_materialized` staff
    ON
        logins.state_code = staff.state_code
        AND email.staff_id IN UNNEST(staff.supervisor_staff_id_array)
        AND logins.login_date BETWEEN staff.start_date AND {nonnull_end_date_exclusive_clause("end_date_exclusive")}
    WHERE
        has_insights_access
)
,
joined_cohorts AS (
    SELECT
        state_code,
        officer_id,
        cohort_month_start_date,
        DATE_ADD(cohort_month_start_date, INTERVAL 1 MONTH) AS cohort_month_end_date,
        metric_id,
        is_outlier_officer,
        login_cohorts.cohort_month_start_date IS NOT NULL AS supervisor_logged_in,
    FROM
        outlier_status_cohorts
    LEFT JOIN
        login_cohorts
    USING
        (state_code, officer_id, cohort_month_start_date)
)
-- Sort into outlier X usage cohort based on the flags for outlier and login status.
-- The categories are OUTLIER, NON_OUTLIER, and OUTLIER_WITH_SUPERVISOR_LOGIN,
-- which is a subset of the OUTLIER cohort.
SELECT
    state_code,
    officer_id,
    metric_id,
    cohort_month_start_date,
    cohort_month_end_date,
    outlier_usage_cohort,
FROM
    joined_cohorts,
    UNNEST(["OUTLIER_ALL", "OUTLIER_WITH_SUPERVISOR_LOGIN", "OUTLIER_NO_SUPERVISOR_LOGIN", "NON_OUTLIER"]) AS outlier_usage_cohort
WHERE
    (outlier_usage_cohort = "OUTLIER_ALL" AND is_outlier_officer)
    OR (outlier_usage_cohort = "OUTLIER_WITH_SUPERVISOR_LOGIN" AND is_outlier_officer AND supervisor_logged_in)
    OR (outlier_usage_cohort = "OUTLIER_NO_SUPERVISOR_LOGIN" AND is_outlier_officer AND NOT supervisor_logged_in)
    OR (outlier_usage_cohort = "NON_OUTLIER" AND NOT is_outlier_officer)
"""
INSIGHTS_OFFICER_OUTLIER_USAGE_COHORT_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=_VIEW_NAME,
    description=_VIEW_DESCRIPTION,
    view_query_template=_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INSIGHTS_OFFICER_OUTLIER_USAGE_COHORT_VIEW_BUILDER.build_and_print()
