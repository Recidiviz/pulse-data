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
"""List of supervisors who have logged in to the dashboard any given month,
whether they have outlier officers, and which staff pages they viewed"""
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state.dataset_config import OUTLIERS_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_USAGE_METRICS_VIEW_NAME = "supervision_usage_metrics"

SUPERVISION_USAGE_METRICS_DESCRIPTION = """List of supervisors who have logged in to the dashboard any given month,
    whether they have outlier officers, and which staff pages they viewed"""

SUPERVISION_USAGE_METRICS_QUERY_TEMPLATE = """
WITH outlier_officers AS (
    SELECT
        officers.state_code,
        supervisor_external_id,
        ARRAY_AGG(external_id) AS outlier_officers,
        EXTRACT(MONTH FROM officers.export_date) AS export_month,
        EXTRACT(YEAR FROM officers.export_date) AS export_year
    FROM `{project_id}.outliers_views.supervision_officers_archive_materialized` officers, UNNEST(supervisor_external_ids) AS supervisor_external_id
    INNER JOIN `{project_id}.outliers_views.supervision_officer_outlier_status_archive_materialized` outliers
    ON officers.state_code=outliers.state_code AND officers.external_id=outliers.officer_id AND officers.export_date=outliers.export_date
    WHERE supervisor_external_id IS NOT NULL
    AND status = 'FAR'
    AND end_date = DATE_TRUNC(outliers.export_date, MONTH)
    GROUP BY 1,2,4,5
)
, supervisors AS (
    SELECT DISTINCT
        supervisors.state_code, 
        supervisors.external_id AS supervisor_external_id, 
        supervisors.pseudonymized_id AS supervisor_pseudonymized_id,
        full_name AS supervisor_name,
        email AS supervisor_email,
        EXTRACT(MONTH FROM export_date) AS export_month,
        EXTRACT(YEAR FROM export_date) AS export_year
    FROM `{project_id}.outliers_views.supervision_officer_supervisors_archive_materialized` supervisors
)
, staff_pages_viewed AS (
    SELECT 
        state_code,
        supervisor_external_id,
        supervisors.supervisor_pseudonymized_id,
        ARRAY_AGG(DISTINCT staff_pseudonymized_id IGNORE NULLS) AS staff_pages_viewed,
        export_month,
        export_year
    FROM supervisors
    LEFT JOIN `{project_id}.pulse_dashboard_segment_metrics.frontend_outliers_staff_page_viewed` staff
    ON supervisors.supervisor_pseudonymized_id = staff.supervisor_pseudonymized_id
        AND supervisors.export_month = EXTRACT(MONTH FROM timestamp)
        AND supervisors.export_year = EXTRACT(YEAR FROM timestamp)
    GROUP BY 1,2,3,5,6
)
, supervisors_with_logins AS (
    SELECT DISTINCT
        supervisors.state_code,
        supervisors.supervisor_external_id,
        supervisors.supervisor_pseudonymized_id,
        supervisors.supervisor_name,
        supervisors.supervisor_email,
        supervisors.export_month,
        supervisors.export_year
    FROM `{project_id}.auth0_prod_action_logs.success_login` logins
    INNER JOIN supervisors
        ON LOWER(supervisors.supervisor_email) = LOWER(logins.email)
        AND supervisors.export_month = EXTRACT(MONTH FROM logins.timestamp)
        AND supervisors.export_year = EXTRACT(YEAR FROM logins.timestamp)
)
, usage AS (
    SELECT
        supervisors_with_logins.state_code,
        supervisors_with_logins.supervisor_external_id AS logged_in_supervisor_external_id,
        supervisors_with_logins.supervisor_pseudonymized_id AS logged_in_supervisor_pseudonymized_id,
        supervisors_with_logins.supervisor_name,
        supervisors_with_logins.supervisor_email,
        outlier_officers.outlier_officers IS NOT NULL AS has_outliers,
        spv.staff_pages_viewed,
        supervisors_with_logins.export_month,
        supervisors_with_logins.export_year
    FROM supervisors_with_logins
    LEFT JOIN staff_pages_viewed spv
        USING(state_code, supervisor_external_id, export_month, export_year)
    LEFT JOIN outlier_officers
    USING(state_code, supervisor_external_id, export_month, export_year)
)
SELECT
    {columns}
FROM usage
"""

SUPERVISION_USAGE_METRICS_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=OUTLIERS_VIEWS_DATASET,
    view_id=SUPERVISION_USAGE_METRICS_VIEW_NAME,
    view_query_template=SUPERVISION_USAGE_METRICS_QUERY_TEMPLATE,
    description=SUPERVISION_USAGE_METRICS_DESCRIPTION,
    should_materialize=True,
    columns=[
        "state_code",
        "logged_in_supervisor_external_id",
        "logged_in_supervisor_pseudonymized_id",
        "supervisor_name",
        "supervisor_email",
        "has_outliers",
        "staff_pages_viewed",
        "export_month",
        "export_year",
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_USAGE_METRICS_VIEW_BUILDER.build_and_print()
