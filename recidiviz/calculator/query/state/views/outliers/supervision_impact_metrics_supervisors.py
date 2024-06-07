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
"""List of supervisors, their permission to access Insights, and if they have outlier officers for any given month"""
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state.dataset_config import (
    OUTLIERS_VIEWS_DATASET,
    REFERENCE_VIEWS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_IMPACT_METRICS_SUPERVISORS_VIEW_NAME = (
    "supervision_impact_metrics_supervisors"
)

SUPERVISION_IMPACT_METRICS_SUPERVISORS_DESCRIPTION = """List of supervisors, their permission to access Insights, and if they have outlier officers for any given month"""


SUPERVISION_IMPACT_METRICS_SUPERVISORS_QUERY_TEMPLATE = """
WITH permissions AS (
    SELECT DISTINCT
        CASE WHEN state_code = 'US_ID' THEN 'US_IX' ELSE state_code END AS state_code,
        external_id AS supervisor_external_id,
        routes_insights,
    FROM `{project_id}.{reference_views_dataset}.product_roster_materialized`
    WHERE routes_insights = true
)
, sups_by_month AS (
    SELECT DISTINCT
        supervisors.state_code,
        supervisors.external_id AS supervisor_external_id,
        supervisors.pseudonymized_id AS supervisor_pseudonymized_id,
        full_name AS supervisor_name,
        email AS supervisor_email,
        EXTRACT(MONTH FROM supervisors.export_date) AS export_month,
        EXTRACT(YEAR FROM supervisors.export_date) AS export_year,
    FROM `{project_id}.outliers_views.supervision_officer_supervisors_archive_materialized` supervisors
)
, officers_by_month AS (
    SELECT DISTINCT
    state_code,
    external_id,
    supervisor_external_id,
    EXTRACT(MONTH FROM export_date) AS export_month,
    EXTRACT(YEAR FROM export_date) AS export_year
    FROM `{project_id}.outliers_views.supervision_officers_archive_materialized` officers, UNNEST(supervisor_external_ids) AS supervisor_external_id
)
, supervisors AS (
    SELECT DISTINCT
        supervisors.state_code,
        supervisors.supervisor_external_id,
        supervisor_pseudonymized_id,
        supervisors.supervisor_name,
        supervisors.supervisor_email,
        LOGICAL_OR(COALESCE(routes_insights, FALSE)) AS has_permission,
        LOGICAL_OR(COALESCE(status, 'UNKNOWN') = 'FAR') AS has_outliers,
        supervisors.export_month,
        supervisors.export_year
    FROM sups_by_month supervisors
    LEFT JOIN permissions USING(state_code, supervisor_external_id)
    LEFT JOIN officers_by_month officers
        USING(state_code, supervisor_external_id, export_month, export_year)
    LEFT JOIN `{project_id}.outliers_views.supervision_officer_outlier_status_archive_materialized` outliers
        ON outliers.officer_id = officers.external_id
        AND outliers.state_code = officers.state_code
        AND officers.export_year = EXTRACT(YEAR FROM outliers.export_date)
        AND officers.export_month = EXTRACT(MONTH FROM outliers.export_date)
        AND end_date = DATE_TRUNC(outliers.export_date, MONTH)
    GROUP BY 1,2,3,4,5,8,9
)
SELECT
    {columns}
FROM supervisors
"""

SUPERVISION_IMPACT_METRICS_SUPERVISORS_VIEW_BUILDER = (
    SelectedColumnsBigQueryViewBuilder(
        dataset_id=OUTLIERS_VIEWS_DATASET,
        reference_views_dataset=REFERENCE_VIEWS_DATASET,
        view_id=SUPERVISION_IMPACT_METRICS_SUPERVISORS_VIEW_NAME,
        view_query_template=SUPERVISION_IMPACT_METRICS_SUPERVISORS_QUERY_TEMPLATE,
        description=SUPERVISION_IMPACT_METRICS_SUPERVISORS_DESCRIPTION,
        should_materialize=True,
        columns=[
            "state_code",
            "supervisor_external_id",
            "supervisor_pseudonymized_id",
            "supervisor_name",
            "supervisor_email",
            "has_permission",
            "has_outliers",
            "export_month",
            "export_year",
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_IMPACT_METRICS_SUPERVISORS_VIEW_BUILDER.build_and_print()
