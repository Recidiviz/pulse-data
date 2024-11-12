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
"""List of outlier officers for any given month"""
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state.dataset_config import OUTLIERS_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_IMPACT_METRICS_OUTLIER_OFFICERS_VIEW_NAME = (
    "supervision_impact_metrics_outlier_officers"
)

SUPERVISION_IMPACT_METRICS_OUTLIER_OFFICERS_DESCRIPTION = (
    """List of outlier officers for any given month"""
)


SUPERVISION_IMPACT_METRICS_OUTLIER_OFFICERS_QUERY_TEMPLATE = """
WITH outlier_officers AS (
    SELECT
        outliers.state_code,
        officer_id AS officer_external_id,
        full_name AS officer_name,
        email AS officer_email,
        ARRAY_AGG(DISTINCT metric_id ORDER BY metric_id) as outlier_metric_ids,
        EXTRACT(MONTH FROM export_date) AS export_month,
        EXTRACT(YEAR FROM export_date) AS export_year
    FROM `{project_id}.outliers_views.supervision_officer_outlier_status_archive_materialized` outliers
    LEFT JOIN `{project_id}.normalized_state.state_staff_external_id` staff_id
    ON outliers.state_code = staff_id.state_code AND outliers.officer_id = staff_id.external_id
    LEFT JOIN `{project_id}.normalized_state.state_staff` staff
    ON staff_id.state_code = staff.state_code AND staff_id.staff_id = staff.staff_id
    WHERE status = 'FAR'
    AND end_date = DATE_TRUNC(export_date, MONTH)
    GROUP BY 1,2,3,4,6,7
)
SELECT
    {columns}
FROM outlier_officers
"""

SUPERVISION_IMPACT_METRICS_OUTLIER_OFFICERS_VIEW_BUILDER = (
    SelectedColumnsBigQueryViewBuilder(
        dataset_id=OUTLIERS_VIEWS_DATASET,
        view_id=SUPERVISION_IMPACT_METRICS_OUTLIER_OFFICERS_VIEW_NAME,
        view_query_template=SUPERVISION_IMPACT_METRICS_OUTLIER_OFFICERS_QUERY_TEMPLATE,
        description=SUPERVISION_IMPACT_METRICS_OUTLIER_OFFICERS_DESCRIPTION,
        should_materialize=True,
        columns=[
            "state_code",
            "officer_external_id",
            "officer_name",
            "officer_email",
            "outlier_metric_ids",
            "export_month",
            "export_year",
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_IMPACT_METRICS_OUTLIER_OFFICERS_VIEW_BUILDER.build_and_print()
