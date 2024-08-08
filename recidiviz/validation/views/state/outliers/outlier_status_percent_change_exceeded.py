# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""A view comparing the most recent archived outlier officers and current
outlier officers by metric to determine if major changes in the number of outliers have occurred.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import OUTLIERS_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

OUTLIER_STATUS_PERCENT_CHANGE_EXCEEDED_VIEW_NAME = (
    "outlier_status_percent_change_exceeded"
)

OUTLIER_STATUS_PERCENT_CHANGE_EXCEEDED_DESCRIPTION = """
Identifies when a considerable change in the number of outliers has occurred for a given metric. 
NOTE: This validation may pass on subsequent runs after failing even if the underlying issue is not resolved.
"""

OUTLIER_STATUS_PERCENT_CHANGE_EXCEEDED_QUERY_TEMPLATE = """
WITH archived_outliers AS (
  -- Query the latest status per metric per end date per officer per category and caseload_type
    SELECT
        state_code, 
        metric_id, 
        officer_id,
        status,
        end_date,
        category_type,
        caseload_type,
        export_date AS last_export_date,
        MAX(export_date) OVER (PARTITION BY state_code, metric_id, end_date) AS max_export_date
    FROM `{project_id}.{outliers_views_dataset}.supervision_officer_outlier_status_archive_materialized`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code, metric_id, end_date, officer_id, category_type, caseload_type ORDER BY export_date DESC) = 1
),
count_of_archived_outliers AS (
    SELECT
        state_code,
        metric_id,
        end_date,
        last_export_date,
        category_type,
        caseload_type,
        COUNTIF(status = 'FAR') AS archived_number_of_outliers
    FROM archived_outliers
    --only count outliers exported on the last export day for that metric/end_date
    WHERE last_export_date = max_export_date
    GROUP BY 1,2,3,4,5,6
),
count_of_current_outliers AS (
  -- Query the current count of outliers
    SELECT
        state_code, 
        metric_id, 
        end_date,
        category_type,
        caseload_type,
        COUNTIF(status = 'FAR') AS current_number_of_outliers
    FROM `{project_id}.{outliers_views_dataset}.supervision_officer_outlier_status_materialized`
    GROUP BY 1,2,3,4,5
)
SELECT
  state_code AS region_code,
  metric_id,
  category_type,
  caseload_type,
  end_date,
  a.last_export_date,
  COALESCE(a.archived_number_of_outliers, 0) AS last_export_count,
  c.current_number_of_outliers AS current_count
FROM count_of_current_outliers c
LEFT JOIN count_of_archived_outliers a
  USING (state_code, metric_id, category_type, caseload_type, end_date)
ORDER BY 1,2,3,4,5
"""

OUTLIER_STATUS_PERCENT_CHANGE_EXCEEDED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=OUTLIER_STATUS_PERCENT_CHANGE_EXCEEDED_VIEW_NAME,
    view_query_template=OUTLIER_STATUS_PERCENT_CHANGE_EXCEEDED_QUERY_TEMPLATE,
    description=OUTLIER_STATUS_PERCENT_CHANGE_EXCEEDED_DESCRIPTION,
    outliers_views_dataset=OUTLIERS_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        OUTLIER_STATUS_PERCENT_CHANGE_EXCEEDED_VIEW_BUILDER.build_and_print()
