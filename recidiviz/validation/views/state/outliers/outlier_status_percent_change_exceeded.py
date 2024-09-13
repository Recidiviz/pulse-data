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
Identifies when a considerable change in the number of outliers has occurred for a given metric within a specific period within the last 7 exports.

NOTES: 

If the change is expected, you can wait for this validation to self-resolve because the failure will cycle out after 7 new exports.  
However, this also means that the validation may pass on subsequent runs after failing even if the underlying issue is not resolved if there has been 7 new exports since. 

This validation only checks outlier counts for metrics, category_types, and caseload_types within periods that have previously been exported.  Also note that this validation does not identify changes in the number of outliers between adjacent periods.
"""

OUTLIER_STATUS_PERCENT_CHANGE_EXCEEDED_QUERY_TEMPLATE = """
WITH 
count_of_current_outliers AS (
  -- Query the current count of outliers
    SELECT
        state_code, 
        metric_id, 
        end_date,
        CURRENT_DATE('US/Eastern') AS date_of_data,
        category_type,
        caseload_type,
        COUNTIF(status = 'FAR') AS outliers_count
    FROM `{project_id}.{outliers_views_dataset}.supervision_officer_outlier_status_materialized`
    GROUP BY 1,2,3,4,5,6
),
current_metrics_types AS (
  SELECT DISTINCT
    state_code, 
    metric_id,
    category_type,
    caseload_type,
    end_date
  FROM count_of_current_outliers
),
archived_outliers AS (
  -- Query the latest status per metric per end date per officer per category and caseload_type per export_date
    SELECT
        state_code, 
        metric_id, 
        officer_id,
        status,
        end_date,
        category_type,
        caseload_type,
        export_date
    FROM `{project_id}.{outliers_views_dataset}.supervision_officer_outlier_status_archive_materialized`
),
count_of_archived_outliers AS (
    SELECT
        state_code,
        metric_id,
        end_date,
        export_date,
        category_type,
        caseload_type,
        COUNTIF(status = 'FAR') AS outliers_count
    FROM archived_outliers
    GROUP BY 1,2,3,4,5,6
),
archived_metrics_types AS (
  SELECT DISTINCT
    state_code, 
    metric_id,
    category_type,
    caseload_type,
    end_date,
    MIN(export_date) OVER(PARTITION BY state_code, metric_id, category_type, caseload_type, end_date) AS earliest_export_date
  FROM count_of_archived_outliers
)

SELECT * EXCEPT(earliest_export_date)
FROM (
  SELECT
    state_code AS region_code,
    metric_id,
    category_type,
    caseload_type,
    end_date,
    date_of_data,
    outliers_count,
    LAG(outliers_count) OVER (PARTITION BY state_code, metric_id, category_type, caseload_type, end_date ORDER BY date_of_data) as prev_outliers_count,
    LAG(date_of_data) OVER (PARTITION BY state_code, metric_id, category_type, caseload_type, end_date ORDER BY date_of_data) as prev_export_date,
    earliest_export_date
  FROM (
    SELECT * FROM count_of_current_outliers
    UNION ALL
    SELECT * FROM count_of_archived_outliers
  )
  INNER JOIN current_metrics_types USING(state_code, metric_id, category_type, caseload_type, end_date)
  INNER JOIN archived_metrics_types USING(state_code, metric_id, category_type, caseload_type, end_date)
)
WHERE date_of_data > earliest_export_date
QUALIFY RANK() OVER(PARTITION BY region_code, metric_id, category_type, caseload_type, end_date ORDER BY date_of_data DESC) <= 7
ORDER BY 1,2,3,4,5,6 
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
