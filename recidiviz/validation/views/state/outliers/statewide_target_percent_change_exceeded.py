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
"""A view comparing the most recent archived statewide target (rate) and current
statewide target by metric to determine if major changes in rates have occurred.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import OUTLIERS_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

STATEWIDE_TARGET_PERCENT_CHANGE_EXCEEDED_VIEW_NAME = (
    "statewide_target_percent_change_exceeded"
)

STATEWIDE_TARGET_PERCENT_CHANGE_EXCEEDED_DESCRIPTION = """
Identifies when a considerable change in the statewide target has occurred for a given metric within a specific period within the last 7 exports.

NOTES: 
If the change is expected, you can wait for this validation to self-resolve because the failure will cycle out after 7 new exports.  
However, this also means that the validation may pass on subsequent runs after failing even if the underlying issue is not resolved if there has been 7 new exports since.

This validation only checks target rates for metrics, category_types, and caseload_types within periods that have previously been exported.  Also note that this validation does not identify changes in the statewide target between adjacent periods.
"""

STATEWIDE_TARGET_PERCENT_CHANGE_EXCEEDED_QUERY_TEMPLATE = """
WITH 
current_targets AS (
  -- Query the current statewide targets
  SELECT
      DISTINCT
      state_code, 
      metric_id, 
      category_type,
      caseload_type,
      target, 
      end_date,
      CURRENT_DATE('US/Eastern') AS date_of_data
  FROM `{project_id}.{outliers_views_dataset}.supervision_officer_outlier_status_materialized`
),
current_metrics_types AS (
  SELECT DISTINCT
    state_code, 
    metric_id,
    category_type,
    caseload_type,
    end_date
  FROM current_targets
),
archived_targets AS (
  SELECT
      DISTINCT
      state_code, 
      metric_id, 
      category_type,
      caseload_type,
      target, 
      end_date,
      export_date
  FROM `{project_id}.{outliers_views_dataset}.supervision_officer_outlier_status_archive_materialized`
),
archived_metrics_types AS (
  SELECT DISTINCT
    state_code, 
    metric_id,
    category_type,
    caseload_type,
    end_date,
    MIN(export_date) OVER(PARTITION BY state_code, metric_id, category_type, caseload_type, end_date) AS earliest_export_date
  FROM archived_targets
)
SELECT * EXCEPT(earliest_export_date)
FROM (
  SELECT
    state_code,
    state_code AS region_code,
    metric_id,
    category_type,
    caseload_type,
    end_date,
    date_of_data,
    target,
    LAG(target) OVER (PARTITION BY state_code, metric_id, category_type, caseload_type, end_date ORDER BY date_of_data) as prev_target,
    LAG(date_of_data) OVER (PARTITION BY state_code, metric_id, category_type, caseload_type, end_date ORDER BY date_of_data) as prev_export_date,
    earliest_export_date
  FROM (
    SELECT * FROM current_targets
    UNION ALL
    SELECT * FROM archived_targets
  )
  INNER JOIN current_metrics_types USING(state_code, metric_id, category_type, caseload_type, end_date)
  INNER JOIN archived_metrics_types USING(state_code, metric_id, category_type, caseload_type, end_date)
)
WHERE date_of_data > earliest_export_date
QUALIFY RANK() OVER(PARTITION BY region_code, metric_id, category_type, caseload_type, end_date ORDER BY date_of_data DESC) <= 7
ORDER BY 1,2,3,4,5,6
"""

STATEWIDE_TARGET_PERCENT_CHANGE_EXCEEDED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=STATEWIDE_TARGET_PERCENT_CHANGE_EXCEEDED_VIEW_NAME,
    view_query_template=STATEWIDE_TARGET_PERCENT_CHANGE_EXCEEDED_QUERY_TEMPLATE,
    description=STATEWIDE_TARGET_PERCENT_CHANGE_EXCEEDED_DESCRIPTION,
    outliers_views_dataset=OUTLIERS_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        STATEWIDE_TARGET_PERCENT_CHANGE_EXCEEDED_VIEW_BUILDER.build_and_print()
