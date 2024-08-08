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
Identifies when a considerable change in the statewide target has occurred for a given metric. 
NOTE: This validation may pass on subsequent runs after failing even if the underlying issue is not resolved.
"""

STATEWIDE_TARGET_PERCENT_CHANGE_EXCEEDED_QUERY_TEMPLATE = """
WITH archived_targets AS (
  -- Query the latest exported targets
    SELECT
        DISTINCT
        state_code, 
        metric_id, 
        category_type,
        caseload_type,
        target, 
        end_date,
        export_date AS last_export_date
    FROM `{project_id}.{outliers_views_dataset}.supervision_officer_outlier_status_archive_materialized`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code, metric_id, category_type, caseload_type, end_date ORDER BY export_date DESC) = 1
),
current_targets AS (
  -- Query the current statewide targets
    SELECT
        DISTINCT
        state_code, 
        metric_id, 
        category_type,
        caseload_type,
        target, 
        end_date 
    FROM `{project_id}.{outliers_views_dataset}.supervision_officer_outlier_status_materialized`
)
SELECT
  state_code AS region_code,
  metric_id,
  category_type,
  caseload_type,
  end_date,
  a.last_export_date,
  COALESCE(a.target, 0) AS last_export_target,
  c.target AS current_target
FROM current_targets c
LEFT JOIN archived_targets a
  USING (state_code, metric_id, category_type, caseload_type, end_date)
ORDER BY 1,2,3,4,5
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
