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
"""A view comparing the most recent archived officer status and supervisor views and the
current officer status and supervisor view by role to determine if major changes
in counts have occurred.
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import OUTLIERS_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views.dataset_config import VIEWS_DATASET

_VIEW_NAME = "outliers_staff_count_percent_change"

_VIEW_DESCRIPTION = """A view comparing the most recent archived officer status and supervisor views and the
current officer status and supervisor view by role to determine if major changes
in counts have occurred. This validation might fail on the first of the month because the end date
of the current export would be for the new period, where as the previous export will have an end date
for a different year period."""

_QUERY_TEMPLATE = """
WITH 
previous_export_date AS (
  SELECT MAX(export_date) AS export_date
  FROM `{project_id}.{outliers_views_dataset}.supervision_officer_outlier_status_archive_materialized`
  WHERE export_date < CURRENT_DATE('US/Pacific')
),
latest_period_end_date AS (
  SELECT MAX(end_date) AS end_date
  FROM `{project_id}.{outliers_views_dataset}.supervision_officer_outlier_status_archive_materialized` 
),
previous_export AS (
  SELECT 
    state_code, 
    officer_id AS external_id,
    archive.export_date AS last_export_date,
    "SUPERVISION_OFFICER" AS role
  FROM `{project_id}.{outliers_views_dataset}.supervision_officer_outlier_status_archive_materialized` archive
  -- Get the previous export
  INNER JOIN previous_export_date USING (export_date)
  -- Get officers who have end dates in the latest period
  INNER JOIN latest_period_end_date USING (end_date)

  UNION ALL

  SELECT 
    state_code,
    external_id,
    archive.export_date AS last_export_date,
    "SUPERVISION_OFFICER_SUPERVISOR" AS role
  FROM `{project_id}.{outliers_views_dataset}.supervision_officer_supervisors_archive_materialized` archive
  -- Get the previous export
  INNER JOIN previous_export_date USING (export_date)
)
, previous_export_count AS (
  SELECT 
    previous_export.state_code,
    role,
    last_export_date,
    COUNT(DISTINCT external_id) AS total_staff_count
  FROM previous_export
  GROUP BY 1, 2, 3
)
, current_staff AS (
  SELECT 
    s.state_code,
    officer_id AS external_id,
    "SUPERVISION_OFFICER" AS role
  FROM `{project_id}.{outliers_views_dataset}.supervision_officer_outlier_status_materialized` s
  -- Get officers who have end dates in the latest period
  INNER JOIN latest_period_end_date USING (end_date)

  UNION ALL

  SELECT 
    state_code,
    external_id,
    "SUPERVISION_OFFICER_SUPERVISOR" AS role
  FROM `{project_id}.{outliers_views_dataset}.supervision_officer_supervisors_materialized`
)

SELECT 
  state_code AS region_code,
  role,
  previous_export_count.last_export_date,
  COALESCE(previous_export_count.total_staff_count, 0) AS last_export_staff_count,
  COUNT(DISTINCT current_staff.external_id) AS current_staff_count
FROM current_staff
FULL OUTER JOIN previous_export_count
  USING (state_code, role)
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2
"""

OUTLIERS_STAFF_COUNT_PERCENT_CHANGE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=VIEWS_DATASET,
    view_id=_VIEW_NAME,
    view_query_template=_QUERY_TEMPLATE,
    description=_VIEW_DESCRIPTION,
    should_materialize=True,
    outliers_views_dataset=OUTLIERS_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        OUTLIERS_STAFF_COUNT_PERCENT_CHANGE_VIEW_BUILDER.build_and_print()
