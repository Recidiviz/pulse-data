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
"""
Helper for the outliers_staff_count_percent_change_<type> validations.
"""


def staff_count_percent_change_helper(pct_change_type: str) -> str:
    if pct_change_type == "intermonth":
        # The intramonth validation should be "skipped"/pass within a month.
        case_when_expr = "CURRENT_DATE('US/Pacific') != DATE_TRUNC(CURRENT_DATE('US/Pacific'), MONTH)"
    elif pct_change_type == "intramonth":
        # The intermonth validation should be "skipped"/pass on the first of the month.
        case_when_expr = (
            "CURRENT_DATE('US/Pacific') = DATE_TRUNC(CURRENT_DATE('US/Pacific'), MONTH)"
        )
    else:
        raise ValueError("Unexpected argument to staff_count_percent_change_helper")

    return f"""
WITH
previous_export_date AS (
  SELECT MAX(export_date) AS export_date
  FROM `{{project_id}}.{{outliers_views_dataset}}.supervision_officer_outlier_status_archive_materialized`
  WHERE export_date < CURRENT_DATE('US/Pacific')
),
previous_export AS (
  SELECT 
    state_code, 
    officer_id AS external_id,
    archive.export_date AS last_export_date,
    "SUPERVISION_OFFICER" AS role
  FROM `{{project_id}}.{{outliers_views_dataset}}.supervision_officer_outlier_status_archive_materialized` archive
  -- Get the previous export
  INNER JOIN previous_export_date USING (export_date)
  -- Get officers who have metrics for the period ending on the first of the month of the previous export 
  WHERE archive.end_date = DATE_TRUNC(previous_export_date.export_date, MONTH)

  UNION ALL

  SELECT 
    state_code,
    external_id,
    archive.export_date AS last_export_date,
    "SUPERVISION_OFFICER_SUPERVISOR" AS role
  FROM `{{project_id}}.{{outliers_views_dataset}}.supervision_officer_supervisors_archive_materialized` archive
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
  FROM `{{project_id}}.{{outliers_views_dataset}}.supervision_officer_outlier_status_materialized` s
  -- Get officers who have metrics for the period ending on the first of the current month
  WHERE end_date = DATE_TRUNC(CURRENT_DATE('US/Pacific'), MONTH)

  UNION ALL

  SELECT 
    state_code,
    external_id,
    "SUPERVISION_OFFICER_SUPERVISOR" AS role
  FROM `{{project_id}}.{{outliers_views_dataset}}.supervision_officer_supervisors_materialized`
)

SELECT 
  state_code,
  state_code AS region_code,
  role,
  previous_export_count.last_export_date,
  CASE
    -- Make the validation pass when the CASE WHEN is evaluated to True;
    -- See the validation description for a full description on the validation logic.
    WHEN {case_when_expr} THEN 0
    ELSE COALESCE(previous_export_count.total_staff_count, 0)
  END AS last_export_staff_count,
  CASE
    WHEN {case_when_expr} THEN 0
    ELSE COUNT(DISTINCT current_staff.external_id)
  END AS current_staff_count,
FROM current_staff
FULL OUTER JOIN previous_export_count
  USING (state_code, role)
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2
"""
