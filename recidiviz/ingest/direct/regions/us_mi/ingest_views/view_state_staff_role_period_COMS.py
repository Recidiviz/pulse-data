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

"""Query containing state staff role period information."""

from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""
WITH case_manager_dates AS (
  -- get all distinct dates associated with each officer's supervision client assignments
  SELECT 
    DISTINCT Case_Manager_Omnni_Employee_Id, 
    COALESCE(DATE(Start_Date), DATE(9999,9,9)) AS period_date
  FROM {{COMS_Case_Managers}}

  UNION DISTINCT

  SELECT 
    DISTINCT Case_Manager_Omnni_Employee_Id, 
    COALESCE(DATE(End_Date), DATE(9999,9,9))  AS period_date
  FROM {{COMS_Case_Managers}}
),
-- for each officer, create distinct spans based on the above dates
case_manager_spans AS (
  SELECT DISTINCT
    Case_Manager_Omnni_Employee_Id,
    period_date AS start_date, 
    LEAD(period_date) OVER(PARTITION BY Case_Manager_Omnni_Employee_Id ORDER BY period_date) AS end_date
  FROM case_manager_dates
),
-- for each officer, merge on the supervision client assignment periods to determine which spans were
-- spans where they were actively surpervising a client vs spans where they were in between client assignments
-- (tracked in active_flag)
case_manager_spans_w_active_flag AS (
  SELECT DISTINCT
    case_manager_spans.*,
    (orig.Case_Manager_Omnni_Employee_Id IS NOT NULL) AS active_flag
  FROM case_manager_spans
  LEFT JOIN {{COMS_Case_Managers}} orig
    ON (DATE(orig.start_date)) <= case_manager_spans.start_date
    AND COALESCE(DATE(orig.end_date), DATE(9999,9,9)) >= case_manager_spans.end_date
    AND orig.Case_Manager_Omnni_Employee_Id = case_manager_spans.Case_Manager_Omnni_Employee_Id
  WHERE case_manager_spans.start_date <> DATE(9999,9,9)
),
-- aggregate adjacent spans
final_periods AS (
    {aggregate_adjacent_spans(
        table_name="case_manager_spans_w_active_flag",
        attribute=["active_flag"],
        index_columns=["Case_Manager_Omnni_Employee_Id"])}
)
-- make period_id based on final periods
SELECT
    Case_Manager_Omnni_Employee_Id,
    start_date,
    CASE WHEN end_date = DATE(9999,9,9) THEN NULL ELSE end_date END AS end_date,
    ROW_NUMBER() OVER(PARTITION BY Case_Manager_Omnni_Employee_Id ORDER BY start_date, end_date NULLS LAST) AS period_id
FROM final_periods
WHERE active_flag
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mi",
    ingest_view_name="state_staff_role_period_COMS",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
