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
"""Query containing supervision staff supervisor periods."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH
-- Filter to employee rows that reflect a supervisor change or when the employee is
-- "logically deleted" (this happens when the employee leaves MEDOC).
supervisor_critical_dates AS (
  SELECT *
  FROM {CIS_900_EMPLOYEE@ALL} e
  -- WHERE Logical_Delete_Ind = 'N'
  QUALIFY 
    -- Captures any time the supervisor changes, including to or from NULL
    COALESCE(e.Cis_900_Employee_Supervisor_Id, "NULL") != COALESCE(LAG(e.Cis_900_Employee_Supervisor_Id) OVER employee_window, "NULL") 

    -- Captures anytime the employees "delete" status changes, including to or from NULL. A person will be deleted if they 
    OR COALESCE(e.Logical_Delete_Ind, "NULL") != COALESCE(LAG(e.Logical_Delete_Ind) OVER employee_window, "NULL") 

  WINDOW employee_window AS (PARTITION BY e.Employee_Id ORDER BY DATE(LEFT(Modified_On_Date, 10)), update_datetime)
),
-- Creates periods by looking at the next critical date. We need to do this in a
-- separate CTE from the last one because otherwise, the LEAD() performed for the
-- end_date column will happen before filtering to the correct critical dates. 
supervisor_periods AS (
  SELECT
    scd.Cis_900_Employee_Supervisor_Id, 
    scd.Employee_Id,
    DATE(LEFT(scd.Modified_On_Date, 10)) AS start_date,
    DATE(LEFT(LEAD(scd.Modified_On_Date) OVER (PARTITION BY scd.Employee_Id ORDER BY Modified_On_Date), 10)) AS end_date,
    Logical_Delete_Ind
  FROM supervisor_critical_dates AS scd
)
SELECT
  Cis_900_Employee_Supervisor_Id,
  Employee_Id,
  start_date,
  end_date
FROM supervisor_periods
WHERE Logical_Delete_Ind = 'N' 
  AND Cis_900_Employee_Supervisor_Id IS NOT NULL
"""


VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_me",
    ingest_view_name="supervision_staff_supervisor_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
