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
"""Query containing state Staff Supervisor Period information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- First, identify the first ever reported surpervisor for each staff member sent to us by TN
first_reported_supervisor AS 
    (SELECT
    UPPER(external_id) as StaffID, 
    UPPER(StaffSupervisorID) as StaffSupervisorID,
    update_datetime as UpdateDate
FROM 
    (SELECT
        external_id,
        UPPER(SupervisorID) as StaffSupervisorID,
        update_datetime,
        ROW_NUMBER() OVER (PARTITION BY external_id ORDER BY update_datetime ASC) as SEQ
    FROM {RECIDIVIZ_REFERENCE_staff_supervisor_and_caseload_roster@ALL} s
    WHERE external_id IS NOT NULL AND SupervisorID IS NOT NULL AND UPPER(Active) IN ('YES', 'Y', 'ACTIVE')) s 
WHERE SEQ = 1
),
-- Then, determine when the supervisor sent for a given person has changed since the last time TN sent us a roster
supervisor_change_recorded AS (
  SELECT 
      UPPER(external_id) as StaffID, 
      UPPER(SupervisorID) as CurrentStaffSupervisorID,
      LAG(UPPER(SupervisorID)) OVER (PARTITION BY external_id ORDER BY update_datetime ASC) as PreviousStaffSupervisorID,
      update_datetime as UpdateDate
  FROM {RECIDIVIZ_REFERENCE_staff_supervisor_and_caseload_roster@ALL}
  WHERE external_id IS NOT NULL
  AND UPPER(Active) IN ('YES', 'Y', 'ACTIVE')
),
-- Start all periods far back in the past, then add any key change dates, to get a list of all dates that a person has changed supervisors
key_supervisor_change_dates AS(
    #arbitrary first period start dates since beginning of time 
    SELECT
    DISTINCT StaffID, 
    StaffSupervisorID, 
    CAST('1900-01-01 00:00:00' AS DATETIME) as UpdateDate
    FROM first_reported_supervisor
    WHERE StaffID IS NOT NULL

    UNION ALL
    
    SELECT 
        StaffID, 
        CurrentStaffSupervisorID as StaffSupervisorID,
        UpdateDate
    FROM supervisor_change_recorded
    WHERE CurrentStaffSupervisorID != PreviousStaffSupervisorID
),
-- ranking rows to insure we don't get duplicate information
ranked_rows AS(
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY StaffID,StaffSupervisorID,UpdateDate ORDER BY UpdateDate DESC) as RecencyRank
    FROM key_supervisor_change_dates
),
-- making rows unique by staff and updatedate
-- NOTE: Add a partition by supervisor ID as well when #34976 is implemented
create_unique_rows AS (
    SELECT 
        StaffID,
        StaffSupervisorID,
        UpdateDate, 
        ROW_NUMBER() OVER (PARTITION BY StaffID ORDER BY UpdateDate ASC) AS SupervisorChangeOrder
    FROM ranked_rows
    WHERE RecencyRank = 1
),
-- creating supervisor periods from key dates
construct_periods AS (
    SELECT 
        StaffID,
        StaffSupervisorID,
        UpdateDate as StartDate,
        LEAD(UpdateDate) OVER person_sequence as EndDate,
        SupervisorChangeOrder
    FROM create_unique_rows 
    WINDOW person_sequence AS (PARTITION BY StaffID ORDER BY SupervisorChangeOrder)
),
-- Getting the most recent status from Staff to see if an employee is active or inactive
current_status AS (
 SELECT 
    StaffID,
    Status
   FROM 
        {Staff@ALL}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY StaffID ORDER BY update_datetime DESC) = 1
),
-- Taking the first update datetime where staff status goes inactive
inactive_dates AS (
  SELECT 
        StaffID,
        update_datetime AS first_inactive_date,
    FROM 
        {Staff@ALL}
    WHERE 
        Status = 'I' AND StaffID IN
                      (SELECT StaffID
                      FROM current_status
                      WHERE Status = 'I')
    QUALIFY 
       ROW_NUMBER() OVER (PARTITION BY StaffID ORDER BY update_datetime) = 1
)
-- Here we are closing the supervisor periods if their latest status in Staff table is inactive,
-- with the first update_datetime of when they became inactive, if it is after the start date of the period
SELECT
    REGEXP_REPLACE(construct_periods.StaffID, r'[^A-Z0-9]', '') AS StaffID,
    REGEXP_REPLACE(StaffSupervisorID, r'[^A-Z0-9]', '') as StaffSupervisorID, 
    construct_periods.StartDate,
    CASE 
        WHEN 
            construct_periods.EndDate IS NULL 
            AND first_inactive_date IS NOT NULL
            AND id.first_inactive_date > construct_periods.StartDate 
            THEN id.first_inactive_date
        ELSE construct_periods.EndDate
    END AS EndDate,
   SupervisorChangeOrder
FROM 
    construct_periods
LEFT JOIN inactive_dates id ON construct_periods.StaffID = id.StaffID
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_tn",
    ingest_view_name="StaffSupervisorPeriods",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
