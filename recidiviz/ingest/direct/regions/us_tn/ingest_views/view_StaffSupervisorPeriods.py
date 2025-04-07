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
-- doing some intial cleaning so the following CTEs don't interpret the raw data differently
upper_case_roster AS (
  SELECT
    UPPER(external_id) as StaffID, 
    UPPER(SupervisorID) as SupervisorID,
    update_datetime as UpdateDate,
    UPPER(CaseloadType) AS CaseloadType,
    IF(UPPER(CaseloadType) LIKE '%SEX%' OR UPPER(CaseloadType) LIKE'%SCU%', 'Y', 'N') AS SCU_caseload,
    UPPER(Active) AS Active
  FROM {RECIDIVIZ_REFERENCE_staff_supervisor_and_caseload_roster@ALL}
),
-- First, identify the first ever reported surpervisor for each staff member sent to us by TN
first_reported_supervisor AS (
  SELECT
    StaffID, 
    SupervisorID AS StaffSupervisorID,
    UpdateDate,
    SCU_caseload,
  FROM upper_case_roster
  WHERE StaffID IS NOT NULL AND SupervisorID IS NOT NULL AND Active IN ('YES', 'Y', 'ACTIVE')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY StaffID ORDER BY UpdateDate ASC) = 1
),
-- Then, determine when the supervisor sent for a given person has changed since the last time TN sent us a roster
supervisor_change_recorded AS (
  SELECT 
      StaffID, 
      SupervisorID AS CurrentStaffSupervisorID,
      LAG(SupervisorID) OVER (PARTITION BY StaffID ORDER BY UpdateDate ASC, SupervisorId) AS PreviousStaffSupervisorID,
      UpdateDate,
      SCU_caseload
  FROM upper_case_roster
  WHERE StaffID IS NOT NULL
  AND Active IN ('YES', 'Y', 'ACTIVE')
),
-- Start all periods far back in the past, then add any key change dates, to get a list of all dates that a person has changed supervisors
key_supervisor_change_dates AS(
  --arbitrary first period start dates since beginning of time 
  SELECT DISTINCT 
    StaffID, 
    StaffSupervisorID, 
    CAST('1900-01-02 00:00:00' AS DATETIME) as UpdateDate, 
    SCU_caseload
  FROM first_reported_supervisor
  WHERE StaffID IS NOT NULL

  UNION ALL
    
  SELECT 
    StaffID, 
    CurrentStaffSupervisorID as StaffSupervisorID,
    UpdateDate,
    SCU_caseload
  FROM supervisor_change_recorded
  WHERE (CurrentStaffSupervisorID != PreviousStaffSupervisorID)
),
-- making rows unique by staff, supervisorid, and updatedate for SCU cases where multiple 
-- supervisors are expected
create_unique_rows_scu AS (
    SELECT 
        StaffID,
        StaffSupervisorID,
        UpdateDate, 
        SCU_caseload,
    FROM key_supervisor_change_dates
    WHERE SCU_caseload = 'Y'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY StaffID,StaffSupervisorID,UpdateDate ORDER BY UpdateDate DESC) = 1
),
-- creating unique rows for non scu cases where we only want to partitition by staff id
-- so we don't allow multiple overlapping supervisors periods
create_unique_rows_non_scu AS (
    SELECT 
        StaffID,
        StaffSupervisorID,
        UpdateDate, 
        SCU_caseload,
    FROM key_supervisor_change_dates
    WHERE SCU_caseload = 'N'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY StaffID,UpdateDate ORDER BY UpdateDate DESC) = 1
), 
-- unioning together the seperate logic for SCU and non SCU caseloads
union_scu_and_non AS (
   SELECT 
        StaffID,
        StaffSupervisorID,
        UpdateDate, 
        SCU_caseload
    FROM create_unique_rows_scu

    UNION ALL 

    SELECT 
        StaffID,
        StaffSupervisorID,
        UpdateDate, 
        SCU_caseload
    FROM create_unique_rows_non_scu
),
-- creating supervisor periods from key dates, using different enddate logic for SCU and non SCU cases
construct_periods AS (
  SELECT 
    StaffID,
    StaffSupervisorID,
    UpdateDate as StartDate,
    IF(SCU_caseload = 'Y', 
        LEAD(UpdateDate) OVER (PARTITION BY StaffID, StaffSupervisorID ORDER BY UpdateDate ASC), 
        LEAD(UpdateDate) OVER (PARTITION BY StaffID ORDER BY UpdateDate ASC))
    AS EndDate,
    ROW_NUMBER() OVER (PARTITION BY StaffID ORDER BY UpdateDate ASC) AS SupervisorChangeOrder
  FROM union_scu_and_non
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
-- NOTE: We are not currently closing periods if they don't show up in the latest manual roster
-- we have made TODO(#35129): to track if this is a change we would like to make in the future
SELECT
    REGEXP_REPLACE(construct_periods.StaffID, r'[^A-Z0-9]', '') AS StaffID,
    REGEXP_REPLACE(StaffSupervisorID, r'[^A-Z0-9]', '') AS StaffSupervisorID, 
    construct_periods.StartDate,
    CASE 
        -- first_inactive_date is set to only be populated if someone's current status in 
        -- the Staff table is inactive. If it's not null, they are inactive in Staff but roster 
        -- information is outdated which is keeping their supervisor periods open.Therefore, 
        -- we close their open periods with the first date there are inactive in the Staff table. 
        WHEN 
            construct_periods.EndDate IS NULL 
            AND id.first_inactive_date IS NOT NULL
            AND id.first_inactive_date > construct_periods.StartDate 
            THEN id.first_inactive_date
        -- However, there are some people who were inactive in Staff before the start date of 
        -- the roster period information, so in this case, we close their incorrectly open periods 
        -- with the StartDate of the periods (effectively making them into zero day periods that will be ignored.       
        WHEN construct_periods.EndDate IS NULL
            AND id.first_inactive_date IS NOT NULL THEN construct_periods.StartDate 
        -- Closing open periods where the StaffSupervisor is inactive with first inactive date
        WHEN 
            construct_periods.EndDate IS NULL 
            AND supid.first_inactive_date IS NOT NULL
            AND supid.first_inactive_date > construct_periods.StartDate 
            THEN supid.first_inactive_date
        -- Closing open periods with the startdate where the StaffSupervisor is inactive 
        -- and inactive_date is before period start date
        WHEN construct_periods.EndDate IS NULL
            AND supid.first_inactive_date IS NOT NULL 
          THEN construct_periods.StartDate 
        ELSE construct_periods.EndDate
    END AS EndDate,
    SupervisorChangeOrder
FROM 
    construct_periods
LEFT JOIN inactive_dates id ON construct_periods.StaffID = id.StaffID
LEFT JOIN inactive_dates supid ON construct_periods.StaffSupervisorID = supid.StaffID
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_tn",
    ingest_view_name="StaffSupervisorPeriods",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
