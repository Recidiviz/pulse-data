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
"""Query containing staff caseload type periods for case officers in TN
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- getting information from @ALL roster
all_periods as (
  SELECT DISTINCT
    external_id, 
    location_district,
    UPPER(CaseloadType) AS CaseloadType, 
    UPPER(Active) as Active,
    SpecializedTeam,
    update_datetime, 
    FROM {RECIDIVIZ_REFERENCE_staff_supervisor_and_caseload_roster@ALL}
    WHERE UPPER(Active) IN ('YES', 'Y', 'ACTIVE')
), 
-- getting rid of roles that aren't considered caseloads
-- #TODO(#31902): Continue to clarify and refine caseload types 
get_real_caseloads AS (
    SELECT * FROM all_periods
    WHERE CASELOADTYPE NOT LIKE '%NO CASELOAD%' 
        AND CASELOADTYPE NOT LIKE '%SECRETARY%' 
        AND CASELOADTYPE NOT LIKE '%DIRECTOR%' 
        AND CASELOADTYPE NOT LIKE '%EMPLOYMENT%' 
        AND CASELOADTYPE NOT LIKE '%PSI%' 
        AND CASELOADTYPE NOT LIKE '%CC%' 
        AND CASELOADTYPE NOT LIKE '%BHS%' --BHS are Social Workers- no caseload (Behavioral Health Specialist)
        AND CASELOADTYPE NOT LIKE 'DATS%' --DAT are Risk and Needs assessors- they do not have a caseload.
        AND CASELOADTYPE NOT IN ('RESIGNED',
                                 'DAT OFFICER', --DAT are Risk and Needs assessors- they do not have a caseload.
                                 'DAATS', --DAT are Risk and Needs assessors- they do not have a caseload.
                                 'N/A',
                                 'PSA', -- Sanction officer, does not carry caseload
                                 'CA', -- Correctional Administrator, does not carry caseload
                                 'DAT',
                                 'Court',
                                 'Court/Reports',
                                 'Programming',
                                 'Sanctions',
                                 'Program Liason',
                                 'Counselor',
                                 '120-Day Employee') 

),
-- First, identify the first ever reported caseloadtype for each staff member sent to us by TN
first_reported_caseload AS (
    SELECT
    external_id as StaffID, 
    CaseloadType,
    update_datetime as UpdateDate
FROM 
    (SELECT
        external_id,
        CaseloadType,
        update_datetime,
        ROW_NUMBER() OVER (PARTITION BY external_id ORDER BY update_datetime ASC) as SEQ
    FROM get_real_caseloads s
    WHERE external_id IS NOT NULL AND CaseloadType IS NOT NULL) s 
WHERE SEQ = 1),

-- Then, determine when the caseload type sent for a given person has changed since the last time TN sent us a roster
caseload_change_recorded AS (
  SELECT 
      external_id as StaffID, 
      CaseloadType as CurrentCaseloadType,
      LAG(CaseloadType) OVER (PARTITION BY external_id ORDER BY update_datetime ASC) as PreviousCaseloadType,
      update_datetime as UpdateDate
  FROM get_real_caseloads
  WHERE external_id IS NOT NULL
),
-- Start all periods 2 years in the past, then add any key change dates, to get a list of all dates that a person has changed caseloads
key_caseload_change_dates AS(
    --arbitrary first period start dates started 2 years before first receiving caseload data
    SELECT
    DISTINCT StaffID, 
    CaseloadType, 
    CAST('2021-10-24 00:00:00' AS DATETIME) as UpdateDate
    FROM first_reported_caseload
    WHERE CaseloadType IS NOT NULL

    UNION ALL
    
    SELECT 
        StaffID, 
        CurrentCaseloadType AS CaseloadType, 
        UpdateDate
    FROM caseload_change_recorded
    WHERE CurrentCaseloadType != PreviousCaseloadType
),
-- creating a ranking to reduce duplicates
ranked_rows AS(
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY StaffID,CaseloadType,UpdateDate ORDER BY UpdateDate DESC) as RecencyRank
    FROM key_caseload_change_dates
),
-- creating unique rows
create_unique_rows AS (
    SELECT 
        StaffID,
        CaseloadType,
        UpdateDate, 
        ROW_NUMBER() OVER (PARTITION BY StaffID ORDER BY UpdateDate ASC) AS CaseloadChangeOrder
    FROM ranked_rows
    WHERE RecencyRank = 1
),
-- creating periods with all the key dates
construct_periods AS (
    SELECT 
        StaffID,
        CaseloadType,
        UpdateDate as StartDate,
        LEAD(UpdateDate) OVER person_sequence as EndDate,
        CaseloadChangeOrder
    FROM create_unique_rows 
    WINDOW person_sequence AS (PARTITION BY StaffID ORDER BY CaseloadChangeOrder)
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
-- with the first update_datetime of when they became inactive, if it is after the start date of the period.
SELECT 
    REGEXP_REPLACE(construct_periods.StaffID, r'[^A-Z0-9]', '') as StaffID, 
    CaseloadType, 
    StartDate,
    CASE 
        -- first_inactive_date is set to only be populated if someone's current status in 
        -- the Staff table is inactive. If it's not null, they are inactive in Staff but roster 
        -- information is outdated which is keeping their supervisor periods open.Therefore, 
        -- we close their open periods with the first date there are inactive in the Staff table. 
        WHEN 
            construct_periods.EndDate IS NULL 
            AND first_inactive_date IS NOT NULL
            AND id.first_inactive_date > construct_periods.StartDate 
            THEN id.first_inactive_date
        -- However, there are some people who were inactive in Staff before the start date of 
        -- the roster period information, so in this case, we close their incorrectly open periods 
        -- with the StartDate of the periods (effectively making them into zero day periods that will be ignored.       
        WHEN construct_periods.EndDate IS NULL
            AND first_inactive_date IS NOT NULL THEN construct_periods.StartDate 
        ELSE construct_periods.EndDate
    END AS EndDate,
    CaseloadChangeOrder
FROM construct_periods
LEFT JOIN inactive_dates id ON construct_periods.StaffID = id.StaffID

"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_tn",
    ingest_view_name="StaffCaseloadTypePeriod",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
