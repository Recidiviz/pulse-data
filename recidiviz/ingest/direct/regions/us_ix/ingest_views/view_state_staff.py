# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Query that generates state staff information."""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# For Idaho, we're going to pull staff information from both CIS (their legacy system) and ATLAS (their new system)
# In CIS, the ID variable for employees is empl_cd, and in ATLAS, the ID variable for employees is EmployeeId
# The key we're using to link staff across systems is StaffId, which in CIS is stored in a field called empl_sdesc,
# and in Atlas is stored either in a field called StaffId or used as part of the email.
# Since there might be multiple records attached to each StaffId within each source, we're going to use EmployeeId, empl_cd,
# and StaffId all as valid id types and allow multiple values for each type.  For name info, we're going to only pull from
# the newest record found for that StaffId.

VIEW_QUERY_TEMPLATE = """

WITH 
unioned as (

    -- pull together all relevant info from Atlas
    (SELECT DISTINCT
        'ATLAS' as Source,
        EmployeeId as SourceId,
        UPPER(COALESCE(StaffId, SPLIT(Email, '@')[SAFE_OFFSET(0)])) as StaffId,
        FirstName,
        MiddleName,
        LastName,
        Suffix,
        Email,
        ROW_NUMBER()OVER(PARTITION BY FirstName,MiddleName,LastName ORDER BY Inactive) as Inactive,
        RANK()OVER(PARTITION BY UPPER(COALESCE(StaffId, SPLIT(Email, '@')[SAFE_OFFSET(0)])) ORDER BY Email) as doubleStaffId
    FROM {ref_Employee})

    UNION ALL

    -- pull together all relevant info from CIS
    --   We pull names from empl_ldesc with takes the format "[last name], [first name] [middle initial if exists]"
    --   Email and Suffix not available in CIS
    (SELECT DISTINCT
        'CIS' as Source,
        empl_cd as SourceId,
        UPPER(empl_sdesc) as StaffId,
        SPLIT(first_plus_middle, ' ')[SAFE_OFFSET(0)] as FirstName,
        CASE
            WHEN first_plus_middle like '% %'
            THEN SPLIT(first_plus_middle, ' ')[OFFSET(1)] 
            ELSE CAST(NULL as STRING)
            END AS MiddleName,
        LastName,
        CAST(NULL as STRING) as Suffix,
        CAST(NULL as STRING) as Email,
        CAST(NULL as INT64) as Inactive,
        CAST(NULL as INT64) as doubleStaffId
    FROM (
        SELECT
            empl_cd,
            empl_sdesc,
            CASE 
                WHEN empl_ldesc like '%, %' 
                THEN (SPLIT(empl_ldesc, ', ')[OFFSET(1)])
                ELSE CAST(NULL as STRING)
                END AS first_plus_middle,
            CASE 
                WHEN empl_ldesc like '%, %' 
                THEN (SPLIT(empl_ldesc, ', ')[OFFSET(0)])
                ELSE CAST(NULL as STRING)
                END AS LastName,
            empl_stat
        FROM {employee}
    ) subquery
    -- only pull active employees
    WHERE empl_stat = 'A')
),
-- There are times when many users can have different emails/names but the same StaffId
-- so we concat a suffix so that two separate State Staff entities can be created
cleaned_union AS (
    SELECT
        Source,
        SourceId,
        CASE
            WHEN doubleStaffId = 2
            THEN CONCAT(StaffId, "-2")
            ELSE StaffId
        END AS StaffId,
        Email,
        FirstName,
        MiddleName,
        LastName,
        Suffix,
        Inactive
    FROM unioned
),
-- It possible for a single StaffId to be associated with different name information 
-- (since we're pulling from two different sources and StaffId is not the PK)
-- Let's make a prioritized list of which name information to use for each StaffId
names as (
    SELECT DISTINCT *
    FROM (
        SELECT 
            StaffId,
            FirstName,
            MiddleName,
            LastName,
            Suffix,
            Email,
            Inactive,
            ROW_NUMBER() OVER(PARTITION BY StaffId ORDER BY CASE Source WHEN 'ATLAS' THEN 1 WHEN 'CIS' THEN 2 END, LPAD(SourceId, 8, '0') DESC) as priority
        FROM cleaned_union
    ) sub_all_names
    WHERE priority = 1
),
-- some names come in strings to denote that the row is a name change, we want to remove
-- this text so it's not present when surfaced to the officers
cleaned_names AS (
    SELECT 
        StaffId,
        REGEXP_REPLACE(
            UPPER(FirstName), 
            '-NAMCHGFLAG|-NAMCHG|-NMCHG|- NC|NMCGH|NMCH|- WRONG|- HISTORY|- DUPLICATE|- OLD', 
            ''
        ) AS FirstName,
        REGEXP_REPLACE(
            UPPER(MiddleName), 
            '-NAMCHGFLAG|-NAMCHG|-NMCHG|- NC|NMCGH|NMCH|- WRONG|- HISTORY|- DUPLICATE|- OLD', 
            ''
        ) AS MiddleName,
        REGEXP_REPLACE(
            UPPER(LastName), 
            '-NAMCHGFLAG|-NAMCHG|-NMCHG|- NC|NMCGH|NMCH|- WRONG|- HISTORY|- DUPLICATE|- OLD', 
            ''
        ) AS LastName,
        Suffix,
        Email,
        Inactive
    FROM names
),
-- Create aggragated string arrarys of all StaffIds related to a staff person
agg_staff_ids AS (
    SELECT 
        Email,
        CASE 
            WHEN COUNT(DISTINCT StaffId) = 1 THEN MAX(StaffId)
            ELSE NULL
        END as StaffId,
        CASE 
            WHEN COUNT(DISTINCT StaffId) > 1 THEN STRING_AGG(DISTINCT StaffId, ',' ORDER BY StaffId)
            ELSE NULL
        END as StaffIds
    FROM cleaned_union u
    GROUP BY Email
),
-- Create aggragated string arrarys of all employeeCodes related to a staff person
agg_employee_cds AS (
    SELECT DISTINCT
        StaffId,
        Source,
        STRING_AGG(DISTINCT SourceId, ',' ORDER BY SourceId) AS employeeCodes
    FROM cleaned_union u 
    WHERE Source = 'CIS'
    GROUP BY StaffId, Source
),
-- Create aggragated string arrarys of all employeeIds related to a staff person
agg_employee_ids AS (
    SELECT DISTINCT
        Email,
        Source,
        STRING_AGG(DISTINCT SourceId, "," ORDER BY SourceId) AS employeeIds,
    FROM cleaned_union u 
    WHERE Source = "ATLAS"
    GROUP BY 1,2
),

-- There are instances where a single staff person has multiple rows because they have 
-- different StaffIds and EmployeeIds and Empl_Cds but the same email so we collapse
-- to one row per email
colapsed_union_on_email as (
SELECT DISTINCT
    UPPER(n.FirstName) as FirstName,
    UPPER(n.MiddleName) as MiddleName,
    UPPER(n.LastName) as LastName,
    UPPER(n.Suffix) as Suffix,
    UPPER(n.Email) as Email,
    a.StaffId,
    a.StaffIds,
    e.employeeIds,
    CASE
        WHEN n.Inactive = 1
        THEN c.employeeCodes
        ELSE NULL
    END AS employeeCodes,
    ROW_NUMBER() OVER (
            PARTITION BY UPPER(n.Email)
            ORDER BY
                (e.employeeIds IS NOT NULL AND c.employeeCodes IS NOT NULL) DESC,
                e.employeeIds,
                c.employeeCodes,
                (e.employeeIds IS NULL AND c.employeeCodes IS NULL)
        ) AS rn
FROM cleaned_union u
LEFT JOIN cleaned_names n USING(StaffId)
LEFT JOIN agg_staff_ids a ON u.Email = a.Email
LEFT JOIN agg_employee_ids e ON u.Email = e.Email
LEFT JOIN agg_employee_cds c ON u.StaffId = c.StaffId),

-- During this CTE, we make sure to remove any emails that deviate from the expected
-- formating. This step is done last because email (regardless of formatting) is used to
-- deduplicate staff.
clean_emails_cte AS 
(
    SELECT DISTINCT
       FirstName,
        MiddleName,
        LastName,
        Suffix,
        CASE 
            WHEN REGEXP_CONTAINS(Email, r"@") 
                AND NOT REGEXP_CONTAINS(Email, r"[ (),:;<>[\\]\\\\]")
                AND SUBSTR(Email, 1, STRPOS(Email, '@') - 1) NOT IN ("X", "UNKNOWN", "NONE", "NONAME", "NOBODY")
            THEN Email
            ELSE NULL
        END AS Email,
        StaffId,
        StaffIds,
        employeeIds,
        employeeCodes
    FROM colapsed_union_on_email
    where rn = 1 AND (FirstName IS NOT NULL OR LastName IS NOT NULL)
)

SELECT
    *
FROM clean_emails_cte;
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ix",
    ingest_view_name="state_staff",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
