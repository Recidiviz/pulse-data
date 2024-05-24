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
        Email
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
        CAST(NULL as STRING) as Email
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
-- It possible for a single StaffId to be associated with different name information 
-- (since we're pulling from two different sources and StaffId is not the PK)
-- Let's make a prioritized list of which name information to use for each StaffId
names as (
    SELECT *
    FROM (
        SELECT 
            StaffId,
            FirstName,
            MiddleName,
            LastName,
            Suffix,
            Email,
            ROW_NUMBER() OVER(PARTITION BY StaffId ORDER BY CASE Source WHEN 'ATLAS' THEN 1 WHEN 'CIS' THEN 2 END, LPAD(SourceId, 8, '0') DESC) as priority
        FROM unioned
        WHERE (
            UPPER(FirstName) NOT LIKE '%NAMCHG%' AND 
            UPPER(FirstName) NOT LIKE '%NMCHG%' AND 
            UPPER(FirstName) NOT LIKE '%- NC' AND
            UPPER(FirstName) NOT LIKE '%NMCGH%' AND
            UPPER(FirstName) NOT LIKE '%NMCH%' AND
            UPPER(FirstName) NOT LIKE '%- WRONG%' AND
            UPPER(FirstName) NOT LIKE '%- HISTORY%' AND
            UPPER(FirstName) NOT LIKE '%- DUPLICATE%' AND
            UPPER(FirstName) NOT LIKE '%- OLD%'
        ) 
        OR UPPER(FirstName) IS NULL
    ) sub_all_names
    WHERE priority = 1
)

SELECT
    u.Source,
    u.SourceId,
    u.StaffId,
    UPPER(n.FirstName) as FirstName,
    UPPER(n.MiddleName) as MiddleName,
    UPPER(n.LastName) as LastName,
    UPPER(n.Suffix) as Suffix,
    UPPER(n.Email) as Email
FROM unioned u
LEFT JOIN names n USING(StaffId)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ix",
    ingest_view_name="state_staff",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
