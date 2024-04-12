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
"""Ingest view for supervision staff role periods from the Docstars system.

The `docstars_officers` table that we receive is always a full
historical export, and has a field called `STATUS` to mark whether or
not officers are active or not. This view uses that field to close periods of an 
officer having a given role. Since the only role currently included in the
StateStaffRolePeriod entity in ND is `StateStaffRoleType.SUPERVISION_OFFICER`, 
these periods are proxies for an officer's employment. There should be no instances of 
officers having an open period while their `STATUS = 0.`
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH staff_from_directory AS (
-- This CTE pulls in relevant data from the monthly P&P directories that we receive
-- via email and upload as raw data. It uses the update_datetime on each file to 
-- track edge dates, as well as the most recent file we received (last_file_update_datetime),
-- and the most recent file that each employee appeared in (last_appearance_datetime).
    SELECT
        -- Make officer IDs uniform across sources 
        CAST(OFFICER AS INT64) AS OFFICER,
        CAST(update_datetime AS DATETIME) AS edge_date,
        '(1)' AS STATUS, 
        MAX(CAST(update_datetime AS DATETIME)) OVER (PARTITION BY TRUE) AS last_file_update_datetime,
        MAX(CAST(update_datetime AS DATETIME)) OVER (PARTITION BY OFFICER) AS last_appearance_datetime, 
        UPPER(JobTitle) AS JobTitle
    FROM {RECIDIVIZ_REFERENCE_PP_directory@ALL}
),
staff_from_docstars AS (
-- This CTE gets the officer data directly from the Docstars system into the same format
-- as above, to the extent possible. In Docstars, RecDate is the date the row was created.
-- There is an additional filtering step necessary here because in Docstars, officers
-- do not stop appearing like they do in the directory when they become inactive. They
-- appear in perpetuity with STATUS = '0'. To accurately track the date an officer
-- became inactive, we filter the Docstars data to only include rows where STATUS='(1)'.
    SELECT * FROM (
        SELECT DISTINCT
        -- Make officer IDs uniform across sources 
        CAST(OFFICER AS INT64) AS OFFICER,
        CAST(RecDate AS DATETIME) AS edge_date, 
        STATUS,
        MAX(CAST(RecDate AS DATETIME)) OVER (PARTITION BY TRUE) AS last_file_update_datetime,
        MAX(CAST(RecDate AS DATETIME)) OVER (PARTITION BY OFFICER) AS last_appearance_datetime, 
        -- There is no JobTitle information in this table in Docstars.
        CAST(NULL AS STRING) AS JobTitle,
    FROM {docstars_officers@ALL} )
    WHERE STATUS = '(1)' 
),
combined_data AS (
-- This CTE combines the data from the two sources. Officers often appear in both, 
-- so we want to consider all rows from both sources when creating periods.
    SELECT 
        OFFICER,
        edge_date,
        STATUS,
        last_file_update_datetime,
        last_appearance_datetime,
        JobTitle
    FROM staff_from_directory
    
    UNION ALL 

    SELECT 
        OFFICER,
        edge_date,
        STATUS,
        last_file_update_datetime,
        last_appearance_datetime,
        JobTitle
    FROM staff_from_docstars 
),
critical_dates AS (
-- This CTE filters the combined data to include only the rows that are relevant to the
-- role periods we are constructing. These are rows where an officer just started working
-- or changed roles. We also include all rows where the edge date is the last update
-- we have about a given officer, to account for rows that signify an officer becoming inactive.
    SELECT *
    FROM (
        SELECT
            OFFICER,
            edge_date,
            STATUS,
            LAG(STATUS) OVER person_window AS prev_status,
            LAG(JobTitle) OVER person_window AS prev_JobTitle, 
            COALESCE(
                JobTitle,
                LAST_VALUE(JobTitle IGNORE NULLS) OVER (person_window range between UNBOUNDED preceding and current row),
                'GENERAL'
            ) AS JobTitle,
            last_file_update_datetime,
            last_appearance_datetime 
        FROM combined_data
        WINDOW person_window AS (PARTITION BY OFFICER ORDER BY edge_date, last_file_update_datetime DESC)
        ) cd
        WHERE 
        -- officer just started working
        ((cd.prev_JobTitle IS NULL AND cd.JobTitle IS NOT NULL) 
        OR STATUS = '(1)' AND prev_status IS NULL)
        -- officer changed roles
        OR cd.JobTitle != cd.prev_JobTitle
        -- include the latest update even if the previous two conditions are not true
        OR edge_date = last_file_update_datetime
        WINDOW person_window AS (PARTITION BY OFFICER ORDER BY edge_date, last_file_update_datetime DESC)
), all_periods AS (
-- This CTE constructs periods using the compiled critical dates. End dates follow this 
-- logic: 
    -- 1. If a person stopped working, close their employment period on the last date we 
    --    receive data that included them as an active employee.
    -- 2. If there is a subsequent row showing a person changed roles, close the existing 
    --    period on the date of that change.
    -- 3. If a there is no subsequent row, and the edge date is the same as the most 
    --    recent date the person has appeared in the data, then leave the period open
    --    to signify that the existing period is still active.
SELECT 
    OFFICER,
    STATUS,
    edge_date AS start_date,
    CASE 
        -- If a staff member stops appearing as active in Docstars, or stops appearing in 
        -- the P&P directory entirely, close their employment period on the last date we 
        -- receive data that included them as an active officer. 
        WHEN LEAD(edge_date) OVER person_window IS NULL 
            AND edge_date < last_appearance_datetime
            THEN last_appearance_datetime 
        -- If there is a more recent update to this person's role, close the existing
        -- period on the date of the update.
        WHEN LEAD(edge_date) OVER person_window IS NOT NULL 
            THEN LEAD(edge_date) OVER person_window 
        -- All currently-employed staff will appear in the latest roster
        ELSE CAST(NULL AS DATETIME)
    END AS end_date,
    JobTitle
FROM critical_dates
WHERE STATUS IS NOT NULL
WINDOW person_window AS (PARTITION BY OFFICER ORDER BY edge_date, last_file_update_datetime DESC)
)
SELECT
-- This CTE cleans up the final periods. 
    OFFICER,
    JobTitle,
    start_date,
    end_date,
    ROW_NUMBER() OVER (PARTITION BY OFFICER ORDER BY start_date) AS period_seq_num,
FROM all_periods
WHERE 
    start_date != end_date
    OR end_date IS NULL
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_nd",
    ingest_view_name="docstars_staff_role_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
