# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

"""Query containing supervision staff supervisor period information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH filtered_data AS (
    SELECT DISTINCT 
        AgentName, 
        SupervisorName, 
        Agent_EmpNum, 
        Supervisor_EmpNum, 
        CAST(LastModifiedDatetime AS DATETIME) AS LastModifiedDatetime,
        MAX(CAST(LastModifiedDateTime AS DATETIME)) OVER (PARTITION BY TRUE) AS last_file_update_datetime,
        MAX(CAST(LastModifiedDateTime AS DATETIME)) OVER (PARTITION BY AgentName) AS last_appearance_datetime,
        'CASE_HISTORY' AS source
    FROM {dbo_RelAgentHistory@ALL}
    WHERE SupervisorName NOT LIKE '%Vacant%'
    AND AgentName NOT LIKE '%Vacant%'
    AND Agent_EmpNum IS NOT NULL
    AND Supervisor_EmpNum IS NOT NULL
), roster_data AS (
    -- This is preprocessed to include Supervisor IDs
    SELECT DISTINCT
        EmployeeID AS Agent_EmpNum,
        Supervisor_EmpNum, 
        CAST(update_datetime AS DATETIME) AS update_datetime,
        MAX(CAST(update_datetime AS DATETIME)) OVER (PARTITION BY TRUE) AS last_file_update_datetime,
        MAX(CAST(update_datetime AS DATETIME)) OVER (PARTITION BY EmployeeID) AS last_appearance_datetime,
        'ROSTER' AS source
    FROM {RECIDIVIZ_REFERENCE_staff_roster@ALL}
    WHERE Supervisor_EmpNum IS NOT NULL
    AND Supervisor_EmpNum != ''
),
critical_dates AS (
    SELECT * FROM (
        SELECT
        Agent_EmpNum, 
        Supervisor_EmpNum,
        LastModifiedDateTime,
        LAG(Supervisor_EmpNum) OVER (
            PARTITION BY Agent_EmpNum 
            ORDER BY LastModifiedDateTime) AS prev_supervisor, 
        last_file_update_datetime,
        last_appearance_datetime,
        source
        FROM (
            SELECT 
                Agent_EmpNum, 
                Supervisor_EmpNum,
                LastModifiedDateTime,
                last_file_update_datetime,
                last_appearance_datetime,
                source
            FROM filtered_data
            
            UNION ALL 

             SELECT 
                Agent_EmpNum, 
                Supervisor_EmpNum,
                update_datetime,
                last_file_update_datetime,
                last_appearance_datetime,
                source
            FROM roster_data
            ) combined_data
        ) d          
    WHERE 
    --  officer just started working
    (d.prev_supervisor IS NULL AND d.Supervisor_EmpNum IS NOT NULL) 
    --  officer changed supervisors
    OR d.prev_supervisor != Supervisor_EmpNum
    -- include the latest update even if the previous two conditions are not true
    OR LastModifiedDateTime = last_file_update_datetime
), all_periods AS (
 SELECT 
    Agent_EmpNum, 
    Supervisor_EmpNum, 
    LastModifiedDateTime AS start_date,
    CASE 
        -- If a staff member stops appearing in the roster, close their employment period
        -- on the last date we receive a roster that included them
        WHEN LEAD(LastModifiedDateTime) OVER person_window IS NULL 
            AND LastModifiedDateTime < last_file_update_datetime
            AND source = 'ROSTER'
            THEN last_appearance_datetime 
        -- There is a more recent update to this person's location
        WHEN LEAD(LastModifiedDateTime) OVER person_window IS NOT NULL 
            THEN LEAD(LastModifiedDateTime) OVER person_window 
        -- All currently-employed staff will appear in the latest roster
        ELSE CAST(NULL AS DATETIME)
    END AS end_date
FROM critical_dates
WHERE Supervisor_EmpNum IS NOT NULL
WINDOW person_window AS (PARTITION BY Agent_EmpNum ORDER BY LastModifiedDateTime,Supervisor_EmpNum)
)
SELECT 
    *, 
    ROW_NUMBER() OVER (PARTITION BY Agent_EmpNum ORDER BY start_date) AS period_seq_num,
FROM all_periods
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_pa",
    ingest_view_name="supervision_staff_supervisor_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
