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

"""Query containing supervision staff role period information.

Starts with handmade roster and uses roles from there to capture the most accurate info 
possible for current employees. Fills in as many of the remaining officers' role info
as possible by using the column they appear in in the agent history data (Agent_EmpNum vs. Supervisor_EmpNum)
and PRL_AGNT_JOB_CLASSIFCTN in the supervision contacts data.

There are overlapping periods for officers who appear as supervisors and agents concurrently. 
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH critical_dates_from_roster AS (
    SELECT DISTINCT 
        EmployeeID AS employeeid,
        UPPER(Role) AS role,
        CAST(update_datetime AS DATETIME) as edge_date,
        MAX(CAST(roster.update_datetime AS DATETIME)) OVER (PARTITION BY TRUE) AS last_roster_update_datetime,
        MAX(CAST(roster.update_datetime AS DATETIME)) OVER (PARTITION BY EmployeeID,role) AS last_appearance_date,
    FROM {RECIDIVIZ_REFERENCE_staff_roster@ALL} roster
),
staff_from_roster AS (
    SELECT * FROM (
        SELECT DISTINCT 
            EmployeeID AS employeeid,
            UPPER(Role) AS role,
            -- first time EmployeeID appeared in dbo_RelAgentHistory 
            MIN(CAST(agent_history.LastModifiedDateTime AS DATETIME)) OVER person_window AS edge_date,  
            MAX(CAST(roster.update_datetime AS DATETIME)) OVER (PARTITION BY TRUE) AS last_roster_update_datetime,
            MAX(CAST(roster.update_datetime AS DATETIME)) OVER (PARTITION BY EmployeeID) AS last_appearance_date,
        FROM {RECIDIVIZ_REFERENCE_staff_roster@ALL} roster
        LEFT JOIN {dbo_RelAgentHistory@ALL} agent_history
        ON(Agent_EmpNum = EmployeeID)
        WHERE Role != 'Parole Spvr'
        WINDOW person_window AS (PARTITION BY EmployeeID, Role ORDER BY roster.update_datetime)
    ) agent_sub
    -- person has a real start date
    WHERE edge_date IS NOT NULL

    UNION ALL 

    SELECT * FROM (
        SELECT DISTINCT 
            EmployeeID AS employeeid,
            UPPER(Role) AS role,
            -- first time EmployeeID appeared in dbo_RelAgentHistory as a supervisor 
            MIN(CAST(agent_history.LastModifiedDateTime AS DATETIME)) OVER person_window AS edge_date,
            MAX(CAST(roster.update_datetime AS DATETIME)) OVER (PARTITION BY TRUE) AS last_roster_update_datetime,
            MAX(CAST(roster.update_datetime AS DATETIME)) OVER (PARTITION BY EmployeeID) AS last_appearance_date,
        FROM {RECIDIVIZ_REFERENCE_staff_roster@ALL} roster
        LEFT JOIN {dbo_RelAgentHistory@ALL} agent_history
        ON(agent_history.Supervisor_EmpNum = EmployeeID)
        WHERE Role = 'Parole Spvr'
        WINDOW person_window AS (PARTITION BY EmployeeID, Role ORDER BY roster.update_datetime)
    ) spvr_sub
    -- person has a real start date
    WHERE edge_date IS NOT NULL

    UNION ALL
    -- to use as approximate edge dates when information can only be found in roster
    SELECT * FROM critical_dates_from_roster
),
prelim_roster_periods AS (
    SELECT DISTINCT 
        employeeid,
        role,
        edge_date AS start_date,
        CASE 
        -- If there is a more recent entry for a staff member
            WHEN LEAD(edge_date) OVER person_window IS NOT NULL THEN LEAD(edge_date) OVER person_window
            -- If a staff member stops appearing in the roster, close their employment period
            -- on the first date we receive a roster that does not include them
            WHEN  LEAD(edge_date) OVER person_window IS NULL
                AND last_appearance_date < last_roster_update_datetime THEN last_appearance_date
            -- All currently-employed staff will appear in the latest roster
            ELSE CAST(NULL AS DATETIME)    
        END AS end_date
    FROM staff_from_roster
    WINDOW person_window AS (PARTITION BY EmployeeID, role ORDER BY edge_date)
),
staff_from_agent_history AS (
    SELECT DISTINCT
        Agent_EmpNum AS employeeid,
        'SUPERVISION_OFFICER' AS role, 
        -- first time EmployeeID appeared in dbo_RelAgentHistory
        MIN(CAST(LastModifiedDateTime AS DATETIME)) AS start_date,
        MAX(CAST(LastModifiedDateTime AS DATETIME)) AS end_date
    FROM {dbo_RelAgentHistory@ALL} agent_history
    WHERE AgentName NOT LIKE '%Vacant%'
    AND Agent_EmpNum IS NOT NULL
    AND Agent_EmpNum NOT IN (SELECT DISTINCT employeeid FROM staff_from_roster)
    GROUP BY Agent_EmpNum
),
supervisors_from_agent_history AS (
    -- can overlap with OFFICER role period for the same person
    SELECT DISTINCT
        Supervisor_EmpNum AS employeeid,
        'SUPERVISION_OFFICER_SUPERVISOR' as role,
        -- first time EmployeeID appeared in dbo_RelAgentHistory
        MIN(CAST(LastModifiedDateTime AS DATETIME)) AS start_date,
        MAX(CAST(LastModifiedDateTime AS DATETIME)) AS end_date
    FROM {dbo_RelAgentHistory@ALL} agent_history
    WHERE SupervisorName NOT LIKE '%Vacant%'
    AND Supervisor_EmpNum IS NOT NULL
    AND Supervisor_EmpNum NOT IN (SELECT DISTINCT employeeid FROM staff_from_roster)
    GROUP BY Supervisor_EmpNum
),
staff_from_contacts AS (
    -- Fill in role info for people who do not appear in dbo_RelAgentHistory
    SELECT DISTINCT
        PRL_AGNT_EMPL_NO AS employeeid,
        UPPER(PRL_AGNT_JOB_CLASSIFCTN) as role,
        -- first time EmployeeID appeared in dbo_PRS_FACT_PAROLEE_CNTC_SUMRY
        MIN(CAST(START_DATE AS DATETIME)) AS start_date,
        MAX(CAST(END_DATE AS DATETIME)) AS end_date
    FROM {dbo_PRS_FACT_PAROLEE_CNTC_SUMRY} agent_history
    WHERE PRL_AGNT_EMPL_NO IS NOT NULL
    AND PRL_AGNT_EMPL_NO NOT IN (SELECT DISTINCT employeeid FROM staff_from_roster)
    AND PRL_AGNT_EMPL_NO NOT IN (SELECT DISTINCT employeeid FROM staff_from_agent_history)
    AND PRL_AGNT_EMPL_NO NOT IN (SELECT DISTINCT employeeid FROM supervisors_from_agent_history)
    GROUP BY PRL_AGNT_EMPL_NO, PRL_AGNT_JOB_CLASSIFCTN
),
all_periods AS (
SELECT DISTINCT * FROM prelim_roster_periods
UNION ALL
SELECT DISTINCT * FROM staff_from_agent_history
UNION ALL 
SELECT DISTINCT * FROM supervisors_from_agent_history
UNION ALL
SELECT DISTINCT * FROM staff_from_contacts
)

SELECT 
    *, 
    ROW_NUMBER() OVER (PARTITION BY EmployeeID ORDER BY start_date, role, end_date) AS period_seq_num
FROM all_periods
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_pa",
    ingest_view_name="supervision_staff_role_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
