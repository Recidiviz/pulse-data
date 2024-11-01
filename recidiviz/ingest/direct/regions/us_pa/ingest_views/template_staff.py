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
"""

Helper templates for the US_PA state staff queries.

Context: 

In PA, there are three different sources for staff information:
- RECIDIVIZ_REFERENCE_staff_roster: which is the manually compiled roster that PA research 
  provides us from time to time.  This includes all staff information for current staff including 
staff names, employee numbers, position numbers, location codes, role descriptions, and supervisor information.
- dbo_RelAgentHistory: which is the supervision officer assignment table that includes staff names, 
  employee numbers, position numbers, and location codes for all supervising officers assigned to a
  client and those supervising officers' supervisors.
- dbo_PRS_FACT_PAROLEE_CNTC_SUMRY: which is the supervision contacts table that includes 
  staff employee numbers, role description, location codes, and names.

Across these two sources, there are two different types of id numbers:
- Employee numbers, which should be unique to an individual staff person
- Position numbers, which are unique to a specific job opening/position, and therefore is 
  attached with multiple staff people over time.

Given that employee number is sometimes missing in the data, and may not exist in the future 
since it's not a part of PA's new staff database, we set up state staff ingest to 
ingest state staff external ids based on both the original employee numbers (EmpNums) and 
a new id we'll construct and call position number ids (PosNoIds) that takes the structure 
of `<position number>-<staff last name><staff first initial>`.

"""

STAFF_QUERY_TEMPLATE = """
    -- This CTE parses all the staff information from the contacts table, keeping one record per
    -- EmpNum and start date
    staff_from_contacts AS (
        SELECT
            LTRIM(PRL_AGNT_EMPL_NO, '0') AS EmpNum, 
            UPPER(PRL_AGNT_FIRST_NAME) as first_name, 
            UPPER(PRL_AGNT_LAST_NAME) as last_name, 
            PRL_AGNT_JOB_CLASSIFCTN as role_description,
            org_cd,
            DATE(START_DATE) AS start_date,
        FROM {dbo_PRS_FACT_PAROLEE_CNTC_SUMRY} c
        LEFT JOIN {RECIDIVIZ_REFERENCE_locations_from_supervision_contacts} lr
            ON UPPER(c.PRL_AGNT_ORG_NAME) = UPPER(lr.ORG_NAME)
        QUALIFY ROW_NUMBER() OVER(PARTITION BY EmpNum, DATE(START_DATE) ORDER BY FACT_PAROLEE_CNTC_SUMRY_ID, PAROLE_NUMBER) = 1
    ),
    -- This CTE parses all the staff information from the manual roster, keeping one record per
    -- PosNo and start date
    roster_parsed AS (
        SELECT *,
            CASE WHEN supervisor_last_name in ('VACANT', 'POSITION')
                    THEN NULL
                 ELSE CONCAT(Supervisor_PosNo, "-", supervisor_last_name, SUBSTR(supervisor_first_name, 1, 1))
                END AS Supervisor_PosNoId,
            MIN(start_date) OVER(PARTITION BY PosNoId) AS first_apperance_date
        FROM (
            SELECT DISTINCT
                LTRIM(EmployeeID, '0') AS EmpNum,
                UPPER(EmployeeLastName) as last_name,
                UPPER(EmployeeFirstName) as first_name, 
                UPPER(CONCAT(LTRIM(AMU_Pos_No, '0'), "-", EmployeeLastName, SUBSTR(EmployeeFirstName, 1, 1))) AS PosNoId,
                UPPER(Email) AS Email,
                DATE(update_datetime) AS start_date,
                Role as role_description, 
                DO_Orgcode AS org_cd,
                UPPER(SupervisorLastName) AS supervisor_last_name,
                UPPER(SupervisorFirstName) AS supervisor_first_name,
                LTRIM(Supervisor_EmpNum, '0') AS Supervisor_EmpNum,
                LTRIM(SupervisorInfo, '0') AS Supervisor_PosNo
            FROM {RECIDIVIZ_REFERENCE_staff_roster@ALL}
        )
        WHERE PosNoId IS NOT NULL OR EmpNUM IS NOT NULL
        QUALIFY ROW_NUMBER() OVER(PARTITION BY PosNoId, start_date ORDER BY Email) = 1
    ),
    -- This CTE parses all the staff information from the RelAgentHistory table, keeping all records
    -- since we'll dedup in later CTEs.  Agent information is found in a single text field called
    -- AgentName that takes the form '<last name> <suffix>, <first name> <middle name or initial> <position number>'.
    -- Supervisor information is found in a single text field called Supervisor Name that takes the form
    -- '<last name> <suffix>, <first name> <middle name or initial> <org_code> <position number>'.
    RelAgent_parsed AS (
        SELECT * EXCEPT(Agent_first_middle_name, Supervisor_first_middle_name),
            Agent_first_middle_name[OFFSET(0)] AS Agent_first_name,
            CASE WHEN ARRAY_LENGTH(Agent_first_middle_name) > 1
                 THEN Agent_first_middle_name[OFFSET(1)]
                 ELSE NULL
                 END AS Agent_middle_name,
            Supervisor_first_middle_name[OFFSET(0)] AS Supervisor_first_name,
            CASE WHEN ARRAY_LENGTH(Supervisor_first_middle_name) > 1
                 THEN Supervisor_first_middle_name[OFFSET(1)]
                 ELSE NULL
                 END AS Supervisor_middle_name
        FROM (
            SELECT * EXCEPT(Agent_last_name, Supervisor_last_name),
                TRIM(REGEXP_REPLACE(Agent_last_name, r' JR| III| IV| JR.| II| SR', "")) AS Agent_last_name,
                TRIM(REGEXP_EXTRACT(Agent_last_name, r' JR| III| IV| JR.| II| SR')) AS Agent_suffix,
                SPLIT(TRIM(REGEXP_REPLACE(REPLACE(AgentName, CONCAT(Agent_last_name, ", "), ""), r'[\\d,]+', "")), ' ') as Agent_first_middle_name,
                TRIM(REGEXP_REPLACE(Supervisor_last_name, r' JR| III| IV| JR.| II| SR', "")) AS Supervisor_last_name,
                TRIM(REGEXP_EXTRACT(Supervisor_last_name, r' JR| III| IV| JR.| II| SR')) AS Supervisor_suffix,
                SPLIT(TRIM(REGEXP_REPLACE(REPLACE(SupervisorName, CONCAT(Supervisor_last_name, ", "), ""), r'[\\d,]+', "")), ' ') as Supervisor_first_middle_name,
            FROM ( 
                SELECT DISTINCT
                    UPPER(AgentName) AS AgentName,
                    LTRIM(Agent_EmpNum, '0') AS Agent_EmpNum,
                    UPPER(TRIM(SPLIT(AgentName, ",")[OFFSET(0)])) as Agent_last_name,
                    NULLIF(LTRIM(TRIM(REGEXP_REPLACE(AgentName, r'[^\\d]+', '')), '0'), "") AS Agent_PosNo,
                    UPPER(SupervisorName) AS SupervisorName,
                    LTRIM(Supervisor_EmpNum, '0') AS Supervisor_EmpNum,
                    UPPER(TRIM(SPLIT(SupervisorName, ",")[OFFSET(0)])) as Supervisor_last_name,
                    TRIM(SPLIT(REGEXP_REPLACE(SupervisorName, r'[^\\d]+', ','), ",")[OFFSET(1)]) AS org_cd,
                    NULLIF(LTRIM(TRIM(SPLIT(REGEXP_REPLACE(SupervisorName, r'[^\\d]+', ','), ",")[OFFSET(2)]), '0'), "") AS Supervisor_PosNo,
                    DATE(LastModifiedDateTime) AS start_date,
                    LastModifiedDateTime 
                FROM {dbo_RelAgentHistory}
            )
        )
    ),
    -- This CTE takes the parsed staff information from RelAgent_parsed and parses it down
    -- further for officer specific information.  If the supervisor associated with the record is a
    -- vacant position place holder, we set supervisor information to be null.  Finally, we 
    -- dedup to one row per employee number, position number, and date.
    agents_from_RelAgent AS (
        SELECT DISTINCT
            Agent_EmpNum AS EmpNum,
            Agent_last_name AS last_name,
            Agent_middle_name AS middle_name,
            Agent_first_name AS first_name,
            Agent_suffix AS suffix,
            CONCAT(Agent_PosNo, "-", Agent_last_name, SUBSTR(Agent_first_name, 1, 1)) AS PosNoId,
            org_cd,
            "[RelAgent] Officer" AS role_description,
            CASE WHEN Supervisor_last_name IN ('VACANT', 'POSITION')
                   THEN NULL
                 ELSE CONCAT(Supervisor_PosNo, "-", Supervisor_last_name, SUBSTR(Supervisor_first_name, 1, 1))
                 END AS supervisor_PosNoId,
            CASE WHEN Supervisor_last_name IN ('VACANT', 'POSITION')
                   THEN NULL
                 ELSE supervisor_EmpNum
                 END AS supervisor_EmpNum,
            start_date
        FROM RelAgent_parsed ra
        WHERE 
            (Agent_last_name IS NULL OR Agent_last_name NOT IN ('VACANT', 'POSITION')) AND
            -- We have either EmpNum or PosNo so that we can connect this agent via an external id
            (Agent_EmpNum IS NOT NULL OR (Agent_PosNo IS NOT NULL and Agent_last_name IS NOT NULL AND Agent_first_name IS NOT NULL))
        -- verified and made sure this is deterministic
        -- if there are two rows per PosNoId, we'll dedup with the ids_map later
        QUALIFY ROW_NUMBER() OVER(PARTITION BY PosNoId, EmpNum, start_date ORDER BY LastModifiedDateTime Desc, supervisor_PosNoId NULLS LAST) = 1
    ),
    -- This CTE takes the parsed staff information from RelAgent_parsed and parses it down
    -- further for officer supervisor information.  We dedup to one row per employee number, position number, and date.
    supervisors_from_RelAgent AS (
        SELECT DISTINCT 
            Supervisor_EmpNum AS EmpNum,
            Supervisor_last_name AS last_name,
            Supervisor_middle_name AS middle_name,
            Supervisor_first_name AS first_name,
            Supervisor_suffix AS suffix,
            CONCAT(Supervisor_PosNo, "-", Supervisor_last_name, SUBSTR(Supervisor_first_name, 1, 1)) AS PosNoId,
            org_cd,
            "[RelAgent] Supervisor" AS role_description,
            start_date
        FROM RelAgent_parsed ra
        WHERE 
            (Supervisor_last_name IS NULL OR Supervisor_last_name NOT IN ('VACANT', 'POSITION')) AND
            -- We have either EmpNum or PosNo so that we can connect this agent via an external id
            (Supervisor_EmpNum IS NOT NULL OR (Supervisor_PosNo IS NOT NULL AND Supervisor_last_name IS NOT NULL))
        -- Confirmed that this is deterministic
        QUALIFY ROW_NUMBER() OVER(PARTITION BY PosNoId, EmpNum, start_date ORDER BY LastModifiedDateTime Desc) = 1
    ),

    -- We create a reference crosswalk that maps between PosNoIds and EmpNums.  If a PosnoId
    -- ever appears associated with multiple EmpNums, this view take the most recent EmpNum
    -- found in the most prioritized source (roster is prioritized over the other tables)
    ids_map AS (
      select DISTINCT
        PosNoId,
        FIRST_VALUE(EmpNum IGNORE NULLS) 
          OVER(PARTITION BY PosNoId
               ORDER BY sort_priority, start_date DESC, EmpNum
               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS EmpNum
      from (
        select distinct PosNoId, EmpNum, start_date, 1 as sort_priority, last_name
        from roster_parsed

        union all

        select PosNoId, EmpNum, start_date, 2 as sort_priority, last_name
        from agents_from_RelAgent

        union all 

        select PosNoId, EmpNum, start_date, 2 as sort_priority, last_name
        from supervisors_from_RelAgent
      )
      where PosNoId IS NOT NULL AND EmpNum IS NOT NULL
    )
"""


def staff_view_template() -> str:
    return STAFF_QUERY_TEMPLATE
