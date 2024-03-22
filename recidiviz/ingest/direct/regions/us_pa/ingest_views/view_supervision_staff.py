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

"""Query containing supervision staff information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH staff_from_roster AS (
    SELECT DISTINCT 
        EmployeeID,
        UPPER(EmployeeLastName) AS EmployeeLastName,
        UPPER(EmployeeFirstName) AS EmployeeFirstName,
        UPPER(Email) AS Email,
    FROM {RECIDIVIZ_REFERENCE_staff_roster}
    WHERE EmployeeID IS NOT NULL 
    AND EmployeeID != ''
), 
raw_staff_from_agent_history AS (
SELECT 
    Agent_EmpNum,
    NAME[SAFE_ORDINAL(1)] AS LastName,
    SPLIT(NAME[SAFE_ORDINAL(2)], ' ') AS FirstName,
    LastModifiedDateTime
    FROM (
    SELECT 
        Agent_EmpNum,
        SPLIT(AgentName, ', ') as NAME,
        LastModifiedDateTime,
    FROM {dbo_RelAgentHistory}
    WHERE AgentName NOT LIKE '%Vacant%'
    AND Agent_EmpNum IS NOT NULL
    ) sub
-- Exclude malformed names. These are all included separately with proper formatting,
-- so will still appear in the final result.
WHERE ARRAY_LENGTH(NAME) > 1
), cleaned_staff_from_agent_history AS (
SELECT 
    Agent_EmpNum,
    LastName,
    FirstName,
    EmployeeEmail
FROM (
    SELECT DISTINCT
        Agent_EmpNum,
        LastName,
        FirstName[SAFE_ORDINAL(1)] AS FirstName,
        CAST(NULL AS STRING) AS EmployeeEmail,
        ROW_NUMBER() OVER (
                PARTITION BY Agent_EmpNum 
                ORDER BY CAST(LastModifiedDateTime as DATETIME) DESC) AS priority
    FROM raw_staff_from_agent_history
    WHERE Agent_EmpNum NOT IN (SELECT DISTINCT EmployeeID FROM staff_from_roster)
) sub
-- ranking to choose the most recent name spelling for each officer
WHERE priority = 1
), raw_supervisors_from_agent_history AS (
SELECT 
    Supervisor_EmpNum,
    NAME[SAFE_ORDINAL(1)] AS LastName,
    SPLIT(NAME[SAFE_ORDINAL(2)], ' ') AS FirstName,
    LastModifiedDateTime
    FROM (
    SELECT 
        Supervisor_EmpNum,
        SPLIT(SupervisorName, ', ') as NAME,
        LastModifiedDateTime
    FROM {dbo_RelAgentHistory}
    WHERE SupervisorName NOT LIKE '%Vacant%'
    AND Supervisor_EmpNum IS NOT NULL
    ORDER BY Supervisor_EmpNum
    ) sub
-- Exclude malformed names. These are all included separately with proper formatting,
-- so will still appear in the final result.
WHERE ARRAY_LENGTH(NAME) > 1
), cleaned_supervisors_from_agent_history AS (
SELECT 
    Supervisor_EmpNum,
    LastName,
    FirstName,
    EmployeeEmail
FROM (
    SELECT DISTINCT
        Supervisor_EmpNum,
        LastName,
        FirstName[SAFE_ORDINAL(1)] AS FirstName,
        CAST(NULL AS STRING) AS EmployeeEmail,
        ROW_NUMBER() OVER (
                PARTITION BY Supervisor_EmpNum 
                ORDER BY CAST(LastModifiedDateTime as DATETIME) DESC) AS priority
    FROM raw_supervisors_from_agent_history
    WHERE Supervisor_EmpNum NOT IN (SELECT DISTINCT EmployeeID FROM staff_from_roster)
    AND Supervisor_EmpNum NOT IN (SELECT DISTINCT Agent_EmpNum FROM cleaned_staff_from_agent_history)
) sub
-- ranking to choose the most recent name spelling for each officer
WHERE priority = 1
)
, staff_from_contacts AS (
SELECT DISTINCT
    PRL_AGNT_EMPL_NO,
    PRL_AGNT_LAST_NAME,
    PRL_AGNT_FIRST_NAME,
    CAST(NULL AS STRING) AS EmployeeEmail,
FROM {dbo_PRS_FACT_PAROLEE_CNTC_SUMRY}
WHERE PRL_AGNT_EMPL_NO IS NOT NULL
AND PRL_AGNT_EMPL_NO NOT IN (SELECT DISTINCT EmployeeID FROM staff_from_roster)
AND PRL_AGNT_EMPL_NO NOT IN (SELECT DISTINCT Agent_EmpNum FROM cleaned_staff_from_agent_history)
AND PRL_AGNT_EMPL_NO NOT IN (SELECT DISTINCT Supervisor_EmpNum FROM cleaned_supervisors_from_agent_history)
)

SELECT DISTINCT *
FROM staff_from_roster

UNION ALL 

SELECT DISTINCT *
FROM cleaned_staff_from_agent_history

UNION ALL 

SELECT DISTINCT * 
FROM cleaned_supervisors_from_agent_history

UNION ALL

SELECT DISTINCT * 
FROM staff_from_contacts
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_pa",
    ingest_view_name="supervision_staff",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
