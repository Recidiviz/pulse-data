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
"""State-specific preprocessing for PA raw data to map agents to current supervisor"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    US_PA_RAW_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_PA_AGENT_SUPERVISOR_PREPROCESSED_VIEW_NAME = "us_pa_agent_supervisor_preprocessed"

US_PA_AGENT_SUPERVISOR_PREPROCESSED_VIEW_DESCRIPTION = """State-specific preprocessing for PA raw data to map agents to
current supervisor. Looks for agents with recent metrics and then joins on their latest known supervisor from PA raw 
data"""

US_PA_AGENT_SUPERVISOR_PREPROCESSED_QUERY_TEMPLATE = """
-- This CTE creates a list of agents with recent metrics (within last 2 quarters)
WITH agents AS (
      SELECT 
        DISTINCT officer_id,
      FROM 
        `{project_id}.{analyst_dataset}.supervision_officer_primary_office_materialized`
      WHERE state_code = 'US_PA'
        AND COALESCE(office,"EXTERNAL_UNKNOWN") != "EXTERNAL_UNKNOWN"
        AND date >= DATE_SUB(
                        DATE_TRUNC(CURRENT_DATE("US/Eastern"),
                        MONTH
                        ), 
                    INTERVAL 6 MONTH
                    )
)
SELECT 
    "US_PA" AS state_code,
    officer_id,
    supervisor_id,
    -- Removes an ID number thats concatenated with supervisor name
    TRIM(REGEXP_REPLACE(supervisor_name,r'[0-9]+','')) AS supervisor_name,
    -- Takes the supervisor name, removes the ID, splits into first/last name, and keeps first initial and last name
    CONCAT(
        LEFT(
            TRIM(
              SPLIT(
                TRIM(
                    REGEXP_REPLACE(
                        supervisor_name,r'[0-9]+',
                        ''
                    )
                ),
                ','
              )[OFFSET(1)]
            ),
            1
        ),
        '. ',
        INITCAP(
            SPLIT(
                TRIM(
                    REGEXP_REPLACE(
                        supervisor_name,r'[0-9]+',
                        ''
                    )
                ),
            ','
            )[OFFSET(0)]
        )
    ) AS supervisor_name_initial, 
FROM agents
LEFT JOIN (
  -- select latest row for each agent
  SELECT Agent_EmpNum AS officer_id,
         Supervisor_EmpNum AS supervisor_id,
         SupervisorName AS supervisor_name,
         AgentName AS agent_name
  FROM 
    `{project_id}.{raw_dataset}.dbo_RelAgentHistory_latest`
  QUALIFY 
    ROW_NUMBER() OVER(PARTITION BY Agent_EmpNum ORDER BY LastModifiedDateTime DESC) = 1  
) 
USING(officer_id)
WHERE supervisor_name NOT LIKE '%Vacant%'
    AND agent_name NOT LIKE '%Vacant%'
"""

US_PA_AGENT_SUPERVISOR_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_PA_AGENT_SUPERVISOR_PREPROCESSED_VIEW_NAME,
    view_query_template=US_PA_AGENT_SUPERVISOR_PREPROCESSED_QUERY_TEMPLATE,
    description=US_PA_AGENT_SUPERVISOR_PREPROCESSED_VIEW_DESCRIPTION,
    raw_dataset=US_PA_RAW_DATASET,
    should_materialize=False,
    analyst_dataset=ANALYST_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_PA_AGENT_SUPERVISOR_PREPROCESSED_VIEW_BUILDER.build_and_print()
