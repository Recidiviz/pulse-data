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

If a person was most recently included as a supervisor, have start_date as the first 
date they appeared as a supervisor.

If a person was most recently included as an officer, have start_date as the first date 
they appeared as an officer.

If a person appears in the roster but never appears in the case history data, they are
currently excluded from the results of this view.
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """

WITH officer_start_dates AS (
-- pull earliest dates from relagenthistory, assume all these people are still employed
    SELECT 
        agent_roster.Employ_Num,
        MIN(agent_history.LastModifiedDateTime) as start_date,
    FROM {RECIDIVIZ_REFERENCE_agent_districts} agent_roster
    LEFT JOIN {dbo_RelAgentHistory@ALL} agent_history
    ON (agent_roster.Employ_Num = agent_history.Agent_EmpNum)
    GROUP BY 1
),
supervisor_start_dates AS( 
    -- pull earliest dates from relagenthistory, assume all these people are still employed
    SELECT 
        agent_roster.Employ_Num,
        MIN(agent_history.LastModifiedDateTime) as start_date,
    FROM {RECIDIVIZ_REFERENCE_agent_districts} agent_roster
    LEFT JOIN  {dbo_RelAgentHistory@ALL} agent_history
    ON (agent_roster.Employ_Num = agent_history.Supervisor_EmpNum)
    GROUP BY 1
),

all_periods AS (
SELECT 
    officer_start_dates.Employ_Num,
    supervisor_start_dates.start_date as sup_start,
     -- if agent has been a supervisor AND an officer, use start date of most recent role
    CASE WHEN
        COALESCE(officer_start_dates.start_date, '1900-01-01') > COALESCE(supervisor_start_dates.start_date, '1900-01-01')
        THEN officer_start_dates.start_date
        ELSE supervisor_start_dates.start_date
    END AS start_date,
    NULL as end_date  -- assume people are still employed since they showed up in recent roster
FROM officer_start_dates
JOIN supervisor_start_dates
USING(Employ_Num)
)

SELECT 
    Employ_Num, 
    start_date,
    end_date,
    CASE 
        WHEN start_date = all_periods.sup_start THEN 'SUPERVISION_OFFICER_SUPERVISOR'
        WHEN start_date IS NULL THEN 'INTERNAL_UNKNOWN'
        ELSE 'SUPERVISION_OFFICER'
    END AS role_subtype,
    1 AS period_seq_num
FROM all_periods
WHERE start_date IS NOT NULL -- excludes people who appear in roster but never in case history


UNION ALL

SELECT
    DISTINCT CAST(ext_id AS STRING),
    '1900-01-01' AS start_date,
    NULL AS end_date,
    role AS role_subtype,
    1 as period_seq_num
FROM {RECIDIVIZ_REFERENCE_field_supervisor_list}
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_pa",
    ingest_view_name="supervision_staff_role_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="Employ_Num",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
