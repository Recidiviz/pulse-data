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
supervisor_start_dates AS ( 
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
SELECT DISTINCT
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
),
periods_with_subtypes AS (
SELECT DISTINCT
    Employ_Num, 
    start_date,
    end_date,
    CASE 
        WHEN start_date = all_periods.sup_start THEN 'SUPERVISION_OFFICER_SUPERVISOR'
        WHEN start_date IS NULL THEN 'INTERNAL_UNKNOWN'
        ELSE 'SUPERVISION_OFFICER'
    END AS role_subtype,
FROM all_periods
WHERE start_date IS NOT NULL -- excludes people who appear in roster but never in case history
),
cntc_base AS (
-- include officers who only appear in supervision contacts data, not any roster
SELECT Employ_Num, start_date, end_date, role_subtype 
FROM (
    SELECT DISTINCT
        PRL_AGNT_EMPL_NO as Employ_Num,
        START_DATE AS start_date,
        NULL AS end_date,
        -- we cannot know whether these agents are still actively employed, so we assume they are.
        UPPER(PRL_AGNT_JOB_CLASSIFCTN) AS role_subtype,
        ROW_NUMBER() OVER (PARTITION BY PRL_AGNT_EMPL_NO, PRL_AGNT_JOB_CLASSIFCTN ORDER BY START_DATE) as rn
    FROM {dbo_PRS_FACT_PAROLEE_CNTC_SUMRY} cntc
    WHERE PRL_AGNT_EMPL_NO NOT IN (
        SELECT DISTINCT Employ_Num
        FROM periods_with_subtypes
        )
    AND PRL_AGNT_JOB_CLASSIFCTN IN ('Prl Agt 1', 'Prl Agt 2','Prl Supv')
    ) cntc_with_row_num
WHERE rn = 1
),
complete_base AS (
SELECT DISTINCT
    Employ_Num,
    start_date,
    end_date,
    role_subtype,
    1 as period_seq_num
FROM periods_with_subtypes 

UNION ALL 

SELECT DISTINCT
    *,
    ROW_NUMBER() OVER (
        PARTITION BY cntc_base.Employ_Num 
        ORDER BY cntc_base.start_date
        ) AS period_seq_num
    FROM cntc_base
)

SELECT DISTINCT
    COALESCE(complete_base.Employ_Num, sup.ext_id) AS Employ_Num, 
    COALESCE(complete_base.start_date, '1900-01-01') AS start_date,
    complete_base.end_date,
    -- if a person supervises someone, even if they also appear in the leadership roster,
    -- keep their role subtype as SUPERVISION_OFFICER_SUPERVISOR for product view compatibility.
    COALESCE(complete_base.role_subtype,UPPER(sup.role)) AS role_subtype,
    COALESCE(complete_base.period_seq_num, 1) as period_seq_num
FROM complete_base 
FULL OUTER JOIN {RECIDIVIZ_REFERENCE_field_supervisor_list} sup
    ON(ext_id = Employ_Num)
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
