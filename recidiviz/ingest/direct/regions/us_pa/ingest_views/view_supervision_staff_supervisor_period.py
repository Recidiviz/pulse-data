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
    SELECT * 
    FROM {dbo_RelAgentHistory@ALL}
    WHERE SupervisorName NOT LIKE '%Vacant%'
    AND AgentName NOT LIKE '%Vacant%'
    AND Agent_EmpNum IS NOT NULL
    AND Supervisor_EmpNum IS NOT NULL
),
critical_dates AS (
    SELECT * FROM (
        SELECT
        Agent_EmpNum, 
        Supervisor_EmpNum,
        LastModifiedDateTime,
        LAG(Supervisor_EmpNum) OVER (PARTITION BY Agent_EmpNum ORDER BY LastModifiedDateTime) AS prev_supervisor, 
        FROM filtered_data) d          
    WHERE 
     -- officer just started working
    (d.prev_supervisor IS NULL AND d.Supervisor_EmpNum IS NOT NULL) 
     -- officer changed supervisors
    OR d.prev_supervisor != Supervisor_EmpNum
     -- INACTIVE OFFICERS WILL BE INCLUDED if they are included in the data; cannot tell when/if an officer's employment was terminated otherwise
 ), 
 all_periods AS (
 SELECT 
     Agent_EmpNum, Supervisor_EmpNum, LastModifiedDateTime AS start_date,
     LEAD(LastModifiedDateTime) OVER (PARTITION BY Agent_EmpNum ORDER BY LastModifiedDateTime) AS end_date
     FROM critical_dates
     WHERE Supervisor_EmpNum IS NOT NULL
 )
 SELECT 
Agent_EmpNum, Supervisor_EmpNum,
ROW_NUMBER() OVER (PARTITION BY Agent_EmpNum ORDER BY start_date) AS period_seq_num,
start_date,
end_date
FROM all_periods
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_pa",
    ingest_view_name="supervision_staff_supervisor_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="Agent_EmpNum, period_seq_num",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
