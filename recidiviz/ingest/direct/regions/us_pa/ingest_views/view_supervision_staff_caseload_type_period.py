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

"""Query containing supervision staff role period information.

If someone is employed, then leaves the department, then is re-employed by the department
later, it will appear that they held their last position for the entirety of the time
they were not employed by the department. 
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH roster_data AS (
    SELECT DISTINCT
        EmployeeID,
        AgentType,
        CAST(update_datetime AS DATETIME) AS update_datetime,
        MAX(CAST(update_datetime AS DATETIME)) OVER (PARTITION BY TRUE) AS last_file_update_datetime,
        MAX(CAST(update_datetime AS DATETIME)) OVER (PARTITION BY EmployeeID) AS last_appearance_date,
    FROM {RECIDIVIZ_REFERENCE_staff_roster@ALL}
),
critical_dates AS (
    SELECT * FROM (
        SELECT
            EmployeeID, 
            AgentType,
            update_datetime,
            last_file_update_datetime,
            last_appearance_date,
            -- Add AgentType to deterministically sort assignments that happened on the same day
            LAG(AgentType) OVER (
                PARTITION BY EmployeeID 
                ORDER BY update_datetime, AgentType) 
                AS prev_caseload_type
        FROM roster_data
        ) cd
    WHERE
    -- officer just started working 
    (cd.prev_caseload_type IS NULL AND cd.AgentType IS NOT NULL) 
    -- officer changed caseload types 
    OR cd.prev_caseload_type != AgentType
    -- include all rows from the most recent roster, even if the above conditions do not apply
    OR update_datetime = last_file_update_datetime
), 
all_periods AS (
SELECT 
    EmployeeID AS employeeid,
    UPPER(TRIM(AgentType)) AS agenttype,
    update_datetime AS start_date,
    CASE 
        -- If a staff member stops appearing in the roster, close their employment period
        -- on the first date we receive a roster that does not include them
        WHEN lead(update_datetime) OVER person_window IS NULL 
        AND update_datetime < last_file_update_datetime 
        THEN last_appearance_date   
        -- All currently-employed staff will appear in the latest roster
        WHEN update_datetime = last_file_update_datetime THEN CAST(NULL AS DATETIME)     
        -- Else there is a more recent entry for a staff member
        ELSE LEAD(update_datetime) OVER person_window
    END AS end_date
FROM critical_dates
window person_window AS (PARTITION BY EmployeeID ORDER BY update_datetime) 
)
SELECT 
    *,    
    ROW_NUMBER() OVER (PARTITION BY EmployeeID ORDER BY start_date) AS period_seq_num 
FROM all_periods
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_pa",
    ingest_view_name="supervision_staff_caseload_type_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
