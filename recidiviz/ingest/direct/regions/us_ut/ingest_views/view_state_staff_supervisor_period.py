# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Query that generates info for all present and past members of the Utah DOC staff, 
their supervisor, and the period during which they were assigned that supervisor.

There are no date fields on the tables that provide this supervisor <> staff relationship, 
so we use the dates on which we received the file as an indicator of when changes were made. 
This means that cannot identify supervisors from before we started importing the hrchy 
raw data file on 2025-04-15. 
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH
-- Get staff <> supervisor assignments from the earliest transfer of the hrchy raw data 
-- file. We will backdate this supervisor assignment for each staff member to the date 
-- they started at the department in a later CTE.
first_hrchy_info AS (
  SELECT
    staff_usr_id,
    supervisor_id,
    first_hrchy_update_dt
  FROM (
    SELECT
      UPPER(staff_usr_id) AS staff_usr_id,
      UPPER(supr_lvl_usr) AS supervisor_id, -- includes NULL, apply UPPER here
      update_datetime AS first_hrchy_update_dt,
      ROW_NUMBER() OVER (PARTITION BY staff_usr_id ORDER BY update_datetime) as rn
    FROM {hrchy@ALL}
  )
  WHERE rn = 1
),

-- Find the earliest date that a record was added or updated in applc_usr for each staff member
earliest_applc_usr_date AS (
    SELECT
        UPPER(usr_id) as staff_usr_id,
        MIN(CAST(updt_dt AS DATETIME)) as min_applc_usr_dt
    FROM {applc_usr@ALL}
    GROUP BY 1
),
-- Detect when a user becomes inactive in the base applc_usr table. We interpret this to 
-- mean that the staff member is no longer employed by UDC. 
inactivation_dates AS (
  SELECT
    staff_usr_id,
    CAST(NULL AS STRING) AS supervisor_id,
    inactivation_datetime AS critical_date
  FROM (
    SELECT
      UPPER(usr_id) AS staff_usr_id,
      CAST(updt_dt AS DATETIME) AS inactivation_datetime,
      vld_flg,
      -- Get the previous flag status for this user, ordered by update time
      LAG(vld_flg) OVER (PARTITION BY UPPER(usr_id) ORDER BY CAST(updt_dt AS DATETIME), update_datetime) AS prev_vld_flg
    FROM {applc_usr@ALL}
  )
  -- Trigger only on the specific Y -> N transition
  WHERE vld_flg = 'N' AND prev_vld_flg = 'Y'
),

-- Generate the initial inferred period IF the user started before the first hrchy record date.
-- This prevents us from losing information if a person appears in the hrchy table with 
-- a supervisor before they appear in the applc_usr table.
initial_period AS (
    SELECT
        e.staff_usr_id,
        f.supervisor_id, -- Supervisor status from the first hrchy record (already uppercased)
        e.min_applc_usr_dt AS critical_date -- Start date is the earliest updt_dt in applc_usr for each user
    FROM earliest_applc_usr_date e
    JOIN first_hrchy_info f ON e.staff_usr_id = f.staff_usr_id
    -- Generate this period only if the inferred start is strictly before the first hrchy update timestamp.
    WHERE e.min_applc_usr_dt < f.first_hrchy_update_dt
),

-- Get all state changes directly from hrchy. These define actual period boundaries on
-- which a staff member's supervisor changed.
hrchy_state_changes AS (
  SELECT
    UPPER(h.staff_usr_id) AS staff_usr_id,
    UPPER(supr_lvl_usr) AS supervisor_id, -- Includes NULLs
    update_datetime AS critical_date -- The hrchy update time marks the start of the state described in that row, since there is no other option
  FROM {hrchy@ALL} h
),

-- Combine the potential initial period with all hrchy changes and the potential inactivation date
critical_dates AS (
  SELECT staff_usr_id, supervisor_id, critical_date FROM initial_period
  UNION ALL
  SELECT staff_usr_id, supervisor_id, critical_date FROM hrchy_state_changes
  UNION ALL
  SELECT staff_usr_id, supervisor_id, critical_date FROM inactivation_dates
), 
-- Combine the dates into periods
periods AS (
SELECT * EXCEPT (PREV_SUPERVISOR_ID), 
LEAD(start_date) OVER (PARTITION BY staff_usr_id ORDER BY start_date) AS end_date,
ROW_NUMBER() OVER (PARTITION BY staff_usr_id ORDER BY start_date) AS sequence_num 
FROM (
SELECT 
    staff_usr_id,
    supervisor_id,
    LAG(supervisor_id) OVER (PARTITION BY staff_usr_id ORDER BY critical_date) as prev_supervisor_id,
    critical_date as start_date,
FROM critical_dates
) 
WHERE supervisor_id IS DISTINCT FROM prev_supervisor_id
)
SELECT s.* FROM periods s
LEFT JOIN inactivation_dates i USING(staff_usr_id)
-- Only include rows that represent updates before the person was marked as inactive.
WHERE (s.start_date < i.critical_date OR i.critical_date IS NULL)
AND s.supervisor_id IS NOT NULL
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ut",
    ingest_view_name="state_staff_supervisor_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
