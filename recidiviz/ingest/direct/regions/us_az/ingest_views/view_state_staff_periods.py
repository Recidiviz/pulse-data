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
"""Query containing all periods location about state staff members on the facilities
and community corrections sides of ADCRR: locations, supervisors, role types, and caseload
types."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- This CTE returns one row for each DPP staff member containing their PERSON_ID, location,
-- and supervisor's ID.
base AS (
SELECT DISTINCT
  AGENT_ID, 
  'SUPERVISION' AS ROLE_TYPE,
  OFFICE_LOCATION_ID AS LOCATION,
  SUPERVISOR_ID,
  dpp_agent.ACTIVE_FLAG,
  NULLIF(dpp_agent.UPDT_DTM, 'NULL') AS UPDT_DTM
FROM {DPP_CASE_AGENT@ALL} dpp_agent

UNION ALL 

SELECT DISTINCT
  AGENT_PERSON_ID, 
  'FACILITY' AS ROLE_TYPE,
  PRISON_CODE AS LOCATION,
  SUPERVISOR_PERSON_ID,
  cm_agent.ACTIVE_FLAG,
  COALESCE(
    NULLIF(cm_agent.UPDT_DTM, 'NULL'),
    NULLIF(cm_agent.CREATE_DTM, 'NULL')) AS UPDT_DTM
FROM {AZ_CM_AGENT@ALL} cm_agent
JOIN {AZ_DOC_PRISON} prison USING(PRISON_ID)
), 
-- This CTE returns the same information as above, but only for rows where some key
-- characteristic of a staff member's employment (location, supervisor, role, or employment
-- status) has changed.
critical_dates AS (
SELECT * FROM (
  SELECT DISTINCT
    AGENT_ID,
    ROLE_TYPE,
    LAG(ROLE_TYPE) OVER person_window AS prev_role, 
    LOCATION,
    LAG(LOCATION) OVER person_window AS prev_location,
    SUPERVISOR_ID,
    LAG(SUPERVISOR_ID) OVER person_window AS prev_supervisor,
    ACTIVE_FLAG,
    LAG(ACTIVE_FLAG) OVER person_window AS prev_active_flag,
    NULLIF(UPDT_DTM, 'NULL') AS UPDT_DTM
  FROM base
  WINDOW person_window AS (PARTITION BY AGENT_ID ORDER BY UPDT_DTM)
)
WHERE 
  -- location changed
  (prev_location != location OR prev_location IS NULL) OR
  -- supervisor changed
  (prev_supervisor != supervisor_id OR prev_supervisor IS NULL) OR
  -- role changed
  (prev_role != ROLE_TYPE OR prev_role IS NULL) OR
  -- person is active or just became inactive
  (active_flag = 'Y' OR (active_flag = 'N' and prev_active_flag = 'Y'))
)

SELECT
  AGENT_ID,
  ROLE_TYPE,
  LOCATION,
  SUPERVISOR_ID,
  UPDT_DTM AS START_DATE,
  LEAD(updt_dtm) OVER person_window AS END_DATE,
  ROW_NUMBER() OVER person_window AS SEQ_NUM
FROM base
WINDOW person_window AS (PARTITION BY AGENT_ID ORDER BY UPDT_DTM)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_az",
    ingest_view_name="state_staff_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
