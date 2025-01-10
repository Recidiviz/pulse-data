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
"""Query that generates info for all present and past members of the Utah DOC staff.

Staff names are formatted in three ways in the raw data that feeds this view: 
1. FIRST LAST
2. FIRST M LAST
3. FIRST

Some entries, particularly those with only one component in their name (#3), are 
non-person organizational categories like FUGITIVE or RELEASE/REENTRY. These have
been assigned to supervise clients in the past, so we ingest them to avoid errors in 
supervision period ingest.

Otherwise, we assume that the final component of a name string is the person's surname,
regardless of how many components there are. 
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT DISTINCT
  usr_id,
  name_array[SAFE_OFFSET(0)] AS fname,
  IF(name_array[SAFE_OFFSET(2)] IS NULL,
    -- If there is no third element, the person does not have a middle name in the database
    NULL,
    -- If there is a third element, store the second element as the person's middle name
    name_array[SAFE_OFFSET(1)]) AS mname,
  IF(ARRAY_LENGTH(name_array) > 1, 
    -- If the name has more than one component, assume the final component is the surname
    name_array[SAFE_OFFSET(ARRAY_LENGTH(name_array)-1)], 
    -- If the name has only one component, there is no surname
    NULL) AS lname,
FROM (
  SELECT 
    UPPER(usr_id) AS usr_id, 
    SPLIT(name,' ') AS name_array, 
    CAST(updt_dt AS DATETIME) AS updt_dt
  FROM {applc_usr}
) 
-- This is only true in two rows and seems to be a data entry error.
WHERE usr_id IS NOT NULL
-- There is one instance of a USR_ID value being reused. This guarantees that we store the most recent name associated with the ID.
QUALIFY ROW_NUMBER() OVER (PARTITION BY usr_id ORDER BY updt_dt DESC) = 1
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ut",
    ingest_view_name="state_staff",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
