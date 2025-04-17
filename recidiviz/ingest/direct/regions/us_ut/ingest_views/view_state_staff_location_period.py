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
their working location, and the period during which they were assigned to that location.
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT * EXCEPT(status, update_datetime),
ROW_NUMBER() OVER (PARTITION BY usr_id ORDER BY start_date, update_datetime) as sequence_num
FROM (
  SELECT DISTINCT
    UPPER(usr_id) AS usr_id, 
    agcy_id,
    CAST(updt_dt AS DATETIME) AS start_date,
    -- There are a few rows with distinct agcy_ids but identical state-provided updt_dt values. 
    -- Use our internal update_datetime value to order them correctly based on when we received the file.
    LEAD(CAST(updt_dt AS DATETIME)) OVER (PARTITION BY usr_id ORDER BY CAST(updt_dt as datetime), update_datetime) AS end_date,
    vld_flg as status,
    update_datetime
  FROM {applc_usr@ALL}
  )
WHERE status = 'Y' 
AND start_date IS DISTINCT FROM end_date
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ut",
    ingest_view_name="state_staff_location_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
