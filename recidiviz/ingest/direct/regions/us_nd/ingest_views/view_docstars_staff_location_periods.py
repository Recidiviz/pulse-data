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
"""Ingest view for supervision staff employment location information from the Docstars system."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
    WITH critical_dates AS (
        SELECT * FROM (
            SELECT
            OFFICER,
            FNAME,
            LNAME,
            SITEID,
            STATUS,
            RecDate,
            LAG(SITEID) OVER (PARTITION BY OFFICER ORDER BY RecDate) AS prev_location, 
        FROM {docstars_officers@ALL}) cd
    WHERE 
    -- officer just started working
    (cd.prev_location IS NULL AND cd.SITEID IS NOT NULL) 
    -- officer changed locations
    OR cd.prev_location != SITEID
    -- officer's employment was terminated (RecDate in these rows is officers' termination date)
    OR cd.STATUS='0'
    ), all_periods AS (
SELECT 
    OFFICER,
    FNAME,
    LNAME,
    SITEID,
    STATUS,
    -- if status is 0, RecDate is previous period's end date, there should not be a period with that as start date
    RecDate AS start_date,
    LEAD(RecDate) OVER (PARTITION BY OFFICER ORDER BY RecDate) AS end_date
    FROM critical_dates
    WHERE SITEID IS NOT NULL)
SELECT 
OFFICER,
FNAME,
LNAME,
SITEID,
STATUS,
-- reset period_seq_num after excluding periods start dates of inactive periods
ROW_NUMBER() OVER (PARTITION BY OFFICER ORDER BY start_date) AS period_seq_num,
start_date,
end_date
FROM all_periods WHERE STATUS != '0'
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_nd",
    ingest_view_name="docstars_staff_location_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="OFFICER, period_seq_num",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
