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
"""Ingest view for supervision staff role periods from the Docstars system."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH critical_dates AS (
SELECT *
FROM (
    SELECT
    CAST(OFFICER AS INT) AS OFFICER,
    FNAME,
    LNAME,
    RecDate,
    STATUS,
    LAG(STATUS) OVER (PARTITION BY OFFICER ORDER BY RecDate) AS prev_status, 
FROM {docstars_officers@ALL}) cd
WHERE 
    -- officer just started working
    (cd.prev_status IS NULL AND cd.STATUS IS NOT NULL) 
    -- officer changed locations
    OR cd.prev_status != STATUS
), all_periods AS (
SELECT 
    OFFICER,
    FNAME,
    LNAME,
    STATUS,
    RecDate AS start_date,
    LEAD(RecDate) OVER (PARTITION BY OFFICER ORDER BY RecDate) AS end_date
FROM critical_dates
WHERE STATUS IS NOT NULL)
SELECT 
    OFFICER,
    FNAME,
    LNAME,
    STATUS,
    ROW_NUMBER() OVER (PARTITION BY OFFICER ORDER BY start_date) AS period_seq_num,
    start_date,
    end_date
FROM all_periods 
WHERE STATUS != '0' -- exclude periods that start with employment termination
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_nd",
    ingest_view_name="docstars_staff_role_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="OFFICER, period_seq_num",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
