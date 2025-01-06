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
WITH 
-- This CTE gets the officer data directly from the Docstars system into the same format
-- as above, to the extent possible. In Docstars, RecDate is the date the row was created.
staff_from_docstars AS (
    SELECT DISTINCT
        -- This is safe because IDs from docstars_officers are exclusively numeric.
        CAST(CAST(OFFICER AS INT64) AS STRING) AS OFFICER,
        SITEID AS location,
        CAST(RecDate AS DATETIME) AS edge_date, 
        STATUS
    FROM {docstars_officers@ALL} 
),
-- This CTE filters the raw data to include only the rows that are relevant to the
-- location periods we are constructing. These are rows where an officer just started working
-- or changed locations. We also include all rows where the edge date is the last update
-- we have about a given officer, to account for rows that signify an officer becoming inactive.
critical_dates AS (
SELECT * FROM (
    SELECT
        OFFICER,
        location,
        LAG(location) OVER (PARTITION BY OFFICER ORDER BY edge_date) AS prev_location, 
        edge_date,
        STATUS,
        LAG(STATUS) OVER (PARTITION BY OFFICER ORDER BY edge_date) AS prev_status, 
    FROM staff_from_docstars) cd
WHERE 
    -- officer just started working 
    (cd.prev_location IS NULL AND cd.location IS NOT NULL) 
    -- officer changed locations
    OR cd.prev_location != location
    -- officer's employment status changed (RecDate in these rows is officers' hire/termination date)
    OR (cd.prev_status != STATUS)
), 
-- This CTE constructs periods using the compiled critical dates. End dates follow this 
-- logic: 
    -- 1. If a person stopped working, close their employment period on the last date we 
    --    receive data that included them as an active employee.
    -- 2. If there is a subsequent row showing a person changed locations, close the existing 
    --    period on the date of that change.
    -- 3. If a there is no subsequent row, and the edge date is the same as the most 
    --    recent date the person has appeared in the data, then leave the period open
    --    to signify that the existing period is still active.
all_periods AS (
SELECT 
    OFFICER,
    location,
    STATUS,
    -- if status is 0, RecDate is previous period's end date, there should not be a period with that as start date
    edge_date AS start_date,
    CASE 
        -- There is a more recent update to this person's location
        WHEN LEAD(edge_date) OVER person_window IS NOT NULL 
            THEN LEAD(edge_date) OVER person_window 
        -- All currently-employed staff will appear in the latest raw data
        ELSE CAST(NULL AS DATETIME)
    END AS end_date,
FROM critical_dates
WHERE STATUS IS NOT NULL
AND STATUS != '0' -- exclude periods that start with employment termination
WINDOW person_window AS (PARTITION BY OFFICER ORDER BY edge_date)
)
SELECT 
    OFFICER,
    location, 
    -- reset period_seq_num after excluding periods start dates of inactive periods
    start_date,
    end_date,
    ROW_NUMBER() OVER (PARTITION BY OFFICER ORDER BY start_date) AS period_seq_num,
FROM all_periods
WHERE location IS NOT NULL
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_nd",
    ingest_view_name="docstars_staff_location_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
