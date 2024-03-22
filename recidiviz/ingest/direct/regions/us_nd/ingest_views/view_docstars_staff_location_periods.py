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
WITH staff_from_directory AS (
    SELECT
        -- Make officer IDs uniform across sources 
        CAST(OFFICER AS INT64) AS OFFICER,
        -- Transform district names into district external IDs
        xref.supervising_district_external_id AS location,
        CAST(update_datetime AS DATETIME) AS edge_date,
        '(1)' AS STATUS, 
        MAX(CAST(update_datetime AS DATETIME)) OVER (PARTITION BY TRUE) AS last_file_update_datetime,
        MAX(CAST(update_datetime AS DATETIME)) OVER (PARTITION BY OFFICER) AS last_appearance_datetime, 
    FROM {RECIDIVIZ_REFERENCE_PP_directory@ALL} dir
    LEFT JOIN {RECIDIVIZ_REFERENCE_supervision_district_id_to_name} xref
    ON(UPPER(dir.Location) = UPPER(xref.supervising_district_name))
),
staff_from_docstars AS (
    SELECT DISTINCT
        -- Make officer IDs uniform across sources 
        CAST(OFFICER AS INT64) AS OFFICER,
        SITEID AS location,
        CAST(RecDate AS DATETIME) AS edge_date, 
        STATUS,
        MAX(CAST(RecDate AS DATETIME)) OVER (PARTITION BY TRUE) AS last_file_update_datetime,
        MAX(CAST(RecDate AS DATETIME)) OVER (PARTITION BY OFFICER) AS last_appearance_datetime, 
    FROM {docstars_officers@ALL} 
),
combined_data AS (
    SELECT 
        OFFICER,
        location,
        edge_date,
        STATUS,
        last_file_update_datetime,
        last_appearance_datetime,
    FROM staff_from_directory
    
    UNION ALL 

    SELECT 
        OFFICER,
        location,
        edge_date,
        STATUS,
        last_file_update_datetime,
        last_appearance_datetime,
    FROM staff_from_docstars 
),
critical_dates AS (
SELECT * FROM (
    SELECT
        CAST(OFFICER AS INT64) AS OFFICER,
        location,
        LAG(location) OVER (PARTITION BY OFFICER ORDER BY edge_date) AS prev_location, 
        edge_date,
        STATUS,
        last_file_update_datetime,
        last_appearance_datetime
    FROM combined_data) cd
WHERE 
    -- officer just started working 
    (cd.prev_location IS NULL AND cd.location IS NOT NULL) 
    -- officer changed locations
    OR cd.prev_location != location
    -- officer's employment was terminated (RecDate in these rows is officers' termination date)
    OR cd.STATUS='0'   
    -- include the latest update even if the previous conditions are not true
    OR edge_date = last_file_update_datetime 
), all_periods AS (
SELECT 
    OFFICER,
    location,
    STATUS,
    -- if status is 0, RecDate is previous period's end date, there should not be a period with that as start date
    edge_date AS start_date,
    CASE 
        -- If a staff member stops appearing in the roster, close their employment period
        -- on the last date we receive a roster that included them
        WHEN LEAD(edge_date) OVER person_window IS NULL 
            AND edge_date < last_file_update_datetime
            THEN last_appearance_datetime 
        -- There is a more recent update to this person's location
        WHEN LEAD(edge_date) OVER person_window IS NOT NULL 
            THEN LEAD(edge_date) OVER person_window 
        -- All currently-employed staff will appear in the latest roster
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
