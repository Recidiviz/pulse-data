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
"""Query containing state Staff Role Period information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH first_reported_title AS(
    SELECT 
        StaffID, 
        StaffTitle
    FROM {Staff@ALL}
    WHERE StaffID IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY StaffID ORDER BY StatusDate ASC) = 1
),
key_status_change_dates AS(
    #arbitrary first period start dates since beginning of time 
    SELECT
    DISTINCT StaffID, 
    'A' as Status, 
    '1900-01-01' as StatusDate, 
    StaffTitle, 
    CAST('1900-01-01 00:00:00' AS DATETIME) as update_datetime
    FROM first_reported_title
    WHERE StaffID IS NOT NULL

    UNION ALL
    
    SELECT 
        StaffID, 
        Status, 
        StatusDate,
        StaffTitle, 
        update_datetime
    FROM {Staff@ALL}
    WHERE StaffID IS NOT NULL AND StatusDate IS NOT NULL
), 
ranked_rows AS(
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY StaffID,Status,StatusDate,StaffTitle ORDER BY update_datetime DESC) as RecencyRank
    FROM key_status_change_dates
),
create_unique_rows AS (
    SELECT 
        StaffID,
        Status,
        StatusDate,
        StaffTitle,
        update_datetime,
        ROW_NUMBER() OVER (PARTITION BY StaffID ORDER BY update_datetime ASC) AS StatusChangeOrder
    FROM ranked_rows
    WHERE RecencyRank = 1
),
construct_periods AS (
    SELECT 
        StaffID,
        Status,
        StatusDate as Start_Date,
        StaffTitle, 
        LEAD(StatusDate) OVER person_sequence as End_Date,
        StatusChangeOrder
    FROM create_unique_rows 
    WINDOW person_sequence AS (PARTITION BY StaffID ORDER BY StatusChangeOrder)
)
SELECT 
    StaffID,
    Status,
    Start_Date,
    StaffTitle,
    End_Date,
    StatusChangeOrder
FROM construct_periods
WHERE Status = 'A' OR (Status = 'I' AND End_Date IS NOT NULL)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_tn",
    ingest_view_name="StaffRolePeriods",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="StaffID, Start_Date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
