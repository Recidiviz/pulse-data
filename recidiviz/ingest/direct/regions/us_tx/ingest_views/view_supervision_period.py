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
"""Query for supervision periods for US_TX."""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- In less than 1 percent of period records, there are multiple records on the same update_datetime date
-- This CTE deterministically chooses a single record per update_datetime date. We also get rid of 
-- any records that were deleted. 
clean_cte AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY Period_ID_Number,update_datetime ORDER BY Supervision_Officer, Supervision_Level, Special_Conditions, Custodial_Authority, Case_Type, Start_Date, Max_Termination_Date asc) AS rn
    FROM {SupervisionPeriod@ALL}
    -- Confusing column naming but this means that the record is not deleted.
    WHERE Deleted_Flag = "Active"
),
-- The lag_cte grabs the traits of the previous period record to filter to only records 
-- that contain a change.
lag_cte AS (
    SELECT 
        SID_Number,
        Period_ID_Number,
        Supervision_Officer,
        Supervision_Level,
        Special_Conditions,
        Custodial_Authority,
        Case_Type,
        Start_Date,
        Max_Termination_Date,
        update_datetime,
        LAG(update_datetime) OVER w AS prev_update_datetime,
        LAG(Supervision_Officer) OVER w AS prev_Supervision_Officer,
        LAG(Supervision_Level) OVER w AS prev_Supervision_Level,
        LAG(Special_Conditions) OVER w AS prev_Special_Conditions,
        LAG(Custodial_Authority) OVER w AS prev_Custodial_Authority,
        LAG(Case_Type) OVER w AS prev_Case_Type
    FROM clean_cte
    WHERE rn = 1
    WINDOW w AS (PARTITION BY Period_ID_Number ORDER BY update_datetime asc)
),
-- Filters down to the records that contain a change to the supervision period. Also uses
-- update_datetime dates as start/end dates for period.
filter_cte AS
(
    SELECT
        SID_Number,
        Period_ID_Number,
        Supervision_Officer,
        Supervision_Level,
        Special_Conditions,
        Custodial_Authority,
        Case_Type,
        Start_Date,
        Max_Termination_Date,
        update_datetime,
        ROW_NUMBER()OVER (PARTITION BY Period_ID_Number ORDER BY update_datetime asc) AS rn,
        LAG(update_datetime) OVER (PARTITION BY Period_ID_Number ORDER BY update_datetime asc) AS prev_update_datetime,
        LEAD(update_datetime) OVER (PARTITION BY Period_ID_Number ORDER BY update_datetime asc) AS lead_update_datetime
    FROM lag_cte 
    WHERE prev_update_datetime IS NULL 
        OR Supervision_Officer IS DISTINCT FROM prev_Supervision_Officer
        OR Supervision_Level IS DISTINCT FROM prev_Supervision_Level
        OR Special_Conditions IS DISTINCT FROM prev_Special_Conditions
        OR Custodial_Authority IS DISTINCT FROM prev_Custodial_Authority
        OR Case_Type IS DISTINCT FROM prev_Case_Type
)
SELECT
    SID_Number,
    Period_ID_Number,
    Supervision_Officer,
    Supervision_Level,
    Special_Conditions,
    Custodial_Authority,
    Case_Type,
    CASE 
        WHEN prev_update_datetime IS NULL
            THEN DATE(Start_Date)
        ELSE update_datetime
    END AS Start_Date,
    CASE WHEN DATE(Max_Termination_Date) < CURRENT_DATE
        THEN Max_Termination_Date
        ELSE NULL
    END AS Max_Termination_Date,
FROM filter_cte
"""
VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_tx",
    ingest_view_name="supervision_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
