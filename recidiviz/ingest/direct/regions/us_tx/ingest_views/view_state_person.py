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
"""Query that generates the state person entity using the following tables: ClientData"""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- Cleans the names of all of the rows in ClientData
clean_name_cte AS
    (SELECT DISTINCT
        CASE 
            WHEN ARRAY_LENGTH(SPLIT(full_name, ',')) > 0  -- Ensure there's at least one part before splitting by comma
                THEN TRIM(SPLIT(full_name, ',')[SAFE_OFFSET(0)])  -- Take the first part as LastName
            ELSE TRIM(full_name)  -- If no comma, treat full_name as the last name
        END AS LastName,
        CASE 
            WHEN ARRAY_LENGTH(SPLIT(full_name, ',')) > 1  -- Ensure there's a second part after splitting by comma
                THEN TRIM(SPLIT(SPLIT(full_name, ',')[SAFE_OFFSET(1)], ' ')[SAFE_OFFSET(0)])  -- Take the first part as FirstName
            ELSE NULL
        END AS FirstName,
        CASE 
            WHEN ARRAY_LENGTH(SPLIT(full_name, ',')) > 1  -- Ensure there's a second part after splitting by comma
                THEN TRIM(SPLIT(SPLIT(full_name, ',')[SAFE_OFFSET(1)], ' ')[SAFE_OFFSET(1)])  -- Take the second part as MiddleName
            ELSE NULL
        END AS MiddleName,
        CASE 
            WHEN ARRAY_LENGTH(SPLIT(full_name, ',')) > 1  -- Ensure there's a second part after splitting by comma
                THEN TRIM(SPLIT(SPLIT(full_name, ',')[SAFE_OFFSET(1)], ' ')[SAFE_OFFSET(2)])  -- Take the third part as Suffix
            ELSE NULL
        END AS Suffix,
        Address,
        Phone_Number,
        NULLIF(SID_Number, '00000000') AS SID_Number,
        NULLIF(TDCJ_Number, '00000000') AS TDCJ_Number,
        Creation_Date
    FROM {ClientData@ALL}
),
-- Aggregates all of the TDCJ_Number IDs
agg_ids_cte AS (
    SELECT
        SID_Number,
        STRING_AGG(DISTINCT TDCJ_Number) as aggs
    FROM clean_name_cte
    GROUP BY SID_Number
)
SELECT DISTINCT
    FirstName,
    MiddleName,
    LastName,
    Suffix,
    SID_Number,
    Address,
    Phone_Number,
    aggs AS TDCJ_Numbers,
FROM clean_name_cte
LEFT JOIN agg_ids_cte USING (SID_Number)
WHERE SID_Number IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY SID_Number ORDER BY Creation_Date desc, CASE WHEN Address IS NULL THEN 1 ELSE 0 END, CASE WHEN Phone_Number IS NULL THEN 1 ELSE 0 END) = 1;
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_tx",
    ingest_view_name="state_person",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
