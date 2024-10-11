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
        TRIM(SPLIT(full_name, ',')[OFFSET(0)]) AS LastName,
        TRIM(SPLIT(SPLIT(full_name, ',')[OFFSET(1)], ' ')[OFFSET(0)]) AS FirstName,
        CASE 
            WHEN ARRAY_LENGTH(SPLIT(SPLIT(full_name, ',')[OFFSET(1)], ' ')) > 1 
                THEN TRIM(SPLIT(SPLIT(full_name, ',')[OFFSET(1)], ' ')[OFFSET(1)])
            ELSE NULL
        END AS MiddleName,
        CASE 
            WHEN ARRAY_LENGTH(SPLIT(SPLIT(full_name, ',')[OFFSET(1)], ' ')) > 2
                THEN TRIM(SPLIT(SPLIT(full_name, ',')[OFFSET(1)], ' ')[OFFSET(2)])
            ELSE NULL
        END AS Suffix,
        Address,
        Phone_Number,
        NULLIF(SID_Number, '00000000') AS SID_Number,
        NULLIF(TDCJ_Number, '00000000') AS TDCJ_Number,
        Creation_Date
    FROM {ClientData}
),
-- Aggregates all of the TDCJ_Number IDs
agg_ids_cte AS (
    SELECT
        SID_Number,
        STRING_AGG(TDCJ_Number) as aggs
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
    aggs AS TDCJ_Numbers
FROM clean_name_cte
LEFT JOIN agg_ids_cte USING (SID_Number)
QUALIFY ROW_NUMBER() OVER (PARTITION BY SID_Number ORDER BY Creation_Date desc) = 1
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_tx",
    ingest_view_name="state_person",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
