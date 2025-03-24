# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Query that generates the state supervision violations and their responses 
using the following tables: 
    - Violations
    - ViolationResponses
"""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- There are around ~10 violation IDs that are reused. We create this CTE to filter to 
-- only the most recently added violations #TODO(#39111) Remove this filter
remove_duplicates AS
(
    SELECT
        VIOLATION_ID,
        COUNT(DISTINCT SID_Number) AS cnt
    FROM {Violations}
    GROUP BY 1  
),
-- Gather all violations and aggregate the violated conditions
violation_cte AS 
(
    SELECT 
        SID_Number,
        VIOLATION_ID,
        VIOLATION_DATE,
        VIOLATION_STATUS, 
    FROM {Violations} v
    LEFT JOIN remove_duplicates USING (VIOLATION_ID)
    WHERE cnt = 1
),
-- Combines the violation with the associated violation response
violation_responses AS (
    SELECT
        v.SID_Number,
        v.VIOLATION_ID,
        v.VIOLATION_STATUS,    
        DATE(VIOLATION_DATE) AS VIOLATION_DATE, 
        DATE(RESPONSE_DATE) AS RESPONSE_DATE,
        HEARING_PERIOD_ID,
        VIOLATION_RESULT,
    FROM violation_cte v
    LEFT JOIN {ViolationResponses} vr
        USING(VIOLATION_ID)
)

SELECT
    SID_Number,
    VIOLATION_ID,
    VIOLATION_DATE,
    VIOLATION_STATUS,
    CASE
        WHEN COUNTIF(HEARING_PERIOD_ID IS NOT NULL) = 0 THEN NULL
        ELSE TO_JSON_STRING(ARRAY_AGG(STRUCT<RESPONSE_DATE DATE, HEARING_PERIOD_ID STRING, VIOLATION_RESULT STRING, VIOLATION_ID STRING>(RESPONSE_DATE, HEARING_PERIOD_ID, VIOLATION_RESULT,VIOLATION_ID) ORDER BY HEARING_PERIOD_ID))
    END AS response_list
FROM violation_responses
GROUP BY SID_Number,VIOLATION_ID,VIOLATION_DATE,VIOLATION_STATUS
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_tx",
    ingest_view_name="supervision_violation",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
