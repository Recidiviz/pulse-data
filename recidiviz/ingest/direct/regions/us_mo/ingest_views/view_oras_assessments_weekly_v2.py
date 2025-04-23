# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Query containing ORAS assessments information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
    assessments_with_duplicates AS (
    SELECT
        DOC_ID,
        ASSESSMENT_NAME,
        RISK_LEVEL,
        OVERALL_ASSESSMENT_SCORE,
        EXTRACT(DATE FROM CAST(ASSESSMENT_DATE AS DATETIME)) AS ASSESSMENT_DATE,
        ROW_NUMBER() OVER (PARTITION BY DOC_ID,ASSESSMENT_DATE,ASSESSMENT_NAME) AS assessment_rank
    FROM (
        SELECT 
            * EXCEPT(ASSESSMENT_DATE),
        CASE 
            WHEN BACKDATED != "1900-12-31 00:00:00" AND CAST(BACKDATED AS DATETIME) <= CURRENT_DATETIME() THEN BACKDATED 
            ELSE ASSESSMENT_DATE
        END AS ASSESSMENT_DATE         
        FROM {ORAS_MO_ASSESSMENTS_DB2}
        WHERE DELETED_AT = "1900-12-31 00:00:00" AND
            ASSESSMENT_STATUS = 'Complete' AND
            -- explicitly filter out any test data from UCCI
            UPPER(OFFENDER_FIRST_NAME) NOT LIKE '%TEST%' AND
            UPPER(OFFENDER_LAST_NAME) NOT LIKE '%TEST%'
    )
),
    duplicate_counts AS (
    SELECT 
        DOC_ID,
        ASSESSMENT_DATE,
        ASSESSMENT_NAME,
        COUNT(DISTINCT OVERALL_ASSESSMENT_SCORE) AS score_count,
        COUNT(DISTINCT RISK_LEVEL) AS rl_count
        FROM assessments_with_duplicates
        GROUP BY DOC_ID,ASSESSMENT_DATE,ASSESSMENT_NAME
)
SELECT 
    DOC_ID,
    ASSESSMENT_NAME AS ASSESSMENT_TYPE,
    CASE WHEN rl_count > 1 THEN NULL ELSE RISK_LEVEL END AS RISK_LEVEL,
    CASE WHEN score_count > 1 THEN NULL ELSE OVERALL_ASSESSMENT_SCORE END AS SCORE,
    ASSESSMENT_DATE AS CREATED_DATE
FROM assessments_with_duplicates 
LEFT JOIN duplicate_counts USING(DOC_ID,ASSESSMENT_DATE,ASSESSMENT_NAME)
WHERE assessment_rank = 1
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mo",
    ingest_view_name="oras_assessments_weekly_v2",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
