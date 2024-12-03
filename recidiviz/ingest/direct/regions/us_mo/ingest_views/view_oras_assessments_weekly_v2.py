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
        ASSESSMENT_TYPE,
        RISK_LEVEL,
        SCORE,
        EXTRACT(DATE FROM CAST(CREATED_DATE AS DATETIME)) AS CREATED_DATE,
        ROW_NUMBER() OVER (PARTITION BY DOC_ID,CREATED_DATE,ASSESSMENT_TYPE) AS assessment_rank
    FROM {ORAS_ORAS_ASSESSMENTS_WEEKLY}
    WHERE
        ASSESSMENT_STATUS = 'Complete'
    -- explicitly filter out any test data from UCCI
        AND OFFENDER_NAME NOT LIKE '%Test%'
        AND OFFENDER_NAME NOT LIKE '%test%'
),
    duplicate_counts AS (
    SELECT 
        DOC_ID,
        CREATED_DATE,
        ASSESSMENT_TYPE,
        COUNT(DISTINCT SCORE) AS score_count,
        COUNT(DISTINCT RISK_LEVEL) AS rl_count
        FROM assessments_with_duplicates
        GROUP BY DOC_ID,CREATED_DATE,ASSESSMENT_TYPE
)
SELECT 
    DOC_ID,
    ASSESSMENT_TYPE,
    CASE WHEN rl_count > 1 THEN NULL ELSE RISK_LEVEL END AS RISK_LEVEL,
    CASE WHEN score_count > 1 THEN NULL ELSE SCORE END AS SCORE,
    CREATED_DATE
FROM assessments_with_duplicates 
LEFT JOIN duplicate_counts USING(DOC_ID,CREATED_DATE,ASSESSMENT_TYPE)
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
