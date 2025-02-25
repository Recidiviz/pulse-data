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
"""Query containing program assignment information from the following table:
RCDVZ_CISPRDDTA_CMOFRH
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- only assessments after 2012 are reliable in this table
risk_assessment AS (
    SELECT 
        RECORD_KEY,
        CUSTODY_NUMBER, 
        ADMISSION_NUMBER,
        RISK_ASSESSMENT_TOTAL,
        CALCULATED_SUPER_LVL,
        ASSESSMENT_DATE,
        CASELOAD,
        COMMUNITY_SUPER_LVL,
        ROW_NUMBER() OVER (PARTITION BY RECORD_KEY ORDER BY ASSESSMENT_DATE) AS AssessmentID
    FROM {RCDVZ_CISPRDDTA_CMOFRH}
    WHERE EXTRACT(YEAR FROM DATE(ASSESSMENT_DATE)) > 2012
)
SELECT  
    RECORD_KEY,
    AssessmentID,
    RISK_ASSESSMENT_TOTAL,
    CALCULATED_SUPER_LVL,
    ASSESSMENT_DATE,
    CASELOAD,
    COMMUNITY_SUPER_LVL,
FROM risk_assessment
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_or",
    ingest_view_name="Risk_Assessment",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
