# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Query for ORAS assessment data. The raw data for this query is currently managed
in a Google-backed table in BigQuery, and exported from BQ as a CSV to our ingest bucket
each night via scheduled query. Eventually, this data should arrive via direct push to 
GCS like the rest of the data we receive from AZ. 
"""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT 
    REGEXP_REPLACE(ADC_NUMBER,'^0','') AS ADC_NUMBER,
    s.date AS assessment_date,
    Total, -- assessment_score
    Supervision_Level, -- assessment_level
    -- From here on is metadata
    CRIMINAL_HISTORY_TOTAL,
    EDUCATION_EMPLOYMENT_AND_FINANCIAL_SITUATION_TOTAL, 
    FAMILY_AND_SOCIAL_SUPPORT_TOTAL, 
    NEIGHBORHOOD_PROBLEMS_TOTAL, 
    SUBSTANCE_USE_TOTAL, 
    PEER_ASSOCIATIONS_TOTAL, 
    CRIMINAL_ATTITUDES_AND_BEHAVIORAL_PATTERNS_TOTAL
FROM {ORAS_results_sheet}  s
WHERE s.date IS NOT NULL
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_az",
    ingest_view_name="oras_assessment",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
