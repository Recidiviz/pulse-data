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
"""Query that generates info for all LSIR assessments."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT 
    ClassificationId,
    a.OffenderId,
    AdministrativeDutyOfficerId,
    ClassificationTypeDesc,
    ScoreSheetTypeId,
    AssessmentDate,
    -- Regular expression to capture the classification score
    REGEXP_EXTRACT(
        c.Comment, -- The column containing the string
        r'preliminary_classification_score:\\s*(-?\\d+)$',  
        1
    ) AS score,
    REGEXP_EXTRACT(
        c.Comment, -- The column containing the string
        r'section_one_score_subtotal:\\s*(-?\\d+)',  
        1
    ) AS section_one_score,
    REGEXP_EXTRACT(
        c.Comment, -- The column containing the string
        r'section_two_score_subtotal:\\s*(-?\\d+)',  
        1
    ) AS section_two_score,
    REGEXP_EXTRACT(
        c.Comment, -- The column containing the string
        r'section_three_score_subtotal:\\s*(-?\\d+)',  
        1
    ) AS section_three_score,
FROM {clsf_Classification} a 
LEFT JOIN {clsf_ClassificationType} b
    USING (ClassificationTypeId) 
LEFT JOIN {clsf_ClassificationScoreSheet} c 
    USING (ClassificationId)
-- As of 11/27/24 there are two classification assessments with a single ClassificationId
QUALIFY ROW_NUMBER()OVER(PARTITION BY ClassificationId ORDER BY AssessmentDate DESC) = 1
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ix",
    ingest_view_name="classification_assessments",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
