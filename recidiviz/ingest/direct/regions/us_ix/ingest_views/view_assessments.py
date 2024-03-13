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
  AssessmentId,
  OffenderId,
  AssessmentTypeDesc,
  AssessmentToolDesc,
  CompletionDate,
  OverallScore,
  ResultNote,
  SPLIT(REGEXP_EXTRACT(ResultNote, r'\\[Desc\\]= ([^\\|]+) \\|'),'=')[SAFE_OFFSET(0)] AS legacy_assessment_degree_level,
  AssessmentDegreeId,
  AssessmentDegreeDesc
FROM {asm_Assessment} 
LEFT JOIN {asm_AssessmentType} USING (AssessmentTypeId)
LEFT JOIN {asm_AssessmentTool} USING (AssessmentToolId)
LEFT JOIN {asm_AssessmentDegree} USING (AssessmentDegreeId)
WHERE AssessmentToolId = '174'
# 174 filters for only LSI-R Assessments from the asm_Assessments table
AND CompletionDate is not null

"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ix",
    ingest_view_name="assessments",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="OffenderId, AssessmentId",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
