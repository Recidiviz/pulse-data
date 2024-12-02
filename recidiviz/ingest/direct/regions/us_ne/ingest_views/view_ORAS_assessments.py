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
"""ORAS Assessment information for all adults about to be or on supervision. It is used 
to determine their supervision level."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- all relevant assessment information from ORASClientRiskLevelAndNeeds table
risk_assessment AS (
  SELECT 
    pkORASClientRiskLevelAndNeedsId, 
    inmateNumber, 
    NULLIF(overrideReasonCode, "NULL") AS overrideReasonCode,
    NULLIF(supervisionLevelOverrideCode, "NULL") AS supervisionLevelOverrideCode,
    overallScore, 
    NULLIF(riskLevel, "NULL") AS riskLevel, 
    assessmentDate,
  FROM {ORASClientRiskLevelAndNeeds}
), 
-- this table is used to decode various codes in ORASClientRiskLevelAndNeeds
codevalues AS (
  SELECT 
    codeId, 
    codeValue 
  FROM {CodeValue}
)
SELECT 
  pkORASClientRiskLevelAndNeedsId, 
  inmateNumber, 
  NULLIF(reason.codevalue, "NULL") AS overrideReasonCode,
  NULLIF(slo.codevalue, "NULL") AS supervisionLevelOverrideCode,
  NULLIF(overallScore, "NULL") AS overallScore, 
  NULLIF(riskLevel, "NULL") AS riskLevel, 
  assessmentDate
FROM risk_assessment
LEFT JOIN codevalues slo
  ON supervisionLevelOverrideCode = slo.codeId
LEFT JOIN codevalues reason
  ON overrideReasonCode = reason.codeId

"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ne",
    ingest_view_name="ORAS_assessments",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
