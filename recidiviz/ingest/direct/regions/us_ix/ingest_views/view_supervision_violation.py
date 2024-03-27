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
"""Query that generates the state supervision_violation entity using the following tables: [ARS]"""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
violated_conditions AS (
  SELECT
    v.OffenderId,
    v.ViolationId,
    vt.ViolationTypeDesc,
    v.ArrestDate AS ArrestDatetime,
    v.ReportSubmissionDate AS ReportSubmissionDatetime,
    vrs.ViolationReportStatusDesc,
    ct.ConditionTypeDesc, 
    vsc.ViolationNotes,
    LOWER(vsc.ViolationNotes) LIKE '%felony%' AS FelonyFlag,
    LOWER(vsc.ViolationNotes) LIKE '%misdemeanor%' AS MisdemeanorFlag
  FROM 
    {scl_Violation} v
    left join {scl_ViolatedSupervisionCondition} vsc USING (ViolationId) -- There are times a violation exists, but no condition is listed as violated
    left join {scl_SupervisionCondition} sc USING (ConditionId)
    left join {scl_ConditionType} ct USING (ConditionTypeId)
    left join {scl_ViolationReportStatus} vrs ON v.ReportStatusId = vrs.ViolationReportStatusId
    left join {scl_ViolationType} vt USING (ViolationTypeId)
),
state_violation AS ( 
-- Should choose min between ReportSubmissionDate and ArrestDate for state_violation_date. Rarely, ArrestDate is second, but it does happen.
  SELECT 
    OffenderId,
    ViolationId,
    ViolationTypeDesc,
    CASE 
      WHEN ArrestDatetime IS NOT NULL and ReportSubmissionDatetime IS NOT NULL 
      THEN LEAST(ArrestDatetime, ReportSubmissionDatetime)
      ELSE COALESCE(ArrestDatetime, ReportSubmissionDatetime)
    END AS EstimatedViolationDate,
    ArrestDatetime,
    ReportSubmissionDatetime,
    ViolationReportStatusDesc,
    CAST(TO_JSON_STRING(ARRAY_AGG(STRUCT<condition_type_desc string, violation_notes string>(ConditionTypeDesc, ViolationNotes))) AS STRING) AS ViolatedConditions,
    LOGICAL_OR(FelonyFlag) AS FelonyFlag,
    LOGICAL_OR(MisdemeanorFlag) AS MisdemeanorFlag
  FROM violated_conditions
  GROUP BY OffenderId, ViolationId, ViolationTypeDesc, ViolationReportStatusDesc, ArrestDatetime, ReportSubmissionDatetime
)
SELECT 
  OffenderId,
  ViolationId,
  ViolationTypeDesc,
  EstimatedViolationDate,
  ViolatedConditions,
  FelonyFlag,
  MisdemeanorFlag,
  ReportSubmissionDatetime
FROM state_violation
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ix",
    ingest_view_name="supervision_violation",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
