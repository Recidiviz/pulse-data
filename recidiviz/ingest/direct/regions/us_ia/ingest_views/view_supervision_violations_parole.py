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
"""Query containing State Supervision Violations from DOC for IA for parole."""

from recidiviz.ingest.direct.regions.us_ia.ingest_views.query_fragments import (
    FEILD_RULE_VIOLATIONS_CTE,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = (
    f"WITH {FEILD_RULE_VIOLATIONS_CTE},"
    + """

-- Part Two: Get the relevant Paroles.
Parole as
 (
   select


     PVR.HearingCompleted
     , PVR.ReviewCompleted
     , RR.ReportType


     , PVR.OffenderCd


     -- Dates
     , PVR.ReviewDt
     , RR.ReportDt
     , RR.InCustodyDt
     , RR.PpviFiledDt
     , RR.SubmittedDt
     , RR.PresentStatus
     , RR.CustodyType
     , RR.ParoleViolationReviewRequest
     , RR.ParoleViolationReviewRecommendation


   -- IDs that are unique to tables
     , ParoleViolationReviewId
     , RR.ParoleViolationReviewReportId
     , PFRVI.H_ParoleViolationReviewFrviId


   -- ID's for adding feild rule related tables.
   , PFRVI.FieldRuleViolationIncidentId


   -- Extra For The JSON
     , RR.CustodyLocation
     , RR.CountyOfStaff
     , RR.CompletedByStaffId
     , RR.ApprovedByStaffId
   from {IA_DOC_PVRReports} as RR
   left join {IA_DOC_ParoleVR} as PVR using (ParoleViolationReviewId, OffenderCd)
   left join {IA_DOC_PVR_FRVI} as PFRVI using (ParoleViolationReviewId, OffenderCd)
 )


select

  OffenderCd

  -- violation ID
  , FieldRuleViolationIncidentId
  -- response ID
  , ParoleViolationReviewReportId

  -- violation_date
  , IncidentDt
  -- response_date
  , cast(ReportDt as DATETIME) as ReportDt

  -- Other columns for mapping
  , ReportType
  , ParoleViolationReviewRecommendation
  , CompletedByStaffId
  , HearingCompleted
  , ReviewCompleted

  -- Extra For The JSON
  , CustodyLocation
  , ApprovedByStaffId
  
from Parole
left join FRV_I using (FieldRuleViolationIncidentId, OffenderCd)
"""
)


VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ia",
    ingest_view_name="supervision_violations_parole",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
