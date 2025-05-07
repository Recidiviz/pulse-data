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
"""Query containing State Supervision Violations from DOC for IA for probations."""

from recidiviz.ingest.direct.regions.us_ia.ingest_views.query_fragments import (
    FIELD_RULE_VIOLATIONS_CTE,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = (
    f"WITH {FIELD_RULE_VIOLATIONS_CTE},"
    + """





-- Part Two: Get the relevant Probations.
Probation as
 (
   select
       ROV.Active as ROV_Active


       , OffenderCd
       , ROV.CaseManagerStaffId
       , ROVD.Decision
       , ROVD.ProbationROVHearingResponseDecisionId
       , CAST(ROVD.EnteredDt AS DATETIME) as decision_EnteredDt


       -- Dates
       , ROV.ReportDt


       -- ID's unique to table
       , ROV.ProbationReportOfViolationId
       , RIV_FRV.ProbationROVFieldRuleViolationIncidentId


       -- JSON Extras
       , ROV.WorkUnitRegionId
       --, ROV.WorkUnitId
       , ROV.RecommendationToCourt
       , ROV.WarrantRequested
       , ROV.Comments
      
       -- Other merging
       , RIV_FRV.FieldRuleViolationIncidentId
   from
     {IA_DOC_ProbationROV} as ROV
   left join
     {IA_DOC_ProbationROVFieldRuleViolationInfo} as RIV_FRV
     using (ProbationReportOfViolationId, OffenderCd)
    left join {IA_DOC_ProbationROVHearingResponses} as ROVHR
      using(

        OffenderCd
        , ProbationReportOfViolationId
      )
    left join {IA_DOC_ProbationROVHearingResponseDecisions} as ROVD
      using(
        OffenderCd
        , ProbationROVHearingResponseId
      )
  where ROV.Active='1' -- Brenda confirmed with the team that this is the correct value between 1,-1 should be removed.
 )

select


  OffenderCd

  -- violation ID
  , FieldRuleViolationIncidentId
  -- response ID
  , ProbationReportOfViolationId

  -- violation_date
  , IncidentDt
  , EnteredDt
  -- response_date
  , cast(ReportDt as DATETIME) as ReportDt


  -- Other columns for mapping
  , CaseManagerStaffId
  , WarrantRequested
  , Decision
  , ProbationROVHearingResponseDecisionId
  , decision_EnteredDt


from probation
left join FRV_I using (FieldRuleViolationIncidentId, OffenderCd)
"""
)


VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ia",
    ingest_view_name="supervision_violations_probation",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
