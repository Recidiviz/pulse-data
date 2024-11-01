# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License AS published by
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
"""Query containing incarceration incident information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- The incident_CTE gathers information about the incident using DiscOffenseRptId
-- as the primary key for incidents.
incident_CTE as (
    SELECT
        DiscOffenseRptId,
        DorOffenseTypeId,
        dor.TermId,
        dor.OffenderId,
        InfractionDate,
        DorOffenseTypeDesc,
        DorOffenseCode,
        DorOffenseCategoryId,
        LocationName,
        SeverityDesc
    FROM {scl_DiscOffenseRpt} dor
    LEFT JOIN {scl_DorOffenseType} dt USING (DorOffenseTypeId)
    LEFT JOIN {ref_Location} m USING (LocationId)
    LEFT JOIN {scl_OffenseCategory} oc on dt.DorOffenseCategoryId = oc.offenseCategoryId
    LEFT JOIN {scl_Severity} USING (severityId)
),
-- The outcomes cte grabs all outcomes we have information for, in this case DORPenaltyId
-- is the primary key.
outcomes AS
(
    SELECT DISTINCT
        DiscOffenseRptId,
        DORPenaltyId,
        DOROffenseTypePenaltyId,
        ImposedValue,
        PenaltyStartDate,
        PenaltyEndDate,
        OffenseDateTime,
        AuditHearingDate,
        DAProcedureStatusDesc,
        da.InsertDate,
    FROM {dsc_DACase} da
    LEFT JOIN {scl_DORPenalty} dorp USING(DiscOffenseRptId)
    LEFT JOIN {dsc_DAProcedureStatus} USING (DAProcedureStatusId)
    LEFT JOIN {dsc_DAProcedure} p USING (DACaseId)
    LEFT JOIN {dsc_DADecisionOfficer} d USING (DAProcedureId)
    LEFT JOIN {dsc_DADecision} e USING (DADecisionId)
),
-- The outcomes_unaggregated creates a single record for each incident and outcome
-- pairing using DiscOffenseRptId to join. 
-- Note, there can be many outcomes for a single incident. 
outcomes_unaggregated AS (
    SELECT DISTINCT
        inc.DiscOffenseRptId,
        inc.TermId,
        inc.OffenderId,
        InfractionDate,
        DorOffenseTypeDesc,
        DorOffenseCode,
        DorOffenseCategoryId,
        LocationName,
        DORPenaltyId,
        DOROffenseTypePenaltyId,
        ImposedValue,
        PenaltyStartDate,
        PenaltyEndDate,
        OffenseDateTime,
        AuditHearingDate,
        DAProcedureStatusDesc,
        InsertDate,
        SeverityDesc
    FROM incident_CTE inc
    LEFT JOIN outcomes USING (DiscOffenseRptId)
    WHERE DORPenaltyId IS NOT NULL
    )
    -- Aggregates all of the outcomes into a JSON column to parse in the mappings.
    SELECT
        DiscOffenseRptId,
        ic.TermId,
        ic.OffenderId,
        ic.InfractionDate,
        ic.DorOffenseTypeDesc,
        ic.DorOffenseCode,
        ic.DorOffenseCategoryId,
        ic.LocationName,
        ic.SeverityDesc,
        ou.DAProcedureStatusDesc,
        CASE WHEN COUNT(DORPenaltyId) < 1 THEN NULL ELSE
        TO_JSON_STRING(
            ARRAY_AGG(STRUCT<offense_date string,hearing_date string, penalty_id string, 
            penalty_start string, penalty_end string, insert_date string>
            (OffenseDateTime,AuditHearingDate,DORPenaltyId,PenaltyStartDate,PenaltyEndDate,
            InsertDate) ORDER BY OffenseDateTime, AuditHearingDate)) 
        END AS outcome_list
    FROM incident_cte ic
    LEFT JOIN outcomes_unaggregated ou USING (DiscOffenseRptId) 
    GROUP BY DiscOffenseRptId,TermId,OffenderId,
            InfractionDate,DorOffenseTypeDesc,DorOffenseCode,
            DorOffenseCategoryId,LocationName,DAProcedureStatusDesc,SeverityDesc
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ix",
    ingest_view_name="incarceration_incident",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
