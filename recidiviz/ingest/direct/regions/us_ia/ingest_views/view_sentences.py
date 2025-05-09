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
"""Query containing sentence, sentence group, and charge information."""

from recidiviz.ingest.direct.regions.us_ia.ingest_views.query_fragments import (
    PENALTIES_TO_KEEP,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""
WITH 
    {PENALTIES_TO_KEEP}

  -- This CTE pulls together each penalty we're interested in (which we'll ingest as StateSentence)
  -- and then all associated sentence and charge information.  For our purposes, we'll ingest each
  -- charge as a StateSentenceGroup because the relationships in IA are such that each charge can 
  -- be associated with multiple sentences, and each sentence can be associated with multiple penalties.
  SELECT DISTINCT
	penalty.OffenderCd,
    penalty.PenaltyId,
    penalty.SentencePenaltyType,
    penalty.SentencePenaltyModifier,
    penalty.PenaltyLife,
    sentence.SentenceId,
    DATE(sentence.SentenceDt) AS SentenceDt,
    charge.ChargeId,
    charge.JurisdictionType,
    -- There are ~ 900 sentences from pre 2003 (aka pre ICON) that have Undefined jurisdiction
    -- We'll null those out for this view for the mappings, but we'll keep that distinct in the raw data
    NULLIF(charge.Jurisdiction, "Undefined") AS Jurisdiction,
    -- Offense date is sometimes NULL, but only for sentences from before the ICON migration (pre 2003)
    -- Only 7 cases where OffenseDt is in 1899 and 9 cases where offense date is in the future and therefore get filtered out
    CASE WHEN DATE(charge.OffenseDt) < DATE(1900,1,2) OR DATE(charge.OffenseDt) > @update_timestamp THEN NULL
         ELSE DATE(charge.OffenseDt) 
         END AS OffenseDt,
    crime_codes.CrimeCd,
    crime_codes.OffenseDesc,
    crime_codes.CrimeCdClass,
    crime_codes.CrimeCdOffenseType,
    crime_codes.CrimeCdOffenseSubType,
    -- ChargeCount is sometimes NULL, but basically only for sentences from before the ICON migration (pre 2003)
    charge.ChargeCount,
    charge.MostSeriousCharge,
    charge.LeadCharge,
    sentence.JudgeFirstNm,
    sentence.JudgeLastNm,
    sentence.JudgeMiddleNm,
    sentence.JudgeLegalPeopleId,
  FROM {{IA_DOC_Penalties}} penalty
  LEFT JOIN {{IA_DOC_Sentences}} sentence USING(OffenderCd, SentenceId)
  LEFT JOIN {{IA_DOC_Charges}} charge USING(OffenderCd, ChargeId)
  LEFT JOIN {{IA_DOC_CrimeCodes}} crime_codes on ConvictingCrimeCdId = CrimeCdId
  INNER JOIN penalties_to_keep USING(PenaltyId, ChargeId)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ia",
    ingest_view_name="sentences",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
