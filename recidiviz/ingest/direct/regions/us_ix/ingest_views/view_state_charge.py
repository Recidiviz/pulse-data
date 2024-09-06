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
"""Query that generates the state charge entity"""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """WITH

-- NOTES ABOUT THIS VIEW:
-- Considering each offense sentence record as a charge record since: 
--   - multiple offenses can attach with one sentence order
--   - multiple orders can be attached to one charge
--   - a single offense can be attached to a single offense sentence record
-- All offense type records are "Conversions" records
-- OffenseClassId always null so not using that for subclassification
-- offense type has both a OffenseTypeDesc and a ShortDescription field that both contain the same description so 
    -- arbitrarily using OffenseTypeDesc

    
-- get a list of the child SentenceOrderId for each sentence order to determine later on if the child sentence order is an error correction
next_sentence_order AS (
    SELECT ParentSentenceOrderId AS current_SentenceOrderId,
        SentenceOrderEventTypeId AS next_SentenceOrderEventTypeId,
        Sequence AS next_Sequence,
        ChargeId AS next_ChargeId
    FROM {scl_SentenceOrder}
    WHERE ParentSentenceOrderId IS NOT NULL
    )
  SELECT 
    sent.OffenderId,
    sentoff.OffenseId,
    (DATE(off.OffenseDate)) as OffenseDate,
    off.Count, 
    offtyp.OffenseTypeDesc, 
    offtyp.Offense_statute, 
    offtyp.VIOLENT_OFFENSE_IND,
    ord.JudgeLegistId, 
    leg.FirstName, 
    leg.MiddleName, 
    leg.LastName, 
    ord.CountyId,
    sent.SentenceId,
    cotyp.ChargeOutcomeTypeDesc
FROM {scl_Sentence} sent
  LEFT JOIN {scl_SentenceLink} sentlink ON sent.SentenceId = sentlink.SentenceId
  LEFT JOIN {scl_SentenceLinkOffense} sentoff ON sentlink.SentenceLinkId = sentoff.SentenceLinkId
  LEFT JOIN {scl_Offense} off ON sentoff.OffenseId = off.OffenseId
  LEFT JOIN {scl_OffenseType} offtyp ON off.OffenseTypeId = offtyp.OffenseTypeId
  LEFT JOIN {scl_SentenceOrder} ord ON off.SentenceOrderId = ord.SentenceOrderId
  LEFT JOIN {scl_Legist} leg ON ord.JudgeLegistId = leg.LegistId
  LEFT JOIN {scl_Charge} chrg ON ord.ChargeId = chrg.ChargeId
  LEFT JOIN {scl_ChargeOutcomeType} cotyp ON cotyp.ChargeOutcomeTypeId = chrg.ChargeOutcomeTypeId
  LEFT JOIN next_sentence_order next ON off.SentenceOrderId = next.current_SentenceOrderId
WHERE sentlink.SentenceLinkClassId = '1' -- only look at offense sentence records
    AND sent.OffenderId IS NOT NULL
    AND off.OffenseId IS NOT NULL
    -- Make sure charge isn't part of an error correction sentence
    AND ((next_SentenceOrderEventTypeId <> '3' or next_SentenceOrderEventTypeId is null)
           OR next_Sequence <> Sequence
           OR next_ChargeId <> ord.ChargeId)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ix",
    ingest_view_name="state_charge",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
