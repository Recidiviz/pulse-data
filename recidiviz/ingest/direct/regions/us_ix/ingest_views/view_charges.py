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
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """

    -- NOTES ABOUT THIS VIEW:
    -- Considering each offense sentence record as a charge record since: 
    --   - multiple offenses can attach with one sentence order
    --   - multiple orders can be attached to one charge
    --   - a single offense can be attached to a single offense sentence record
    -- All offense type records are "Conversions" records
    -- OffenseClassId always null so not using that for subclassification
    -- offense type has both a OffenseTypeDesc and a ShortDescription field that both contain the same description so 
       -- arbitrarily using OffenseTypeDesc
    -- Charge status is always 531 (Active) or 535 (Satisfied) so I don't think we can use it for charge status, and current
       -- ID mapping uses 'present without info' #TODO(#17184): Revisit for scl_Charge.ChargeOutcomeTypeId if want more granular status

  SELECT 
    sent.OffenderId,
    sentoff.OffenseId,
    (DATE(off.OffenseDate)) as OffenseDate,
    off.OffenseTypeId,
    off.Count, 
    offtyp.OffenseTypeDesc, 
    offtyp.OffenseCategoryId, 
    offtyp.Offense_statute, 
    offtyp.VIOLENT_OFFENSE_IND,
    ord.JudgeLegistId, 
    leg.FirstName, 
    leg.MiddleName, 
    leg.LastName, 
    leg.NameSuffix,
    ord.CountyId,
    ord_type.SentenceOrderCategoryId,
    sentlink2.SentenceId as _sentence_external_id
FROM {scl_Sentence} sent
  LEFT JOIN {scl_SentenceLink} sentlink ON sent.SentenceId = sentlink.SentenceId
  LEFT JOIN {scl_SentenceLinkOffense} sentoff ON sentlink.SentenceLinkId = sentoff.SentenceLinkId
  LEFT JOIN {scl_Offense} off ON sentoff.OffenseId = off.OffenseId
  LEFT JOIN {scl_OffenseType} offtyp ON off.OffenseTypeId = offtyp.OffenseTypeId
  LEFT JOIN {scl_SentenceOrder} ord ON off.SentenceOrderId = ord.SentenceOrderId
  LEFT JOIN {scl_SentenceOrderType} ord_type ON ord.SentenceOrderTypeId = ord_type.SentenceOrderTypeId
  LEFT JOIN {scl_Legist} leg ON ord.JudgeLegistId = leg.LegistId
  -- 
  -- Charge records and sentence information are all stored in the same tables (scl_Sentence and scl_SentenceLink) but 
  -- denoted differently by SentenceLinkClassId and use different association tables (scl_SentenceLinkSentenceOrder 
  -- and scl_SentenceLinkOffense).  It broadly looks like this:
  -- 
  --                        scl_Sentence
  --                              |
  --                       scl_SentenceLink
  --                      |               |
  -- scl_SentenceLinkSentenceOrder     scl_SentenceLinkOffense
  --                       |                      | 
  --                scl_SentenceOrder  ---  scl_Offense
  -- 
  -- So here we're taking the SentenceOrder we have attached to the charge record and
  -- joining on scl_SentenceLinkSentenceOrder and scl_SentenceLink (again) 
  -- to go from SentenceOrder > SentenceOrder/SentenceLink > SentenceLink > Sentence in order to
  -- grab the SentenceId for the sentence record attached to the same sentence order 
  -- that this charge record is attached to.  In other words, from sent and sentlink we grab information
  -- about the charge, whereas from sentlink2 we grab information about the associated sentence.
  LEFT JOIN {scl_SentenceLinkSentenceOrder} linkorder ON ord.SentenceOrderId = linkorder.SentenceOrderId
  LEFT JOIN {scl_SentenceLink} sentlink2 ON sentlink2.SentenceLinkId = linkorder.SentenceLinkId
WHERE sentlink.SentenceLinkClassId = '1' -- only look at offense sentence records
    AND sent.OffenderId IS NOT NULL
    AND off.OffenseId IS NOT NULL
    -- Are we concerned that non-conversion offense types will be populated differently? 
    -- If so, we might want to include a filter here to just include conversion records
    -- and upper(Comments) like '%CONVERSION%' #TODO(#17186): Revisit in 01/2023 to test with non-conversion data
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_ix",
    ingest_view_name="charges",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="OffenderId, OffenseId",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
