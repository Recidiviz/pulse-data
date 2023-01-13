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
"""Helper templates for the US_IX sentence queries."""

SENTENCE_QUERY_TEMPLATE = """
    SentenceBase AS (
        -- Links all "Sentence" sentences with their associated sentence orders and then any "Offense" sentences associated with the same sentence order
        -- Here, the relationship between sentence_SentenceId <> offense_SentenceId is 1 <> many
        SELECT  
            sent.SentenceId as sentence_SentenceId, 
            sent.OffenderId, 
            ord.SentenceOrderId,
            ord.CountyId,
            ord.EffectiveDate, 
            ord.SentenceDate,
            ord.Sequence,
            ord.ChargeId,
            ord_type.SentenceOrderTypeCode,
            ord_type.SentenceOrderCategoryId,
            ord.SentenceOrderEventTypeId,
            term.DpedApprovedDate,
            term.FtrdApprovedDate,
            term_status.TermStatusDesc,
            event_ref.SentenceOrderEventTypeName,
            parent_link.SentenceId as _ParentSentenceId,
            off.OffenseId,
            off_link.SentenceId as offense_SentenceId,
            ord.CorrectionsCompactEndDate
        FROM {scl_Sentence} sent
            LEFT JOIN {scl_SentenceLink} link ON sent.SentenceId = link.SentenceId
            LEFT JOIN {scl_SentenceLinkSentenceOrder} linkorder ON link.SentenceLinkId = linkorder.SentenceLinkId
            LEFT JOIN {scl_SentenceOrder} ord ON linkorder.SentenceOrderId = ord.SentenceOrderId
            LEFT JOIN {scl_SentenceOrderType} ord_type ON ord.SentenceOrderTypeId = ord_type.SentenceOrderTypeId
            LEFT JOIN {scl_Term} term ON sent.TermId = term.TermId
            LEFT JOIN {scl_TermStatus} term_status ON term.TermStatusId = term_status.TermStatusId
            LEFT JOIN {scl_SentenceOrderEventType} event_ref on ord.SentenceOrderEventTypeId = event_ref.SentenceOrderEventTypeId
            LEFT JOIN {scl_SentenceLinkSentenceOrder} parent_order ON ord.ParentSentenceOrderId = parent_order.SentenceOrderId
            LEFT JOIN {scl_SentenceLink} parent_link ON parent_order.SentenceLinkId = parent_link.SentenceLinkId
            LEFT JOIN {scl_Offense} off on ord.SentenceOrderId = off.SentenceOrderId
            LEFT JOIN {scl_SentenceLinkOffense} linkoffense on off.OffenseId = linkoffense.OffenseId
            LEFT JOIN {scl_SentenceLink} off_link on linkoffense.SentenceLinkId = off_link.SentenceLinkId
        WHERE link.SentenceLinkClassId = '3' -- keep only "Sentence" sentences (as opposed to "Offense" sentences)
            AND sent.OffenderId IS NOT NULL
        ),
    RelatedSentence as (
        -- Makes a list of sentence relationships by SentenceId
        --
        -- In the scl_RelatedSentence table, each relationship is described by the relationship type, the OriginSentenceId, and theTargetSentenceId
        -- the OriginSentenceId and the TargetSentenceId could (annoying and inexplicably) be either a "Sentence [Order]" SentenceId or an "Offense" SentenceId.
        -- For our purposes, we want the sentence relationships in terms of "Sentence" SentenceIds.  So here we'll translate the OriginSentenceId/TargetSentenceId
        -- into the "Sentence" SentenceIds if it isn't already by merging SentenceBase on by offense_SentenceId and seeing if there's a match.  Since both types
        -- of SentenceId come from the same table, there should only be a match if the given OriginSentenceId/TargetSentenceId is an "Offense" SentenceId.
        -- 
        -- In addition, the scl_RelatedSentence table also includes "Independent" relationships where OriginSentenceId = TargetSentenceId.  Since that doesn't 
        -- really tell us anything, we'll filter those out. 
        -- 
        -- Lastly, because a sentence could have relationships (e.g. concurrent) with multiple other sentences, we'll group to one row per sentence
        SELECT 
            SentenceId,
            STRING_AGG(distinct CONCAT(SentenceRelationshipDesc, '_', TargetSentenceId), ',' ORDER BY CONCAT(SentenceRelationshipDesc, '_', TargetSentenceId)) as relationships
        FROM (
            SELECT
                COALESCE(b_origin_1.sentence_SentenceId, rel.OriginSentenceId) as SentenceId,
                ref.SentenceRelationshipDesc,
                COALESCE(b_target_1.sentence_SentenceId, rel.TargetSentenceId) as TargetSentenceId
            FROM {scl_RelatedSentence} rel
            LEFT JOIN {scl_SentenceRelationship} ref on rel.SentenceRelationshipId = ref.SentenceRelationshipId
            LEFT JOIN SentenceBase b_origin_1 on rel.OriginSentenceId = b_origin_1.offense_SentenceId
            LEFT JOIN SentenceBase b_target_1 on rel.TargetSentenceId = b_target_1.offense_SentenceId
            WHERE COALESCE(b_origin_1.sentence_SentenceId, rel.OriginSentenceId) <> COALESCE(b_target_1.sentence_SentenceId, rel.TargetSentenceId)
        ) sub
        GROUP BY SentenceId
    ),
    final_sentences as (
        SELECT 
            DISTINCT
            sent.sentence_SentenceId as SentenceId,
            sent.OffenderId, 
            sent.CountyId,
            (DATE(sent.EffectiveDate)) as EffectiveDate, 
            (DATE(sent.SentenceDate)) as SentenceDate,
            (DATE(sent.DpedApprovedDate)) as DpedApprovedDate,
            (DATE(sent.FtrdApprovedDate)) as FtrdApprovedDate,
            sent.TermStatusDesc, 
            sent.SentenceOrderEventTypeName,
            sent._parentsentenceid,
            sent.Sequence,
            sent.ChargeId,
            rel.relationships as relationships,
            (DATE(sent.CorrectionsCompactEndDate)) as CorrectionsCompactEndDate,
            sent.SentenceOrderCategoryId,
            sent.SentenceOrderEventTypeId,
            sent.SentenceOrderId,
            sent.SentenceOrderTypeCode
        FROM SentenceBase sent
        LEFT JOIN RelatedSentence rel ON sent.sentence_SentenceId = rel.SentenceId
    )
"""


def sentence_view_template() -> str:
    return SENTENCE_QUERY_TEMPLATE
