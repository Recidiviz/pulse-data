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
            sent.SentenceId as offense_SentenceId, 
            sent.OffenderId,
            sent.SentenceStatusId, 
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
            event_ref.SentenceOrderEventTypeName,
            ord.CorrectionsCompactEndDate,
            detail.SegmentMaxYears,
            detail.SegmentMaxMonths,
            detail.SegmentMaxDays,
            detail.SegmentPED,
            detail.SegmentSatisfactionDate,
            detail.SegmentStartDate,
            detail.SegmentEndDate,
            detail.SegmentYears,
            detail.SegmentMonths,
            detail.SegmentDays,
            detail.OffenseSentenceTypeId,
            sentence_order_link.SentenceId as sentence_SentenceId,
            off.OffenseSortingOrder
        FROM {scl_Sentence} sent
            LEFT JOIN {scl_SentenceLink} link ON sent.SentenceId = link.SentenceId
            LEFT JOIN {scl_SentenceLinkOffense} linkoffense on link.SentenceLinkId = linkoffense.SentenceLinkId
            LEFT JOIN {scl_Offense} off ON linkoffense.OffenseId = off.OffenseId
            LEFT JOIN {scl_SentenceOrder} ord ON off.SentenceOrderId = ord.SentenceOrderId
            LEFT JOIN {scl_SentenceOrderType} ord_type ON ord.SentenceOrderTypeId = ord_type.SentenceOrderTypeId
            LEFT JOIN {scl_Term} term ON sent.TermId = term.TermId
            LEFT JOIN {scl_SentenceOrderEventType} event_ref on ord.SentenceOrderEventTypeId = event_ref.SentenceOrderEventTypeId
            LEFT JOIN {scl_SentenceDetail} detail on sent.SentenceId = detail.SentenceId
            LEFT JOIN {scl_SentenceLinkSentenceOrder} linkorder ON ord.SentenceOrderId = linkorder.SentenceOrderId
            LEFT JOIN {scl_SentenceLink} sentence_order_link ON linkorder.SentenceLinkId = sentence_order_link.SentenceLinkId
        WHERE link.SentenceLinkClassId = '1' -- keep only "Offense" sentences (as opposed to "Sentence Order" sentences)
            AND sent.OffenderId IS NOT NULL
        ),
    RelatedSentence as (
        -- Makes a list of sentence relationships by SentenceId
        --
        -- In the scl_RelatedSentence table, each relationship is described by the relationship type, the OriginSentenceId, and theTargetSentenceId
        -- the OriginSentenceId and the TargetSentenceId could (annoying and inexplicably) be either a "Sentence [Order]" SentenceId or an "Offense" SentenceId.
        -- For our purposes, we want the sentence relationships in terms of "Offense" SentenceIds.  So here we'll translate the OriginSentenceId/TargetSentenceId
        -- into the "Offense" SentenceIds if it isn't already by merging SentenceBase on by sentence_SentenceId and seeing if there's a match.  Since both types
        -- of SentenceId come from the same table, there should only be a match if the given OriginSentenceId/TargetSentenceId is an "Sentence [Order]" SentenceId.
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
                COALESCE(b_origin_1.offense_SentenceId, rel.OriginSentenceId) as SentenceId,
                ref.SentenceRelationshipDesc,
                COALESCE(b_target_1.offense_SentenceId, rel.TargetSentenceId) as TargetSentenceId
            FROM {scl_RelatedSentence} rel
            LEFT JOIN {scl_SentenceRelationship} ref on rel.SentenceRelationshipId = ref.SentenceRelationshipId
            LEFT JOIN SentenceBase b_origin_1 on rel.OriginSentenceId = b_origin_1.sentence_SentenceId
            LEFT JOIN SentenceBase b_target_1 on rel.TargetSentenceId = b_target_1.sentence_SentenceId
            LEFT JOIN {scl_SentenceLink} link ON COALESCE(b_origin_1.offense_SentenceId, rel.OriginSentenceId) = link.SentenceId
            WHERE COALESCE(b_origin_1.offense_SentenceId, rel.OriginSentenceId) <> COALESCE(b_target_1.offense_SentenceId, rel.TargetSentenceId)
            AND link.SentenceLinkClassId = '1'
        ) sub
        GROUP BY SentenceId
    ),
    -- get a list of the child SentenceOrderId for each sentence order to determine later on if the child sentence order is an error correction
    next_sentence_order AS (
        select ParentSentenceOrderId AS current_SentenceOrderId,
            SentenceOrderEventTypeId AS next_SentenceOrderEventTypeId,
            Sequence AS next_Sequence,
            ChargeId AS next_ChargeId
        from {scl_SentenceOrder}
        where ParentSentenceOrderId is not null
    ),
    final_sentences AS (
        SELECT 
            DISTINCT
            sent.offense_SentenceId as SentenceId,
            sent.OffenderId, 
            sent.SentenceStatusId, 
            sent.CountyId,
            (DATE(sent.EffectiveDate)) as EffectiveDate, 
            (DATE(sent.SentenceDate)) as SentenceDate,
            (DATE(sent.DpedApprovedDate)) as DpedApprovedDate,
            (DATE(sent.FtrdApprovedDate)) as FtrdApprovedDate,
            sent.SentenceOrderEventTypeName,
            sent.Sequence,
            sent.ChargeId,
            rel.relationships as relationships,
            (DATE(sent.CorrectionsCompactEndDate)) as CorrectionsCompactEndDate,
            sent.SentenceOrderCategoryId,
            sent.SentenceOrderEventTypeId,
            sent.SentenceOrderId,
            sent.SentenceOrderTypeCode,
            sent.SegmentMaxYears,
            sent.SegmentMaxMonths,
            sent.SegmentMaxDays,
            (DATE(sent.SegmentPED)) as SegmentPED,
            (DATE(sent.SegmentSatisfactionDate)) as SegmentSatisfactionDate,
            (DATE(sent.SegmentStartDate)) as SegmentStartDate,
            (DATE(sent.SegmentEndDate)) as SegmentEndDate,
            sent.SegmentYears,
            sent.SegmentMonths,
            sent.SegmentDays,
            sent.OffenseSentenceTypeId,
            sent.OffenseSortingOrder
        FROM SentenceBase sent
        LEFT JOIN RelatedSentence rel ON sent.offense_SentenceId = rel.SentenceId
        LEFT JOIN next_sentence_order next ON sent.SentenceOrderId = next.current_SentenceOrderId
        -- we want to only keep only if the next child sentence order of this corrent sentence isn't an error correction
        -- sentence order with the same ChargeId and Sequence
        -- i.e. we want to exclude all rows where (next_SentenceOrderEventTypeId = '3' and next_Sequence = Sequence and next_ChargeId = ChargeId)
        WHERE (next_SentenceOrderEventTypeId <> '3' or next_SentenceOrderEventTypeId is null)
           OR next_Sequence <> Sequence
           OR next_ChargeId <> ChargeId    
    )
"""

NEW_SENTENCE_QUERY_TEMPLATE = """
    -- Links all "Sentence" sentences with their associated sentence orders and then any "Offense" sentences associated with the same sentence order
    -- Here, the relationship between sentence_SentenceId <> offense_SentenceId is 1 <> many
    SentenceBase AS (
        SELECT  
            sent.SentenceId AS offense_SentenceId, 
            sent.OffenderId,
            sent.TermId, 
            ord.SentenceOrderId,
            ord.CountyId,
            ord.SentenceDate,
            ord.CorrectionsCompactStartDate,
            ord.Sequence,
            ord.ChargeId,
            ord_type.SentenceOrderCategoryId,
            ord.SentenceOrderEventTypeId,
            ord.CorrectionsCompactEndDate,
            sentence_order_link.SentenceId AS sentence_SentenceId,
            off.OffenseSortingOrder,
            sent_type.OffenseSentenceTypeName,
            loc.LocationName AS inState
        FROM {scl_Sentence} sent
            LEFT JOIN {scl_SentenceLink} link ON sent.SentenceId = link.SentenceId
            LEFT JOIN {scl_SentenceLinkOffense} linkoffense on link.SentenceLinkId = linkoffense.SentenceLinkId
            LEFT JOIN {scl_Offense} off ON linkoffense.OffenseId = off.OffenseId
            LEFT JOIN {scl_SentenceOrder} ord ON off.SentenceOrderId = ord.SentenceOrderId
            LEFT JOIN {scl_SentenceOrderType} ord_type ON ord.SentenceOrderTypeId = ord_type.SentenceOrderTypeId
            LEFT JOIN {scl_Term} term ON sent.TermId = term.TermId
            LEFT JOIN {scl_SentenceDetail} detail on sent.SentenceId = detail.SentenceId
            LEFT JOIN {scl_SentenceLinkSentenceOrder} linkorder ON ord.SentenceOrderId = linkorder.SentenceOrderId
            LEFT JOIN {scl_SentenceLink} sentence_order_link ON linkorder.SentenceLinkId = sentence_order_link.SentenceLinkId
            LEFT JOIN {scl_OffenseSentenceType} sent_type ON detail.OffenseSentenceTypeId = sent_type.OffenseSentenceTypeId
            LEFT JOIN {ref_Location} loc ON ord.StateId = loc.LocationId
        WHERE link.SentenceLinkClassId = '1' -- keep only "Offense" sentences (AS opposed to "Sentence Order" sentences)
            AND sent.OffenderId IS NOT NULL
        ),

    -- Makes a list of sentence relationships by SentenceId
    --
    -- In the scl_RelatedSentence table, each relationship is described by the relationship type, the OriginSentenceId, and theTargetSentenceId
    -- the OriginSentenceId and the TargetSentenceId could (annoying and inexplicably) be either a "Sentence [Order]" SentenceId or an "Offense" SentenceId.
    -- For our purposes, we want the sentence relationships in terms of "Offense" SentenceIds.  So here we'll translate the OriginSentenceId/TargetSentenceId
    -- into the "Offense" SentenceIds if it isn't already by merging SentenceBase on by sentence_SentenceId and seeing if there's a match.  Since both types
    -- of SentenceId come from the same table, there should only be a match if the given OriginSentenceId/TargetSentenceId is an "Sentence [Order]" SentenceId.
    -- 
    -- In addition, the scl_RelatedSentence table also includes "Independent" relationships where OriginSentenceId = TargetSentenceId.  Since that doesn't 
    -- really tell us anything, we'll filter those out. 
    -- 
    -- Lastly, because a sentence could have relationships (e.g. concurrent) with multiple other sentences, we'll group to one row per sentence
    RelatedSentence AS (
        SELECT 
            SentenceId,
            STRING_AGG(distinct TargetSentenceId, ',') AS relationships
        FROM (
            SELECT
                COALESCE(b_origin_1.offense_SentenceId, rel.OriginSentenceId) AS SentenceId,
                ref.SentenceRelationshipDesc,
                COALESCE(b_target_1.offense_SentenceId, rel.TargetSentenceId) AS TargetSentenceId
            FROM {scl_RelatedSentence} rel
            LEFT JOIN {scl_SentenceRelationship} ref on rel.SentenceRelationshipId = ref.SentenceRelationshipId
            LEFT JOIN SentenceBase b_origin_1 on rel.OriginSentenceId = b_origin_1.sentence_SentenceId
            LEFT JOIN SentenceBase b_target_1 on rel.TargetSentenceId = b_target_1.sentence_SentenceId
            LEFT JOIN {scl_SentenceLink} link ON COALESCE(b_origin_1.offense_SentenceId, rel.OriginSentenceId) = link.SentenceId
            WHERE COALESCE(b_origin_1.offense_SentenceId, rel.OriginSentenceId) <> COALESCE(b_target_1.offense_SentenceId, rel.TargetSentenceId)
            AND link.SentenceLinkClassId = '1'
        ) sub
        WHERE SentenceRelationshipDesc = "Consecutive"
        GROUP BY SentenceId
    ),
    -- get a list of the child SentenceOrderId for each sentence order to determine later on if the child sentence order is an error correction
    next_sentence_order AS (
        select ParentSentenceOrderId AS current_SentenceOrderId,
            SentenceOrderEventTypeId AS next_SentenceOrderEventTypeId,
            Sequence AS next_Sequence,
            ChargeId AS next_ChargeId
        from {scl_SentenceOrder}
        where ParentSentenceOrderId is not null
    ),
    -- In final_sentences, we left join the RelatedSentences and next_sentence_order to
    -- make sure that we only non-error correction sentence
    final_sentences AS (
        SELECT 
            DISTINCT
            sent.offense_SentenceId AS SentenceId,
            sent.OffenderId, 
            sent.CountyId,
            (DATE(sent.SentenceDate)) AS SentenceDate,
            rel.relationships AS relationships,
            (DATE(sent.CorrectionsCompactStartDate)) AS CorrectionsCompactStartDate,
            (DATE(sent.CorrectionsCompactEndDate)) AS CorrectionsCompactEndDate,
            sent.SentenceOrderCategoryId,
            sent.SentenceOrderEventTypeId,
            sent.OffenseSentenceTypeName,
            sent.TermId AS SentenceGroupId,
            inState
        FROM SentenceBase sent
        LEFT JOIN RelatedSentence rel ON sent.offense_SentenceId = rel.SentenceId
        LEFT JOIN next_sentence_order next ON sent.SentenceOrderId = next.current_SentenceOrderId
        -- we want to only keep only if the next child sentence order of this corrent sentence isn't an error correction
        -- sentence order with the same ChargeId and Sequence
        -- i.e. we want to exclude all rows where (next_SentenceOrderEventTypeId = '3' and next_Sequence = Sequence and next_ChargeId = ChargeId)
        WHERE (next_SentenceOrderEventTypeId <> '3' or next_SentenceOrderEventTypeId is null)
           OR next_Sequence <> Sequence
           OR next_ChargeId <> ChargeId    
    )
"""


def sentence_view_template() -> str:
    return SENTENCE_QUERY_TEMPLATE


def new_sentence_view_template() -> str:
    return NEW_SENTENCE_QUERY_TEMPLATE
