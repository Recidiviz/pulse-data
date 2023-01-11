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
    SELECT  
        sent.SentenceId, 
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
        parent_link.SentenceId as _ParentSentenceId
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
    WHERE link.SentenceLinkClassId = '3'  -- keep only "Sentence" sentences (as opposed to "Offense" sentences)
        AND sent.OffenderId IS NOT NULL
"""


def sentence_view_template() -> str:
    return SENTENCE_QUERY_TEMPLATE
