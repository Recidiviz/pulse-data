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
"""Query for state sentence groups."""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH
-- Get term level information and get previous rows to filter down to row with changes
filter_term AS (
    SELECT 
        OffenderId,
        TermId,
        FtrdApprovedDate,
        -- There are 6 edge cases where the FTRD is before the tentative parole date as of 09/2024
        CASE WHEN
            DATE(TentativeParoleDate) <= DATE(FtrdApprovedDate)
            THEN TentativeParoleDate
            ELSE NULL
        END AS TentativeParoleDate,
        InitialParoleHearingDate,
        update_datetime,
        LAG(FtrdApprovedDate) OVER wind AS prev_FtrdApprovedDate,
        LAG(TentativeParoleDate) OVER wind AS prev_TentativeParoleDate,
        LAG(InitialParoleHearingDate) OVER wind AS prev_InitialParoleHearingDate,
        LAG(update_datetime) OVER wind AS prev_updt
    FROM {scl_Term@ALL} term
    -- Make sure all sentence groups created have a corresponding sentence that is an offense sentence 
    WHERE TermId IN (
        SELECT 
            TermId 
        FROM {scl_Sentence} sent 
        LEFT JOIN {scl_SentenceLink} link ON sent.SentenceId = link.SentenceId 
        WHERE link.SentenceLinkClassId = '1')
    WINDOW wind AS (PARTITION BY TermId ORDER BY update_datetime asc)
),

-- Filter down to rows where there were changes in the term
filtered_term AS
(
    SELECT 
        OffenderId,
        TermId,
        FtrdApprovedDate,
        TentativeParoleDate,
        InitialParoleHearingDate,
        update_datetime
    FROM filter_term 
    WHERE prev_updt IS NULL 
    OR FtrdApprovedDate IS DISTINCT FROM prev_FtrdApprovedDate 
    OR TentativeParoleDate IS DISTINCT FROM prev_TentativeParoleDate 
    OR InitialParoleHearingDate IS DISTINCT FROM prev_InitialParoleHearingDate
),

-- get a list of the child SentenceOrderId for each sentence order to determine later on if the child sentence order is an error correction
next_sentence_order AS (
    SELECT 
        ParentSentenceOrderId AS current_SentenceOrderId,
        SentenceOrderEventTypeId AS next_SentenceOrderEventTypeId,
        Sequence AS next_Sequence,
        ChargeId AS next_ChargeId
    FROM {scl_SentenceOrder}
    WHERE ParentSentenceOrderId is not null
),

-- We exclude empty (not having an imposed date) sentences from being created 
-- so we make sure that we also filter out the terms that are only connected to empty sentences
non_empty_terms_cte AS (
    SELECT 
        sent.TermId
    FROM {scl_Sentence} sent
    LEFT JOIN {scl_SentenceLink} link ON sent.SentenceId = link.SentenceId
    LEFT JOIN {scl_SentenceLinkOffense} linkoffense on link.SentenceLinkId = linkoffense.SentenceLinkId
    LEFT JOIN {scl_Offense} off ON linkoffense.OffenseId = off.OffenseId
    LEFT JOIN {scl_SentenceOrder} ord ON off.SentenceOrderId = ord.SentenceOrderId
    LEFT JOIN next_sentence_order next ON ord.SentenceOrderId = next.current_SentenceOrderId
    WHERE (SentenceDate IS NOT NULL OR CorrectionsCompactStartDate IS NOT NULL)
    -- we want to keep only if the next child sentence order of this current sentence isn't an error correction
    -- sentence order with the same ChargeId and Sequence
    -- i.e. we want to exclude all rows where (next_SentenceOrderEventTypeId = '3' and next_Sequence = Sequence and next_ChargeId = ChargeId)   
    AND ((next_SentenceOrderEventTypeId <> '3' or next_SentenceOrderEventTypeId is null)
        OR next_Sequence <> Sequence
        OR next_ChargeId <> ChargeId)  
)

-- Final SELECT to get the rows ordered by update_datetime
SELECT
    TermId,
    OffenderId,
    FtrdApprovedDate,
    TentativeParoleDate,
    update_datetime,
    InitialParoleHearingDate,
    ROW_NUMBER() OVER(PARTITION BY TermId ORDER BY update_datetime) as rn
FROM
    filtered_term
WHERE TermId IN (SELECT * FROM non_empty_terms_cte)
"""
VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ix",
    ingest_view_name="state_sentence_group",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
