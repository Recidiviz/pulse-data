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
"""Query for discharge_from_incarceration task deadline."""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
    WITH 
    term_base AS (
        SELECT
            TermId,
            -- Coalesce FtrdApprovedDate to string 'NULL' if NULL so that later on we can distinguish instances where FTRD was NULLed out in the raw data
            COALESCE(FtrdApprovedDate, 'NULL') AS FtrdApprovedDate,
            OffenderId,
            update_datetime,
            LAG(COALESCE(FtrdApprovedDate, 'NULL')) OVER (PARTITION BY TermId ORDER BY update_datetime) AS PREV_FtrdApprovedDate,
            LAG(TermId) OVER (PARTITION BY TermId ORDER BY update_datetime) AS PREV_TermId,        
        FROM {scl_Term@ALL} 
    ),

    termOrderDates AS (
        SELECT
            OffenderId,
            TermId,
            FtrdApprovedDate,
            update_datetime
        FROM term_base
        WHERE FtrdApprovedDate != PREV_FtrdApprovedDate 
            OR (PREV_FtrdApprovedDate IS NULL and FtrdApprovedDate IS NOT NULL)
            OR (FtrdApprovedDate IS NULL and PREV_TermId IS NULL)
    ),

    sentences_base AS (
    -- Gather critical info about each relevant sentence which we will be joining to the @ALL tables later
    -- This returns one row per SentenceId - updatedatetime.
        SELECT
            sent.SentenceId,
            sent.OffenderId,
            sent.TermId,
            ord.CorrectionsCompactEndDate,
            ord.update_datetime
        FROM {scl_Sentence} sent
            LEFT JOIN {scl_SentenceLink} link 
                ON sent.SentenceId = link.SentenceId
            LEFT JOIN {scl_SentenceLinkOffense} linkoffense 
                ON link.SentenceLinkId = linkoffense.SentenceLinkId
            LEFT JOIN {scl_Offense} off 
                ON linkoffense.OffenseId = off.OffenseId
            LEFT JOIN {scl_SentenceOrder@ALL} ord 
                ON off.SentenceOrderId = ord.SentenceOrderId
            LEFT JOIN {scl_SentenceOrderType} ord_type 
                ON ord.SentenceOrderTypeId = ord_type.SentenceOrderTypeId
        WHERE ord_type.SentenceOrderCategoryId = '2'
            AND ord.SentenceOrderEventTypeId IN ('1', '2', '3', '5')
            AND link.SentenceLinkClassId = '1'
   ),

    lag_cte AS (
        SELECT
            SentenceId,
            OffenderId,
            TermId,
            -- Coalesce CorrectionsCompactEndDate to string 'NULL' if NULL so that later on we can distinguish instances where CorrectionsCompactEndDate was NULLed out in the raw data
            COALESCE(CorrectionsCompactEndDate, 'NULL') AS CorrectionsCompactEndDate,
            update_datetime,
            LAG(COALESCE(CorrectionsCompactEndDate, 'NULL')) OVER (PARTITION BY SentenceId ORDER BY update_datetime) AS PREV_CorrectionsCompactEndDate,
            -- Using lag on SentenceId to determine if it's the first row
            LAG(SentenceId) OVER (PARTITION BY SentenceID ORDER BY update_datetime) AS PREV_SentenceId
        FROM sentences_base
    ),

    SentenceOrderDates AS (
        SELECT 
            OffenderId,
            TermId,
            SentenceId,
            CorrectionsCompactEndDate,
            update_datetime
        FROM lag_cte
        WHERE CorrectionsCompactEndDate != PREV_CorrectionsCompactEndDate 
            OR (PREV_CorrectionsCompactEndDate IS NULL AND CorrectionsCompactEndDate IS NOT NULL)
            OR (CorrectionsCompactEndDate IS NULL AND PREV_SentenceId IS NULL)
    ),

    rows_with_eligible AS
    (
       SELECT DISTINCT
            t1.OffenderId,
            t1.TermId,
            t1.CorrectionsCompactEndDate,
            t1.SentenceId,
            CAST(NULL AS STRING) as FTRDApprovedDate,
            t1.update_datetime
        FROM 
            SentenceOrderDates t1

        UNION ALL

        SELECT DISTINCT
            t2.OffenderId,
            t2.TermId,
            CAST(NULL AS STRING) as CorrectionsCompactEndDate,
            s.SentenceId,
            t2.FTRDApprovedDate,
            t2.update_datetime
        FROM 
            termOrderDates t2
        LEFT JOIN sentences_base s 
            ON s.TermId = t2.TermId
    ),
    
    -- collapse so that there's only one row per update_datetime
    collapsed_by_update_datetime AS 
    (
        SELECT 
            OffenderId,
            TermId,
            SentenceId,
            update_datetime,
            MAX(CorrectionsCompactEndDate) AS CorrectionsCompactEndDate,
            MAX(FTRDApprovedDate) AS FTRDApprovedDate
        FROM rows_with_eligible
        GROUP BY 1,2,3,4
    ),

    last_val_cte AS (
        SELECT
            OffenderId,
            TermId,
            SentenceId,
            CorrectionsCompactEndDate,
            FtrdApprovedDate,
            LAST_VALUE(CorrectionsCompactEndDate IGNORE NULLS) OVER (PARTITION BY TermId, SentenceId ORDER BY update_datetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sentence_eligible_date,
            LAST_VALUE(FtrdApprovedDate IGNORE NULLS) OVER (PARTITION BY TermId, SentenceId ORDER BY update_datetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS term_eligible_date,
            update_datetime
        FROM collapsed_by_update_datetime
        WHERE update_datetime IS NOT NULL
        ORDER BY TermId, SentenceId
    )

    SELECT DISTINCT
       lvc.OffenderId,
       lvc.SentenceId,
       COALESCE(
            NULLIF(lvc.term_eligible_date, 'NULL'),
            NULLIF(lvc.sentence_eligible_date, 'NULL')
       ) AS eligible_date,
       lvc.update_datetime
    FROM last_val_cte lvc
    WHERE SentenceId IS NOT NULL
    """
VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ix",
    ingest_view_name="discharge_from_incarceration_deadline",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
