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
"""
Query containing parole term task deadline information.
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """

WITH
-- This CTE grabs the projected parole term end date from the current Release info table for each parole term
tasks_from_current AS (
    SELECT 
        ParoleNumber, 
        ParoleCountId, 
        -- there are a small number of RelMaxDate that are null.  We'll ingest these null
        -- dates as 1111-1-1 for now to null out later (we can't just keep them as NULLs 
        -- since we use a LAST_VALUE later that ignores nulls)
        COALESCE(
            NULLIF(
                GREATEST(
                    COALESCE(SAFE.PARSE_DATE('%Y%m%d', RelMaxDate), DATE(1111,1,1)),
                    COALESCE(SAFE.PARSE_DATE('%Y%m%d', RelMaxDate1), DATE(1111,1,1)),
                    COALESCE(SAFE.PARSE_DATE('%Y%m%d', RelMaxDate2), DATE(1111,1,1)),
                    COALESCE(SAFE.PARSE_DATE('%Y%m%d', RelMaxDate3), DATE(1111,1,1))
                ),
                DATE(1111,1,1)
            ),
            DATE(1111,1,1)
        ) AS RelMaxDate,
        update_datetime, 
    FROM {dbo_ReleaseInfo@ALL} rel
),
-- This CTE grabs the final parole term end date from the archived Release history table for each parole term.
-- We exclude history records with delete code 51 since those are records that were closed in error
tasks_from_hist AS (
    SELECT
        ParoleNumber, 
        ParoleCountId, 
        -- there are many rows set to LIFE/INDE, 144 rows set to some other non-parseable string as of 5/1/2024
        -- we'll ingest the life sentence dates as the magic date, and we'll ingest the unparseable HReMaxDate as 1111-1-1 for now to null out later
        -- (we can't just keep them as NULLs since we use a LAST_VALUE later that ignores nulls)
        CASE 
            WHEN HReMaxDate = 'LIFE' OR HReMaxDate LIKE '%INDE%'
                THEN DATE(2999, 12, 31)
            ELSE
                COALESCE(
                    NULLIF(
                        GREATEST(
                            COALESCE(SAFE.PARSE_DATE('%Y%m%d', HReMaxDate), DATE(1111,1,1)),
                            COALESCE(SAFE.PARSE_DATE('%Y%m%d', HReMaxa), DATE(1111,1,1)), 
                            COALESCE(SAFE.PARSE_DATE('%Y%m%d', HReMaxb), DATE(1111,1,1)), 
                            COALESCE(SAFE.PARSE_DATE('%Y%m%d', HReMaxc), DATE(1111,1,1))
                        ),
                        DATE(1111,1,1)
                    ),
                    DATE(1111,1,1) 
                )
            END AS HReMaxDate,
        update_datetime
    FROM {dbo_Hist_Release@ALL}
    WHERE HReDelCode <> '51'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY ParoleNumber, ParoleCountId, update_datetime ORDER BY CAST(HReleaseId AS INT64) DESC) = 1
),
-- This CTE unions the current data and the archived historical table, 
-- prioritizing the most recent date from the archived historical table if there is both a date from the current and historical table,
-- and then orders them to calculate prev_RelMaxDate to be used for deduplicating in the last step
combined AS (
    SELECT *,
        LAG(RelMaxDate) OVER(PARTITION BY ParoleNumber, ParoleCountId ORDER BY update_datetime) as prev_RelMaxDate
    FROM (
        SELECT
            ParoleNumber,
            ParoleCountId,
            -- when RelMaxDate shows up for an update datetime, but the ParoleNumber/ParoleCountId has already showed up in the archived
            -- table, use the date from the archived table
            COALESCE(most_recent_HReMaxDate, RelMaxDate) as RelMaxDate,
            update_datetime
            FROM (
                SELECT
                    ParoleNumber,
                    ParoleCountId,
                    RelMaxDate,
                    -- keep track of the most recent value of HReMaxDate that has appeard in the archive table
                    LAST_VALUE(HReMaxDate IGNORE NULLS) OVER(PARTITION BY ParoleNumber, ParoleCountId ORDER BY update_datetime) as most_recent_HReMaxDate,
                    update_datetime
                FROM tasks_from_current curr
                FULL OUTER JOIN tasks_from_hist hist USING(ParoleNumber, ParoleCountId, update_datetime)
            )
    )
),

-- This cte compiles parole count ids that were opened in error that we should exclude
delete_code_50 AS (
    SELECT ParoleNumber, ParoleCountId, True as deleted
    FROM (
        SELECT
            ParoleNumber,
            ParoleCountId,
            HReDelCode
        FROM {dbo_Hist_Release@ALL} hist 
        QUALIFY ROW_NUMBER() OVER(PARTITION BY ParoleNumber, ParoleCountId ORDER BY update_datetime DESC, CAST(HReleaseId AS INT64) DESC) = 1
    )
    WHERE HReDelCode = '50'
),

-- This CTE grabs the sentence level information (since the above CTEs were all at the parole term level)
-- so that we can joni on sentence external id in the last step
dbo_Sentence_deduped AS (
SELECT
    ParoleNumber,
    ParoleCountId,
    Sent16DGroupNumber,
    SentenceId,
    -- only 3 unparseable dates as of Aug 2024
    SAFE.PARSE_DATE('%Y%m%d', CONCAT(SentYear, SentMonth, SentDay)) as SentDate,
FROM {dbo_Sentence} 
QUALIFY 
    ROW_NUMBER() 
    OVER(
            PARTITION BY 
            ParoleNumber, ParoleCountId, SentTerm, SentYear, SentMonth, SentDay, SentOffense, SentCounty, sentCodeSentOffense
            ORDER BY 
            CAST(Sent16DGroupNumber AS INT64) DESC, CAST(SentenceId AS INT64) DESC
        ) = 1
)

SELECT
    combined.ParoleNumber,
    combined.ParoleCountId,
    Sent16DGroupNumber,
    SentenceId,
    -- reset '1111-1-1' (from unparseable dates) and '2999-12-31' (from life sentences) to NULL
    NULLIF(NULLIF(RelMaxDate, DATE(1111,1,1)), DATE(2999, 12, 31)) AS RelMaxDate,
    update_datetime,
FROM combined
LEFT JOIN delete_code_50 USING(ParoleNumber, ParoleCountId)
-- We only want to join on dbo_Sentence_deduped if the update_datetime is on or after that sentence's sentence date
-- (since combined is on the sentence term level and we only want to join on the relevant sentences at each update_datetime)
LEFT JOIN dbo_Sentence_deduped 
    ON combined.ParoleNumber = dbo_Sentence_deduped.ParoleNumber
    AND combined.ParoleCountId = dbo_Sentence_deduped.ParoleCountId
    AND update_datetime >= SentDate
WHERE
    -- We only keep the rows where we see a change in RelMaxDate
    (prev_RelMaxDate <> RelMaxDate
    OR (prev_RelMaxDate IS NULL AND RelMaxDate IS NOT NULL)
    OR (prev_RelMaxDate IS NOT NULL AND RelMaxDate IS NULL))
    -- We exclude parole count -1 since those are invalid parole counts and parole count 0 since those are incarceration related
    AND combined.ParoleCountId NOT IN ('-1', '0')
    -- We exclude parole counts that were opened in error
    AND deleted is NULL
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_pa",
    ingest_view_name="parole_sentences_task_deadlines",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
