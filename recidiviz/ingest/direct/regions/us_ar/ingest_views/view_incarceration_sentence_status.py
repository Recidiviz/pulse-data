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
"""Query containing incarceration sentence status information."""

from recidiviz.ingest.direct.regions.us_ar.ingest_views.us_ar_view_query_fragments import (
    INCARCERATION_SENTENCE_LENGTHS_FRAGMENT,
    SUPVTIMELINE_CLEANED_FRAGMENT,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""
WITH
/*
Incarceration sentence status is mainly pulled from RELEASEDATECHANGE, which shows status
changes that occur as a sentence is being served. This covers the time within a sentence span,
but to get the initial and terminal statuses for a sentence, we need to pull in data from
SENTENCECOMPONENT (which has the imposition date for a sentence and the date it started
to be served) and SUPVTIMELINE (which allows us to see the date a sentence's full term has
a closing status).
*/
{INCARCERATION_SENTENCE_LENGTHS_FRAGMENT},
-- Use the cleaned version of SUPVTIMELINE, documented in the fragment itself.
{SUPVTIMELINE_CLEANED_FRAGMENT},
-- Get each sentence's SENTENCEIMPOSEDDATE and SENTENCEBEGINDATE from SENTENCECOMPONENT.
sentence_start_dates AS (
    SELECT DISTINCT 
        sl.OFFENDERID,
        sl.COMMITMENTPREFIX,
        sl.SENTENCECOUNT,
        sc.SENTENCEIMPOSEDDATE,
        sc.SENTENCEBEGINDATE
    FROM sentence_lengths_life_flags sl
    LEFT JOIN {{SENTENCECOMPONENT}} sc
    ON 
        sl.OFFENDERID = sc.OFFENDERID AND
        sl.COMMITMENTPREFIX = sc.COMMITMENTPREFIX AND
        sl.SENTENCECOUNT = sc.SENTENCECOMPONENT
),
-- SENTENCECOMPUTE has data for terminal statuses and their dates, but it's unreliable since
-- the table doesn't usually get updated once someone leaves a facility. Here, we pull the
-- relevant statuses and dates from SUPVTIMELINE for sentences that have a supervision component.
full_completions AS (
    SELECT DISTINCT 
        sl.OFFENDERID,
        sl.COMMITMENTPREFIX,
        sl.SENTENCECOUNT,
        st.SUPVPERIODBEGINDATE,
        st.SUPVTIMESTATUSFLAG
    FROM sentence_lengths_life_flags sl
    INNER JOIN st_cleaned st
    ON 
        sl.OFFENDERID = st.OFFENDERID AND
        sl.COMMITMENTPREFIX = st.COMMITMENTPREFIX AND
        sl.SENTENCECOUNT = st.SENTENCECOMPONENT
    -- We only need terminal statuses from SUPVTIMELINE, so this CTE only looks at records
    -- with a "complete" or "vacated" status. We also limit it to parole data only, since
    -- Probation Plus sentences can include both prison and probation time, and the probation
    -- part of the sentence showing up as "complete" in SUPVTIMELINE doesn't mean the sentence
    -- itself has the same status. These cases should have sufficient status data in SENTENCECOMPUTE
    -- and RELEASEDATECHANGE, so it makes sense to exclude them from this CTE.
    WHERE st.LENGTHPROB = 0 AND
        st.SUPVTIMESTATUSFLAG IN (
            '9', -- Complete
            'V' -- Vacated
        )  
),
-- Union together the status records from RELEASEDATECHANGE / SENTENCECOMPUTE with the initial
-- status dates from SENTENCECOMPONENT and the terminal status dates from SUPVTIMELINE.
unioned_statuses AS (
    -- Take the statuses and their dates from the sentence lengths fragment. This fragment
    -- has the data we need, with the benefit of ensuring that we're only looking at sentences
    -- that are actually being ingested.
    SELECT 
        OFFENDERID,
        COMMITMENTPREFIX,
        SENTENCECOUNT,
        rd_update_datetime,
        SENTENCESTATUSFLAG
    FROM sentence_lengths_life_flags
    WHERE SENTENCESTATUSFLAG IS NOT NULL
    -- Union with the imposed dates from SENTENCECOMPONENT
    UNION ALL (
        SELECT   
            OFFENDERID,
            COMMITMENTPREFIX,
            SENTENCECOUNT,
            SENTENCEIMPOSEDDATE AS rd_update_datetime,
            'SENTENCECOMPONENT_IMPOSED' AS SENTENCESTATUSFLAG
        FROM sentence_start_dates
    )
    -- Union with the sentence start dates from SENTENCECOMPONENT
    UNION ALL (
        SELECT   
            OFFENDERID,
            COMMITMENTPREFIX,
            SENTENCECOUNT,
            SENTENCEBEGINDATE AS rd_update_datetime,
            'SENTENCECOMPONENT_BEGIN' AS SENTENCESTATUSFLAG
        FROM sentence_start_dates
        -- A small handful of sentences have SENTENCEBEGINDATE values in the future, seemingly
        -- due to data entry errors. These records are removed, since sentence status records
        -- can't be in the future.
        WHERE SENTENCEBEGINDATE <= CAST(CURRENT_DATETIME() AS STRING)
    )
    -- Union with the supervision end dates from SUPVTIMELINE
    UNION ALL (
        SELECT   
            OFFENDERID,
            COMMITMENTPREFIX,
            SENTENCECOUNT,
            SUPVPERIODBEGINDATE AS rd_update_datetime,
            CONCAT('SUPVTIMELINE_',SUPVTIMESTATUSFLAG) AS SENTENCESTATUSFLAG
        FROM full_completions
    )
)

-- The union covers all relevant status changes for each sentence. Because these are pulled
-- from multiple sources, we need to dedup to ensure sentences only have one status per date.
-- We do this by ranking the sources; the bounding statuses are given higher priority over
-- the intermediate statuses in RELEASEDATECHANGE.

-- Some sentences have statuses that should map to COMPLETED/VACATED, but are followed by
-- other statuses. To avoid ingest errors, these cases are handled in one of two ways:
-- 1. If a COMPLETED/VACATED status is followed by other non-COMPLETED/VACATED statuses,
-- then the COMPLETED/VACATED is assumed to be in error and set to 'MARKED_COMPLETE_TOO_EARLY',
-- so that the status will be mapped to INTERNAL_UNKNOWN instead of COMPLETE/VACATED.
-- 2. If a COMPLETED/VACATED is followed by other COMPLETED/VACATED statuses, then only the
-- first COMPLETED/VACATED status is kept and the others are dropped using the QUALIFY at the
-- end of this query. Because we set statuses to MARKED_COMPLETE_TOO_EARLY before this step,
-- this won't filter out any non-COMPLETED/VACATED statuses.

-- TODO(#38496): Investigate these erroneous-looking COMPLETED/VACATED statuses and determine if
-- this approach makes sense.
SELECT *
FROM (
    SELECT 
        OFFENDERID,
        COMMITMENTPREFIX,
        SENTENCECOUNT,
        rd_update_datetime,
        CASE 
            WHEN SENTENCESTATUSFLAG IN (
                '2',
                '8',
                'SUPVTIMELINE_9',
                '7',
                '9',
                'SUPVTIMELINE_V'
                ) AND NOT only_followed_by_completions 
            THEN 'MARKED_COMPLETE_TOO_EARLY' 
            ELSE SENTENCESTATUSFLAG 
        END AS SENTENCESTATUSFLAG,
        seq_num
    FROM (
        SELECT 
            *,
            LOGICAL_AND(SENTENCESTATUSFLAG IN (
                    '2',
                    '8',
                    'SUPVTIMELINE_9',
                    '7',
                    '9',
                    'SUPVTIMELINE_V'
                )
            ) 
            OVER (
                PARTITION BY OFFENDERID,COMMITMENTPREFIX,SENTENCECOUNT 
                ORDER BY seq_num 
                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
            ) AS only_followed_by_completions
        FROM (
            SELECT 
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY 
                        OFFENDERID,
                        COMMITMENTPREFIX,
                        SENTENCECOUNT
                    ORDER BY rd_update_datetime
                ) AS seq_num
            FROM (
                SELECT *
                FROM unioned_statuses
                QUALIFY ROW_NUMBER() OVER (
                PARTITION BY 
                    OFFENDERID,
                    COMMITMENTPREFIX,
                    SENTENCECOUNT,
                    rd_update_datetime
                ORDER BY 
                    CASE 
                        WHEN SENTENCESTATUSFLAG LIKE 'SUPVTIMELINE%' THEN 0
                        -- The most relevant part of the ranking is that SENTENCECOMPONENT_BEGIN is
                        -- higher priority than SENTENCECOMPONENT_IMPOSED, since SENTENCECOMPONENT_BEGIN
                        -- statuses are used to identify when a sentence started being served. The dates
                        -- for these two statuses are often the same.
                        WHEN SENTENCESTATUSFLAG='SENTENCECOMPONENT_BEGIN' THEN 1
                        WHEN SENTENCESTATUSFLAG='SENTENCECOMPONENT_IMPOSED' THEN 2
                    ELSE 3 END
                ) = 1
            )
        ) 
    ) 
)
QUALIFY COALESCE(LAG(SENTENCESTATUSFLAG) OVER (
    PARTITION BY OFFENDERID,COMMITMENTPREFIX,SENTENCECOUNT 
    ORDER BY seq_num
    ),
    'NA'
) NOT IN (
    '2',
    '8',
    'SUPVTIMELINE_9',
    '7',
    '9',
    'SUPVTIMELINE_V'
)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ar",
    ingest_view_name="incarceration_sentence_status",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
