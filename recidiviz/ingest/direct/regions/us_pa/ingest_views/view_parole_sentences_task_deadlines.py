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
-- grab the projected parole term end date from the current Release info table
tasks_from_current AS (
    SELECT 
        ParoleNumber, 
        ParoleCountId, 
        -- there are a small number of RelMaxDate that aren't parseable
        SAFE.PARSE_DATE('%Y%m%d', RelMaxDate) AS RelMaxDate, 
        update_datetime, 
    FROM {dbo_ReleaseInfo@ALL} rel
    WHERE 
      -- We only keep rows where ParoleCountID is not -1 because -1 is used for parole numbers that are invalid (there is no information, errors 
      -- in general with generation, parole numbers that should be voided because its duplicated)
        ParoleCountID <> '-1'
),
-- grab the final parole term end date from the archived Release history table
tasks_from_hist AS (
    SELECT 
        ParoleNumber,
        ParoleCountId,
        HReMaxDate,
        update_datetime
    FROM (
        SELECT
            ParoleNumber, 
            ParoleCountId, 
            -- there are many rows set to LIFE/INDE, 144 rows set to some other non-parseable string as of 5/1/2024
            -- we'll ingest the life sentence dates as the magic date, and we'll ingest the unparseable HReMaxDate as 1111-1-1 for now to null out later
            -- (we can't just keep them as NULLs since we use a LAST_VALUE later that ignores nulls)
            CASE 
                WHEN HReMaxDate = 'LIFE' OR HReMaxDate LIKE '%INDE%'
                    THEN DATE(2999, 12, 31)
                ELSE COALESCE(SAFE.PARSE_DATE('%Y%m%d', HReMaxDate), DATE(1111,1,1))
                END AS HReMaxDate, 
            update_datetime, 
            CAST(HReleaseId AS INT64) AS HReleaseID,
            MAX(CAST(HReleaseID AS INT64)) OVER(PARTITION By ParoleNumber, ParoleCountID, update_datetime) as max_HReleaseID
        FROM {dbo_Hist_Release@ALL} hist
    )
    WHERE 
      -- We only keep rows where this the most recent HReleaseID for that update_datetime
      (HReleaseID = max_HReleaseID OR max_HReleaseID IS NULL)
      -- and where ParoleCountID is not -1 because -1 is used for parole numbers that are invalid (there is no information, errors 
      -- in general with generation, parole numbers that should be voided because its duplicated)
      AND ParoleCountID <> '-1'
),
-- union the current data and the archived historical table, 
-- prioritize the most recent date from the archived historical table if there is both a date from the current and historical table,
-- and then order them to calculate prev_RelMaxDate to be used for deduplicating in the last step
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
)

SELECT
    ParoleNumber,
    ParoleCountId,
    -- reset '1111-1-1' (from unparseable dates) and '2999-12-31' (from life sentences) to NULL
    NULLIF(NULLIF(RelMaxDate, DATE(1111,1,1)), DATE(2999, 12, 31)) AS RelMaxDate,
    update_datetime,
FROM combined
WHERE
    -- We only keep the rows where we see a change in RelMaxDate
    prev_RelMaxDate <> RelMaxDate
    OR (prev_RelMaxDate IS NULL AND RelMaxDate IS NOT NULL)
    OR (prev_RelMaxDate IS NOT NULL AND RelMaxDate IS NULL)

"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_pa",
    ingest_view_name="parole_sentences_task_deadlines",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
