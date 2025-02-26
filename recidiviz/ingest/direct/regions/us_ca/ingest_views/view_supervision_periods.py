# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License AS published by
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
"""Query containing CDCR supervision period information. For more information,
including to-do's and things we could UXR, see us_ca.md.
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH IncarcerationParole_parsed AS (
    -- Use DISTINCT because it's possible to get some duplicates from this query
    -- which will cause problems later on with the LEAD/LAG functions
    SELECT DISTINCT
        OffenderId,
        CAST(out_date AS DATETIME) AS out_date,
        -- The raw data will either have the end date as 'null' or '9999-01-31' to
        -- represent an open period. We send everything to null because the BigQuery
        -- emulator had a problem with '9999-01-31' (see TODO(#21149))
        IF(SUBSTRING(IFNULL(ParoleEndDate, '9999'), 1, 4) = '9999', NULL, CAST(ParoleEndDate AS DATETIME)) AS ParoleEndDate,
    FROM {IncarcerationParole}
),

/*
    The Delta file contains a record of when someone left the Parole population (for any number of reasons).
    For these purposes we are only concerned with the fact that someone left and WHEN they left. However,
    we exclude "Date Change" because these records refer to when someone is removed from the population of
    people who will become eligible for Parole within 90 days.
    This CTE will be used to close open periods from the IncarcerationParole table.
*/
delta_file AS (
    SELECT
        OffenderId,
        CAST(StatusDate AS DATETIME) AS StatusDate,
        OffenderGroup,
    FROM {Delta}
    WHERE OffenderGroup != 'Date Change'
),

merged_supervision_periods AS (
  WITH cleaned_parole_periods AS (
    SELECT
      OffenderId,
      CAST(ip.out_date AS DATETIME) AS parole_start_date,
      CASE
          -- This condition will be true only for records where there is no valid end
          -- date
          WHEN ParoleEndDate IS NULL THEN
              -- If the LEAD window function returns NULL, it means we are at the latest
              -- (current) period of parole OR this is a period that was mistakenly /
              -- confusingly left open. This LEAD(out_date) will resolve to null in the
              -- former case and infer an end date as the day before the following
              -- period in the latter case.
              LEAD(out_date) OVER (
                  PARTITION BY OffenderId
                  ORDER BY out_date
              ) - INTERVAL '1' DAY
          ELSE ParoleEndDate
      END AS inferred_parole_end_date
    FROM IncarcerationParole_parsed ip
  ),
  msp_with_filter_for_recency as (
    SELECT
      cpp.OffenderId,
      cpp.parole_start_date,
      -- We will join in the Delta file only on open parole periods -- if we are able to
      -- join, that open period should be closed. Therefore, overwrite the end date from
      -- IncarcerationParole with the matching StatusDate from Delta. 
      IFNULL(
          df.StatusDate,
          cpp.inferred_parole_end_date
      ) AS inferred_parole_end_date,
      -- There are a few cases where the Delta file lists multiple supervision exits
      -- during an IncarcerationParole parole period. This ROW_NUMBER will be used to
      -- select the first StatusDate within a given parole period as the true end of the
      -- parole period.
      ROW_NUMBER() OVER (
        PARTITION BY cpp.OffenderId, cpp.parole_start_date
        ORDER BY df.StatusDate
      ) = 1 as filter_row
    FROM cleaned_parole_periods cpp
    LEFT JOIN 
      delta_file df
        ON cpp.OffenderId = df.OffenderId
        -- TODO(#21351) We're checking that the inferred_parole_end_date is null, but
        -- what happens if someone on parole leaves parole and then comes back?
        AND cpp.inferred_parole_end_date IS NULL 
    ORDER BY cpp.OffenderId, parole_start_date ASC
  )
  SELECT 
    OffenderId,
    parole_start_date,
    inferred_parole_end_date,
  FROM msp_with_filter_for_recency
  WHERE filter_row
),
offender_group_transitions_no_filter AS (
  SELECT 
      OffenderId,
      CAST(LastParoleDate AS DATETIME) AS LastParoleDate,
      OffenderGroup,
      update_datetime,

      -- Keep transitions between offender groups and also if LastParoleDate changes 
      -- (which indicates they may have left and then restarted parole)

      -- TODO(#21467) We do not currently correctly close these periods as sometimes
      -- information is missing from the Delta file. This can lead to extending our
      -- OffenderGroup information onto new periods of parole by up to a week-ish.
      (  
        (
          LAG(OffenderGroup) OVER (PARTITION BY OffenderId ORDER BY update_datetime ASC) != OffenderGroup OR
          LAG(OffenderGroup) OVER (PARTITION BY OffenderId ORDER BY update_datetime ASC) IS NULL
        )
      ) as filter_row
  FROM {PersonParole@ALL} pp
),
offender_group_transitions as (
  SELECT * from offender_group_transitions_no_filter where filter_row
),
-- We recieve some information about a persons parole status from PersonParole. Since we
-- recieve full historical periods in IncarcerationParole, we need to get @ALL rows from
-- PersonParole to determine when during those periods (historical and current) someone
-- was absconded.
offender_group_periods AS (
  SELECT
    OffenderId,
    LastParoleDate,
    OffenderGroup,
    update_datetime AS og_start_date,
    LEAD(update_datetime) OVER (PARTITION BY OffenderId ORDER BY update_datetime ASC) AS og_end_date,
  FROM offender_group_transitions
),
supervision_level_periods AS (
  SELECT
    OffenderId,
    CAST(SupvLevelChangeDate AS DATETIME) AS SupvLevelChangeDate,
    CAST(LEAD(SupvLevelChangeDate) OVER (PARTITION BY OffenderId ORDER BY SupvLevelChangeDate) AS DATETIME) AS sl_end_date,
    SupervisionLevel
  FROM {SupervisionSentence}
  ORDER BY OffenderId, SupvLevelChangeDate asc
),
transition_dates as (
  -- We only need the start dates here. og_end_dates are just lead(start_date), so there are no new values there.
  SELECT DISTINCT
    OffenderId,
    og_start_date AS transition_date
  FROM offender_group_periods

  UNION DISTINCT

  SELECT DISTINCT
    OffenderId,
    parole_start_date AS transition_date
  FROM merged_supervision_periods

  UNION DISTINCT

  SELECT DISTINCT
    OffenderId,
    SupvLevelChangeDate AS transition_date
  FROM supervision_level_periods

  UNION DISTINCT

  -- We filter out `null` end_dates. These reflect open periods, and are not a
  -- transition date. These transition dates will become start_dates, and it is
  -- definitely not a start_date.
  SELECT DISTINCT
    OffenderId,
    inferred_parole_end_date AS transition_date
  FROM merged_supervision_periods
  WHERE inferred_parole_end_date IS NOT NULL
),
periods_cte AS (
  SELECT
      td.OffenderId,
      td.transition_date AS start_date,
      LEAD(transition_date) OVER (PARTITION BY td.OffenderId ORDER BY transition_date) AS end_date
  FROM transition_dates td
),
periods_cte_dropping_incarceration_periods AS (
  SELECT
    p.OffenderId,
    p.start_date,
    p.end_date,
    msp.parole_start_date,
    msp.inferred_parole_end_date
  FROM periods_cte p
  -- This join should only preserve periods if the start and end date of that period
  -- fall within a period that exists in IncarcerationParole.
  JOIN merged_supervision_periods msp ON
      p.OffenderId = msp.OffenderId AND
      (
        start_date >= parole_start_date AND (
          start_date <= inferred_parole_end_date OR inferred_parole_end_date IS NULL
        )
      ) AND (
        (
          end_date >= parole_start_date OR end_date IS NULL
        ) AND (
          end_date <= inferred_parole_end_date OR inferred_parole_end_date IS NULL
        )
      )
),
-- Using the approach used here: https://github.com/Recidiviz/pulse-data/blob/main/recidiviz/ingest/direct/regions/us_ix/ingest_views/view_supervision_period.py
merged_supervision_periods_with_offender_groups AS (
  SELECT DISTINCT
    p.OffenderId,
    p.start_date,
    p.end_date,
    ogp.OffenderGroup
  FROM periods_cte_dropping_incarceration_periods p
    LEFT JOIN offender_group_periods ogp
        ON p.OffenderId = ogp.OffenderId
        AND p.start_date >= ogp.og_start_date 
        AND (p.end_date <= ogp.og_end_date OR ogp.og_end_date IS NULL)
),
merged_supervision_periods_with_supervision_level_and_offender_groups AS (
  SELECT DISTINCT
    p.OffenderId,
    p.start_date,
    p.end_date,
    p.OffenderGroup,
    slp.SupervisionLevel,
    -- TODO(#21351) Can we count on this being stable? 
    ROW_NUMBER() OVER (PARTITION BY p.OffenderId ORDER BY p.start_date) AS period_sequence_number
  FROM merged_supervision_periods_with_offender_groups p
  LEFT JOIN supervision_level_periods slp
    ON p.OffenderId = slp.OffenderId
    AND p.start_date >= slp.SupvLevelChangeDate
    AND (p.end_date <= slp.sl_end_date OR slp.sl_end_date IS NULL)
)
SELECT
    OffenderId,
    start_date,
    end_date,
    OffenderGroup,
    SupervisionLevel,
    period_sequence_number
FROM merged_supervision_periods_with_supervision_level_and_offender_groups
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ca",
    ingest_view_name="supervision_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="OffenderId, start_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
