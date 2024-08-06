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
WITH 
/*
`IncarcerationParole_parsed` gets out_date (reflecting a parole start) and
ParoleEndDate (reflecting the end of parole). Super simple, our basic period, should
be easy -- WRONG! The first step in a pretty involved view. Buckle up.
*/
IncarcerationParole_parsed AS (
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
The Delta file contains a record of when someone left the Parole population (for any
number of reasons).  For these purposes we are only concerned with the fact that
someone left and WHEN they left. However, we exclude "Date Change" because these
records refer to when someone is removed from the population of people who will become
eligible for Parole within 90 days.  This CTE will be used to close open periods from
the IncarcerationParole table.
*/
delta_file AS (
    SELECT
        OffenderId,
        CAST(StatusDate AS DATETIME) AS StatusDate,
        OffenderGroup,
    FROM {Delta}
    WHERE OffenderGroup != 'Date Change'
),

/*
`merged_supervision_periods` contains multiple CTEs. They combine to ultimately infer
missing end dates and then close periods with the Delta file, if applicable (this
would be applicable if someone left the parole population for any reason, as that's
when someone appears in the Delta file.)
*/ 
merged_supervision_periods AS (
 WITH 
  /*
  `cleaned_parole_periods` takes IncarcerationParole and infers end dates when there
  is none, resulting in closed periods of parole. See other comments in CTE for
  details on how this works.
  */ 
  cleaned_parole_periods AS (
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
                  ORDER BY out_date, ParoleEndDate
              ) - INTERVAL '1' DAY
          ELSE ParoleEndDate
      END AS inferred_parole_end_date
    FROM IncarcerationParole_parsed ip
  ),
  /*
  `msp_with_filter_for_recency` uses the Delta file to infer end dates on final, open
  periods. It also adds a filter which, in the case of multiple erroneous closures in
  the Delta file, selects the earliest date as the true closure.
  */
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
  )
  SELECT 
    OffenderId,
    parole_start_date,
    inferred_parole_end_date,
  FROM msp_with_filter_for_recency
  WHERE filter_row
),

/*
`offender_group_transitions_no_filter` begins the process of making offender_group
periods. The offender_group can specify if an individual is absconded, or other
statuses. It adds a filter row, which applied in a following CTE since it can helpful
to have this functionality separated for future debugging. When the filter row is
applied, we will end up with critical dates.
*/
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

/*
`offender_group_transitions` simply applies the filter above, ensuring we only retain
rows that reflect an actual OffenderGroup change.
*/
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
/*
`nullBadgeCTE` converts nulls to a string for easier processing.
*/
nullBadgeCTE AS (
  SELECT 
    OffenderId, 
    COALESCE(BadgeNumber,'Null') AS BadgeNumber,
    update_datetime,
  FROM {PersonParole@ALL}
),
/*
`supervision_officer_prep_cte` gets the previous badge number for an offender -- later
we'll check if that badge number changed, and if so, create a new supervision period.
*/
supervision_officer_prep_cte AS
(
  SELECT 
    OffenderId, 
    BadgeNumber,
    update_datetime,
    LAG(BadgeNumber) OVER (PARTITION BY OffenderId ORDER BY update_datetime) AS prev_BadgeNumber 
  FROM nullBadgeCTE
),
/*
`supervision_officer_periods` create periods for when the supervising officer changes.
*/
supervision_officer_periods AS (
  SELECT 
    OffenderId, 
    CASE 
      WHEN BadgeNumber = 'Null' 
      THEN NULL ELSE BadgeNumber 
    END AS BadgeNumber,
    update_datetime AS PeriodStart,
    LEAD(update_datetime) OVER (PARTITION BY OffenderId ORDER BY update_datetime) AS PeriodEnd
  FROM supervision_officer_prep_cte
  WHERE BadgeNumber != prev_BadgeNumber OR prev_BadgeNumber IS NULL OR BadgeNumber = 'Null'
),
/*
`supervision_location_prep_cte` begins making periods for when the supervision location
changes.
*/
supervision_location_prep_cte AS
(
  SELECT 
    OffenderId, 
    ifnull(ParoleUnit, 'null') AS ParoleUnit,
    update_datetime, 
    -- We could probably remove Region and District from this -- see TODO(#28585).
    CONCAT(ifnull(ParoleRegion, 'null'),'@@',ifnull(ParoleDistrict, 'null'),'@@',ifnull(ParoleUnit, 'null')) AS fullLoc
  FROM {PersonParole@ALL}
),
/*
`supervision_location_prep_cte2` checks to see when any ParoleRegion, ParoleDistrict, or
ParoleUnit changes by getting the previous location. Later we compare the previous
location and the current location.
*/
supervision_location_prep_cte2 AS (
  SELECT
    OffenderId, 
    ParoleUnit,
    fullLoc,
    lag(fullLoc) over (Partition by OffenderId order by update_datetime) AS prev_fullLoc,
    update_datetime
  from supervision_location_prep_cte
),
/*
`supervision_location_periods` finishes making the periods by comparing previous location to current location.
*/
supervision_location_periods AS (
  SELECT 
    OffenderId, 
    ParoleUnit,
    fullLoc,
    update_datetime as PeriodStart,
    lead(update_datetime) OVER (Partition by OffenderId order by update_datetime) as PeriodEnd
  from supervision_location_prep_cte2
  WHERE fullLoc != prev_fullLoc or prev_fullLoc is null 
),
/*
`supervision_level_changes` First, we consider every date for which the supervision
level is supposed to have changed for an offender -- we choose the information that was
given to us most recently. IE if we received information on 1/1/2020 (IE update_datetime
is 1/1/2020) that Offender A changed to supervision level Cat A on 1/1/2020 (IE
SupvLevelChangeDate is 1/1/2020), but then received more information the following week
(update_datetime is 1/8/2020) that says actually on 1/1/2020 (SupvLevelChangeDate =
1/1/2020) the supervision level was Category B, we prioritize Category B over Category A
because Category B reflects the most recent information we have for 1/1/2020.
*/
supervision_level_changes AS (
  SELECT
    OffenderId,
    CAST(SupvLevelChangeDate AS DATETIME) AS SupvLevelChangeDate,
    SupervisionLevel
  -- TODO(#27027) consider changing the raw data config for SupervisionSentence to only contain
  -- OffenderId and SupvLevelChangeDate as the PK. 
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY OffenderId, SupvLevelChangeDate ORDER BY update_datetime DESC
      ) AS rn
    FROM {SupervisionSentence@ALL}
  ) AS a
  WHERE rn = 1
),
/*
`supervision_level_periods` creates periods by creating an end date which reflects the next time supervision level changed.
*/
supervision_level_periods AS (
  SELECT
    OffenderId,
    SupvLevelChangeDate,
    LEAD(SupvLevelChangeDate) OVER (
      PARTITION BY OffenderId ORDER BY SupvLevelChangeDate
    ) AS sl_end_date,
    SupervisionLevel
  FROM supervision_level_changes
),
/*
`transition_dates` (often called `critical_dates` in other views) gets all the dates
anything changed -- so for all the periods we've created above, we get the start date
and the end date and call these `transition_dates`.
*/
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

  SELECT DISTINCT
    OffenderId,
    PeriodStart AS transition_date
  FROM supervision_location_periods

  UNION DISTINCT

  SELECT DISTINCT
    OffenderId,
    PeriodStart AS transition_date
  FROM supervision_officer_periods

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
/*
`periods_cte` creates unfilled periods by taking end_date to be the next
transition_date.
*/
periods_cte AS (
  SELECT
      td.OffenderId,
      td.transition_date AS start_date,
      LEAD(transition_date) OVER (PARTITION BY td.OffenderId ORDER BY transition_date) AS end_date
  FROM transition_dates td
),
/*
`periods_cte_dropping_incarceration_periods` joins in supervision_periods which have
already been properly closed according to the delta file if necessary.
*/
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
/* 
`merged_supervision_periods_with_offender_groups`: Uses the approach used here:
https://github.com/Recidiviz/pulse-data/blob/main/recidiviz/ingest/direct/regions/us_ix/ingest_views/view_supervision_period.py
Similar to the CTE above, we join in offender group information.
*/
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
/*
`merged_supervision_periods_with_supervision_level_and_offender_groups` Merge in
supervision level information.
*/
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
),
/*
`merged_supervision_periods_with_location` Merges in location information.
*/
merged_supervision_periods_with_location AS (
  SELECT DISTINCT
    p.OffenderId,
    p.start_date,
    p.end_date,
    p.OffenderGroup,
    p.SupervisionLevel,
    p.period_sequence_number,
    slp.ParoleUnit
  FROM merged_supervision_periods_with_supervision_level_and_offender_groups p
  LEFT JOIN supervision_location_periods slp
    ON p.OffenderId = slp.OffenderId
    AND p.start_date >= slp.PeriodStart
    AND (p.end_date <= slp.PeriodEnd OR slp.PeriodEnd IS NULL)
),
/*
`merged_supervision_periods_with_officer` merges in officer information. We apply a
regex after merging everything which only retains supervision periods associated with
officers with valid badge numbers. This eliminates PSAs. We retain supervision periods
associated with null badge numbers as there are instances when we don't know someone's
supervising officer -- these are still periods that we care about. 
*/
merged_supervision_periods_with_officer AS (
  SELECT DISTINCT
    p.OffenderId,
    p.start_date,
    p.end_date,
    p.OffenderGroup,
    p.SupervisionLevel,
    p.period_sequence_number,
    p.ParoleUnit,
    spo.BadgeNumber
  FROM merged_supervision_periods_with_location p
  LEFT JOIN supervision_officer_periods spo
    ON p.OffenderId = spo.OffenderId
    AND p.start_date >= spo.PeriodStart
    AND (p.end_date <= spo.PeriodEnd OR spo.PeriodEnd IS NULL)
  WHERE (
    REGEXP_CONTAINS(spo.BadgeNumber, r"^[0-9][0-9][0-9][0-9]$") OR spo.BadgeNumber IS NULL
  )
)
SELECT
    OffenderId,
    start_date,
    end_date,
    OffenderGroup,
    SupervisionLevel,
    period_sequence_number,
    UPPER(ParoleUnit) as upper_ParoleUnit,
    BadgeNumber
FROM merged_supervision_periods_with_officer
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ca",
    ingest_view_name="supervision_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
