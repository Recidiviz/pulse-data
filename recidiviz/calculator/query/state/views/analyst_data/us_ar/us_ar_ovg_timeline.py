# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Pulls together AR supervision sanction data from several sources into a timeline
recording each time somebody gains or loses OVG (sanction) points. This view provides us
with a picture of the OVG totals for each client in AR over time and geography, which
is not possible using raw tables or ingested data."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_AR_OVG_TIMELINE_VIEW_NAME = "us_ar_ovg_timeline"

US_AR_OVG_TIMELINE_VIEW_DESCRIPTION = """Pulls together AR supervision sanction data from
several sources into a timeline recording each time somebody gains or loses OVG (sanction)
points. This view provides us with a picture of the OVG totals for each client in AR over
time and geography, which is not possible using raw tables or ingested data."""

US_AR_OVG_TIMELINE_QUERY_TEMPLATE = f"""
-- TODO(#30279): Follow up on remaining discrepancies and policy logic
-- TODO(#31020): Revisit moving some of this information into upstream ingest 
WITH
point_gains_start_end AS (
  /*
  Union the start/end dates for standard violation events with the start/end dates
  for adjustment events, with the adjustment dates for adjusted violations being
  used as the start dates for the inverted adjustment spans.
  */
  SELECT
    person_id,
    state_code,
    start_date,
    end_date,
    event_id,
    event_sub_id,
    points,
    super_session_start,
    super_session_end
  FROM `{{project_id}}.{{analyst_dataset}}.us_ar_ovg_events_preprocessed_materialized`
  WHERE event_id != 'INCENTIVE'
  
  UNION ALL
  
  SELECT
    person_id,
    state_code,
    adjustment_date AS start_date,
    end_date,
    event_id,
    event_sub_id,
    points * -1 AS points,
    super_session_start,
    super_session_end
  FROM `{{project_id}}.{{analyst_dataset}}.us_ar_ovg_events_preprocessed_materialized`
  WHERE
    adjustment_date IS NOT NULL AND
    event_id != 'INCENTIVE'
),
sub_sessions_1 AS (
  -- Generate sub-sessions for each event to determine the controlling violation at each
  -- point in time, as other violations in the intervention group fall off due to adjustments.
  WITH {
    create_sub_sessions_with_attributes(
      table_name="point_gains_start_end",
      index_columns=['person_id','state_code','event_id']
      )
    }
  SELECT * FROM sub_sessions_with_attributes
),
deduped_events AS (
  -- Deduplicate each sub-session by using only the points associated with the controlling
  -- (i.e. most severe) violation.
  SELECT
    person_id,
    state_code,
    start_date,
    end_date,
    event_id,
    MAX(points) AS points,
    ANY_VALUE(super_session_start) AS super_session_start,
    ANY_VALUE(super_session_end) AS super_session_end,
    end_date AS event_specific_end_date
  FROM (
    SELECT
        person_id,
        state_code,
        start_date,
        end_date,
        event_id,
        event_sub_id, 
        SUM(points) AS points,
        ANY_VALUE(super_session_start) AS super_session_start,
        ANY_VALUE(super_session_end) AS super_session_end,
        end_date AS event_specific_end_date
    FROM sub_sessions_1
    GROUP BY 1,2,3,4,5,6
  )
  GROUP BY 1,2,3,4,5
),
unioned_incentives_points AS (
  SELECT *
  FROM deduped_events
  UNION ALL
  SELECT 
    * EXCEPT(adjustment_date,event_sub_id,supervision_office),
    CAST(NULL AS DATETIME) AS event_specific_end_date
  FROM `{{project_id}}.{{analyst_dataset}}.us_ar_ovg_events_preprocessed_materialized`
  WHERE event_id = 'INCENTIVE'
),
sub_sessions_2 AS (
  WITH {
    create_sub_sessions_with_attributes(
      table_name="unioned_incentives_points",
      index_columns=['person_id','state_code']
    )
  }
  SELECT * FROM sub_sessions_with_attributes
),
negative_dates AS (
  -- Calculate each person's OVG total over time, recording the dates on which an incentive
  -- makes this total turn negative.
  SELECT
    person_id,
    super_session_start,
    start_date AS date_of_negative_points
  FROM (
    SELECT
      person_id,
      start_date,
      SUM(points) AS points_raw,
      ANY_VALUE(super_session_start) AS super_session_start,
    FROM sub_sessions_2
    GROUP BY 1,2
  )
  WHERE points_raw < 0
),
prior_point_end_dates AS (
  /* 
  Incentives stop contributing to someone's OVG total once the points they had when the
  incentive was applied have fully decayed. To find when this happens so that the date
  can be used to set the incentive end date, we pull the decay date for the last point-gain
  event prior to an incentive.
  */
  SELECT DISTINCT 
    person_id,
    super_session_start,
    start_date,
    last_point_end_date
  FROM (
    SELECT 
      *,
      MAX(event_specific_end_date) OVER(
        PARTITION BY person_id, super_session_start, start_date) AS last_point_end_date
    FROM sub_sessions_2
  ) 
  WHERE event_id = 'INCENTIVE'
),
incentives_with_incentive_ends AS (
  /*
  The next 3 CTEs are essentially repeats of the CTEs prior to generating the last
  roud of sub-sessions (incentives, unioned_incentives_points, and all_points_and_locations).
  The only difference is the incentive spans are updated with new end dates that terminate
  the span once the incentive would make someon's OVG score negative.
  */
  SELECT
    person_id,
    state_code,
    start_date,
    /*
    Incentive end dates are based on session boundaries. There are two cases where they
    need to be overriden: when there's a date on which the incentive causes someone's total
    to become negative, or when there's a date on which the points the incentive was applied
    to have fully decayed. Here, we join the incentives to each of these possible dates,
    and use the earliest one as the incentive's new end date.
    */
    LEAST(
      COALESCE(MIN(date_of_negative_points),'9999-01-01'),
      COALESCE(
        ANY_VALUE(last_point_end_date),
        ANY_VALUE(end_date)
      )
    ) AS end_date,
    event_id,
    points,
    super_session_start,
    super_session_end
  FROM (
    SELECT
      i.*,
      nd.date_of_negative_points,
      pp.last_point_end_date
    FROM `{{project_id}}.{{analyst_dataset}}.us_ar_ovg_events_preprocessed_materialized` i
    LEFT JOIN negative_dates nd
    ON 
      i.person_id = nd.person_id AND 
      i.super_session_start = nd.super_session_start AND
      {nonnull_end_date_clause('nd.date_of_negative_points')} >= i.start_date
    LEFT JOIN prior_point_end_dates pp
    ON 
      i.person_id = pp.person_id AND
      i.super_session_start = pp.super_session_start AND
      i.start_date = pp.start_date
    WHERE i.event_id = 'INCENTIVE'
  )
  GROUP BY
    person_id,
    state_code,
    start_date,
    points,
    event_id,
    super_session_start,
    super_session_end
),
unioned_incentives_points_incentive_ends AS (
  SELECT * EXCEPT(event_specific_end_date)
  FROM deduped_events
  UNION ALL
  SELECT *
  FROM incentives_with_incentive_ends
),
supervision_super_sessions AS (
  SELECT
    person_id,
    state_code,
    start_date,
    end_date,
    CAST(NULL AS STRING) AS event_id,
    CAST(NULL AS INT64) AS points,
    start_date AS super_session_start,
    end_date AS super_session_end,
    compartment_level_2_start,
  FROM `{{project_id}}.{{sessions_dataset}}.compartment_level_0_super_sessions_materialized`
  WHERE state_code = 'US_AR' AND 
    compartment_level_0 = 'SUPERVISION' AND
    DATE_DIFF({nonnull_end_date_clause('end_date')}, start_date, DAY) != 0
),
point_loc_ss_union AS (
  -- Union together the violations from before, incentives, and locations, to be used
  -- in the next round of sub-sessions.
  SELECT
    *,
    CAST(NULL AS STRING) AS compartment_level_2_start,
  FROM unioned_incentives_points_incentive_ends
  
  UNION ALL
  
  SELECT * FROM supervision_super_sessions
),
sub_sessions_3 AS (
  WITH {
    create_sub_sessions_with_attributes(
      table_name="point_loc_ss_union",
      index_columns=['person_id','state_code']
    )
  }
  SELECT * FROM sub_sessions_with_attributes
),
summed_points AS (
  -- Get sub-sessions with relevant point data (i.e., omit sub-sessions with location data
  -- only), and sum the points for each.
  SELECT *
  FROM (
    SELECT
      person_id,
      state_code,
      start_date,
      end_date,
      ANY_VALUE(compartment_level_2_start) AS compartment_level_2_start,
      COALESCE(GREATEST(SUM(points),0),0) AS points,
      ANY_VALUE(super_session_start) AS super_session_start,
      ANY_VALUE(super_session_end) AS super_session_end
    FROM sub_sessions_3
    GROUP BY 1,2,3,4
  )
),
cleaned_timeline AS (
  SELECT sp.*
  FROM summed_points sp
  LEFT JOIN `{{project_id}}.{{sessions_dataset}}.compartment_level_0_super_sessions_materialized` ss
  ON 
    sp.person_id = ss.person_id AND 
    sp.super_session_start = ss.start_date
  WHERE ss.compartment_level_0 = 'SUPERVISION'
)
SELECT 
      *,
      CASE 
        WHEN COALESCE(points,0) = 0 THEN 0
        WHEN COALESCE(points,0) BETWEEN 1 and 9 THEN 1
        WHEN COALESCE(points,0) BETWEEN 10 and 19 THEN 2
        WHEN COALESCE(points,0) BETWEEN 20 and 29 THEN 3
        WHEN COALESCE(points,0) BETWEEN 30 and 39 THEN 4
        WHEN COALESCE(points,0) >= 40 THEN 5
      END AS tranche
      
FROM ({aggregate_adjacent_spans(table_name='cleaned_timeline',
                       attribute=['points'],
                       end_date_field_name='end_date')})
"""
US_AR_OVG_TIMELINE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_AR_OVG_TIMELINE_VIEW_NAME,
    view_query_template=US_AR_OVG_TIMELINE_QUERY_TEMPLATE,
    description=US_AR_OVG_TIMELINE_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    should_materialize=True,
)
if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_AR_OVG_TIMELINE_VIEW_BUILDER.build_and_print()
