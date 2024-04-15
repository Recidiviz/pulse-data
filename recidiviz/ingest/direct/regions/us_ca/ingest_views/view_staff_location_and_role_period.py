# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Query to create staff location periods and staff role periods. The final periods are
not collapsed -- IE if someones location changes, their role period will also split, and
even though the role has stayed the same, we will end up with two consecutive role
periods.

This view is structured into two large CTEs which are finally merged for the final
output. The first CTE reflects PA Is locations, the second CTE is for supervisor
locations. When these are merged, we add a `type` that is used to specify the staff's
role. We don't care currently if someone is a PA II or a PA III -- we think of them as a
supervisor no matter what. See TODO(#28927).

There is similar logic in view_staff and view_supervisor_periods. We should do a pass
over these views and abstract this common logic into fragments we can reuse.
TODO(#28926)

This means that a PA I (or PA II if they have a caseload) is considered as working in a
unit if they have a caseload in that unit. If they have a caseload in multiple units at
the same time (supposedly impossible, but the case for about 56 people on certain
weeks), we consider that PA I to operate in whichever unit they have the most clients in
(see #28597). I chose not to care about what AgentParole says for the supervisee
information -- this is because if AgentParole said a PA I was in unit 2, but they're
supervising 50 clients in unit 3 and 0 in unit 2, they're clearly in unit 3.

### Notes
1. May to July 2023 we stopped receiving badge numbers, so there are breaks in the
supervision periods for this time. TODO(#28472)
"""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH pa_1_location_periods AS (
  -- `pa_1_location_periods` is the first large CTE. It creates location periods for
  -- anyone who is listed as a PA I in AgentParole.
  WITH 
  -- `all_dates_cross_badge_units`` CTE will contain all update datetimes crossed with
  -- all badge numbers. We'll use this later to see which parole unit(s) each PA I is
  -- associated with at any given time, with nulls in between. We call it
  -- possible_ParoleUnit and possible_BadgeNumber because we don't know if there was
  -- actually a staff member then until we join back to agents in the next CTE.
  all_dates_cross_badge_units AS (
    SELECT
      a.BadgeNumber AS possible_BadgeNumber,
      a.ParoleUnit AS possible_ParoleUnit,
      ud.update_datetime AS start_date,
      LEAD(ud.update_datetime) OVER (PARTITION BY a.BadgeNumber, a.ParoleUnit ORDER BY update_datetime) AS end_date,
    FROM 
      (SELECT DISTINCT update_datetime FROM {PersonParole@ALL}) ud,
      (SELECT DISTINCT BadgeNumber, ParoleUnit FROM {PersonParole@ALL}) a 
  ),
  -- `pa_1_locations_per_date` does a few things:
  --    1. Filters to only PA Is by using Agent Parole
  --    2. Computes the caseload for each agent, and selects the location where the
  --    agents supevises the most people (see comment below).
  pa_1_locations_per_date AS (
    SELECT
      pp.BadgeNumber,
      pp.ParoleUnit,
      pp.update_datetime,
      -- TODO(#28597) Supposedly officers can only supervise folks in a single district
      -- at a time. However, we have 22 PA Is on specific days when this does not appear
      -- to be true -- however, officers that do appear to supervise people as PA Is in
      -- multiple units only ever have 1 client in one of the units and many in the
      -- other. So in these situations, we just say the officer is supervised in
      -- whatever unit they have the most clients in. We use the qualify statement below
      -- to assign PAs to the case where they have the most cases.
      COUNT(DISTINCT OffenderId) caseload_in_unit
    FROM {PersonParole@ALL} pp
    LEFT JOIN {AgentParole@ALL} ap ON pp.BadgeNumber = ap.BadgeNumber AND pp.update_datetime = ap.update_datetime AND pp.ParoleUnit = ap.ParoleUnit
    WHERE pp.BadgeNumber IS NOT NULL AND ap.AgentClassification = "Parole Agent I, AP" 
    GROUP BY
      pp.BadgeNumber,
      pp.ParoleUnit,
      pp.update_datetime 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY BadgeNumber, update_datetime ORDER BY caseload_in_unit DESC) = 1 
  ),
  -- `filled_all_dates` creates a table where each row shows whether a PA I was in a
  -- specific unit at a update_datetime.
  filled_all_dates AS (
    SELECT
      ad.start_date,
      ad.end_date,
      ad.possible_ParoleUnit,
      ad.possible_BadgeNumber,
      lpd.BadgeNumber, -- If there is actually a staff_member, this will be populated
      lpd.ParoleUnit,
    FROM all_dates_cross_badge_units ad
    LEFT JOIN pa_1_locations_per_date lpd
    ON
      lpd.BadgeNumber = ad.possible_BadgeNumber
      AND lpd.update_datetime = ad.start_date
      AND lpd.ParoleUnit = ad.possible_ParoleUnit 
  ),
  -- `proto_staff_location_periods` drops any periods where there was not a change in
  -- ParoleUnit. The result is two "proto periods" that reflect the start and end of
  -- location periods (IE the first period has the correct start_date, the second period
  -- as the correct end_date). If the period is only a single week, there will only be 1
  -- "proto period". In the next CTE, we combine these periods into one.
  proto_staff_location_periods AS (
    SELECT
      BadgeNumber,
      ParoleUnit,
      start_date,
      end_date,
      LAG(ParoleUnit) OVER (PARTITION BY possible_BadgeNumber, possible_ParoleUnit ORDER BY start_date) IS NOT NULL AS previous_unit_is_same,
      LEAD(ParoleUnit) OVER (PARTITION BY possible_BadgeNumber, possible_ParoleUnit ORDER BY start_date) IS NOT NULL AS next_unit_is_same
    FROM
      filled_all_dates 
    WHERE True -- Required because of https://github.com/google/zetasql/issues/124
    QUALIFY ParoleUnit IS NOT NULL AND (
      -- This clause captures when a supervisor starts in a new unit. Specifically:
      -- 1. the unit side of null -> unit transitions
      -- 2. the B side of unit A -> unit B transitions
      -- (previous_parole_unit_continued is null OR previous_parole_unit_continued <> ParoleUnit)
      NOT previous_unit_is_same OR
      -- This clause captures when a supervisor ends with a unit. Specifically:
      -- 1. the unit side of unit -> null transitions
      -- 2. the A side of unit A -> unit B transitions
      NOT next_unit_is_same
    )
  ), 
  -- `proto_staff_location_periods_2` sets the correct end_date for the rows we will
  -- eventually keep. Remember that each proto_period is either: 
  --  1. A pair of periods, where the first period has the correct start date and the
  --  second period has the correct end date 
  --  2. A single period wich already has the correct end date since it's only a week
  --  long.  
  -- This view will select the correct end_date for each of these situations.
  proto_staff_location_periods_2 AS (
    SELECT
      start_date,
      -- Usually we want to use the next periods end date, but if it's a single week
      -- period, we already have the correct end date.
      IF(NOT previous_unit_is_same AND NOT next_unit_is_same,
        end_date,
        LEAD(end_date) OVER (PARTITION BY BadgeNumber, ParoleUnit ORDER BY start_date)
      ) as end_date,
      BadgeNumber,
      ParoleUnit,
      previous_unit_is_same,
      next_unit_is_same
    FROM proto_staff_location_periods
  )
  -- Finally, we keep only the "starting" proto periods which now have the correct
  -- end_dates.
  SELECT *
  FROM proto_staff_location_periods_2
  WHERE NOT previous_unit_is_same
),
-- `supervisor_location_periods` is the second of the large CTEs. We construct
-- supervisor location periods through similar logic to above, but retaining only
-- supervisor Agents, and not caring about where they supervise clients.
supervisor_location_periods AS (
  -- 
  WITH supervisors AS (
    SELECT
      DISTINCT BadgeNumber,
      ParoleUnit,
      update_datetime,
    FROM AgentParole__ALL_generated_view
    WHERE
      BadgeNumber IS NOT NULL
      AND AgentClassification IN ('Parole Agent II, AP (Supv)', 'Parole Agent III, AP')
  ),
  -- This CTE will contain all update datetimes crossed with all badge numbers. We'll
  -- use this later to see which parole unit(s) each supervisor is associated with at
  -- any given time, with nulls in between. We call it possible_ParoleUnit and
  -- possible_BadgeNumber because we don't know if there was actually a supervisor then
  -- until we join back to agents in the next CTE.
  all_dates_cross_badge_units AS (
    SELECT
      s.BadgeNumber AS possible_BadgeNumber,
      s.ParoleUnit AS possible_ParoleUnit,
      ud.update_datetime AS start_date,
      LEAD(ud.update_datetime) OVER (PARTITION BY s.BadgeNumber, s.ParoleUnit ORDER BY update_datetime) AS end_date,
    FROM 
      (SELECT DISTINCT update_datetime FROM AgentParole__ALL_generated_view) ud,
      (SELECT DISTINCT BadgeNumber, ParoleUnit FROM supervisors) s 
  ),
  filled_all_dates AS (
    SELECT
      ad.start_date,
      ad.end_date,
      ad.possible_ParoleUnit,
      ad.possible_BadgeNumber,
      s.BadgeNumber, -- If there is actually a supervisor, this will be populated
      s.ParoleUnit,
    FROM all_dates_cross_badge_units ad
    LEFT JOIN supervisors s
    ON
      s.BadgeNumber = ad.possible_BadgeNumber
      AND s.update_datetime = ad.start_date
      AND s.ParoleUnit = ad.possible_ParoleUnit 
  ),
  -- `proto_supervisor_location_periods` drops any periods where there was not a change in
  -- ParoleUnit. The result is two "proto periods" that reflect the start and end of
  -- location periods (IE the first period has the correct start_date, the second period
  -- as the correct end_date). If the period is only a single week, there will only be 1
  -- "proto period". In the next CTE, we combine these periods into one.
  proto_supervisor_location_periods AS (
    SELECT
      start_date,
      end_date,
      possible_ParoleUnit,
      possible_BadgeNumber,
      BadgeNumber,
      ParoleUnit,
      LAG(ParoleUnit) OVER (PARTITION BY possible_BadgeNumber, possible_ParoleUnit ORDER BY start_date) IS NOT NULL AS previous_unit_is_same,
      LEAD(ParoleUnit) OVER (PARTITION BY possible_BadgeNumber, possible_ParoleUnit ORDER BY start_date) IS NOT NULL AS next_unit_is_same
    FROM
      filled_all_dates 
    WHERE True -- Required because of https://github.com/google/zetasql/issues/124
    QUALIFY (
      ParoleUnit IS NOT NULL AND (
        -- This clause captures when a supervisor starts in a new unit. Specifically:
        -- 1. the unit side of null -> unit transitions
        -- 2. the B side of unit A -> unit B transitions
        -- (previous_parole_unit_continued is null OR previous_parole_unit_continued <> ParoleUnit)
        NOT previous_unit_is_same OR
        -- This clause captures when a supervisor ends with a unit. Specifically:
        -- 1. the unit side of unit -> null transitions
        -- 2. the A side of unit A -> unit B transitions
        NOT next_unit_is_same
      )
    ) 
  ),
  -- `proto_supervisor_location_periods_2` sets the correct end_date for the rows we
  -- will eventually keep. Remember that each proto_period is either: 
  --  1. A pair of periods, where the first period has the correct start date and the
  --  second period has the correct end date 
  --  2. A single period wich already has the correct end date since it's only a week
  --  long.  
  -- This view will select the correct end_date for each of these situations.
  proto_supervisor_location_periods_2 AS (
    SELECT
      start_date,
      -- Usually we want to use the next periods end date, but if it's a single week period, 
      -- we already have the correct end date.
      IF(NOT previous_unit_is_same AND NOT next_unit_is_same,
        end_date,
        LEAD(end_date) OVER (PARTITION BY BadgeNumber, ParoleUnit ORDER BY start_date)
      ) as end_date,
      BadgeNumber,
      ParoleUnit,
      previous_unit_is_same,
      next_unit_is_same
    FROM
      proto_supervisor_location_periods
  )
  -- Finally, we keep only the "starting" proto periods which now have the correct
  -- end_dates.
  SELECT *
  FROM proto_supervisor_location_periods_2
  WHERE NOT previous_unit_is_same
)
-- Lastly, we join the two location CTEs together, and add the `type` column to identify
-- if period reflects a PA I period or a Supervisor period. We use this to fill out the
-- role periods this view also hydrates.
SELECT
  BadgeNumber,
  start_date,
  end_date,
  ParoleUnit,
  'PA I, AP' as type
FROM pa_1_location_periods

UNION DISTINCT

SELECT
  BadgeNumber,
  start_date,
  end_date, 
  ParoleUnit,
  'Supv' as type -- We don't care if they're a PA II or PA III right now. TODO(#28927)
FROM supervisor_location_periods
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ca",
    ingest_view_name="staff_location_and_role_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
