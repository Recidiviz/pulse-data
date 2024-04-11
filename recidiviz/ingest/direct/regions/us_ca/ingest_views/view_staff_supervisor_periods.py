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
"""Query to define supervisor relationships between agents with a caseload (whether they
are PA Is or PA IIs) and their supervisors. 

In CA, supervisor periods are created based on the Parole Unit someone is in. If you are
in a supervisor position (PA II and PA III) within a unit, you will have supervisor
periods for all agents with a caseload in that unit.

A PA II or a PA III is considered a supervisor of any unit they are listed under that unit in
AgentParole. A PA I (or PA II if they have a caseload) is considered as working in a
unit if they have a caseload in that unit. If they have a caseload in multiple units at
the same time (supposedly impossible, but the case for about 56 people on certain
weeks), we consider that PA I to operate in whichever unit they have the most clients in
(see #28597). I chose not to care about what AgentParole says for the supervisee
information -- this is because if AgentParole said a PA was in unit 2, but they're
supervising 50 clients in unit 3, they're clearly in unit 3.

### Notes
1. May to July 2023 we stopped receiving badge numbers, so there are breaks in the
supervision periods for this time.
"""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH agents AS (
  SELECT
    DISTINCT BadgeNumber,
    ParoleUnit,
    update_datetime,
  FROM {AgentParole@ALL}
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
    a.BadgeNumber AS possible_BadgeNumber,
    a.ParoleUnit AS possible_ParoleUnit,
    ud.update_datetime AS start_date,
    LEAD(ud.update_datetime) OVER (PARTITION BY a.BadgeNumber, a.ParoleUnit ORDER BY update_datetime) AS end_date,
  FROM 
    (SELECT DISTINCT update_datetime FROM {AgentParole@ALL}) ud,
    (SELECT DISTINCT BadgeNumber, ParoleUnit FROM agents) a 
),
filled_all_dates AS (
  SELECT
    ad.start_date,
    ad.end_date,
    ad.possible_ParoleUnit,
    ad.possible_BadgeNumber,
    a.BadgeNumber, -- If there is actually a supervisor, this will be populated
    a.ParoleUnit,
  FROM all_dates_cross_badge_units ad
  LEFT JOIN agents a
  ON
    a.BadgeNumber = ad.possible_BadgeNumber
    AND a.update_datetime = ad.start_date
    AND a.ParoleUnit = ad.possible_ParoleUnit 
),
proto_supervisor_periods AS (
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
  WHERE True
  -- The result of this QUALIFY will be to have two proto periods that will eventually
  -- become one. The first proto period reflects the first week this supervisor worked
  -- within this unit, and the next protoperiod (if it exists) will be the last week
  -- this person worked on the unit. In the next CTE, we take the start date from the
  -- first proto period and the end date from the second to make our final period.
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
proto_supervisor_periods_2 AS (
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
    proto_supervisor_periods
),
-- This CTE simply drops the "second" proto periods since we already got the end dates
-- from them in the proto_supervisor_periods_2. We have to use a separate CTE for this
-- because WHERE clauses are evaluated before window functions.
collapsed_supervisor_periods AS (
  SELECT *
  FROM proto_supervisor_periods_2
  WHERE NOT previous_unit_is_same
),
-- This CTE starts to identify where agents have caseloads.
staff_location_periods AS (
  WITH staff_PersonParole AS (
    SELECT
      BadgeNumber,
      ParoleUnit,
      update_datetime,
      -- TODO(#28597) Supposedly officers can only supervise folks in a single
      -- district at a time. However, we have 56 agents on specific days when this
      -- does not appear to be true. In these situations, we just say the officer is
      -- supervised in whatever unit they have the most clients in. We use the qualify
      -- statement below to assign PAs to the case where they have the most cases.
      COUNT(DISTINCT OffenderId) caseload_in_unit
    FROM
      {PersonParole@ALL}
    WHERE
      BadgeNumber IS NOT NULL
    GROUP BY
      BadgeNumber,
      ParoleUnit,
      update_datetime 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY BadgeNumber, update_datetime ORDER BY caseload_in_unit DESC) = 1 
  ),
  prioritized AS (
    SELECT
      BadgeNumber,
      ParoleUnit,
      update_datetime,
      LAG(ParoleUnit) OVER (PARTITION BY BadgeNumber ORDER BY update_datetime) AS previous_parole_unit
    FROM
      staff_PersonParole 
    WHERE True
    QUALIFY previous_parole_unit IS NULL OR previous_parole_unit != ParoleUnit 
  )
  SELECT
    BadgeNumber,
    ParoleUnit,
    update_datetime AS start_date,
    LEAD(update_datetime) OVER (PARTITION BY BadgeNumber ORDER BY update_datetime) AS end_date
  FROM prioritized 
),
-- This is a relatively complicated CTE! It unions two tables together, each of which
-- starts with staff location periods (which is where agents have caseloads) and joins
-- in supervisor periods. 
-- 
-- The first table in the union merges in periods during which a supervisor moves into
-- a unit while an agent is already in the unit (therefore a new supervisor period
-- needs to be started for this agent and others in the unit).
--
-- The second table in the union considers when a supervisor is already working in a
-- unit when the staff joins the unit. The start date here will always be when the
-- staff joined the unit, but the period may end if the supervisor changes units
-- before the staff member does.
supervisors_joined_to_supervisees AS (
  WITH all_starts AS (
    SELECT
      slp.BadgeNumber AS supervisee_BadgeNumber,
      slp.ParoleUnit AS ParoleUnit,
      csp_moving_into_unit.start_date AS start_date,
      LEAST(IFNULL(csp_moving_into_unit.end_date, '9999-12-31'), IFNULL(slp.end_date, '9999-12-31')) AS end_date,
      csp_moving_into_unit.BadgeNumber AS supervisor_BadgeNumber
    FROM
      staff_location_periods slp
      -- Join in starts caused by supervisors moving into the unit
    LEFT JOIN collapsed_supervisor_periods csp_moving_into_unit
    ON
      csp_moving_into_unit.ParoleUnit = slp.ParoleUnit AND (
        csp_moving_into_unit.start_date >= slp.start_date AND (
          csp_moving_into_unit.start_date < slp.end_date OR slp.end_date IS NULL
        )
      )
    
    UNION DISTINCT
    
    SELECT
      slp.BadgeNumber AS supervisee_BadgeNumber,
      slp.ParoleUnit AS ParoleUnit,
      slp.start_date AS start_date,
      LEAST(IFNULL(csp_already_supervising_unit.end_date, '9999-12-31'), IFNULL(slp.end_date, '9999-12-31')),
      csp_already_supervising_unit.BadgeNumber AS supervisor_BadgeNumber
    FROM staff_location_periods slp
      -- Join in starts caused by supervisees moving into the unit
    LEFT JOIN collapsed_supervisor_periods csp_already_supervising_unit
    ON
      csp_already_supervising_unit.ParoleUnit = slp.ParoleUnit AND (
        csp_already_supervising_unit.start_date <= slp.start_date AND (
          csp_already_supervising_unit.end_date > slp.start_date OR csp_already_supervising_unit.end_date IS NULL
        )
      ) 
  ),
  -- Normalizes by:
  -- 1) removing non-supervisor periods (which is where supervisor_BadgeNumber is null)
  -- 2) returning magic end dates to null
  normalized AS (
    SELECT
      supervisee_BadgeNumber,
      ParoleUnit,
      start_date,
      IF(end_date = '9999-12-31', NULL, end_date) AS end_date,
      supervisor_BadgeNumber
    FROM all_starts
    WHERE supervisor_BadgeNumber IS NOT NULL 
  )
  SELECT *
  FROM normalized 
)
SELECT *
FROM supervisors_joined_to_supervisees
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ca",
    ingest_view_name="staff_supervisor_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
