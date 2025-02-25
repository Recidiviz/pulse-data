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
  WITH
  -- `supervisee_location_and_role_periods` creates location + role periods for agents
  -- who we will eventually create supervisor_periods for. If an agent is supervising
  -- any clients, we will potentially create a supervisor period for that agent, with
  -- that agent as the supervisee (assuming there is a supervisor in their unit).
  supervisee_location_and_role_periods as (
    -- `agents_with_caseload` selects agents which have caseloads. Ultimately, this
    -- makes up the population of supervisees agents who are supervised by PA II and PA
    -- IIIs.
    WITH agents_with_caseload AS (
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
    -- `agents_with_caseload_and_classification` joins `agents_with_caseload` against
    -- AgentParole to determine the AgentClassification. Not infrequently, there is a
    -- disagreement between the ParoleUnit according to AgentParole and the ParoleUnit
    -- where the person actually seems to be supervising individuals in PersonParole. We
    -- chose to trust PersonParole over AgentParole as the source of truth here. This
    -- disagreement requires us to make some guesses about what an agents classification
    -- is when the agent is listed in different unit in AgentParole. See the below
    -- comment for how we handle this logic.
    agents_with_caseload_and_classification as (
      select distinct
        ac.BadgeNumber,
        ac.ParoleUnit,
        ac.update_datetime,
        -- If AgentParole says someone has a specific classification in some unit, say
        -- that they have that classification. If they don't have a classification for
        -- that unit, choose the classification they are most commonly associated with.
        -- If we STILL aren't sure what their classification is, default them to PA I.
        coalesce(
          ap_first_choice.AgentClassification,
          ap_second_choice.AgentClassification,
          'Parole Agent I, AP'
        ) as AgentClassification
      from agents_with_caseload ac
      -- TODO(#30137) Joining on update_datetime may fail here (and other places in this
      -- file) with automatic data pruning in the future.
      left join {AgentParole@ALL} ap_first_choice using (BadgeNumber, update_datetime, ParoleUnit)
      -- This subquery counts how many times an agent occurs with a certain
      -- AgentClassification for a given update_datetime. We then keep only the most
      -- commonly occuring AgentClassification, and potentially use this in the above
      -- COALESCE as ap_second_choice.
      left join (
        select BadgeNumber, update_datetime, AgentClassification, count(*) as occurences
        from {AgentParole@ALL} ap_second_choice
        group by BadgeNumber, update_datetime, AgentClassification
        qualify row_number() over (partition by BadgeNumber, update_datetime, AgentClassification order by occurences desc) = 1
      ) ap_second_choice using (BadgeNumber, update_datetime)
    ),
    -- This CTE will contain all update datetimes crossed with all badge numbers. We'll
    -- use this later to see which parole unit(s) each supervisor is associated with at
    -- any given time, with nulls in between. We call it possible_ParoleUnit and
    -- possible_BadgeNumber because we don't know if there was actually a supervisor
    -- then until we join back to agents in the next CTE.
    all_dates_cross_badge_units AS (
      SELECT
        s.BadgeNumber AS possible_BadgeNumber,
        s.ParoleUnit AS possible_ParoleUnit,
        s.AgentClassification AS possible_AgentClassification,
        ud.update_datetime AS start_date,
        LEAD(ud.update_datetime) OVER (PARTITION BY s.BadgeNumber, s.ParoleUnit, s.AgentClassification ORDER BY update_datetime) AS end_date,
      FROM
        (SELECT DISTINCT update_datetime FROM {PersonParole@ALL}) ud,
        (SELECT DISTINCT BadgeNumber, ParoleUnit, AgentClassification FROM agents_with_caseload_and_classification) s
    ),
    -- `filled_all_dates` joins supervisees into all_dates_cross_badge_units. The result
    -- are many records reflecting the time when a supervisee is in a specific location
    -- (ParoleUnit) with a specific role (AgentClassification). Next we turn in this
    -- into periods.
    filled_all_dates AS (
      SELECT
        ad.start_date,
        ad.end_date,
        ad.possible_ParoleUnit,
        ad.possible_BadgeNumber,
        ad.possible_AgentClassification,
        s.BadgeNumber, -- If there is actually an agent, this will be populated
        s.ParoleUnit,
        s.AgentClassification
      FROM all_dates_cross_badge_units ad
      LEFT JOIN agents_with_caseload_and_classification s
      ON
        s.BadgeNumber = ad.possible_BadgeNumber
        AND s.update_datetime = ad.start_date
        AND s.ParoleUnit = ad.possible_ParoleUnit
        and s.AgentClassification = ad.possible_AgentClassification
    ),
    -- `proto_supervisee_location_periods` drops any periods where there was not a
    -- change in ParoleUnit or agent classification. The result is two "proto periods"
    -- that reflect the start and end of location/role periods (IE the first period has
    -- the correct start_date, the second period as the correct end_date). If the period
    -- is only a single week, there will only be 1 "proto period". In the next CTE, we
    -- combine these periods into one.
    proto_supervisee_location_periods AS (
      SELECT
        start_date,
        end_date,
        possible_ParoleUnit,
        possible_BadgeNumber,
        possible_AgentClassification,
        BadgeNumber,
        ParoleUnit,
        AgentClassification,
        -- If the following or preceding BadgeNumber (or ParoleUnit or
        -- AgentClassification) is null, that means that on this date, the join in
        -- `filled_all_dates` failed, and that on this date, this officer is not X
        -- AgentClassification in Y ParoleUnit -- IE this location / role period should
        -- be closed, which we will do in the next couple CTEs.
        LAG(BadgeNumber) OVER (PARTITION BY possible_BadgeNumber, possible_ParoleUnit, possible_AgentClassification ORDER BY start_date) IS NOT NULL AS previous_unit_and_classification_is_same,
        LEAD(BadgeNumber) OVER (PARTITION BY possible_BadgeNumber, possible_ParoleUnit, possible_AgentClassification ORDER BY start_date) IS NOT NULL AS next_unit_and_classification_is_same
      FROM
        filled_all_dates
      WHERE True -- Required because of https://github.com/google/zetasql/issues/124
      QUALIFY (
        ParoleUnit IS NOT NULL AND (
          -- This clause captures when a supervisor starts in a new unit. Specifically:
          -- 1. the unit side of `null -> unit` transitions
          -- 2. the B side of `unit A -> unit B` transitions
          NOT previous_unit_and_classification_is_same OR
          -- This clause captures when a supervisor ends with a unit. Specifically:
          -- 1. the unit side of unit -> null transitions
          -- 2. the A side of unit A -> unit B transitions
          NOT next_unit_and_classification_is_same
        )
      )
    ),
    -- `proto_supervisee_location_periods_2` sets the correct end_date for the rows we
    -- will eventually keep. Remember that each proto_period is either:
    --  1. A pair of periods, where the first period has the correct start date and the
    --  second period has the correct end date
    --  2. A single period wich already has the correct end date since it's only a week
    --  long.
    -- This view will select the correct end_date for each of these situations.
    proto_supervisee_location_periods_2 AS (
      SELECT
        start_date,
        -- Usually we want to use the next periods end date, but if it's a single week period,
        -- we already have the correct end date.
        IF(NOT previous_unit_and_classification_is_same AND NOT next_unit_and_classification_is_same,
          end_date,
          LEAD(end_date) OVER (PARTITION BY BadgeNumber, ParoleUnit, AgentClassification ORDER BY start_date)
        ) as end_date,
        BadgeNumber,
        ParoleUnit,
        AgentClassification,
        previous_unit_and_classification_is_same,
        next_unit_and_classification_is_same
      FROM
        proto_supervisee_location_periods
    )
    -- Finally, we keep only the "starting" proto periods which now have the correct
    -- end_dates. We also convert the agent's role to a numeric value, which we use
    -- later to ensure agents only supervise agents of a lower classification.  
    SELECT *,
      CASE AgentClassification 
        WHEN 'Parole Agent I, AP' THEN 1
        WHEN 'Parole Agent II, AP (Supv)' THEN 2
      END AS AgentClassification_numeric
    FROM proto_supervisee_location_periods_2
    WHERE NOT previous_unit_and_classification_is_same
  ),
  -- `supervisor_location_periods` is the second of the large CTEs. We construct
  -- supervisor location periods through similar logic to above, but retaining only
  -- supervisor Agents, and not caring about where they supervise clients.
  supervisor_location_periods AS (
    -- `supervisors` selects PA IIs and PA IIIs from AgentParole.
    WITH supervisors AS (
      SELECT
        DISTINCT BadgeNumber,
        ParoleUnit,
        update_datetime,
        AgentClassification
      FROM {AgentParole@ALL}
      WHERE
        BadgeNumber IS NOT NULL
        AND AgentClassification IN ('Parole Agent II, AP (Supv)', 'Parole Agent III, AP')
    ),
    -- `all_dates_cross_badge_units` contains all update datetimes crossed with all
    -- badge numbers. We'll use this later to see which parole unit(s) each supervisor
    -- is associated with at any given time, with nulls in between. We call it
    -- possible_ParoleUnit and possible_BadgeNumber because we don't know if there was
    -- actually a supervisor then until we join back to agents in the next CTE.
    all_dates_cross_badge_units AS (
      SELECT
        s.BadgeNumber AS possible_BadgeNumber,
        s.ParoleUnit AS possible_ParoleUnit,
        s.AgentClassification AS possible_AgentClassification,
        ud.update_datetime AS start_date,
        LEAD(ud.update_datetime) OVER (PARTITION BY s.BadgeNumber, s.ParoleUnit, s.AgentClassification ORDER BY update_datetime) AS end_date,
      FROM
        (SELECT DISTINCT update_datetime FROM {AgentParole@ALL}) ud,
        (SELECT DISTINCT BadgeNumber, ParoleUnit, AgentClassification FROM supervisors) s
    ),
    -- `filled_all_dates` joins supervisors into all_dates_cross_badge_units. The result
    -- are many records reflecting the time when a supervisor is in a specific location
    -- (ParoleUnit) with a specific role (AgentClassification). Next we turn in this
    -- into periods.
    filled_all_dates AS (
      SELECT
        ad.start_date,
        ad.end_date,
        ad.possible_ParoleUnit,
        ad.possible_BadgeNumber,
        ad.possible_AgentClassification,
        s.BadgeNumber, -- If there is actually a supervisor, this will be populated
        s.ParoleUnit,
        s.AgentClassification
      FROM all_dates_cross_badge_units ad
      LEFT JOIN supervisors s
      ON
        s.BadgeNumber = ad.possible_BadgeNumber
        AND s.update_datetime = ad.start_date
        AND s.ParoleUnit = ad.possible_ParoleUnit
        and s.AgentClassification = ad.possible_AgentClassification
    ),
    -- `proto_supervisor_location_periods` drops any periods where there was not a
    -- change in ParoleUnit or agent classification. The result is two "proto periods"
    -- that reflect the start and end of location/role periods (IE the first period has
    -- the correct start_date, the second period as the correct end_date). If the period
    -- is only a single week, there will only be 1 "proto period". In the next CTE, we
    -- combine these periods into one.
    proto_supervisor_location_periods AS (
      SELECT
        start_date,
        end_date,
        possible_ParoleUnit,
        possible_BadgeNumber,
        possible_AgentClassification,
        BadgeNumber,
        ParoleUnit,
        AgentClassification,
        -- If the following or preceding BadgeNumber (or ParoleUnit or
        -- AgentClassification) is null, that means that on this date, the join in
        -- `filled_all_dates` failed, and that on this date, this officer is not X
        -- AgentClassification in Y ParoleUnit -- IE this location / role period should
        -- be closed, which we will do in the next couple CTEs.
        LAG(BadgeNumber) OVER (PARTITION BY possible_BadgeNumber, possible_ParoleUnit, possible_AgentClassification ORDER BY start_date) IS NOT NULL AS previous_unit_and_classification_is_same,
        LEAD(BadgeNumber) OVER (PARTITION BY possible_BadgeNumber, possible_ParoleUnit, possible_AgentClassification ORDER BY start_date) IS NOT NULL AS next_unit_and_classification_is_same
      FROM
        filled_all_dates
      WHERE True -- Required because of https://github.com/google/zetasql/issues/124
      QUALIFY (
        ParoleUnit IS NOT NULL AND (
          -- This clause captures when a supervisor starts in a new unit. Specifically:
          -- 1. the unit side of `null -> unit` transitions
          -- 2. the B side of `unit A -> unit B` transitions
          NOT previous_unit_and_classification_is_same OR
          -- This clause captures when a supervisor ends with a unit. Specifically:
          -- 1. the unit side of `unit -> null` transitions
          -- 2. the A side of `unit A -> unit B` transitions
          NOT next_unit_and_classification_is_same
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
        -- Usually we want to use the next periods end date, but if it's a single week
        -- period, we already have the correct end date.
        IF(NOT previous_unit_and_classification_is_same AND NOT next_unit_and_classification_is_same,
          end_date,
          LEAD(end_date) OVER (PARTITION BY BadgeNumber, ParoleUnit, AgentClassification ORDER BY start_date)
        ) as end_date,
        BadgeNumber,
        ParoleUnit,
        AgentClassification,
        previous_unit_and_classification_is_same,
        next_unit_and_classification_is_same
      FROM
        proto_supervisor_location_periods
    )
    -- Finally, we keep only the "starting" proto periods which now have the correct
    -- end_dates. We also convert the agent's role to a numeric value, which we use
    -- later to ensure agents only supervise agents of a lower classification. 
    SELECT *,
      CASE AgentClassification 
        WHEN 'Parole Agent II, AP (Supv)' THEN 2
        WHEN 'Parole Agent III, AP' THEN 3
      END AS AgentClassification_numeric
    FROM proto_supervisor_location_periods_2
    WHERE NOT previous_unit_and_classification_is_same
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
        supervisee_location_and_role_periods slp
        -- Join in starts caused by supervisors moving into the unit
      LEFT JOIN supervisor_location_periods csp_moving_into_unit
      ON
        csp_moving_into_unit.ParoleUnit = slp.ParoleUnit AND (
          csp_moving_into_unit.start_date >= slp.start_date AND (
            csp_moving_into_unit.start_date < slp.end_date OR slp.end_date IS NULL
          )
        )
        AND csp_moving_into_unit.BadgeNumber != slp.BadgeNumber
        AND csp_moving_into_unit.AgentClassification_numeric > slp.AgentClassification_numeric

      UNION DISTINCT

      SELECT
        slp.BadgeNumber AS supervisee_BadgeNumber,
        slp.ParoleUnit AS ParoleUnit,
        slp.start_date AS start_date,
        LEAST(IFNULL(csp_already_supervising_unit.end_date, '9999-12-31'), IFNULL(slp.end_date, '9999-12-31')),
        csp_already_supervising_unit.BadgeNumber AS supervisor_BadgeNumber
      FROM supervisee_location_and_role_periods slp
        -- Join in starts caused by supervisees moving into the unit
      LEFT JOIN supervisor_location_periods csp_already_supervising_unit
      ON
        csp_already_supervising_unit.ParoleUnit = slp.ParoleUnit AND (
          csp_already_supervising_unit.start_date <= slp.start_date AND (
            csp_already_supervising_unit.end_date > slp.start_date OR csp_already_supervising_unit.end_date IS NULL
          )
        )
      AND csp_already_supervising_unit.BadgeNumber != slp.BadgeNumber
      AND csp_already_supervising_unit.AgentClassification_numeric > slp.AgentClassification_numeric
    ),
    -- Normalizes by:
    -- 1) removing non-supervisor periods (when supervisor_BadgeNumber is null)
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
