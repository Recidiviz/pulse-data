# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Query containing incarceration period information extracted from multiple PADOC files."""

from recidiviz.ingest.direct.controllers.direct_ingest_big_query_view_types import \
    DirectIngestPreProcessedIngestViewBuilder
from recidiviz.utils.environment import GAE_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


VIEW_QUERY_TEMPLATE = """
WITH movements_base AS (
  SELECT 
      m.mov_cnt_num AS control_number,
      m.mov_cur_inmt_num AS inmate_number,
      m.mov_move_date AS move_date,
      m.mov_move_time AS move_time,
      m.mov_sent_stat_cd AS sentence_status_code,
      m.parole_stat_cd AS parole_status_code,
      m.mov_move_code AS movement_code,
      CAST(mov_seq_num AS INT64) AS sequence_number,
      CASE 
        -- When the person is on parole, we null out any location that may have ended up in the move_to_loc col
        WHEN m.mov_move_to_loc = '' OR  m.mov_sent_stat_cd = 'P' THEN NULL 
        ELSE m.mov_move_to_loc END
      AS move_location,
      m.mov_sent_stat_cd = 'P' AS is_on_parole,
      m.parole_stat_cd IN ('TPV', 'CPV', 'TCV') AS is_confirmed_parole_violator_parole_status
  FROM {dbo_Movrec} m
),
movements AS (
  SELECT 
    *,
    LAST_VALUE(move_location IGNORE NULLS) OVER (
        PARTITION BY control_number, is_on_parole
        ORDER BY sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS location,
    LAG(is_confirmed_parole_violator_parole_status) OVER (
        PARTITION BY control_number 
        ORDER BY sequence_number
    ) AS previous_is_confirmed_parole_violator_parole_status
  FROM movements_base
),
movements_with_location_change_identifier AS (
    SELECT 
      *,
      CASE WHEN sequence_number - previous_sequence_number_within_location = 1 THEN 0 ELSE 1 END AS new_location
    FROM (
      SELECT 
        *,
        LAG(sequence_number) OVER (PARTITION BY control_number, location 
                                   ORDER BY sequence_number) AS previous_sequence_number_within_location
      FROM movements 
    )
),
critical_movements AS (
  SELECT
    *,
    LAG(sequence_number) OVER (
        PARTITION BY control_number
        ORDER BY sequence_number
    ) AS prev_critical_movement_sequence_number,
    (
        NOT previous_is_confirmed_parole_violator_parole_status AND is_confirmed_parole_violator_parole_status
    ) AS is_confirmed_parole_violation_movement
  FROM movements_with_location_change_identifier
  WHERE
    -- This person changed locations or was released to parole - we will create a new period when this happens
    new_location = 1 
    -- This person has transitioned from Parole Violator Pending to actual convicted parole violator
    OR (NOT previous_is_confirmed_parole_violator_parole_status AND is_confirmed_parole_violator_parole_status)
),
periods AS (
  SELECT
    start_movement.control_number,
    start_movement.inmate_number,
    start_movement.sequence_number,
    start_movement.is_on_parole,
    start_movement.move_date AS start_movement_date,
    end_movement.move_date AS end_movement_date,
    start_movement.location,
    start_movement.sentence_status_code AS start_sentence_status_code,
    end_movement.sentence_status_code AS end_sentence_status_code,
    start_movement.parole_status_code AS start_parole_status_code,
    end_movement.parole_status_code AS end_parole_status_code,
    start_movement.movement_code AS start_movement_code,
    end_movement.movement_code AS end_movement_code,
    'S' AS sentence_type,
    CASE WHEN start_movement.is_confirmed_parole_violation_movement THEN 1 ELSE 0 END AS is_parole_violation_admission
  FROM 
    critical_movements start_movement
  LEFT OUTER JOIN
    critical_movements end_movement
  ON start_movement.control_number = end_movement.control_number 
    AND end_movement.prev_critical_movement_sequence_number = start_movement.sequence_number
),
filtered_periods AS (
  SELECT * EXCEPT (is_on_parole, is_parole_violation_admission)
  FROM periods
  WHERE
    NOT is_on_parole
    -- Termination movement codes
    AND start_movement_code NOT IN ('D', 'DA', 'DIT')
    -- This person is on a bus being transferred between locations
    AND location != 'BUS'
    -- TODO(3447): Is this the right thing to do here? Do we want to include these people in the incarceration count?
    AND start_sentence_status_code != 'WT'
)
-- TODO(3447): This query produces numbers that are off by about 2.5 - should be 47K for 12/31/2017 per this site: 
-- https://nicic.gov/state-statistics/2017/pennsylvania#:~:text=As%20of%20December%2031%2C%202017,and%20budget%20of%20%242.28%20billion.
-- TODO(3447): Investigate why there are a bunch of non-prison location codes in the results    
SELECT *
FROM filtered_periods
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region='us_pa',
    ingest_view_name='incarceration_period',
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols='control_number, sequence_number',
)

if __name__ == '__main__':
    with local_project_id_override(GAE_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
