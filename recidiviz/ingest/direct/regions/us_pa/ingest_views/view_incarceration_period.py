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
from recidiviz.ingest.direct.regions.us_pa.ingest_views.templates_person_external_ids import MASTER_STATE_IDS_FRAGMENT
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


VIEW_QUERY_TEMPLATE = f"""
WITH 
{MASTER_STATE_IDS_FRAGMENT},
movements_base AS (
  SELECT
      -- In a few old cases, we see a control number with no corresponding row match in recidiviz_master_person_ids -
      -- in this case, we fall back on the control number in dbo_Movrec
      COALESCE(recidiviz_master_person_id, m.mov_cnt_num) AS recidiviz_master_person_id,
      COALESCE(control_number, m.mov_cnt_num) AS control_number,
      m.mov_cur_inmt_num AS inmate_number,
      m.mov_move_date AS move_date,
      m.mov_sent_stat_cd AS sentence_status_code,
      m.parole_stat_cd AS parole_status_code,
      m.mov_move_code AS movement_code,
      -- Sort by date, only using PA sequence numbers when the date is the same. Sort death statuses after all other 
      -- entries no matter what. No bringing people back to life.
      ROW_NUMBER() OVER (PARTITION BY recidiviz_master_person_id 
                         ORDER BY 
                            m.mov_move_date, 
                            IF(m.mov_sent_stat_cd IN ('DA', 'DN', 'DS', 'DX', 'DZ'), 99999, CAST(mov_seq_num AS INT64))
      ) AS sequence_number,
      CASE 
        WHEN
          -- When the person is on parole, we null out any location that may have ended up in the move_to_loc col - we 
          -- only want to keep incarceration facilities here, but PA uses this column to record the county/state that
          --  this person is serving parole in as well.
          m.mov_sent_stat_cd = 'P'

          -- Any deletion row means they are going to a location that is not an incarceration facility
          OR m.mov_move_code IN ('D', 'DA', 'DIT')

          -- This person is on a bus being transferred between locations - do not use this location. We will count this
          -- person towards their previous location until they are checked into a new location.
          OR m.mov_move_to_loc = 'BUS'
          -- If the person is out on a WRIT/ATA, they may have moved to a county jail temporarily, but we count them as 
          -- being under the jurisdiction of the PA DOC and their permanent location is tracked as the facility they
          -- came from - ignore this person's location change for now.
          OR m.mov_sent_stat_cd = 'WT'
        THEN NULL 
        ELSE m.mov_move_to_loc END
      AS move_location,
      m.mov_move_code IN ('D', 'DA', 'DIT') AS is_delete_movement,
      m.parole_stat_cd IN ('TPV', 'CPV', 'TCV') AS is_confirmed_parole_violator_parole_status,
  FROM {{dbo_Movrec}} m
  LEFT OUTER JOIN (
    SELECT DISTINCT recidiviz_master_person_id, control_number 
    FROM recidiviz_master_person_ids 
    WHERE control_number IS NOT NULL
  ) ids
  ON m.mov_cnt_num = ids.control_number
  WHERE
    -- This is the 'Bogus record' flag, if 'Y', indicates if a record should be ignored, 'N' otherwise.
    m.mov_rec_del_flag = 'N'
),
movements AS (
  SELECT 
    *,
    LAST_VALUE(move_location IGNORE NULLS) OVER (
        PARTITION BY recidiviz_master_person_id
        ORDER BY sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS location,
    LAG(is_confirmed_parole_violator_parole_status) OVER (
        PARTITION BY recidiviz_master_person_id 
        ORDER BY sequence_number
    ) AS previous_is_confirmed_parole_violator_parole_status
  FROM movements_base
),
movements_with_inflection_indicators AS (
    SELECT 
      *,
      CASE WHEN sequence_number - previous_sequence_number_within_location = 1 THEN 0 ELSE 1 END AS new_location,
      (NOT previous_is_confirmed_parole_violator_parole_status 
        AND is_confirmed_parole_violator_parole_status) AS is_new_revocation,
    FROM (
      SELECT 
        *,
        LAG(sequence_number) OVER (PARTITION BY recidiviz_master_person_id, location 
                                   ORDER BY sequence_number) AS previous_sequence_number_within_location,
        LAG(is_delete_movement) OVER (PARTITION BY recidiviz_master_person_id
                                      ORDER BY sequence_number) AS prev_is_delete_movement,
      FROM movements 
    )
),
critical_movements AS (
  SELECT
    *,
    LAG(sequence_number) OVER (PARTITION BY recidiviz_master_person_id
                               ORDER BY sequence_number) AS prev_critical_movement_sequence_number
  FROM movements_with_inflection_indicators
  WHERE
    -- This person changed locations or was released to parole - we will create a new period when this happens
    new_location = 1 
    -- This person has transitioned from Parole Violator Pending to actual convicted parole violator
    OR is_new_revocation
    -- This is a release and does not indicate a status change while in the same location
    OR is_delete_movement
    -- This record comes after the person has been released - we need to make sure to include this (could be same
    -- facility as the release row, so new_location = 1 may not indicate that they have changed)
    OR prev_is_delete_movement
),
sentence_types AS (
  SELECT 
    curr_inmate_num AS inmate_number,
    -- It seems there is only ever one sentence per inmate number, so this is just a precaution
    MAX(type_of_sent) AS sentence_type
  FROM {{dbo_Senrec}}
  GROUP BY curr_inmate_num
),
periods AS (
  SELECT
    start_movement.control_number,
    start_movement.inmate_number,
    ROW_NUMBER() OVER (PARTITION BY start_movement.recidiviz_master_person_id
                       ORDER BY start_movement.sequence_number) AS sequence_number,
    start_movement.move_date AS start_movement_date,
    end_movement.move_date AS end_movement_date,
    start_movement.location,
    start_movement.sentence_status_code AS start_sentence_status_code,
    end_movement.sentence_status_code AS end_sentence_status_code,
    start_movement.parole_status_code AS start_parole_status_code,
    end_movement.parole_status_code AS end_parole_status_code,
    start_movement.movement_code AS start_movement_code,
    end_movement.movement_code AS end_movement_code,
    start_movement.is_new_revocation AS start_is_new_revocation,
    sentence_types.sentence_type,
  FROM 
    critical_movements start_movement
  LEFT OUTER JOIN
    critical_movements end_movement
  ON start_movement.recidiviz_master_person_id = end_movement.recidiviz_master_person_id 
    AND end_movement.prev_critical_movement_sequence_number = start_movement.sequence_number
  LEFT OUTER JOIN
    sentence_types
  ON start_movement.inmate_number = sentence_types.inmate_number 
  WHERE NOT start_movement.is_delete_movement
)
SELECT *
FROM periods
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region='us_pa',
    ingest_view_name='incarceration_period',
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols='control_number, sequence_number',
    materialize_raw_data_table_views=True
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
