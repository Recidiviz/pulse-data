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
"""This query creates a table of associated (control_number, state_id, parole_number) tuples along with a
master_state_id col which tells us which collections of ids correspond to the same real world person. If the
master_state_id is NULL, then there are no state ids associated with this person and the nonnull id in that row (will be
either a parole_number or control_number) is the only ID associated with this person.

The query does a sufficient # of iterations of a recursive walk along all control_number <-> state_id and
state_id <-> parole_number edges to cluster all groups of associated edges around a single master state_id.

Since the state_id <-> parole_number relationship in `dbo_Offender` is strictly {0,1} to many, we just have to do a full
outer join on that table to connect any parole numbers to their single state id.

However, since the control_number <-> state_id relationship in `dbo_tblSearchInmateInfo` is many to many, we need to do
several rounds of self-joins until we find no rows with control numbers with different recidiviz_master_person_ids.

Here's the degenerate situation that actually happens in PA that this is solving for:
C1 - S1
   /
C2 - S2
   /
C3 - S3
   /
C4

In this example, all rows with control numbers (C1, C2, C3, C4) or state_ids (S1, S2, S3) end up with master_state_id
S1.
"""

EXPLORE_GRAPH_BY_COLUMN_TEMPLATE = """SELECT
        control_number, state_id, parole_number,
        IF(new_master_state_id_candidate IS NULL OR master_state_id < new_master_state_id_candidate,
           master_state_id, new_master_state_id_candidate) AS master_state_id
      FROM (
        SELECT 
          control_number, state_id, parole_number, master_state_id,
          MIN(new_row_master_state_id_candidate) OVER (PARTITION BY master_state_id) AS new_master_state_id_candidate
        FROM (
          SELECT 
            primary.control_number, primary.state_id, primary.parole_number, primary.master_state_id,
            MIN(control_joins.master_state_id) AS new_row_master_state_id_candidate
          FROM 
            {base_table_name} primary
          LEFT OUTER JOIN
            {base_table_name} control_joins
          ON primary.{join_col_name} = control_joins.{join_col_name} 
            AND primary.master_state_id > control_joins.master_state_id
          GROUP BY primary.control_number, primary.state_id, primary.parole_number, primary.master_state_id
        )
      )"""


def explore_graph_by_column_query(
        base_table_name: str,
        join_col_name: str) -> str:
    """Generates a query that explores one level of graph edges along the provided |join_col_name| column, merging
    clusters of master_state_id that are linked by that edge.
    """
    return EXPLORE_GRAPH_BY_COLUMN_TEMPLATE.format(
        base_table_name=base_table_name,
        join_col_name=join_col_name,
    )


MASTER_STATE_IDS_FRAGMENT = f"""recidiviz_master_person_ids AS (
    WITH
    distinct_control_to_sid AS (
        SELECT
          DISTINCT
          -- Sometimes parole numbers (which contain letters) are transposed into the state id column in 
          -- this table - clear these out
          IF(REGEXP_CONTAINS(state_id_num, r"[A-Z]") OR state_id_num = '', NULL, state_id_num) AS state_id,
          control_number
        FROM {{dbo_tblSearchInmateInfo}}
    ),
    control_number_to_state_id_edges AS (
      SELECT * 
      FROM distinct_control_to_sid
      WHERE 
      -- Due to inconsistencies in dbo_tblSearchInmateInfo, distinct_control_to_sid sometimes will have two rows like 
      -- (control_num_A, <null>) and (control_num_A, control_num_B). We want to throw out all rows that have one null
      -- in the pair, unless that is the only place that singluar ID is referenced.
      (state_id IS NOT NULL 
        OR control_number NOT IN (SELECT control_number FROM distinct_control_to_sid WHERE state_id IS NOT NULL))
      AND 
      (control_number IS NOT NULL 
        OR state_id NOT IN (SELECT state_id FROM distinct_control_to_sid WHERE control_number IS NOT NULL))
    ),
    state_id_to_parole_number_edges AS (
      SELECT 
        -- Set garbage ids to null so we don't treat these as the same person
        IF(
          TRIM(OffSID) != '' 
          AND OffSID NOT IN (
            '0', '00', '0000', '00000', '0000000', '00000000', '000000000', '00000001', '000000001', '000000003',
            '000000010', '000000012', '000000013'
          )
          -- Some random ids are like '.' or other random chars - get rid of these
          AND SAFE_CAST(OffSID AS INT64) IS NOT NULL, 
          TRIM(OffSID),
          NULL
        ) AS state_id,
        ParoleNumber AS parole_number
      FROM {{dbo_Offender}}
    ),
    base_table AS (
      SELECT control_number, state_id, parole_number, state_id AS master_state_id
      FROM 
        control_number_to_state_id_edges
      FULL OUTER JOIN
        state_id_to_parole_number_edges
      USING (state_id)
      GROUP BY control_number, state_id, parole_number, master_state_id
    ),
    explore_control_number AS (
        {explore_graph_by_column_query(base_table_name='base_table',
                                       join_col_name='control_number')}
    ),
    explore_state_id_1 AS (
        {explore_graph_by_column_query(base_table_name='explore_control_number',
                                       join_col_name='state_id')}
    ),
    explore_state_id_2 AS (
        {explore_graph_by_column_query(base_table_name='explore_state_id_1',
                                       join_col_name='state_id')}),
    master_state_ids AS (
        SELECT control_number, state_id, parole_number, master_state_id
        FROM explore_state_id_2
        GROUP BY control_number, state_id, parole_number, master_state_id
    )
    SELECT
        CASE 
            WHEN master_state_id IS NOT NULL THEN CONCAT('RECIDIVIZ_MASTER_STATE_ID_', master_state_id)
            WHEN control_number IS NOT NULL THEN CONCAT('RECIDIVIZ_MASTER_CONTROL_NUMBER_', control_number)
            WHEN parole_number IS NOT NULL THEN CONCAT('RECIDIVIZ_MASTER_PAROLE_NUMBER_', parole_number)
        END AS recidiviz_master_person_id,
        control_number, state_id, parole_number
    FROM master_state_ids
)"""
