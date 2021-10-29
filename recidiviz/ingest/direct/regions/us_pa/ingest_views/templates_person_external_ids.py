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
"""This query creates a table of associated (control_number, inmate_number,
parole_number) tuples along with a primary_control_number col which tells us which
collections of ids correspond to the same real world person. If the
primary_control_number is NULL, then there are no control numbers associated with this
person and the nonnull IDs in that row (will be a parole_number and/or inmate number)
are the only IDs associated with this person.

The query does a sufficient # of iterations of a recursive walk along all control_number
<-> inmate_number and inmate_number <-> parole_number edges to cluster all groups of
associated edges around a single primary control_number.

Since the inmate_number <-> control_number relationship in `dbo_tblSearchInmateInfo` is
strictly {0,1} to many, we just have to do a full outer join on that table to connect
any parole numbers to their single state id.

However, since the parole_number <-> inmate relationship in `dbo_ParoleCount`
is many to many (though primarly 1:1), we need to do several rounds of self-joins until
we find no rows with parole numbers with different recidiviz_primary_person_ids.

Here's the degenerate situation that actually happens in PA that this is solving for:
C1 - I1 - P1
  \
     I2 - P2
        /
C2 - I3 - P3
  \
    I4
In this example, all rows with control numbers (C1, C2) or parole_numbers (P1, P2, or P3)
end up with primary_control_number C1.
"""

from recidiviz.utils.string import StrictStringFormatter

EXPLORE_GRAPH_BY_COLUMN_TEMPLATE = """SELECT
        control_number, inmate_number, parole_number, pseudo_linking_id,
        IF(new_primary_control_number_candidate IS NULL OR primary_control_number < new_primary_control_number_candidate,
           CAST(primary_control_number AS STRING),
           -- Cast here only necessary for view to play nicely with Postgres tests
           CAST(new_primary_control_number_candidate AS STRING)
        ) AS primary_control_number
      FROM (
        SELECT 
          control_number, inmate_number, parole_number, pseudo_linking_id, primary_control_number,
          MIN(new_row_primary_control_number_candidate) OVER (
            PARTITION BY COALESCE(primary_control_number, new_row_primary_control_number_candidate)
          ) AS new_primary_control_number_candidate
        FROM (
          SELECT 
            primary_table.control_number, primary_table.inmate_number, primary_table.parole_number, primary_table.pseudo_linking_id, primary_table.primary_control_number,
            MIN(control_joins.primary_control_number) AS new_row_primary_control_number_candidate
          FROM 
            {base_table_name} primary_table
          LEFT OUTER JOIN
            {base_table_name} control_joins
          ON primary_table.{join_col_name} = control_joins.{join_col_name} 
            AND (
                (primary_table.primary_control_number IS NULL AND control_joins.primary_control_number IS NOT NULL) 
                OR primary_table.primary_control_number > control_joins.primary_control_number
            )
          GROUP BY primary_table.control_number, primary_table.inmate_number, primary_table.parole_number, primary_table.pseudo_linking_id, primary_table.primary_control_number
        ) a
      ) b"""


def explore_graph_by_column_query(base_table_name: str, join_col_name: str) -> str:
    """Generates a query that explores one level of graph edges along the provided |join_col_name| column, merging
    clusters of primary_control_number that are linked by that edge.
    """
    return StrictStringFormatter().format(
        EXPLORE_GRAPH_BY_COLUMN_TEMPLATE,
        base_table_name=base_table_name,
        join_col_name=join_col_name,
    )


def make_primary_person_ids_fragment() -> str:
    return f"""
recidiviz_primary_person_ids AS (
    WITH
    distinct_control_to_inmate AS (
        SELECT
            DISTINCT
                UPPER(inmate_number) AS inmate_number,
                UPPER(control_number) AS control_number
        FROM {{dbo_tblSearchInmateInfo}}
    ),
    inmate_to_parole_number_edges AS (
        SELECT 
            DISTINCT UPPER(ParoleNumber) AS parole_number,
            IF(
                REGEXP_CONTAINS(UPPER(ParoleInstNumber), '^[A-Z][A-Z][0-9][0-9][0-9][0-9]$'),
                UPPER(ParoleInstNumber),
                NULL
            ) AS inmate_number
        FROM {{dbo_ParoleCount}}
    ),
    base_table AS (
        SELECT
            control_number,
            inmate_number, parole_number,
            control_number AS primary_control_number,
            pseudo_linking_id
        FROM 
            distinct_control_to_inmate
        FULL OUTER JOIN
            inmate_to_parole_number_edges
        USING (inmate_number)
        FULL OUTER JOIN
            {{RECIDIVIZ_REFERENCE_control_number_linking_ids}}
        USING (control_number)
        GROUP BY 
            control_number,
            inmate_number,
            parole_number,
            primary_control_number,
            pseudo_linking_id
    ),
    explore_control_number AS (
        {explore_graph_by_column_query(base_table_name='base_table',
                                       join_col_name='control_number')}
    ),
    explore_inmate_number AS (
        {explore_graph_by_column_query(base_table_name='explore_control_number',
                                       join_col_name='inmate_number')}
    ),
    explore_parole_number AS (
        {explore_graph_by_column_query(base_table_name='explore_inmate_number',
                                       join_col_name='parole_number')}
    ),
    explore_pseudo_linking_id AS (
        {explore_graph_by_column_query(base_table_name='explore_parole_number',
                                       join_col_name='pseudo_linking_id')}
    ),
    primary_control_numbers AS (
        SELECT control_number, inmate_number, parole_number, primary_control_number
        FROM explore_pseudo_linking_id
        GROUP BY control_number, inmate_number, parole_number, primary_control_number
    )
    SELECT
        CASE 
            WHEN primary_control_number IS NOT NULL
                THEN CONCAT('RECIDIVIZ_PRIMARY_CONTROL_NUMBER_', primary_control_number)
            WHEN parole_number IS NOT NULL
                THEN CONCAT('RECIDIVIZ_PRIMARY_PAROLE_NUMBER_', parole_number)
        END AS recidiviz_primary_person_id,
        control_number, inmate_number, parole_number
    FROM primary_control_numbers
)"""


PRIMARY_STATE_IDS_FRAGMENT_V2 = make_primary_person_ids_fragment()
