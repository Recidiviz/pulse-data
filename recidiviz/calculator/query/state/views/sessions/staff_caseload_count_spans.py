# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Function that returns a view builder that calculates the caseload size assigned
to a given staff member over some period of time."""

from recidiviz.calculator.query.bq_utils import list_to_query_string


def build_caseload_count_spans_query_template(
    assignment_sessions_sql_source: str,
    index_cols: list[str],
    start_date_col_name: str = "start_date",
    end_date_exclusive_col_name: str = "end_date_exclusive",
) -> str:
    """Returns a query template that calculates the number of person_id's associated
    with a set of index cols from the source assignment sessions"""
    query_template = f"""
WITH assignment_spans AS (
    SELECT
        {list_to_query_string(index_cols)},
        {start_date_col_name} AS start_date,
        {end_date_exclusive_col_name} AS end_date,
    FROM ({assignment_sessions_sql_source})
)
,
population_change_dates AS (
    -- Start dates increase population by 1
    SELECT
        {list_to_query_string(index_cols)},
        start_date AS change_date,
        1 AS change_value
    FROM assignment_spans
    UNION ALL
    -- End dates decrease population by 1
    SELECT
        {list_to_query_string(index_cols)},
        end_date AS change_date,
        -1 AS change_value
    FROM assignment_spans
    WHERE end_date IS NOT NULL
)
,
population_change_dates_agg AS (
    SELECT
        {list_to_query_string(index_cols)},
        change_date, 
        SUM(change_value) AS change_value
    FROM 
        population_change_dates
    GROUP BY 1, 2, 3
)
SELECT
    {list_to_query_string(index_cols)},
    change_date AS start_date,
    LEAD(change_date) OVER (PARTITION BY {list_to_query_string(index_cols)} ORDER BY change_date) AS end_date,
    SUM(change_value) OVER (PARTITION BY {list_to_query_string(index_cols)} ORDER BY change_date) AS caseload_count,
FROM 
    population_change_dates_agg
"""
    return query_template
