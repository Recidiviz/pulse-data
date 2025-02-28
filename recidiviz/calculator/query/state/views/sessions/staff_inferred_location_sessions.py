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
"""Function that returns a view builder that calculates the primary location of
a staff member based on the modular locations of their assigned clients."""

from recidiviz.calculator.query.bq_utils import (
    list_to_query_string,
    nonnull_end_date_exclusive_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.utils.string_formatting import fix_indent


def build_staff_inferred_location_sessions_query_template(
    assignment_sessions_sql_source: str,
    staff_index_cols: list[str],
    location_cols: list[str],
    start_date_col_name: str = "start_date",
    end_date_exclusive_col_name: str = "end_date_exclusive",
) -> str:
    """Returns a query template that calculates the primary location (for all location_cols)
    for the officer represented by `staff_index_cols` in the given assignment sessions view
    based on the number of clients in each location assigned to an officer. If there is a tie
    between locations, the function will use `location_cols` in the inputted order to deduplicate."""

    order_by_query_template = fix_indent(
        "\n".join(
            [
                f"IF({location_name} IS NULL, 1, 0) ASC, -- give priority to non-null {location_name}"
                for location_name in location_cols
            ]
        )
        + "\n"
        + list_to_query_string(["caseload_count DESC", *location_cols]),
        indent_level=16,
    )
    rename_location_cols_with_primary_prefix_query_fragment = ",\n".join(
        [f"{col} AS primary_{col}" for col in location_cols]
    )
    location_cols_with_primary_prefix = [f"primary_{col}" for col in location_cols]

    query_template = f"""
WITH assignment_sessions AS (
{fix_indent(assignment_sessions_sql_source, indent_level=4)}
)
, population_change_dates AS (
    -- Start dates increase population by 1
    SELECT
        {list_to_query_string(staff_index_cols)},
        {list_to_query_string(location_cols)},
        {start_date_col_name} AS change_date,
        1 AS change_value,
    FROM
        assignment_sessions
    
    UNION ALL
    
    -- End dates decrease population by 1
    SELECT
        {list_to_query_string(staff_index_cols)},
        {list_to_query_string(location_cols)},
        {end_date_exclusive_col_name} AS change_date,
        -1 AS change_value,
    FROM
        assignment_sessions
    WHERE
        end_date_exclusive IS NOT NULL
)

, population_change_dates_agg AS (
    SELECT
        {list_to_query_string(staff_index_cols)},
        {list_to_query_string(location_cols)},
        change_date,
        SUM(change_value) AS change_value,
    FROM 
        population_change_dates
    GROUP BY {list_to_query_string(staff_index_cols)}, {list_to_query_string(location_cols)}, change_date
)

, staff_location_caseload_counts AS (
    SELECT
        {list_to_query_string(staff_index_cols)},
        {list_to_query_string(location_cols)},
        change_date AS start_date,
        LEAD(change_date) OVER w AS end_date_exclusive,
        SUM(change_value) OVER w AS caseload_count,
    FROM 
        population_change_dates_agg
    WINDOW w AS (
        PARTITION BY {list_to_query_string(staff_index_cols)}, {list_to_query_string(location_cols)}
        ORDER BY change_date ASC
    )
)

/*
At this point we have caseload spans for staff-locations.
Strategy: at each change date there may be overlapping spans. At each change date,
choose the location with the greatest caseload count.
*/
, primary_locations AS (
    SELECT
        {list_to_query_string(staff_index_cols, table_prefix="a")},
        change_date,
        {list_to_query_string(location_cols)},
        caseload_count > 0 AS nonzero_caseload,
    FROM (
        -- each change date is a potential place for a new primary office span
        SELECT DISTINCT
            {list_to_query_string(staff_index_cols)},
            change_date,
        FROM
            population_change_dates
    ) a
    INNER JOIN 
        staff_location_caseload_counts b
    USING
        ({list_to_query_string(staff_index_cols)})
    WHERE a.change_date BETWEEN b.start_date AND
            {nonnull_end_date_exclusive_clause("b.end_date_exclusive")}
    QUALIFY
        -- keep non-null location with greatest caseload count, tiebreak by names
        ROW_NUMBER() OVER (
            PARTITION BY {list_to_query_string(staff_index_cols, table_prefix="a")}, change_date
            ORDER BY
{order_by_query_template}
        ) = 1
)

-- now turn primary location-transition_days to spans
, non_overlapping_primary_location_spans AS (
    SELECT
        {list_to_query_string(staff_index_cols)},
        change_date AS start_date,
        LEAD(change_date) OVER (
            PARTITION BY {list_to_query_string(staff_index_cols)} ORDER BY change_date ASC
        ) AS end_date_exclusive,
        {rename_location_cols_with_primary_prefix_query_fragment},
        nonzero_caseload,
    FROM
        primary_locations
)

-- remove zero caseload periods
-- this comes after the previous CTE so we can get end dates for the final caseload period
, zeros_removed AS (
    SELECT
        * EXCEPT(nonzero_caseload),
    FROM
        non_overlapping_primary_location_spans
    WHERE
        nonzero_caseload
)

-- now re-sessionize adjacent spans and return
, sessionized_cte AS (
{aggregate_adjacent_spans(
    table_name="zeros_removed",
    index_columns=staff_index_cols,
    attribute=location_cols_with_primary_prefix,
    session_id_output_name="session_id",
    end_date_field_name="end_date_exclusive",
)})

SELECT
    {list_to_query_string(staff_index_cols)},
    session_id,
    start_date,
    end_date_exclusive,
    {list_to_query_string(location_cols_with_primary_prefix)},
FROM
    sessionized_cte
"""
    return query_template
