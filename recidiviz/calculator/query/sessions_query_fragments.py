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
"""Helper functions for building BQ sessions views."""
# pylint: disable=line-too-long

from typing import List, Optional, Union

from recidiviz.calculator.query.bq_utils import (
    join_on_columns_fragment,
    list_to_query_string,
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
    nonnull_start_date_clause,
    revert_nonnull_end_date_clause,
)


def create_sub_sessions_with_attributes(
    table_name: str,
    use_magic_date_end_dates: bool = False,
    end_date_field_name: str = "end_date",
    index_columns: Optional[List[str]] = None,
) -> str:
    """Creates the `sub_sessions_with_attributes` CTE by:
    1) Creating non-overlapping sub-session time spans that cover the time period
     represented by the input sessions.
    2) For each sub-session, producing one row per input session that overlaps with this
     sub-session, including zero-day sessions, with all attribute values from that
     overlapping input session preserved.

    The |table_name| must have the following columns: start_date, end_date, and all columns in `index_columns`
    (default `person_id`, `state_code`)

    Sessions must be end-date exclusive such that the end date of one session is equal
    to the start date of the adjacent session.

    If |use_magic_date_end_dates| is True then open sub-sessions will have the
    MAGIC_END_DATE end_date, otherwise open sub-sessions will have a NULL end_date."""
    if index_columns is None:
        index_columns = ["person_id", "state_code"]
    index_col_str = list_to_query_string(index_columns)
    return f"""
/*
Create the periods CTE with non-null end dates for easier date logic
*/
periods_cte AS (
    SELECT
        input.* EXCEPT({end_date_field_name}),
        {nonnull_end_date_clause(f'input.{end_date_field_name}')} AS {end_date_field_name},
    FROM {table_name} input
),
/*
Generates a set of unique period boundary dates based on the start and end dates of periods.
*/
period_boundary_dates AS (
    SELECT DISTINCT
        {index_col_str},
        boundary_date,
    FROM periods_cte,
    UNNEST([start_date, {end_date_field_name}]) AS boundary_date
),
/*
Generates sub-sessions based on each boundary date and its subsequent date. 
Note that this does not associate zero-day (same day start and end sessions) as 
these sessions are added separately in a subsequent CTE.
*/
sub_sessions AS (
    SELECT
        {index_col_str},
        boundary_date AS start_date,
        LEAD(boundary_date) OVER (PARTITION BY {index_col_str} ORDER BY boundary_date) AS {end_date_field_name},
    FROM
        period_boundary_dates
),
/*
Add the attributes from the original periods to the overlapping sub-sessions and union
in zero-day sessions (same-day start and end) directly from the original periods CTE.
*/
sub_sessions_with_attributes AS (
    SELECT
        {index_col_str},
        se.start_date,
        {f'se.{end_date_field_name}' if use_magic_date_end_dates else revert_nonnull_end_date_clause(f'se.{end_date_field_name}')} AS {end_date_field_name},
        c.* EXCEPT ({index_col_str}, start_date, {end_date_field_name}),
    FROM
        sub_sessions se
    INNER JOIN
        periods_cte c
    USING
        ({index_col_str})
    WHERE
        se.start_date BETWEEN c.start_date AND DATE_SUB(c.{end_date_field_name}, INTERVAL 1 DAY)
    
    UNION ALL

    /*
    Add the zero-day sessions, which cannot be divided further into sub-sessions, along
    with *all* the attributes from the overlapping sessions which includes the
    attributes of the zero-day session and the attributes of the session(s) that the
    zero-day period overlaps with.
    */
    SELECT
        {index_col_str},
        single_day.start_date,
        single_day.{end_date_field_name},
        all_periods.* EXCEPT ({index_col_str}, start_date, {end_date_field_name}),
    FROM
        periods_cte single_day
    INNER JOIN
        periods_cte all_periods
    USING
        ({index_col_str})
    -- Add the attributes of the zero-day period as well as any sessions that it
    -- falls within
    WHERE 
        single_day.start_date BETWEEN all_periods.start_date AND all_periods.{end_date_field_name}
        AND single_day.start_date = single_day.{end_date_field_name}
)
"""


def aggregate_adjacent_spans(
    table_name: str,
    index_columns: Optional[List[str]] = None,
    attribute: Optional[Union[str, List[str]]] = None,
    session_id_output_name: Optional[str] = "session_id",
    is_struct: Optional[bool] = False,
    end_date_field_name: str = "end_date",
) -> str:
    """
    Function that aggregates together temporally adjacent spans for which the specified attribute(s) do not
    change. Sessions must be end-date exclusive such that the end date of one session is equal
    to the start date of the adjacent session.

    The |table_name| must have the following columns: person_id, state_code, start_date, end_date.

    Params:
    ------
    table_name : str
        Name of the CTE of spans to be sessionized

    index_columns : Optional[List[str]]
        List of column names to use as index columns for the input spans. If no index columns
        are provided, default to [`person_id`, `state_code`].

    attribute : Optional[Union[str, List[str]]], default None
        The name of the column(s) for which a change in value triggers a new session. This parameter can be
        (1) a string representing a column name, (2) a list of strings representing column names for which a
        change in *any* column triggers a new session, (3) a string representing the name of a struct of ordered
        values for which a string representation of the struct is compared across adjacent sessions.

        If the string specified here represents a struct, the `is_struct` function parameter needs to be set
        to TRUE. If specifying a struct, there can only be one string `attribute` value (this function does not
        support sessionizing on both a struct and a non-struct column).

        If this value is not specified, adjacent spans will be aggregated solely based on date adjacency.

    session_id_output_name : Optional[str], default "session_id"
        Desired name of the output field that contains ids for each session. If not specified, the output
        will be `session_id`

    is_struct : Optional[bool], default False
        Boolean indicating whether or not the string specified in the `attribute` parameter represents a
        struct in the view. If this flag is True, there can only be one value specified in the `attribute`
        parameter.
    """
    # Default index columns are `person_id` and `state_code`.
    if index_columns is None:
        index_columns = ["person_id", "state_code"]
    index_col_str = ", ".join(index_columns)

    if attribute:

        # If only one attribute is specified, turn it into a single-element list. This is done to reduce
        # repeated logic below to handle both situations separately.
        attribute_list = [attribute] if not isinstance(attribute, List) else attribute

        if len(attribute_list) > 1 and is_struct:
            raise ValueError("Sessionization on struct only allows one attribute value")

        # Cast each field as a string for comparisons, to allow comparison of null values for all data types
        attribute_list_as_str = [
            f'COALESCE(CAST({attribute} AS STRING), "")' for attribute in attribute_list
        ]

        # Create a string from the column names in the list to be used in the query
        attribute_col_str = ", ".join(attribute_list)
        attribute_col_str_as_str = ", ".join(attribute_list_as_str)

        # Create a string specifying how attributes will be aggregated together in the final sessionized view.
        # Because the session_id field is incremented every time one of these attribute values changes,
        # all attribute values within a given person_id, state_code, and session_id will by definition have the
        # same value. This is why the ANY_VALUE aggregation function is used. Note, that this would be identical
        # to just including the attribute column names in the GROUP BY, but this approach is slightly more
        # generalizable and also works with structs (structs cannot be grouped by)
        aggregation_str = ", ".join(
            [f"ANY_VALUE({att}) AS {att}" for att in attribute_list]
        )

        # Different logic exists for whether we are comparing columns or a struct.
        if not is_struct:
            # Look for a change in the value of the concatenation of the attribute column(s). That value is
            # COALESCED so that adjacent sessions with NULL values will be aggregated together.
            attribute_change_str = f"CONCAT({attribute_col_str_as_str}) != LAG(CONCAT({attribute_col_str_as_str})) OVER w AS attribute_change,"
        else:
            # If a struct is specified, look for a change in the string representation of that struct
            attribute_change_str = f"TO_JSON_STRING({attribute}) != LAG(TO_JSON_STRING({attribute})) OVER w AS attribute_change,"
    # If no attribute is specified, the attribute column string and the attribute aggregation strings are left blank.
    # The string that creates the boolean in SQL indicating whether the attribute has changed is set to FALSE (since
    # there is no attribute change that should result in a new session being created)
    else:
        attribute_col_str = ""
        aggregation_str = ""
        attribute_change_str = "FALSE AS attribute_change,"

    return f"""
    SELECT
        {index_col_str},
        {session_id_output_name},
        date_gap_id,
        MIN(start_date) AS start_date,
        {revert_nonnull_end_date_clause(f'MAX({end_date_field_name})')} AS {end_date_field_name},
        {aggregation_str}
    FROM
        (
        SELECT 
            *,
            SUM(IF(date_gap OR attribute_change,1,0)) OVER(PARTITION BY {index_col_str}
                ORDER BY start_date, {end_date_field_name}) AS {session_id_output_name},
            SUM(IF(date_gap,1,0)) OVER(PARTITION BY {index_col_str}
                ORDER BY start_date, {end_date_field_name}) AS date_gap_id
        FROM
            (
            SELECT
                {index_col_str},
                start_date,
                {nonnull_end_date_clause(f'{end_date_field_name}')} AS {end_date_field_name},
                COALESCE(LAG({end_date_field_name}) OVER w != start_date, TRUE) AS date_gap,
                {attribute_change_str}
                {attribute_col_str}
            FROM {table_name}
            WINDOW w AS (PARTITION BY {index_col_str} ORDER BY start_date, {nonnull_end_date_clause(f'{end_date_field_name}')})
            )
        )
        GROUP BY {index_col_str}, {session_id_output_name}, date_gap_id
"""


def _compartment_where_clause(
    compartment_types_to_overlap: Optional[Union[str, List[str]]] = None,
    level: Optional[int] = 1,
) -> str:
    """Returns a WHERE clause specifying a compartment level that matches some value or list of values"""
    if compartment_types_to_overlap:
        if isinstance(compartment_types_to_overlap, str):
            compartment_types_to_overlap = [compartment_types_to_overlap]

        return f"AND compartment_level_{level} in ({list_to_query_string(compartment_types_to_overlap, quoted=True)})"
    return ""


def join_sentence_spans_to_compartment_sessions(
    compartment_level_1_to_overlap: Optional[Union[str, List[str]]] = None,
    compartment_level_2_to_overlap: Optional[Union[str, List[str]]] = None,
) -> str:
    """Returns a query fragment to join sentence_spans with all the sentence attributes from
    sentences_preprocessed to compartment_sessions where sentence_spans overlap with particular
    kinds of sessions.
    """
    compartment_level_1_clause = _compartment_where_clause(
        compartment_types_to_overlap=compartment_level_1_to_overlap, level=1
    )
    compartment_level_2_clause = _compartment_where_clause(
        compartment_types_to_overlap=compartment_level_2_to_overlap, level=2
    )
    return f"""
    FROM `{{project_id}}.{{sessions_dataset}}.sentence_spans_materialized` span,
    UNNEST (sentences_preprocessed_id_array) AS sentences_preprocessed_id
    INNER JOIN `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` sent
      USING (state_code, person_id, sentences_preprocessed_id)
    INNER JOIN `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` sess
        ON span.state_code = sess.state_code
        AND span.person_id = sess.person_id
        -- Restrict to spans that overlap with particular compartment levels
        {compartment_level_1_clause}
        {compartment_level_2_clause}
        -- Use strictly less than for exclusive end_dates
        AND span.start_date < {nonnull_end_date_clause('sess.end_date_exclusive')}
        AND sess.start_date < {nonnull_end_date_clause('span.end_date_exclusive')}
"""


def create_intersection_spans(
    table_1_name: str,
    table_2_name: str,
    index_columns: List[str],
    use_left_join: Optional[bool] = False,
    include_zero_day_intersections: Optional[bool] = False,
    table_1_columns: Optional[List[str]] = None,
    table_2_columns: Optional[List[str]] = None,
    table_1_start_date_field_name: Optional[str] = "start_date",
    table_2_start_date_field_name: Optional[str] = "start_date",
    table_1_end_date_field_name: Optional[str] = "end_date_exclusive",
    table_2_end_date_field_name: Optional[str] = "end_date_exclusive",
) -> str:
    """
    Generates a query fragment taking the intersection of two overlapping span tables.
    Input tables must be end date exclusive.

    Function performs an inner join by default. If `use_left_join` parameter is set to True,
    function will perform a left join using table 1 as the primary table, which will result in preserving
    the original table 1 span in cases where no intersection exists with table 2.

    """
    select_columns_str = list_to_query_string(index_columns, table_prefix=table_1_name)
    if table_1_columns:
        select_columns_str = list_to_query_string(
            [
                select_columns_str,
                list_to_query_string(table_1_columns, table_prefix=table_1_name),
            ]
        )
    if table_2_columns:
        select_columns_str = list_to_query_string(
            [
                select_columns_str,
                list_to_query_string(table_2_columns, table_prefix=table_2_name),
            ]
        )

    end_date_clause_function = (
        nonnull_end_date_clause
        if include_zero_day_intersections
        else nonnull_end_date_exclusive_clause
    )
    join_fragment = "INNER"
    if use_left_join:
        join_fragment = "LEFT"
    return f"""
    SELECT
        {select_columns_str},
        GREATEST({table_1_name}.{table_1_start_date_field_name}, {nonnull_start_date_clause(f'{table_2_name}.{table_2_start_date_field_name}')}) AS start_date,
        {revert_nonnull_end_date_clause(f"LEAST({nonnull_end_date_clause(f'{table_1_name}.{table_1_end_date_field_name}')},{nonnull_end_date_clause(f'{table_2_name}.{table_2_end_date_field_name}')})")} AS end_date_exclusive,
    FROM 
        {table_1_name}
    {join_fragment} JOIN
        {table_2_name}
    ON
        {join_on_columns_fragment(columns=index_columns, table1 = table_1_name, table2 = table_2_name)}
        AND (
            {table_1_name}.{table_1_start_date_field_name} BETWEEN {table_2_name}.{table_2_start_date_field_name} AND {end_date_clause_function(f"{table_2_name}.{table_2_end_date_field_name}")}
            OR {table_2_name}.{table_2_start_date_field_name} BETWEEN {table_1_name}.{table_1_start_date_field_name} AND {end_date_clause_function(f"{table_1_name}.{table_1_end_date_field_name}")}
        )
    """
