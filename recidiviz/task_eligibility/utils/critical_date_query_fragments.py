# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Helper SQL fragments that do standard queries against tables with "critical dates",
such as eligibility dates, due dates, etc.
"""
from typing import List, Optional

from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    revert_nonnull_end_date_clause,
)


def critical_date_spans_cte() -> str:
    """Helper method that returns a CTE with start and end datetime columns
    representing the span when the critical date had a particular value, with zero day
    spans removed. End date is exclusive and adjacent spans with the same critical date
    are not collapsed.

    There must be a CTE defined before this clause with the name
    |critical_date_update_datetimes| that has columns state_code (string),
    person_id (string), update_datetime (datetime), critical_date (date).
    """
    return f"""critical_date_spans AS (
        SELECT
            state_code,
            person_id,
            critical_date,
            update_datetime AS start_datetime,
            -- Use the subsequent row's update datetime as the end datetime for this span
            LEAD(update_datetime) OVER (
                PARTITION BY state_code, person_id
                ORDER BY
                    update_datetime ASC,
                    -- Prioritize the closest critical date when there is more than one
                    -- row with the same update datetime
                    {nonnull_end_date_clause('critical_date')} DESC
            ) AS end_datetime,
        FROM critical_date_update_datetimes
        -- Drop all zero day spans
        QUALIFY CAST(start_datetime AS DATE)
            != CAST({nonnull_end_date_clause('end_datetime')} AS DATE)
    )"""


def critical_date_has_passed_spans_cte(
    meets_criteria_leading_window_time: int = 0,
    attributes: Optional[List[str]] = None,
    date_part: str = "DAY",
    table_name: str = "critical_date_spans",
    cte_suffix: str = "",
) -> str:
    """Returns a CTE that indicates the span of time where a particular critical date
    was set and comes on or before the current date. The
    |meets_criteria_leading_window_days| modifier can move up the start_date by a
    constant value to account, for example, for time before the critical date where some
     criteria is met. The output spans are not collapsed so there can be two
    adjacent spans with the same `critical_date_has_passed` value.

    There must be a CTE defined before this clause with the name |critical_date_spans|
    that has columns state_code (string), person_id (string), start_datetime (datetime),
    end_datetime (datetime), critical_date (date).

    Params:
    ------
    meets_criteria_leading_window_time : int
        Modifier to move the start_date by a constant value to account, for example, for time before the critical date
        where some criteria is met. Defaults to 0.

    attributes : Optional[List[str]]
        List of column names that will be passed through to the output CTE

    date_part (str, optional): Supports any of the BigQuery date_part values:
        "DAY", "WEEK","MONTH","QUARTER","YEAR". Defaults to "MONTH".

    table_name (str, optional): The name of the table that the critical date spans are
        stored in. Defaults to "critical_date_spans".

    cte_suffix (str, optional): Suffix to append to the CTE names to avoid name
        collisions. Defaults to "".
    """

    if attributes:
        attribute_str = ", ".join(attributes)
    else:
        attribute_str = ""

    return f"""
    /*
    Cast datetimes to dates, convert null dates to future dates, and create the
    `critical_or_in_window_date` column from the critical date by subtracting
    {meets_criteria_leading_window_time} days from the critical date to indicate the
    date when the criteria is met
    */
    critical_date_spans_no_nulls{cte_suffix} AS (
        SELECT
            state_code,
            person_id,
            CAST(start_datetime AS DATE) AS start_date,
            {nonnull_end_date_clause('CAST(end_datetime AS DATE)')} AS end_date,
            {nonnull_end_date_clause(f'''
                DATE_SUB(
                    critical_date,
                    INTERVAL {meets_criteria_leading_window_time} {date_part}
                )'''
            )} AS critical_or_in_window_date,
            -- Maintain the original critical date for the final output
            critical_date,
            {attribute_str}
        FROM {table_name}
    ),
    criteria_spans{cte_suffix} AS (
        /*
        Create a FALSE criteria span for the period leading up to the critical date,
        if the critical date comes after the span end date then the whole criteria
        span is FALSE
        */
        SELECT
            state_code,
            person_id,
            start_date,
            -- Prioritize the date that came first as the criteria span end date
            LEAST(end_date, critical_or_in_window_date) AS end_date,
            FALSE AS critical_date_has_passed,
            critical_date,
            {attribute_str}
        FROM critical_date_spans_no_nulls{cte_suffix}
        WHERE start_date < critical_or_in_window_date
        UNION ALL
        /*
        Create a TRUE span for the period after the critical date, if the critical
        date comes before the span start date then the whole criteria span is TRUE
        */
        SELECT
            state_code,
            person_id,
            -- Prioritize the date that came the latest as the criteria span start date
            GREATEST(start_date, critical_or_in_window_date) AS start_date,
            end_date,
            TRUE AS critical_date_has_passed,
            critical_date,
            {attribute_str}
        FROM critical_date_spans_no_nulls{cte_suffix}
        WHERE critical_or_in_window_date < end_date
    ),
    critical_date_has_passed_spans{cte_suffix} AS (
        SELECT
            state_code,
            person_id,
            start_date,
            -- Return the far future dates back to NULLs
            {revert_nonnull_end_date_clause('end_date')} AS end_date,
            critical_date_has_passed,
            critical_date,
            {attribute_str}
        FROM criteria_spans{cte_suffix}
    )"""


def critical_date_exists_spans_cte() -> str:
    """Returns a CTE with the spans of time that indicate if the critical date was set
    (non-NULL), indicating the individual has a particular status at that point of time.
    The output spans are not collapsed and so there can be two adjacent spans with the
    same `critical_date_exists` value.

    There must be a CTE defined before this clause with the name
    |critical_date_update_datetimes| that has columns state_code (string),
    person_id (string), update_datetime (datetime), critical_date (date).
    """
    return f"""
    {critical_date_spans_cte()},
    critical_date_exists_spans AS (
        SELECT
            state_code,
            person_id,
            CAST(start_datetime AS DATE) AS start_date,
            CAST(end_datetime AS DATE) AS end_date,
            critical_date IS NOT NULL AS critical_date_exists,
            critical_date,
        FROM critical_date_spans
    )"""
