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
from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    revert_nonnull_end_date_clause,
)


def _critical_date_spans_cte() -> str:
    """Private helper method that returns a CTE with start and end datetime columns
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
    meets_criteria_leading_window_days: int = 0,
) -> str:
    """Returns a CTE that indicates the span of time where a particular critical date
    was set and comes on or before the current date. The
    |meets_criteria_leading_window_days| modifier can move up the start_date by a
    constant value to account, for example, for time before the critical date where some
     criteria is met. The output spans are not collapsed so there can be two
    adjacent spans with the same `critical_date_has_passed` value.

    There must be a CTE defined before this clause with the name
    |critical_date_update_datetimes| that has columns state_code (string),
    person_id (string), update_datetime (datetime), critical_date (date)."""
    return f"""
    {_critical_date_spans_cte()},
    /*
    Cast datetimes to dates, convert null dates to future dates, and create the
    `critical_or_in_window_date` column from the critical date by subtracting
    {meets_criteria_leading_window_days} days from the critical date to indicate the
    date when the criteria is met
    */
    critical_date_spans_no_nulls AS (
        SELECT
            state_code,
            person_id,
            CAST(start_datetime AS DATE) AS start_date,
            {nonnull_end_date_clause('CAST(end_datetime AS DATE)')} AS end_date,
            {nonnull_end_date_clause(f'''
                DATE_SUB(
                    critical_date,
                    INTERVAL {meets_criteria_leading_window_days} DAY
                )'''
            )} AS critical_or_in_window_date,
            -- Maintain the original critical date for the final output
            critical_date,
        FROM critical_date_spans
    ),
    criteria_spans AS (
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
        FROM critical_date_spans_no_nulls
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
        FROM critical_date_spans_no_nulls
        WHERE critical_or_in_window_date < end_date
    ),
    critical_date_has_passed_spans AS (
        SELECT
            state_code,
            person_id,
            start_date,
            -- Return the far future dates back to NULLs
            {revert_nonnull_end_date_clause('end_date')} AS end_date,
            critical_date_has_passed,
            critical_date,
        FROM criteria_spans
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
    {_critical_date_spans_cte()},
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
